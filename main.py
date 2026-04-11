from http.server import HTTPServer, BaseHTTPRequestHandler
import json, io, re, pdfplumber, os
from datetime import datetime, timezone

# All 55 WV counties — used to validate county detection
WV_COUNTIES = {
    "BARBOUR","BERKELEY","BOONE","BRAXTON","BROOKE","CABELL","CALHOUN","CLAY",
    "DODDRIDGE","FAYETTE","GILMER","GRANT","GREENBRIER","HAMPSHIRE","HANCOCK",
    "HARDY","HARRISON","JACKSON","JEFFERSON","KANAWHA","LEWIS","LINCOLN","LOGAN",
    "MARION","MARSHALL","MASON","MCDOWELL","MERCER","MINERAL","MINGO","MONONGALIA",
    "MONROE","MORGAN","NICHOLAS","OHIO","PENDLETON","PLEASANTS","POCAHONTAS",
    "PRESTON","PUTNAM","RALEIGH","RANDOLPH","RITCHIE","ROANE","SUMMERS","TAYLOR",
    "TUCKER","TYLER","UPSHUR","WAYNE","WEBSTER","WETZEL","WIRT","WOOD","WYOMING"
}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_GET(self):
        body = b'WV Tax Lien PDF Parser - OK'
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            ct = self.headers.get('Content-Type', '')
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)

            if 'multipart/form-data' not in ct:
                return self.respond({'success': False, 'error': 'Expected multipart/form-data'})

            boundary = ct.split('boundary=')[1].strip().encode()
            pdf_data = None
            for part in body.split(b'--' + boundary):
                if b'filename=' in part and b'.pdf' in part.lower():
                    hend = part.find(b'\r\n\r\n')
                    if hend != -1:
                        pdf_data = part[hend + 4:].rstrip(b'\r\n-')
                        break

            if not pdf_data:
                return self.respond({'success': False, 'error': 'No PDF found in request'})

            self.respond(parse_pdf(pdf_data))

        except Exception as e:
            self.respond({'success': False, 'error': str(e)})

    def respond(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self._cors()
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)


def extract_county_from_line(line):
    """
    Try to extract a valid WV county name from a line of text.
    Returns the full county string (e.g. 'NICHOLAS COUNTY') if found,
    or None if the line doesn't match any known county.
    """
    upper = line.upper().strip()
    # Pattern: "XXXX COUNTY" where XXXX is a known county name
    m = re.match(r'^([A-Z]+(?:\s+[A-Z]+)?)\s+COUNTY$', upper)
    if m:
        name = m.group(1).strip()
        if name in WV_COUNTIES:
            return f"{name} COUNTY"
    return None


def parse_pdf(pdf_bytes):
    result = {
        'county': '',
        'date': '',
        'time': '',
        'location': '',
        'rows': [],
        'lastUpdated': datetime.now(timezone.utc).isoformat()
    }

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:

            # ── HEADER PARSING ─────────────────────────────────────────
            lines = [l.strip() for l in (pdf.pages[0].extract_text() or '').split('\n') if l.strip()]

            county_found = False
            date_found = False

            for i, line in enumerate(lines[:20]):
                # Skip obvious non-data lines
                if any(skip in line for skip in ['WEB HANDOUT','CERTIFICATE','TICKET','DISTRICT','ASSESSED','LEGAL','MINIMUM']):
                    continue
                # Stop once we hit actual data rows
                if re.match(r'^\d{4}-C-\d+', line):
                    break

                # ── County: validate against the 55-county list ──────
                if not county_found:
                    county = extract_county_from_line(line)
                    if county:
                        result['county'] = county
                        county_found = True
                        continue  # move on, don't try to parse date from same line

                # ── Date / Time ──────────────────────────────────────
                if not date_found and '/' in line and len(line) < 35:
                    dm = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)$', line, re.I)
                    if dm:
                        result['date'] = dm.group(1)
                        result['time'] = dm.group(2).strip()
                        date_found = True
                        # Next non-header line = location
                        for j in range(i + 1, min(i + 5, len(lines))):
                            next_line = lines[j]
                            if not any(skip in next_line for skip in ['CERTIFICATE','TICKET','DISTRICT','ASSESSED']):
                                if not re.match(r'^\d{4}-C-\d+', next_line):
                                    result['location'] = next_line
                                    break

            # ── ROW EXTRACTION ─────────────────────────────────────────
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    for row in table:
                        if not row or not row[0]:
                            continue
                        cert = (row[0] or '').replace('\n', ' ').strip()
                        if not re.match(r'^\d{4}-C-\d+$', cert):
                            continue

                        # Join multi-line district names (e.g. "CAMDEN ON\nGAULEY CORP")
                        district = (row[2] or '').replace('\n', ' ').strip()

                        result['rows'].append({
                            'cert': cert,
                            'ticket': (row[1] or '').replace('\n', ' ').strip(),
                            'district': district,
                            'map': (row[3] or '').replace('\n', ' ').strip(),
                            'parcel': (row[4] or '').replace('\n', ' ').strip(),
                            'sub': (row[5] or '0000').replace('\n', ' ').strip() or '0000',
                            'subsub': (row[6] or '0000').replace('\n', ' ').strip() or '0000',
                            'name': (row[7] or '').replace('\n', ' ').strip(),
                            'desc': (row[8] or '').replace('\n', ' ').strip(),
                            'minBid': (row[9] or '').replace('\n', ' ').strip(),
                        })

        if not result['county']:
            return {'success': False, 'error': 'Could not identify a valid WV county name in this PDF. Make sure it is a WVSAO land sale handout.'}

        if not result['rows']:
            return {'success': False, 'error': 'No property rows found in PDF'}

        return {'success': True, 'data': result}

    except Exception as e:
        return {'success': False, 'error': f'PDF parsing error: {str(e)}'}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'PDF Parser running on port {port}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
