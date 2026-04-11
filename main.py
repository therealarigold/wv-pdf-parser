from http.server import HTTPServer, BaseHTTPRequestHandler
import json, io, pdfplumber, os
from datetime import datetime, timezone

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


def parse_pdf(pdf_bytes):
    result = {'county':'','date':'','time':'','location':'','rows':[],'lastUpdated':datetime.now(timezone.utc).isoformat()}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            lines = [l.strip() for l in (pdf.pages[0].extract_text() or '').split('\n') if l.strip()]
            for i, line in enumerate(lines[:12]):
                if ('COUNTY' in line or 'CORP' in line) and 'CERTIFICATE' not in line and 'WEB' not in line and len(line) < 60:
                    result['county'] = line.strip()
                if '/' in line and len(line) < 25 and result['county']:
                    parts = line.split(' ', 1)
                    result['date'] = parts[0]
                    result['time'] = parts[1].strip() if len(parts) > 1 else ''
                    if i+1 < len(lines) and not any(k in lines[i+1] for k in ['CERTIFICATE','TICKET','DISTRICT']):
                        result['location'] = lines[i+1]
                    break
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    for row in table:
                        if not row or not row[0]: continue
                        cert = (row[0] or '').replace('\n',' ').strip()
                        if not cert.startswith('20') or '-C-' not in cert: continue
                        result['rows'].append({
                            'cert': cert,
                            'ticket': (row[1] or '').replace('\n',' ').strip(),
                            'district': (row[2] or '').replace('\n',' ').strip(),
                            'map': (row[3] or '').replace('\n',' ').strip(),
                            'parcel': (row[4] or '').replace('\n',' ').strip(),
                            'sub': (row[5] or '0000').replace('\n',' ').strip() or '0000',
                            'subsub': (row[6] or '0000').replace('\n',' ').strip() or '0000',
                            'name': (row[7] or '').replace('\n',' ').strip(),
                            'desc': (row[8] or '').replace('\n',' ').strip(),
                            'minBid': (row[9] or '').replace('\n',' ').strip(),
                        })
        return {'success': True, 'data': result} if result['rows'] else {'success': False, 'error': 'No property rows found in PDF'}
    except Exception as e:
        return {'success': False, 'error': f'PDF parsing error: {str(e)}'}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'PDF Parser running on port {port}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
