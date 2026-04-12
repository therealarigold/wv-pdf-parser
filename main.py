from http.server import HTTPServer, BaseHTTPRequestHandler
import json, io, re, pdfplumber, os, urllib.request, urllib.parse
from datetime import datetime, timezone
from html.parser import HTMLParser

# All 55 WV counties
WV_COUNTIES = {
    "BARBOUR","BERKELEY","BOONE","BRAXTON","BROOKE","CABELL","CALHOUN","CLAY",
    "DODDRIDGE","FAYETTE","GILMER","GRANT","GREENBRIER","HAMPSHIRE","HANCOCK",
    "HARDY","HARRISON","JACKSON","JEFFERSON","KANAWHA","LEWIS","LINCOLN","LOGAN",
    "MARION","MARSHALL","MASON","MCDOWELL","MERCER","MINERAL","MINGO","MONONGALIA",
    "MONROE","MORGAN","NICHOLAS","OHIO","PENDLETON","PLEASANTS","POCAHONTAS",
    "PRESTON","PUTNAM","RALEIGH","RANDOLPH","RITCHIE","ROANE","SUMMERS","TAYLOR",
    "TUCKER","TYLER","UPSHUR","WAYNE","WEBSTER","WETZEL","WIRT","WOOD","WYOMING"
}

# ── COUNTY SYSTEM REGISTRY ────────────────────────────────────────────────────
# Maps county name → CAMA URL template and IDX URL
# CAMA: Wood County assessor model (GST platform) - returns owner, deed book/page, sales history
# IDX:  Document search (grantor/grantee deed index)
COUNTY_REGISTRY = {
    "WOOD": {
        "cama_url": "https://inquiries.woodcountywv.com/CAMA/prc.aspx?PARID={parid}",
        "cama_parid_format": "{dist:02d}++{map}{parcel}0000000",
        "idx_url": "https://inquiries.woodcountywv.com/legacywebinquiry/default.aspx",
        "idx_search_url": "https://inquiries.woodcountywv.com/legacywebinquiry/",
        "idx_type": "wood_legacy",
        "has_cama": True,
        "has_idx": True,
    },
    "BARBOUR": {
        "idx_url": "http://129.71.117.241/WEBInquiry/Default.aspx",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "BROOKE": {
        "idx_url": "http://129.71.117.252/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "CABELL": {
        "idx_url": "http://www.recordscabellcountyclerk.org/Default.aspx",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "DODDRIDGE": {
        "idx_url": "http://129.71.205.241/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "FAYETTE": {
        "idx_url": "http://129.71.202.7/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "GILMER": {
        "idx_url": "http://www.gilmercountywv.gov/idxsearch/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "GRANT": {
        "idx_url": "http://129.71.112.124/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "GREENBRIER": {
        "idx_url": "http://129.71.205.208/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "HAMPSHIRE": {
        "idx_url": "http://129.71.205.207/idxsearch",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "HARRISON": {
        "idx_url": "http://lookup.harrisoncountywv.com/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "JEFFERSON": {
        "idx_url": "http://documents.jeffersoncountywv.org/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "LEWIS": {
        "idx_url": "http://inquiry.lewiscountywv.org/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "LINCOLN": {
        "idx_url": "http://129.71.206.62/Default.aspx",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "MARSHALL": {
        "idx_url": "http://129.71.117.225/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
    "WIRT": {
        "idx_url": "http://records.wirtcountywv.net/",
        "idx_type": "webinquiry",
        "has_cama": False,
        "has_idx": True,
    },
}

# ── HTML PARSERS ──────────────────────────────────────────────────────────────

class CAMAParser(HTMLParser):
    """Parse Wood County CAMA page for owner, deed book/page, sales history, assessment."""
    def __init__(self):
        super().__init__()
        self.data = {
            "owner": "", "mailing_address": "", "deed_book": "", "deed_page": "",
            "sales_history": [], "assessments": [], "legal_description": "",
            "district": "", "map_parcel": "", "raw_text": ""
        }
        self._text_parts = []
        self._in_table = False
        self._current_row = []
        self._current_cell = ""
        self._table_rows = []
        self._current_tables = []
        self._cell_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._in_table = True
            self._current_tables.append([])
        elif tag == "tr":
            self._current_row = []
        elif tag in ("td", "th"):
            self._current_cell = ""
            self._cell_depth += 1

    def handle_endtag(self, tag):
        if tag == "table" and self._current_tables:
            self._table_rows.append(self._current_tables.pop())
        elif tag == "tr" and self._current_tables:
            self._current_tables[-1].append(self._current_row[:])
        elif tag in ("td", "th"):
            self._cell_depth -= 1
            self._current_row.append(self._current_cell.strip())

    def handle_data(self, data):
        d = data.strip()
        if d:
            self._text_parts.append(d)
            if self._cell_depth > 0:
                self._current_cell += " " + d

    def extract(self):
        full_text = " ".join(self._text_parts)
        self.data["raw_text"] = full_text

        # Extract owner and deed book/page from raw text
        # Pattern: "CURRENT OWNER: NAME MAILING ADDRESS: ADDR DEED BOOK/PAGE: B/P"
        owner_m = re.search(r'CURRENT OWNER:\s*([A-Z][^\n]+?)(?:MAILING ADDRESS:|DEED BOOK)', full_text, re.I)
        if owner_m:
            self.data["owner"] = owner_m.group(1).strip()

        addr_m = re.search(r'MAILING ADDRESS:\s*([^\n]+?)(?:DEED BOOK|SALES HISTORY)', full_text, re.I)
        if addr_m:
            self.data["mailing_address"] = addr_m.group(1).strip()

        deed_m = re.search(r'DEED BOOK/PAGE:\s*(\d+)/(\d+)', full_text, re.I)
        if deed_m:
            self.data["deed_book"] = deed_m.group(1)
            self.data["deed_page"] = deed_m.group(2)

        legal_m = re.search(r'LEGAL DESCRIPTION:\s*([^\n]+?)(?:CURRENT OWNER|$)', full_text, re.I)
        if legal_m:
            self.data["legal_description"] = legal_m.group(1).strip()

        district_m = re.search(r'TAXING DISTRICT:\s*([^\n]+?)(?:STREET|$)', full_text, re.I)
        if district_m:
            self.data["district"] = district_m.group(1).strip()

        mapparcel_m = re.search(r'Map/Parcel:\s*([\d\s]+)', full_text, re.I)
        if mapparcel_m:
            self.data["map_parcel"] = mapparcel_m.group(1).strip()

        # Parse sales history from tables
        for table in self._table_rows:
            for row in table:
                cells = [c.strip() for c in row if c.strip()]
                # Sales history row: date, price, book, page
                if len(cells) >= 4:
                    date_m = re.match(r'(\d{1,2}/\d{1,2}/\d{4})', cells[0])
                    price_m = re.match(r'\$[\d,]+', cells[1])
                    if date_m and price_m:
                        try:
                            self.data["sales_history"].append({
                                "date": cells[0].split()[0],
                                "price": cells[1],
                                "book": cells[2] if len(cells) > 2 else "",
                                "page": cells[3] if len(cells) > 3 else "",
                            })
                        except:
                            pass
                # Assessment row: year, land, building, total, assessed, class
                if len(cells) >= 4 and re.match(r'^20\d{2}$', cells[0]):
                    try:
                        self.data["assessments"].append({
                            "year": cells[0],
                            "land": cells[1] if len(cells) > 1 else "",
                            "building": cells[2] if len(cells) > 2 else "",
                            "total": cells[3] if len(cells) > 3 else "",
                            "assessed": cells[4] if len(cells) > 4 else "",
                        })
                    except:
                        pass

        return self.data


class IDXParser(HTMLParser):
    """Parse generic WV IDX/WebInquiry search results for deed records."""
    def __init__(self):
        super().__init__()
        self.records = []
        self._in_table = False
        self._headers = []
        self._rows = []
        self._current_row = []
        self._current_cell = ""
        self._cell_depth = 0
        self._header_row = True

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._in_table = True
        elif tag == "tr":
            self._current_row = []
        elif tag in ("td", "th"):
            self._current_cell = ""
            self._cell_depth += 1

    def handle_endtag(self, tag):
        if tag == "tr" and self._in_table:
            row = [c.strip() for c in self._current_row]
            if any(row):
                if self._header_row and any(h in " ".join(row).upper() for h in ["GRANTOR","GRANTEE","BOOK","DATE","TYPE","INSTRUMENT"]):
                    self._headers = row
                    self._header_row = False
                elif not self._header_row:
                    self._rows.append(row)
        elif tag in ("td", "th"):
            self._cell_depth -= 1
            self._current_row.append(self._current_cell.strip())

    def handle_data(self, data):
        d = data.strip()
        if d and self._cell_depth > 0:
            self._current_cell += " " + d

    def extract(self):
        for row in self._rows:
            if len(row) < 3:
                continue
            record = {}
            for i, header in enumerate(self._headers):
                if i < len(row):
                    record[header.lower().replace(" ", "_")] = row[i]
            if record:
                self.records.append(record)
        return self.records


# ── FETCH HELPERS ─────────────────────────────────────────────────────────────

def fetch_url(url, timeout=15, post_data=None):
    """Fetch a URL with a browser-like user agent. Returns HTML string or raises."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = urllib.request.Request(url, headers=headers)
    if post_data:
        req.data = urllib.parse.urlencode(post_data).encode()
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = "utf-8"
        ct = resp.headers.get("Content-Type", "")
        if "charset=" in ct:
            charset = ct.split("charset=")[1].strip()
        return resp.read().decode(charset, errors="replace")


def build_wood_parid(dist, map_num, parcel):
    """Build Wood County CAMA PARID: e.g. '10++44009800000000'"""
    # Format: district (2 digits, no pad) ++ map(4 digits) parcel(4 digits) 0000000
    dist_str = str(int(dist)).zfill(2) if dist else "01"
    map_str = str(map_num).zfill(4)
    parcel_str = str(parcel).zfill(4)
    return f"{dist_str}++{map_str}{parcel_str}0000000"


# ── CAMA LOOKUP ───────────────────────────────────────────────────────────────

def lookup_cama(county_name, dist, map_num, parcel):
    """Look up CAMA record for a parcel. Returns structured data."""
    county = county_name.upper().replace(" COUNTY", "").strip()
    info = COUNTY_REGISTRY.get(county)

    if not info or not info.get("has_cama"):
        return {"success": False, "error": f"No CAMA system configured for {county} County"}

    try:
        if county == "WOOD":
            parid = build_wood_parid(dist, map_num, parcel)
            url = info["cama_url"].format(parid=parid)
            html = fetch_url(url)
            parser = CAMAParser()
            parser.feed(html)
            data = parser.extract()

            # Compute ownership duration from most recent sale
            years_owned = None
            first_sale_year = None
            if data["sales_history"]:
                # Sort by date descending
                sorted_sales = sorted(
                    data["sales_history"],
                    key=lambda s: s["date"],
                    reverse=True
                )
                most_recent = sorted_sales[0]
                try:
                    yr = int(most_recent["date"].split("/")[-1])
                    years_owned = datetime.now().year - yr
                    first_sale_year = yr
                except:
                    pass

            return {
                "success": True,
                "county": county,
                "parid": parid,
                "cama_url": url,
                "owner": data["owner"],
                "mailing_address": data["mailing_address"],
                "deed_book": data["deed_book"],
                "deed_page": data["deed_page"],
                "legal_description": data["legal_description"],
                "district_label": data["district"],
                "map_parcel_label": data["map_parcel"],
                "years_owned": years_owned,
                "acquisition_year": first_sale_year,
                "sales_history": data["sales_history"],
                "assessments": data["assessments"][:5],  # Last 5 years
            }

    except Exception as e:
        return {"success": False, "error": f"CAMA lookup failed: {str(e)}"}


# ── IDX SEARCH ────────────────────────────────────────────────────────────────

def search_idx(county_name, grantor_name):
    """Search IDX document system by grantor name. Returns list of deed records."""
    county = county_name.upper().replace(" COUNTY", "").strip()
    info = COUNTY_REGISTRY.get(county)

    if not info or not info.get("has_idx"):
        return {
            "success": False,
            "error": f"No IDX system configured for {county} County",
            "county_has_idx": False
        }

    try:
        idx_url = info["idx_url"]
        # Most WV IDX systems accept GET params for name search
        search_url = idx_url + f"?name={urllib.parse.quote(grantor_name)}&searchType=grantor"
        html = fetch_url(search_url, timeout=20)
        parser = IDXParser()
        parser.feed(html)
        records = parser.extract()

        return {
            "success": True,
            "county": county,
            "idx_url": idx_url,
            "search_url": search_url,
            "grantor_searched": grantor_name,
            "records_found": len(records),
            "records": records[:25],  # Cap at 25
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"IDX search failed: {str(e)}",
            "county_has_idx": True,
            "idx_url": info.get("idx_url", ""),
        }


def get_county_registry():
    """Return the county registry for the app to display."""
    result = {}
    for county, info in COUNTY_REGISTRY.items():
        result[county] = {
            "has_cama": info.get("has_cama", False),
            "has_idx": info.get("has_idx", False),
            "idx_url": info.get("idx_url", ""),
        }
    return result


# ── HTTP HANDLER ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/counties":
            return self.respond({"success": True, "counties": get_county_registry()})

        body = b'WV Tax Lien API - PDF Parser + CAMA/IDX Lookup'
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
            path = self.path.split("?")[0]

            # ── /cama endpoint ──────────────────────────────────────
            if path == "/cama":
                data = json.loads(body)
                result = lookup_cama(
                    data.get("county", ""),
                    data.get("dist", "01"),
                    data.get("map", "0001"),
                    data.get("parcel", "0001"),
                )
                return self.respond(result)

            # ── /idx endpoint ───────────────────────────────────────
            if path == "/idx":
                data = json.loads(body)
                result = search_idx(
                    data.get("county", ""),
                    data.get("name", ""),
                )
                return self.respond(result)

            # ── /parse PDF endpoint (original) ──────────────────────
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


# ── PDF PARSING (unchanged) ───────────────────────────────────────────────────

def extract_county_from_line(line):
    upper = line.upper().strip()
    m = re.match(r'^([A-Z]+(?:\s+[A-Z]+)?)\s+COUNTY$', upper)
    if m:
        name = m.group(1).strip()
        if name in WV_COUNTIES:
            return f"{name} COUNTY"
    return None


def parse_pdf(pdf_bytes):
    result = {
        'county': '', 'date': '', 'time': '', 'location': '', 'rows': [],
        'lastUpdated': datetime.now(timezone.utc).isoformat()
    }

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            lines = [l.strip() for l in (pdf.pages[0].extract_text() or '').split('\n') if l.strip()]
            county_found = False
            date_found = False

            for i, line in enumerate(lines[:20]):
                if any(skip in line for skip in ['WEB HANDOUT','CERTIFICATE','TICKET','DISTRICT','ASSESSED','LEGAL','MINIMUM']):
                    continue
                if re.match(r'^\d{4}-C-\d+', line):
                    break
                if not county_found:
                    county = extract_county_from_line(line)
                    if county:
                        result['county'] = county
                        county_found = True
                        continue
                if not date_found and '/' in line and len(line) < 35:
                    dm = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)$', line, re.I)
                    if dm:
                        result['date'] = dm.group(1)
                        result['time'] = dm.group(2).strip()
                        date_found = True
                        for j in range(i + 1, min(i + 5, len(lines))):
                            next_line = lines[j]
                            if not any(skip in next_line for skip in ['CERTIFICATE','TICKET','DISTRICT','ASSESSED']):
                                if not re.match(r'^\d{4}-C-\d+', next_line):
                                    result['location'] = next_line
                                    break

            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    for row in table:
                        if not row or not row[0]:
                            continue
                        cert = (row[0] or '').replace('\n', ' ').strip()
                        if not re.match(r'^\d{4}-C-\d+$', cert):
                            continue
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
            return {'success': False, 'error': 'Could not identify a valid WV county name.'}
        if not result['rows']:
            return {'success': False, 'error': 'No property rows found in PDF'}
        return {'success': True, 'data': result}

    except Exception as e:
        return {'success': False, 'error': f'PDF parsing error: {str(e)}'}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'WV Tax Lien API running on port {port}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
