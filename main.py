from http.server import HTTPServer, BaseHTTPRequestHandler
import json, io, re, pdfplumber, os, urllib.request, urllib.parse
from datetime import datetime, timezone
from html.parser import HTMLParser

WV_COUNTIES = {
    "BARBOUR","BERKELEY","BOONE","BRAXTON","BROOKE","CABELL","CALHOUN","CLAY",
    "DODDRIDGE","FAYETTE","GILMER","GRANT","GREENBRIER","HAMPSHIRE","HANCOCK",
    "HARDY","HARRISON","JACKSON","JEFFERSON","KANAWHA","LEWIS","LINCOLN","LOGAN",
    "MARION","MARSHALL","MASON","MCDOWELL","MERCER","MINERAL","MINGO","MONONGALIA",
    "MONROE","MORGAN","NICHOLAS","OHIO","PENDLETON","PLEASANTS","POCAHONTAS",
    "PRESTON","PUTNAM","RALEIGH","RANDOLPH","RITCHIE","ROANE","SUMMERS","TAYLOR",
    "TUCKER","TYLER","UPSHUR","WAYNE","WEBSTER","WETZEL","WIRT","WOOD","WYOMING"
}

# ── ALL 55 COUNTY CAMA REGISTRY ───────────────────────────────────────────────
# 54 counties use wvassessor.com (GST platform) with prc.aspx?PARID=
# Wood County uses its own URL at inquiries.woodcountywv.com
# PARID format: DD++MMMMPPPP0000000 (district 2dig, map 4dig, parcel 4dig, 7 zeros)

def build_parid_variants(dist, map_num, parcel):
    """Return list of PARID variants to try - different counties use different formats."""
    dist_int = int(dist) if str(dist).isdigit() else 1
    dist2 = str(dist_int).zfill(2)
    map_str = str(map_num).zfill(4)
    parcel_str = str(parcel).zfill(4)
    zeros = "0000000"
    return [
        f"{dist2}++{map_str}{parcel_str}{zeros}",   # Format A: Wood, Marion, Kanawha
        f"{dist2}+++{map_str}{parcel_str}{zeros}",  # Format B: Wyoming, Hampshire, Preston
        f"{dist2}+{map_str}{parcel_str}{zeros}",    # Format C: single plus
        f"{dist2}  {map_str}{parcel_str}{zeros}",   # Format D: spaces
    ]

def build_standard_parid(dist, map_num, parcel):
    return build_parid_variants(dist, map_num, parcel)[0]

def build_wood_parid(dist, map_num, parcel):
    return build_parid_variants(dist, map_num, parcel)[0]

# Standard counties — all use COUNTY.wvassessor.com
STANDARD_CAMA_COUNTIES = [
    "BARBOUR","BERKELEY","BOONE","BRAXTON","BROOKE","CABELL","CALHOUN","CLAY",
    "DODDRIDGE","FAYETTE","GILMER","GRANT","GREENBRIER","HAMPSHIRE","HANCOCK",
    "HARDY","HARRISON","JACKSON","JEFFERSON","KANAWHA","LEWIS","LINCOLN","LOGAN",
    "MARION","MARSHALL","MASON","MCDOWELL","MERCER","MINERAL","MINGO","MONONGALIA",
    "MONROE","MORGAN","NICHOLAS","OHIO","PENDLETON","PLEASANTS","POCAHONTAS",
    "PRESTON","PUTNAM","RALEIGH","RANDOLPH","RITCHIE","ROANE","SUMMERS","TAYLOR",
    "TUCKER","TYLER","UPSHUR","WAYNE","WEBSTER","WETZEL","WIRT","WYOMING"
]

# IDX document search counties
IDX_COUNTIES = {
    "BARBOUR": "http://129.71.117.241/WEBInquiry/Default.aspx",
    "BROOKE":  "http://129.71.117.252/",
    "CABELL":  "http://www.recordscabellcountyclerk.org/Default.aspx",
    "DODDRIDGE":"http://129.71.205.241/",
    "FAYETTE": "http://129.71.202.7/",
    "GILMER":  "http://www.gilmercountywv.gov/idxsearch/",
    "GRANT":   "http://129.71.112.124/",
    "GREENBRIER":"http://129.71.205.208/",
    "HAMPSHIRE":"http://129.71.205.207/idxsearch",
    "HANCOCK": "https://hancockwv.compiled-technologies.com/",
    "HARRISON":"http://lookup.harrisoncountywv.com/",
    "JEFFERSON":"http://documents.jeffersoncountywv.org/",
    "LEWIS":   "http://inquiry.lewiscountywv.org/",
    "LINCOLN": "http://129.71.206.62/Default.aspx",
    "MARSHALL":"http://129.71.117.225/",
    "OHIO":    "https://ohiocountywvclerk.com/",
    "WETZEL":  "https://www.wetzelcountywv.gov/county-clerk-responsibilities",
    "WIRT":    "http://records.wirtcountywv.net/",
    "WOOD":    "https://inquiries.woodcountywv.com/legacywebinquiry/default.aspx",
}

def get_cama_url(county):
    if county == "WOOD":
        return "https://inquiries.woodcountywv.com/CAMA/prc.aspx?PARID={parid}"
    return f"https://{county.lower()}.wvassessor.com/prc.aspx?PARID={{parid}}"

def get_county_registry():
    result = {}
    for c in STANDARD_CAMA_COUNTIES:
        result[c] = {"has_cama": True, "has_idx": c in IDX_COUNTIES,
                     "cama_url": f"https://{c.lower()}.wvassessor.com/",
                     "idx_url": IDX_COUNTIES.get(c, "")}
    result["WOOD"] = {"has_cama": True, "has_idx": True,
                      "cama_url": "https://inquiries.woodcountywv.com/CAMA/",
                      "idx_url": IDX_COUNTIES["WOOD"]}
    return result

# ── HTML PARSERS ──────────────────────────────────────────────────────────────

class CAMAParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.data = {"owner":"","mailing_address":"","deed_book":"","deed_page":"",
                     "sales_history":[],"assessments":[],"legal_description":"",
                     "district":"","map_parcel":"","raw_text":""}
        self._text_parts = []
        self._current_row = []
        self._current_cell = ""
        self._table_rows = []
        self._current_tables = []
        self._cell_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table": self._current_tables.append([])
        elif tag == "tr": self._current_row = []
        elif tag in ("td","th"): self._current_cell = ""; self._cell_depth += 1

    def handle_endtag(self, tag):
        if tag == "table" and self._current_tables:
            self._table_rows.append(self._current_tables.pop())
        elif tag == "tr" and self._current_tables:
            self._current_tables[-1].append(self._current_row[:])
        elif tag in ("td","th"):
            self._cell_depth -= 1
            self._current_row.append(self._current_cell.strip())

    def handle_data(self, data):
        d = data.strip()
        if d:
            self._text_parts.append(d)
            if self._cell_depth > 0: self._current_cell += " " + d

    def extract(self):
        full_text = " ".join(self._text_parts)
        self.data["raw_text"] = full_text

        m = re.search(r'CURRENT OWNER:\s*([A-Z][^\n]+?)(?:MAILING ADDRESS:|DEED BOOK)', full_text, re.I)
        if m: self.data["owner"] = m.group(1).strip()

        m = re.search(r'MAILING ADDRESS:\s*([^\n]+?)(?:DEED BOOK|SALES HISTORY)', full_text, re.I)
        if m: self.data["mailing_address"] = m.group(1).strip()

        m = re.search(r'DEED BOOK/PAGE:\s*(\d+)/(\d+)', full_text, re.I)
        if m: self.data["deed_book"] = m.group(1); self.data["deed_page"] = m.group(2)

        m = re.search(r'LEGAL DESCRIPTION:\s*([^\n]+?)(?:CURRENT OWNER|TAXING|$)', full_text, re.I)
        if m: self.data["legal_description"] = m.group(1).strip()

        m = re.search(r'TAXING DISTRICT:\s*([^\n]+?)(?:STREET|PARCEL|$)', full_text, re.I)
        if m: self.data["district"] = m.group(1).strip()

        # Also try alternate patterns for wvassessor.com layout
        if not self.data["deed_book"]:
            m = re.search(r'Book[:/\s]+(\d+)\s*[/\s,]+\s*Page[:/\s]+(\d+)', full_text, re.I)
            if m: self.data["deed_book"] = m.group(1); self.data["deed_page"] = m.group(2)

        if not self.data["owner"]:
            m = re.search(r'Owner[:\s]+([A-Z][A-Z\s,]+?)(?:Address|Deed|Mailing|$)', full_text, re.I)
            if m: self.data["owner"] = m.group(1).strip()

        for table in self._table_rows:
            for row in table:
                cells = [c.strip() for c in row if c.strip()]
                if len(cells) >= 4:
                    dm = re.match(r'(\d{1,2}/\d{1,2}/\d{4})', cells[0])
                    pm = re.match(r'\$[\d,]+', cells[1]) if len(cells) > 1 else None
                    if dm and pm:
                        try:
                            self.data["sales_history"].append({
                                "date": cells[0].split()[0],
                                "price": cells[1],
                                "book": cells[2] if len(cells) > 2 else "",
                                "page": cells[3] if len(cells) > 3 else "",
                            })
                        except: pass
                if len(cells) >= 4 and re.match(r'^20\d{2}$', cells[0]):
                    try:
                        self.data["assessments"].append({
                            "year": cells[0],
                            "land": cells[1] if len(cells) > 1 else "",
                            "building": cells[2] if len(cells) > 2 else "",
                            "total": cells[3] if len(cells) > 3 else "",
                            "assessed": cells[4] if len(cells) > 4 else "",
                        })
                    except: pass
        return self.data


class IDXParser(HTMLParser):
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
        if tag == "table": self._in_table = True
        elif tag == "tr": self._current_row = []
        elif tag in ("td","th"): self._current_cell = ""; self._cell_depth += 1

    def handle_endtag(self, tag):
        if tag == "tr" and self._in_table:
            row = [c.strip() for c in self._current_row]
            if any(row):
                if self._header_row and any(h in " ".join(row).upper() for h in ["GRANTOR","GRANTEE","BOOK","DATE","TYPE","INSTRUMENT"]):
                    self._headers = row; self._header_row = False
                elif not self._header_row:
                    self._rows.append(row)
        elif tag in ("td","th"):
            self._cell_depth -= 1
            self._current_row.append(self._current_cell.strip())

    def handle_data(self, data):
        d = data.strip()
        if d and self._cell_depth > 0: self._current_cell += " " + d

    def extract(self):
        for row in self._rows:
            if len(row) < 3: continue
            record = {}
            for i, header in enumerate(self._headers):
                if i < len(row): record[header.lower().replace(" ","_")] = row[i]
            if record: self.records.append(record)
        return self.records


# ── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_url(url, timeout=20, post_data=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = urllib.request.Request(url, headers=headers)
    if post_data:
        req.data = urllib.parse.urlencode(post_data).encode()
        req.add_header("Content-Type","application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = "utf-8"
        ct = resp.headers.get("Content-Type","")
        if "charset=" in ct: charset = ct.split("charset=")[1].strip().split(";")[0]
        return resp.read().decode(charset, errors="replace")


# ── CAMA LOOKUP ───────────────────────────────────────────────────────────────

def lookup_cama(county_name, dist, map_num, parcel):
    county = county_name.upper().replace(" COUNTY","").strip()
    has_cama = county in STANDARD_CAMA_COUNTIES or county == "WOOD"
    if not has_cama:
        return {"success": False, "error": f"No CAMA system for {county} County"}
    try:
        cama_url_tmpl = get_cama_url(county)
        # Try multiple PARID formats until we get real data
        parids = build_parid_variants(dist, map_num, parcel)
        html = None
        parid = parids[0]
        data = None
        for p in parids:
            try:
                test_url = cama_url_tmpl.format(parid=p)
                test_html = fetch_url(test_url, timeout=15)
                # Check if we got real data (not just an empty/error page)
                if 'CURRENT OWNER' in test_html.upper() or 'DEED BOOK' in test_html.upper() or 'OWNER NAME' in test_html.upper() or 'SALES HISTORY' in test_html.upper():
                    html = test_html
                    parid = p
                    url = test_url
                    break
            except:
                continue
        if not html:
            # Fall back to first format even if no data found
            parid = parids[0]
            url = cama_url_tmpl.format(parid=parid)
            html = fetch_url(url, timeout=15)
        parser = CAMAParser()
        parser.feed(html)
        data = parser.extract()

        years_owned = None
        acquisition_year = None
        if data["sales_history"]:
            sorted_sales = sorted(data["sales_history"], key=lambda s: s["date"], reverse=True)
            try:
                yr = int(sorted_sales[0]["date"].split("/")[-1])
                years_owned = datetime.now().year - yr
                acquisition_year = yr
            except: pass

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
            "years_owned": years_owned,
            "acquisition_year": acquisition_year,
            "sales_history": data["sales_history"],
            "assessments": data["assessments"][:5],
        }
    except Exception as e:
        return {"success": False, "error": f"CAMA lookup failed: {str(e)}", "cama_url": url if 'url' in dir() else ""}


# ── IDX SEARCH ────────────────────────────────────────────────────────────────

def search_idx(county_name, grantor_name):
    county = county_name.upper().replace(" COUNTY","").strip()
    idx_url = IDX_COUNTIES.get(county)
    if not idx_url:
        return {"success": False, "error": f"No IDX system for {county} County", "county_has_idx": False}
    try:
        search_url = idx_url + f"?name={urllib.parse.quote(grantor_name)}&searchType=grantor"
        html = fetch_url(search_url, timeout=20)
        parser = IDXParser()
        parser.feed(html)
        records = parser.extract()
        return {
            "success": True, "county": county, "idx_url": idx_url,
            "grantor_searched": grantor_name,
            "records_found": len(records), "records": records[:25],
        }
    except Exception as e:
        return {"success": False, "error": f"IDX search failed: {str(e)}",
                "county_has_idx": True, "idx_url": idx_url}



# ── CLAUDE AI PROXY ───────────────────────────────────────────────────────────

def call_claude(prompt):
    """Call Anthropic Claude API server-side and return the analysis text."""
    if not prompt:
        return {"success": False, "error": "No prompt provided"}
    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return {"success": False, "error": "ANTHROPIC_API_KEY not set on server"}

        payload = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1200,
            "messages": [{"role": "user", "content": prompt}]
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            }
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            text = result.get("content", [{}])[0].get("text", "")
            return {"success": True, "text": text}
    except Exception as e:
        return {"success": False, "error": f"Claude API error: {str(e)}"}


# ── MAPWV PARCEL LOOKUP ───────────────────────────────────────────────────────

def fetch_mapwv_owner(mapwv_url, county_key, map_num, parcel_num):
    """
    Fetch the current owner from MapWV by hitting their parcel data API.
    The pid is in the URL we already build correctly in the app.
    MapWV loads data via: /parcel/php/getparceldata.php?pid=XX-XX-XXXX-XXXX-XXXX
    """
    try:
        pid = None
        if 'pid=' in mapwv_url:
            pid = mapwv_url.split('pid=')[1].split('&')[0]

        if not pid:
            return {'success': False, 'error': 'No pid in URL'}

        # Try MapWV internal data endpoints
        endpoints = [
            f'https://mapwv.gov/parcel/php/getparceldata.php?pid={pid}',
            f'https://mapwv.gov/parcel/php/getparcelinfo.php?pid={pid}',
            f'https://mapwv.gov/parcel/php/parcelinfo.php?pid={pid}',
        ]

        for endpoint in endpoints:
            try:
                html = fetch_url(endpoint, timeout=12)
                if not html or len(html) < 10:
                    continue
                # Try JSON first
                try:
                    data = json.loads(html)
                    # Look for owner in various key names
                    for key in ['OwnerName','owner','OWNERNAME','Owner','fullownername','FullOwnerName']:
                        if key in data and data[key]:
                            addr = data.get('OwnerAddress','') or data.get('address','') or data.get('OWNERADDRESS','')
                            return {'success':True,'owner':str(data[key]).strip(),'address':str(addr).strip(),'pid':pid}
                    # If it's a list
                    if isinstance(data, list) and len(data) > 0:
                        item = data[0]
                        for key in ['OwnerName','owner','OWNERNAME','Owner']:
                            if key in item and item[key]:
                                return {'success':True,'owner':str(item[key]).strip(),'address':'','pid':pid}
                except (json.JSONDecodeError, TypeError):
                    # Try regex on raw HTML/text
                    m = re.search(r'[Oo]wner[^:]*:[\s"]*([A-Z][A-Z ,&.]+)', html)
                    if m:
                        return {'success':True,'owner':m.group(1).strip(),'address':'','pid':pid}
            except Exception:
                continue

        return {'success': False, 'error': 'MapWV API did not return owner data', 'pid': pid}

    except Exception as e:
        return {'success': False, 'error': str(e)}


# ── MAPWV ASSESSMENT DETAIL LOOKUP ───────────────────────────────────────────

def fetch_assessment_detail(map_pid):
    """
    Fetch the MapWV Assessment Detail page.
    map_pid is the pid from the MapWV URL e.g. "22-01-0011-0010-0005"
    Assessment URL: https://mapwv.gov/Assessment/Detail/?PID=22010011001000050000
    """
    try:
        # Convert pid to assessment PID: remove dashes, pad to 20 chars with zeros
        clean = map_pid.replace('-', '')
        assessment_pid = clean.ljust(20, '0')
        url = f'https://mapwv.gov/Assessment/Detail/?PID={assessment_pid}'

        html = fetch_url(url, timeout=20)
        if not html:
            return {'success': False, 'error': 'Empty response'}

        result = {
            'success': True,
            'assessment_url': url,
            'pid': map_pid,
            'assessment_pid': assessment_pid,
            'owner': '',
            'mailing_address': '',
            'deed_book': '',
            'deed_page': '',
            'last_sale_date': '',
            'last_sale_price': '',
            'tax_class': '',
            'land_use': '',
            'appraised_value': '',
            'assessed_value': '',
            'improvements': '',
            'raw': ''
        }

        # Parse the HTML
        # This is an ASP.NET page with labeled fields
        # Look for patterns like: Owner Name: VALUE, Deed Book: VALUE etc.

        # Strip scripts and styles for cleaner text
        clean_html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL|re.I)
        clean_html = re.sub(r'<style[^>]*>.*?</style>', ' ', clean_html, flags=re.DOTALL|re.I)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', clean_html)
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        result['raw'] = text[:3000]  # Keep for debugging

        # Owner name
        # Try multiple owner name patterns on cleaned text
        owner_found = False
        for pat in [
            r'Owner\s*Name\s*[:\s]+([A-Z][A-Z ,&.\-]{4,}?)(?:\s{2,}|Mailing|Address|Deed)',
            r'Owner[:\s]+([A-Z][A-Z ,&.\-]{4,}?)(?:\s{2,}|Mailing|Address|Deed|Tax)',
            r'Grantee[:\s]+([A-Z][A-Z ,&.\-]{4,}?)(?:\s{2,}|Grantor|Deed|Date)',
        ]:
            m = re.search(pat, text, re.I)
            if m:
                candidate = m.group(1).strip().rstrip('.,- ')
                skip_words = {'OWNER','NAME','ADDRESS','DEED','BOOK','PAGE','CLASS','VALUE','SALE','DATE','MAILING','PHYSICAL'}
                words = [w.upper() for w in candidate.split()]
                if len(candidate) >= 5 and not all(w in skip_words for w in words):
                    result['owner'] = candidate
                    owner_found = True
                    break

        # Mailing address
        m = re.search(r'Mailing[^:]*:[^A-Z]*([A-Za-z0-9 ,.-]+)', text, re.I)
        if m: result['mailing_address'] = m.group(1).strip()

        # Deed book and page
        m = re.search(r'Deed\s*Book[:\s/]+(\d+)\s*[/\s,]+\s*(?:Page[:\s]+)?(\d+)', text, re.I)
        if m: result['deed_book'] = m.group(1); result['deed_page'] = m.group(2)

        # Last sale date
        m = re.search(r'(?:Last\s*Sale|Sale\s*Date|Date\s*of\s*Sale)[:\s]+(\d{1,2}/\d{1,2}/\d{4})', text, re.I)
        if m: result['last_sale_date'] = m.group(1)

        # Last sale price
        m = re.search(r'(?:Last\s*Sale|Sale\s*Price|Amount)[:\s]+\$?([\d,]+)', text, re.I)
        if m: result['last_sale_price'] = '$'+m.group(1)

        # Tax class
        m = re.search(r'(?:Tax\s*)?Class[:\s]+([12IViv]+)', text, re.I)
        if m: result['tax_class'] = m.group(1).strip()

        # Land use / property type
        m = re.search(r'(?:Land\s*Use|Property\s*(?:Type|Class|Use))[:\s]+([A-Z][A-Za-z\s]+?)(?:\s{2,}|Tax|Class|$)', text, re.I)
        if m: result['land_use'] = m.group(1).strip()

        # Appraised value
        m = re.search(r'(?:Total\s*)?Appraised[:\s]+\$?([\d,]+)', text, re.I)
        if m: result['appraised_value'] = '$'+m.group(1)

        # Assessed value
        m = re.search(r'(?:Total\s*)?Assessed[:\s]+\$?([\d,]+)', text, re.I)
        if m: result['assessed_value'] = '$'+m.group(1)

        # Improvements (building type)
        m = re.search(r'(?:Improvement|Building|Structure)[:\s]+([A-Za-z\s]+?)(?:\s{2,}|Year|Value|$)', text, re.I)
        if m: result['improvements'] = m.group(1).strip()

        return result

    except Exception as e:
        return {'success': False, 'error': str(e)}

# ── HTTP HANDLER ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type')

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/counties":
            return self.respond({"success": True, "counties": get_county_registry()})
        body = b'WV Tax Lien API - PDF + CAMA (55 counties) + IDX'
        self.send_response(200)
        self.send_header('Content-Type','text/plain')
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Content-Length',len(body))
        self.end_headers(); self.wfile.write(body)

    def do_POST(self):
        try:
            ct = self.headers.get('Content-Type','')
            length = int(self.headers.get('Content-Length',0))
            body = self.rfile.read(length)
            path = self.path.split("?")[0]

            if path == "/cama":
                data = json.loads(body)
                return self.respond(lookup_cama(
                    data.get("county",""), data.get("dist","01"),
                    data.get("map","0001"), data.get("parcel","0001")))

            if path == "/idx":
                data = json.loads(body)
                return self.respond(search_idx(data.get("county",""), data.get("name","")))

            if path == "/mapwv":
                data = json.loads(body)
                return self.respond(fetch_mapwv_owner(
                    data.get("url",""),
                    data.get("countyKey",""),
                    data.get("map",""),
                    data.get("parcel","")
                ))

            if path == "/assessment":
                data = json.loads(body)
                return self.respond(fetch_assessment_detail(data.get("pid","")))

            if path == "/analyze":
                data = json.loads(body)
                return self.respond(call_claude(data.get("prompt","")))

            if 'multipart/form-data' not in ct:
                return self.respond({'success':False,'error':'Expected multipart/form-data'})

            boundary = ct.split('boundary=')[1].strip().encode()
            pdf_data = None
            for part in body.split(b'--' + boundary):
                if b'filename=' in part and b'.pdf' in part.lower():
                    hend = part.find(b'\r\n\r\n')
                    if hend != -1:
                        pdf_data = part[hend+4:].rstrip(b'\r\n-')
                        break

            if not pdf_data:
                return self.respond({'success':False,'error':'No PDF found'})
            self.respond(parse_pdf(pdf_data))

        except Exception as e:
            self.respond({'success':False,'error':str(e)})

    def respond(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self._cors()
        self.send_header('Content-Length',len(body))
        self.end_headers(); self.wfile.write(body)


# ── PDF PARSING ───────────────────────────────────────────────────────────────

def extract_county_from_line(line):
    upper = line.upper().strip()
    m = re.match(r'^([A-Z]+(?:\s+[A-Z]+)?)\s+COUNTY$', upper)
    if m and m.group(1).strip() in WV_COUNTIES:
        return f"{m.group(1).strip()} COUNTY"
    return None

def parse_pdf(pdf_bytes):
    result = {'county':'','date':'','time':'','location':'','rows':[],
              'lastUpdated':datetime.now(timezone.utc).isoformat()}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            lines = [l.strip() for l in (pdf.pages[0].extract_text() or '').split('\n') if l.strip()]
            county_found = date_found = False
            for i, line in enumerate(lines[:20]):
                if any(s in line for s in ['WEB HANDOUT','CERTIFICATE','TICKET','DISTRICT','ASSESSED','LEGAL','MINIMUM']): continue
                if re.match(r'^\d{4}-C-\d+', line): break
                if not county_found:
                    county = extract_county_from_line(line)
                    if county: result['county'] = county; county_found = True; continue
                if not date_found and '/' in line and len(line) < 35:
                    dm = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)$', line, re.I)
                    if dm:
                        result['date'] = dm.group(1); result['time'] = dm.group(2).strip(); date_found = True
                        for j in range(i+1, min(i+5, len(lines))):
                            nl = lines[j]
                            if not any(s in nl for s in ['CERTIFICATE','TICKET','DISTRICT','ASSESSED']):
                                if not re.match(r'^\d{4}-C-\d+', nl):
                                    result['location'] = nl; break
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    for row in table:
                        if not row or not row[0]: continue
                        cert = (row[0] or '').replace('\n',' ').strip()
                        if not re.match(r'^\d{4}-C-\d+$', cert): continue
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
        if not result['county']: return {'success':False,'error':'Could not identify WV county.'}
        if not result['rows']: return {'success':False,'error':'No property rows found.'}
        return {'success':True,'data':result}
    except Exception as e:
        return {'success':False,'error':f'PDF parsing error: {str(e)}'}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'WV Tax Lien API running on port {port} — 55 counties CAMA enabled')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
