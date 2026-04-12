from http.server import HTTPServer, BaseHTTPRequestHandler
import subprocess, sys

def ensure_chromium():
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=300
        )
        print("Chromium ready:", result.returncode)
    except Exception as e:
        print(f"Chromium install warning: {e}")

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
    Fetch MapWV Assessment Detail page and parse all fields.
    map_pid example: "22-01-0011-0010-0005"
    Assessment URL: https://mapwv.gov/Assessment/Detail/?PID=22010011001000050000
    """
    try:
        # Build assessment PID: remove dashes, pad to 20 chars with zeros
        clean = map_pid.replace('-', '')
        assessment_pid = clean.ljust(20, '0')
        url = f'https://mapwv.gov/Assessment/Detail/?PID={assessment_pid}'

        html = fetch_url(url, timeout=25)
        if not html or len(html) < 500:
            return {'success': False, 'error': 'Empty or invalid response from assessment page', 'assessment_url': url}

        result = {
            'success': True,
            'assessment_url': url,
            'pid': map_pid,
            'assessment_pid': assessment_pid,
            'owner': '',
            'mailing_address': '',
            'tax_class': '',
            'deed_book': '',
            'deed_page': '',
            'legal_description': '',
            'property_class': '',
            'land_use': '',
            'total_appraisal': '',
            'sales_history': [],
            'parcel_history': [],
        }

        # ── Strip scripts/styles, get clean text ──────────────────
        clean_html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL|re.I)
        clean_html = re.sub(r'<style[^>]*>.*?</style>', ' ', clean_html, flags=re.DOTALL|re.I)

        # ── Owner name ─────────────────────────────────────────────
        # The page has: <td>Owner(s)</td><td>KEENEY DON</td>
        m = re.search(r'Owner[(]s[)]\s*</td>\s*<td[^>]*>\s*([^<]+)', clean_html, re.I)
        if not m:
            m = re.search(r'Owner[(]s[)][^<]*<[^>]+>\s*([A-Z][A-Z\s,&.\-]+)', clean_html, re.I)
        if m:
            result['owner'] = m.group(1).strip()

        # ── Mailing address ────────────────────────────────────────
        m = re.search(r'Mailing\s*Address\s*</td>\s*<td[^>]*>\s*([^<]+)', clean_html, re.I)
        if not m:
            m = re.search(r'Mailing\s*Address[^<]*<[^>]+>\s*([A-Z0-9][^<]{5,})', clean_html, re.I)
        if m:
            result['mailing_address'] = m.group(1).strip()

        # ── Tax class ──────────────────────────────────────────────
        # "Tax Class" header then value in next cell
        m = re.search(r'Tax\s*Class\s*</th>.*?<td[^>]*>\s*(\d+)', clean_html, re.I|re.DOTALL)
        if not m:
            m = re.search(r'Tax\s*Class[^<]*</td>\s*<td[^>]*>\s*(\d+)', clean_html, re.I)
        if m:
            result['tax_class'] = m.group(1).strip()

        # ── Book / Page ────────────────────────────────────────────
        m = re.search(r'Book\s*/\s*Page\s*</th>.*?<td[^>]*>\s*(\d+)\s*/\s*(\d+)', clean_html, re.I|re.DOTALL)
        if not m:
            m = re.search(r'>(\d{3,})\s*/\s*(\d{2,})<', clean_html)
        if m:
            result['deed_book'] = m.group(1).strip()
            result['deed_page'] = m.group(2).strip()

        # ── Legal description ──────────────────────────────────────
        m = re.search(r'Legal\s*Description\s*</th>.*?<td[^>]*>\s*([^<]{5,})', clean_html, re.I|re.DOTALL)
        if not m:
            m = re.search(r'Legal\s*Description[^<]*</td>\s*<td[^>]*>([^<]{5,})', clean_html, re.I)
        if m:
            result['legal_description'] = m.group(1).strip()

        # ── Property class ─────────────────────────────────────────
        m = re.search(r'Property\s*Class\s*</td>\s*<td[^>]*>\s*([^<]+)', clean_html, re.I)
        if m:
            result['property_class'] = m.group(1).strip()

        # ── Land use ───────────────────────────────────────────────
        m = re.search(r'Land\s*Use\s*</td>\s*<td[^>]*>\s*([^<]+)', clean_html, re.I)
        if m:
            result['land_use'] = m.group(1).strip()

        # ── Total appraisal ────────────────────────────────────────
        m = re.search(r'Total\s*Appraisal\s*</td>\s*<td[^>]*>\s*\$?([\d,]+)', clean_html, re.I)
        if m:
            result['total_appraisal'] = '$' + m.group(1).strip()

        # ── Sales history ──────────────────────────────────────────
        # Find the Sales History section
        sales_section = re.search(r'Sales\s*History(.*?)(?:Parcel\s*History|</table>.*?<table)', clean_html, re.I|re.DOTALL)
        if sales_section:
            section = sales_section.group(1)
            # Each row: date, price, sale type, source code, validity code, book, page
            rows = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})\s*</td>.*?\$([\d,]+).*?</td>.*?([^<]{3,})</td>.*?(\d+)</td>.*?(\d+)</td>.*?(\d+)</td>.*?(\d+)</td>', section, re.DOTALL)
            for r in rows[:5]:
                result['sales_history'].append({
                    'date': r[0],
                    'price': '$'+r[1],
                    'type': r[2].strip(),
                    'book': r[5],
                    'page': r[6],
                })
            # Simpler fallback if above doesn't work
            if not result['sales_history']:
                dates = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})', section)
                prices = re.findall(r'\$([\d,]+)', section)
                books = re.findall(r'(\d{3,})</td>\s*<td[^>]*>(\d{2,})</td>', section)
                for i in range(min(len(dates), len(prices), 5)):
                    entry = {'date': dates[i], 'price': '$'+prices[i], 'type': '', 'book': '', 'page': ''}
                    if i < len(books):
                        entry['book'] = books[i][0]
                        entry['page'] = books[i][1]
                    result['sales_history'].append(entry)

        # ── Parcel history (owner per year) ────────────────────────
        parcel_section = re.search(r'Parcel\s*History(.*?)$', clean_html, re.I|re.DOTALL)
        if parcel_section:
            section = parcel_section.group(1)
            rows = re.findall(r'(20\d\d)\s*</td>.*?(\d+)\s*</td>.*?([A-Z][A-Z\s,&.]+)\s*</td>', section, re.DOTALL)
            for r in rows[:6]:
                result['parcel_history'].append({
                    'year': r[0],
                    'tax_class': r[1],
                    'owner': r[2].strip(),
                })

        return result

    except Exception as e:
        return {'success': False, 'error': str(e), 'assessment_url': url if 'url' in dir() else ''}



# ── TITLE SEARCH VIA PLAYWRIGHT ───────────────────────────────────────────────

IDX_COUNTY_URLS = {
    "LINCOLN":    "http://129.71.206.62/Default.aspx",
    "LOGAN":      "https://loganwv.compiled-technologies.com",
    "NICHOLAS":   "http://129.71.205.250/Default.aspx",
    "GILMER":     "http://www.gilmercountywv.gov/idxsearch/",
    "HARRISON":   "http://lookup.harrisoncountywv.com/",
    "LEWIS":      "http://inquiry.lewiscountywv.org/",
    "MARSHALL":   "http://129.71.117.225/",
    "BARBOUR":    "http://129.71.117.241/WEBInquiry/Default.aspx",
    "GRANT":      "http://129.71.112.124/",
    "GREENBRIER": "http://129.71.205.208/",
    "HAMPSHIRE":  "http://129.71.205.207/idxsearch",
    "FAYETTE":    "http://129.71.202.7/",
    "DODDRIDGE":  "http://129.71.205.241/",
    "CABELL":     "http://www.recordscabellcountyclerk.org/Default.aspx",
    "JEFFERSON":  "http://documents.jeffersoncountywv.org/",
    "WIRT":       "http://records.wirtcountywv.net/",
}

# Instrument types that indicate debt/encumbrance
LIEN_TYPES = [
    "DEED OF TRUST", "TRUST DEED", "MORTGAGE", "LIEN",
    "JUDGMENT", "MECHANIC", "UCC", "TAX LIEN", "FEDERAL",
    "STATE TAX", "IRS", "ATTACHMENT"
]

# Instrument types that indicate release/satisfaction
RELEASE_TYPES = [
    "RELEASE", "SATISFACTION", "DISCHARGE", "RECONVEYANCE",
    "PARTIAL RELEASE", "FULL RELEASE"
]

# Instrument types that indicate deed/ownership transfer
DEED_TYPES = [
    "DEED", "SPECIAL WARRANTY", "GENERAL WARRANTY", "QUITCLAIM",
    "EXECUTOR", "ADMINISTRATOR", "TRUSTEE DEED", "COMMISSIONER"
]

def get_playwright_browser():
    """Launch a headless Chromium browser with minimal memory footprint."""
    from playwright.sync_api import sync_playwright
    print("[IDX] Launching Chromium browser...", flush=True)
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
            "--no-zygote",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--memory-pressure-off",
            "--max_old_space_size=512",
            "--js-flags=--max-old-space-size=256",
        ]
    )
    print("[IDX] Browser launched OK", flush=True)
    return p, browser

def idx_select_search_type(page, search_type):
    """
    Set cboKey value directly via JavaScript and trigger postback.
    We know the exact field name: CallFormPanel$contentSplitter$CallToolPanel$cboKey
    """
    print(f"[IDX] Setting cboKey to: {search_type}", flush=True)
    try:
        # Set value directly via JS - no clicking needed
        result = page.evaluate(f"""() => {{
            var inputs = document.querySelectorAll('input[type="text"]');
            for (var inp of inputs) {{
                if (inp.name && inp.name.endsWith('cboKey')) {{
                    inp.value = '{search_type}';
                    inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                    inp.dispatchEvent(new Event('blur', {{bubbles: true}}));
                    return 'set cboKey to: ' + inp.value + ' name: ' + inp.name;
                }}
            }}
            return 'cboKey not found';
        }}""")
        print(f"[IDX] JS result: {result}", flush=True)
        page.wait_for_timeout(500)

        # Trigger ASP.NET postback using the exact control name
        page.evaluate("""() => {
            if (typeof __doPostBack !== 'undefined') {
                __doPostBack('CallFormPanel$contentSplitter$CallToolPanel$cboKey', '');
            }
        }""")
        print("[IDX] Postback triggered", flush=True)
        page.wait_for_timeout(4000)

        # Check what fields are visible now
        inputs = page.query_selector_all('input[type="text"]')
        visible = [(inp.get_attribute('name') or '', inp.input_value()) for inp in inputs if inp.is_visible()]
        print(f"[IDX] Visible inputs after postback: {visible}", flush=True)

        # Verify cboKey changed
        for name, val in visible:
            if 'cboKey' in name:
                print(f"[IDX] cboKey is now: {val}", flush=True)
                if val == search_type:
                    print("[IDX] SUCCESS - correct search type selected", flush=True)
                    return True
                else:
                    print(f"[IDX] WRONG - expected {search_type} got {val}", flush=True)

    except Exception as e:
        print(f"[IDX] Error: {e}", flush=True)
    return False

def idx_fill_and_search(page, fields):
    """Fill Book # and Page # fields after switching to Book & Page mode."""
    page.wait_for_timeout(1000)
    print(f"[IDX] Filling fields: {fields}", flush=True)

    # After switching to Book & Page, new inputs appear
    # Get all visible text inputs excluding dates and cboKey
    inputs = page.query_selector_all('input[type="text"]')
    candidates = []
    for inp in inputs:
        if not inp.is_visible(): continue
        nm = inp.get_attribute('name') or ''
        if any(skip in nm for skip in ['FromDate', 'ThruDate', 'cboKey']): continue
        if nm == '': continue  # skip page size input
        candidates.append((nm, inp))
        print(f"[IDX] Candidate field: {nm}", flush=True)

    field_values = list(fields.values())
    for i, (nm, inp) in enumerate(candidates[:2]):
        val = field_values[i] if i < len(field_values) else ''
        try:
            inp.click()
            page.wait_for_timeout(200)
            inp.fill('')
            inp.type(str(val), delay=50)
            page.wait_for_timeout(200)
            print(f"[IDX] Filled {nm} = {val}", flush=True)
        except Exception as e:
            print(f"[IDX] Fill error for {nm}: {e}", flush=True)

    # Click Index Search
    try:
        page.click('text=Index Search', timeout=5000)
        page.wait_for_timeout(6000)
        print("[IDX] Index Search clicked", flush=True)
    except Exception as e:
        print(f"[IDX] Index Search click failed: {e}", flush=True)

def idx_search_book_page(page, book, pg):
    """Search IDX by Book & Page number."""
    print(f"[IDX] Searching Book={book} Page={pg}", flush=True)
    # Extra wait to ensure JS is fully rendered
    page.wait_for_timeout(3000)
    try:
        page.screenshot(path="/tmp/idx_before.png")
    except: pass
    idx_select_search_type(page, "Book & Page")
    page.wait_for_timeout(800)
    try:
        page.screenshot(path="/tmp/idx_after_dropdown.png")
    except: pass
    idx_fill_and_search(page, {"Book #": book, "Page #": pg})
    try:
        page.screenshot(path="/tmp/idx_results.png")
        text = page.inner_text('body')
        print(f"[IDX] Page text preview: {text[:800]}", flush=True)
    except Exception as e:
        print(f"[IDX] Page content error: {e}", flush=True)
    results = parse_idx_results(page)
    print(f"[IDX] Results: {len(results)} records", flush=True)
    return results


def idx_search_name(page, last_name, first_name="", from_year=None):
    """
    Search IDX by Individual name.
    Based on screenshots: dropdown -> Individual -> Last, First, Middle fields -> Index Search
    """
    from_date = f"01/01/{from_year}" if from_year else "01/01/1700"
    
    try:
        page.select_option('select', 'Individual')
        page.wait_for_timeout(1000)
    except:
        try:
            page.click('text=Book & Page')
            page.wait_for_timeout(300)
            page.click('text=Individual')
            page.wait_for_timeout(1000)
        except: pass

    try:
        last_input = page.query_selector('input[placeholder="Last"]') or                      page.query_selector('input[aria-label*="Last"]')
        if last_input:
            last_input.fill(last_name)
    except: pass

    if first_name:
        try:
            first_input = page.query_selector('input[placeholder="First"]') or                           page.query_selector('input[aria-label*="First"]')
            if first_input:
                first_input.fill(first_name)
        except: pass

    try:
        page.fill('input[aria-label*="From"]', from_date)
        page.fill('input[aria-label*="Thru"]', datetime.now().strftime('%m/%d/%Y'))
    except: pass

    try:
        page.click('text=Index Search')
        page.wait_for_timeout(10000)  # Wait longer for JS to fully render
    except: pass

    return parse_idx_results(page)

def parse_idx_results(page):
    """
    Parse results from the IDX results grid.
    The results appear in a table in the main area.
    Bottom panels show Names, Description, Cross References.
    """
    records = []
    try:
        # Wait for results to load
        page.wait_for_timeout(2000)
        
        # Get all rows from the results table
        rows = page.query_selector_all('tr')
        headers = []
        
        for row in rows:
            cells = row.query_selector_all('td, th')
            cell_texts = [c.inner_text().strip() for c in cells]
            
            if not any(cell_texts): continue
            
            # Detect header row
            row_text = ' '.join(cell_texts).upper()
            if any(h in row_text for h in ['INSTRUMENT', 'RECORDED', 'BOOK', 'GRANTOR', 'TYPE', 'DATE']):
                headers = cell_texts
                continue
            
            # Skip empty rows
            if len([t for t in cell_texts if t]) < 2: continue
            
            # Build record
            record = {}
            if headers and len(headers) == len(cell_texts):
                for i, h in enumerate(headers):
                    record[h.lower().strip()] = cell_texts[i]
            else:
                # Try to identify fields by position/content
                record['raw'] = cell_texts
                # Try to extract date
                for t in cell_texts:
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', t):
                        record['date'] = t
                        break
                # Try to extract book/page (format like "434/138" or separate cells)
                for t in cell_texts:
                    m = re.match(r'(\d+)/(\d+)', t)
                    if m:
                        record['book'] = m.group(1)
                        record['page'] = m.group(2)
                        break
            
            if record:
                records.append(record)
                
    except Exception as e:
        records.append({"error": str(e)})
    
    return records

def do_title_search(county_name, deed_book, deed_page, current_owner_name, years_back=25):
    """
    Full title search workflow:
    1. Search by Book/Page to get the current deed
    2. Extract grantor (seller) from that deed
    3. Search grantor name for their deed (chain backwards)
    4. For each owner: search for mortgages, liens, deeds of trust
    5. Match with releases
    6. Return full title report
    """
    county = county_name.upper().replace(" COUNTY", "").strip()
    url = IDX_COUNTY_URLS.get(county)
    if not url:
        return {"success": False, "error": f"No IDX configured for {county} County"}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"success": False, "error": "Playwright not installed"}

    title_report = {
        "success": True,
        "county": county,
        "starting_book": deed_book,
        "starting_page": deed_page,
        "current_owner": current_owner_name,
        "chain_of_title": [],
        "open_liens": [],
        "released_liens": [],
        "all_instruments": [],
        "errors": []
    }

    try:
        p, browser = get_playwright_browser()
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        )
        page = context.new_page()
        page.set_default_timeout(20000)
        page.set_default_navigation_timeout(25000)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=25000)
        except Exception as nav_err:
            title_report["errors"].append(f"Navigation warning: {str(nav_err)}")
        page.wait_for_timeout(4000)

        cutoff_year = datetime.now().year - years_back

        # ── Step 1: Search by Book/Page to get current deed ──────────
        print(f"[IDX] Step 1: Searching Book {deed_book} / Page {deed_page}", flush=True)
        step1_records = idx_search_book_page(page, deed_book, deed_page)
        print(f"[IDX] Step 1 complete: {len(step1_records)} records found", flush=True)
        
        current_deed = None
        grantors_to_search = []
        
        for rec in step1_records:
            raw = rec.get('raw', [])
            rec_text = ' '.join(raw).upper() if raw else ' '.join(str(v) for v in rec.values()).upper()
            
            # Identify as deed
            is_deed = any(dt in rec_text for dt in ['DEED', 'CONVEYANCE', 'GRANT'])
            if is_deed or not step1_records:
                current_deed = rec
                # Extract grantor (seller) - usually the party who conveyed to current owner
                for r in raw:
                    r_up = r.upper()
                    if r_up and r_up != current_owner_name.upper() and len(r_up) > 3:
                        if not any(skip in r_up for skip in ['DEED', 'BOOK', 'PAGE', 'DATE', '/']):
                            grantors_to_search.append(r)
                            break
            
            title_report["all_instruments"].append({
                "type": "DEED (current)",
                "book": rec.get('book', deed_book),
                "page": rec.get('page', deed_page),
                "details": rec
            })

        title_report["chain_of_title"].append({
            "owner": current_owner_name,
            "deed_book": deed_book,
            "deed_page": deed_page,
            "records_from_book_page_search": step1_records
        })

        # ── Step 2: Search current owner for liens/mortgages ─────────
        print(f"[IDX] Step 2: Searching name {current_owner_name}", flush=True)
        owner_parts = current_owner_name.strip().split()
        if owner_parts:
            last = owner_parts[-1] if len(owner_parts) > 1 else owner_parts[0]
            first = owner_parts[0] if len(owner_parts) > 1 else ""
            
            owner_records = idx_search_name(page, last, first, cutoff_year)
            
            liens = []
            releases = []
            
            for rec in owner_records:
                raw = rec.get('raw', [])
                rec_text = ' '.join(raw).upper() if raw else ' '.join(str(v) for v in rec.values()).upper()
                
                is_lien = any(lt in rec_text for lt in LIEN_TYPES)
                is_release = any(rt in rec_text for rt in RELEASE_TYPES)
                
                if is_release:
                    releases.append(rec)
                    title_report["released_liens"].append(rec)
                elif is_lien:
                    liens.append(rec)
                
                title_report["all_instruments"].append({
                    "type": "LIEN/RELEASE" if is_lien or is_release else "OTHER",
                    "is_lien": is_lien,
                    "is_release": is_release,
                    "details": rec
                })
            
            # Find unreleased liens
            for lien in liens:
                lien_text = ' '.join(str(v) for v in lien.values()).upper()
                # Check if there's a matching release
                released = False
                for rel in releases:
                    rel_text = ' '.join(str(v) for v in rel.values()).upper()
                    # Match by book/page reference or name
                    lien_book = lien.get('book', '')
                    if lien_book and lien_book in rel_text:
                        released = True
                        break
                
                if not released:
                    title_report["open_liens"].append({
                        "status": "OPEN - NO RELEASE FOUND",
                        "details": lien
                    })

        print("[IDX] Closing browser", flush=True)
        browser.close()
        p.stop()
        print("[IDX] Browser closed OK", flush=True)

        return title_report

    except Exception as e:
        print(f"[IDX] ERROR: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e), "county": county}


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

            if path == "/idx-search":
                data = json.loads(body)
                return self.respond(do_title_search(
                    data.get("county",""),
                    data.get("deed_book",""),
                    data.get("deed_page",""),
                    data.get("owner_name",""),
                    int(data.get("years_back", 25))
                ))

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
    ensure_chromium()
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
