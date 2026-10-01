"""📨 Client agreement emails (portal.annelabes.com) — sent through Resend from marci@annelabes.com.

A background thread takes one queued email at a time from the database (engagement_outbox_next), sends it
and reports back (engagement_outbox_done):
  agreement_email   — "please review and sign" with the client's private signing link
  signed_copy_email — after signing: their signed agreement as a PDF, and what happens next
  make_paylink      — (needs SQUARE_ACCESS_TOKEN) a Square payment link for the total; the signing page then
                      shows the Pay button and a paylink_email goes out
  paylink_email     — "pay for your title work" with the Square link
Square's webhook (POST /square-webhook in main.py -> handle_square_webhook) marks the agreement paid when
the payment completes; the liens' holds lift and Fernando starts.
Texts are not handled here yet; those rows stay queued.
Square env: SQUARE_ACCESS_TOKEN, SQUARE_LOCATION_ID, SQUARE_WEBHOOK_SIGNATURE_KEY
            (SQUARE_WEBHOOK_URL defaults to https://wv-pdf-parser.onrender.com/square-webhook).
In TEST MODE the payment link is for $1.00, and a completed $1 test payment counts as paying the agreement.

Render environment:
  RESEND_API_KEY   — needed; without it the thread only waits (nothing is sent)
  ENG_LIVE=1       — send to the real clients. Anything else = TEST MODE: every email goes only to
                     ENG_TEST_TO (default ari@eqoppa.com), with the client's address in the subject.
  ENG_FROM         — default "Marci at Anne Labes, Esq. <marci@annelabes.com>"
  ENG_REPLY_TO     — default marci@annelabes.com
"""
import base64, hashlib, hmac, html, json, os, re, time, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta

SB_URL = "https://uhunhyfgwvoknqnkzlmr.supabase.co"
KINDS = ["agreement_email", "signed_copy_email"]   # + paylink_email / make_paylink when their keys are set
try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:   # no time-zone data on this machine: Eastern Daylight Time
    ET = timezone(timedelta(hours=-4))


def _env(k, d=""):
    return (os.environ.get(k) or d).strip()


def _rpc(name, args):
    key = _env("SUPABASE_SECRET_KEY")
    req = urllib.request.Request(f"{SB_URL}/rest/v1/rpc/{name}", data=json.dumps(args).encode(), method="POST",
                                 headers={"apikey": key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = r.read()
    return json.loads(body) if body else None


def _money(n):
    return "${:,.2f}".format(float(n or 0))


def _et(ts):
    if not ts:
        return "—"
    d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    d = d.astimezone(ET)
    return d.strftime("%B %-d, %Y at %-I:%M:%S %p ET") if os.name != "nt" else d.strftime("%B %d, %Y at %I:%M:%S %p ET")


# ── the signed agreement as a letter (same layout as the portal's "Signed copy") ────────────────
def signed_html(e):
    esc = lambda s: html.escape(str(s if s is not None else ""), quote=True)
    f = e.get("fields") or {}
    sig = '<img class="sig" src="' + esc(e.get("signature")) + '" alt="signature">'
    parts = []
    text = str(e.get("text") or "").replace("\r\n", "\n").replace("\r", "\n")
    import re
    for par in re.split(r"\n{2,}", text):
        lines = [esc(x) for x in par.split("\n")]
        head = ""
        first = par.split("\n")[0]
        if re.match(r"^\d+\.\s[^.]+$", first) and len(lines[0]) < 90:
            head = "<h3>" + lines.pop(0) + "</h3>"
        h = "<br>".join(lines).replace("Signature: signed electronically (signature below)", "Signature:<br>" + sig)
        if re.match(r"^(CLIENT ACKNOWLEDGES|IF YOU PURCHASE)", par):
            h = "<b>" + h + "</b>"
        if re.match(r"^(FOR THE CLIENT|FOR THE ATTORNEY):", par):
            parts.append('<div class="sign">' + h + "</div>"); continue
        if re.match(r"^CLIENT INFORMATION:", par):
            parts.append('<div class="info">' + h + "</div>"); continue
        parts.append(head + ("<p>" + h + "</p>" if h else ""))
    rec = [("Signed by", esc(e.get("signer_name"))), ("Signed on", _et(e.get("signed_at"))), ("Agreement sent", _et(e.get("created_at"))),
           ("First opened by the client", _et(e.get("viewed_at"))), ("IP address", esc(e.get("signer_ip") or "—")),
           ("Device", esc(e.get("signer_agent") or "—")), ("Agreement version", esc(e.get("version") or "")), ("Document ID", esc(e.get("id")))]
    css = ("@page{size:letter;margin:0.8in 0.9in}body{font:11.5pt/1.5 Georgia,'Times New Roman',serif;color:#111;max-width:7in;margin:0 auto}"
           ".lh{text-align:center;border-bottom:2px solid #1e3a8a;padding-bottom:10px;margin-bottom:22px}.lh .n{font-size:20pt;letter-spacing:.5px;color:#1e3a8a}"
           ".lh .s{font:9.5pt Arial,sans-serif;color:#555;margin-top:2px}p{margin:0 0 10px;text-align:justify}h3{font-size:12pt;margin:16px 0 6px}"
           ".sign{margin:14px 0;break-inside:avoid}.sig{width:260px;height:auto;display:block;border-bottom:1px solid #333;margin:2px 0 4px}"
           ".info{border:1px solid #bbb;border-radius:6px;padding:10px 14px;margin:14px 0;break-inside:avoid;background:#fafafa}"
           ".rec{font:9pt/1.45 Arial,sans-serif;color:#333;border:1px solid #1e3a8a;border-radius:6px;padding:10px 14px;margin-top:22px;break-inside:avoid}"
           ".rec h4{margin:0 0 6px;font-size:10pt;color:#1e3a8a}.rec td{padding:1px 10px 1px 0;vertical-align:top}.rec td:first-child{color:#666;white-space:nowrap}")
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Signed agreement — " + esc(f.get("client_name")) + "</title><style>" + css
            + "</style></head><body><div class='lh'><div class='n'>Anne Labes, Esq.</div><div class='s'>WV Tax Lien Title Services · anne@annelabes.com</div></div>"
            + "".join(parts)
            + "<div class='rec'><h4>Electronic signature record</h4><table>" + "".join("<tr><td>" + a + "</td><td>" + b + "</td></tr>" for a, b in rec) + "</table>"
            + "<div style='margin-top:6px;color:#666'>Signed electronically on portal.annelabes.com. The client typed their name, drew their signature and ticked "
            + "“I have read this agreement and I agree to it.” The text above is exactly what the client saw and signed.</div></div></body></html>")


def signed_pdf(e):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        b = p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            pg = b.new_page()
            pg.set_content(signed_html(e), wait_until="load")
            return pg.pdf(format="Letter", print_background=True, margin={"top": "0.8in", "bottom": "0.8in", "left": "0.9in", "right": "0.9in"})
        finally:
            b.close()


# ── the emails ────────────────────────────────────────────────────────────────────────────────
def _wrap(inner):
    return ("<div style=\"font:15px/1.55 -apple-system,'Segoe UI',Arial,sans-serif;color:#1f2937;max-width:560px\">"
            "<div style='font:20px Georgia,serif;color:#1e3a8a;border-bottom:2px solid #1e3a8a;padding-bottom:6px;margin-bottom:16px'>Anne Labes, Esq.</div>"
            + inner +
            "<p style='color:#6b7280;font-size:13px;margin-top:24px'>Marci · Office of Anne Labes, Esq.<br>WV Tax Lien Title Services · reply to this email with any questions</p></div>")


def _button(url, label):
    return ("<p style='margin:22px 0'><a href='" + html.escape(url, quote=True) + "' style='background:#166534;color:#fff;text-decoration:none;"
            "font-weight:700;padding:12px 22px;border-radius:8px;display:inline-block'>" + html.escape(label) + "</a></p>")


def build_email(kind, e):
    f = e.get("fields") or {}
    name = (e.get("client_name") or f.get("client_name") or "").strip()
    first = name.split(" ")[0] if name else "there"
    total, n = _money(e.get("total")), int(e.get("liens") or 0)
    liens = html.escape(f.get("certificates") or "")
    if kind == "agreement_email":
        subject = "Please sign your agreement — %d tax lien%s (%s)" % (n, "" if n == 1 else "s", total)
        body = (f"<p>Hi {html.escape(first)},</p><p>Thank you for choosing us for your title work. Before we start, please review and sign our "
                f"representation agreement online — it takes about two minutes on your phone or computer.</p>"
                f"<p><b>Liens:</b> {liens}<br><b>Fee:</b> {n} × $500 = <b>{total}</b></p>"
                + _button(e["link"], "Review and sign") +
                ("<p>After you sign, please send your payment by check, or as arranged with our office. We start the title work as soon as it arrives.</p>"
                 if e.get("pay_by") == "check" else
                 "<p>After you sign, you'll get a secure link to pay. We start the title work as soon as the fee is paid.</p>"))
        text = f"Hi {first},\n\nPlease review and sign your agreement ({n} lien(s), {total}):\n{e['link']}\n\nMarci · Office of Anne Labes, Esq."
        return subject, _wrap(body), text, None
    if kind == "signed_copy_email":
        subject = "Your signed agreement — Anne Labes, Esq."
        if e.get("pay_by") == "bill_later" or e.get("status") == "bill_later":
            nxt = "<p>We're starting your title work now.</p>"
        elif e.get("pay_by") == "check":
            nxt = f"<p><b>Next step:</b> please send your payment of <b>{total}</b> by check, or as arranged with our office. We start as soon as it arrives.</p>"
        else:
            nxt = (f"<p><b>Next step:</b> payment of <b>{total}</b>. Your secure payment link will appear on your agreement page "
                   f"(and we'll send it to you). We start the title work as soon as it's paid.</p>" + _button(e["link"], "Open my agreement page"))
        body = (f"<p>Hi {html.escape(first)},</p><p>Thank you for signing. Your signed agreement is attached as a PDF for your records.</p>" + nxt)
        text = f"Hi {first},\n\nThank you for signing. Your signed agreement is attached.\n{e['link']}\n\nMarci · Office of Anne Labes, Esq."
        pdf = signed_pdf(e)
        safe = "".join(c for c in (name or "client") if c.isalnum() or c in " -_").strip().replace(" ", "-") or "client"
        return subject, _wrap(body), text, [{"filename": f"Signed-Agreement-{safe}.pdf", "content": base64.b64encode(pdf).decode()}]
    if kind == "paylink_email":
        url = e.get("pay_link_url") or e["link"]
        subject = "Payment for your title work — " + total
        body = (f"<p>Hi {html.escape(first)},</p><p>Thank you for signing. The last step is the fee for your title work: "
                f"<b>{n} × $500 = {total}</b>.</p><p><b>Liens:</b> {liens}</p>"
                + _button(url, "Pay " + total + " securely") +
                "<p>Payment is handled by Square. We start the title work as soon as it's paid (the agreement asks for payment "
                "within 7 days of signing). Prefer to pay by check? Just reply to this email.</p>")
        text = f"Hi {first},\n\nPlease pay {total} for your title work here:\n{url}\n\nMarci · Office of Anne Labes, Esq."
        return subject, _wrap(body), text, None
    raise ValueError("unknown kind " + kind)


# ── Square ────────────────────────────────────────────────────────────────────────────────────
SQ_API = "https://connect.squareup.com"


def _square(method, path, body=None):
    req = urllib.request.Request(SQ_API + path, data=json.dumps(body).encode() if body is not None else None, method=method,
                                 headers={"Authorization": "Bearer " + _env("SQUARE_ACCESS_TOKEN"), "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.loads(r.read() or b"{}")


def _square_location():
    loc = _env("SQUARE_LOCATION_ID")
    if loc:
        return loc
    locs = [l for l in _square("GET", "/v2/locations").get("locations", []) if l.get("status") == "ACTIVE"]
    if not locs:
        raise RuntimeError("no active Square location")
    return locs[0]["id"]


def make_paylink(job_id, e):
    live = _env("ENG_LIVE") == "1"
    f = e.get("fields") or {}
    n = int(e.get("liens") or 0)
    cents = int(round(float(e.get("total") or 0) * 100)) if live else 100
    name = f"Title search fee — {n} tax lien{'' if n == 1 else 's'}" + ("" if live else " [TEST $1]")
    body = {"idempotency_key": f"eng-{e['id']}-{job_id}",
            "order": {"location_id": _square_location(), "reference_id": str(e["id"])[:40],
                      "line_items": [{"name": name[:500], "quantity": "1", "note": (f.get("certificates") or "")[:500],
                                      "base_price_money": {"amount": cents, "currency": "USD"}}]},
            "checkout_options": {"redirect_url": e["link"], "ask_for_shipping_address": False},
            "payment_note": f"Anne Labes agreement {e['id']} · bidder {f.get('bidder', '')}"[:500]}
    em = (e.get("email") or "").strip().lower()
    if live and re.match(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$", em):   # Square rejects anything it doesn't like; test mode leaves it out
        body["pre_populated_data"] = {"buyer_email": em}
    try:
        pl = _square("POST", "/v2/online-checkout/payment-links", body)["payment_link"]
    except urllib.error.HTTPError as ex:
        err = ex.read().decode("utf-8", "replace")[:300]
        return ("queued" if ex.code in (429, 500, 502, 503) else "failed"), f"Square HTTP {ex.code}: {err}"
    _rpc("engagement_set_paylink", {"p_id": e["id"], "p_url": pl["url"], "p_link_id": pl["id"], "p_order_id": pl.get("order_id")})
    return "sent", ("" if live else "TEST $1 · ") + "link " + pl["id"]


def handle_square_webhook(raw, signature):
    """Square -> POST /square-webhook. Checks Square's signature, then marks the agreement paid."""
    key = _env("SQUARE_WEBHOOK_SIGNATURE_KEY")
    if not key:
        return 503, "webhook signature key not set"
    url = _env("SQUARE_WEBHOOK_URL", "https://wv-pdf-parser.onrender.com/square-webhook")
    want = base64.b64encode(hmac.new(key.encode(), url.encode() + raw, hashlib.sha256).digest()).decode()
    if not hmac.compare_digest(want, (signature or "").strip()):
        print("[square] webhook with a bad signature ignored", flush=True)
        return 403, "bad signature"
    ev = json.loads(raw or b"{}")
    if ev.get("type") not in ("payment.updated", "payment.created"):
        return 200, "ignored"
    p = ((ev.get("data") or {}).get("object") or {}).get("payment") or {}
    if p.get("status") != "COMPLETED" or not p.get("order_id"):
        return 200, "not completed"
    eng = _rpc("engagement_by_order", {"p_order_id": p["order_id"]})
    if not eng:
        return 200, "not one of ours"
    amount = ((p.get("amount_money") or {}).get("amount") or 0) / 100.0
    method = "Square card"
    if _env("ENG_LIVE") != "1" and amount <= 1.0:          # the $1 test link pays the test agreement
        amount, method = float(eng["total"]), "Square TEST ($1 paid)"
    _rpc("engagement_paid", {"p_id": eng["id"], "p_amount": amount, "p_ref": p["id"], "p_method": method})
    print(f"[square] agreement {eng['id']} paid {amount} ({p['id']})", flush=True)
    return 200, "ok"


def send(kind, e):
    to = (e.get("email") or "").strip()
    if not to:
        return "skipped", "no email address"
    subject, body, text, attachments = build_email(kind, e)
    return _resend(to, subject, body, text, attachments, bcc=True)


# Client emails also go, hidden (bcc), to the office so it sees exactly what the client got (Ari 2026-10-01: Marci).
# Not the portal welcome / password emails (main.py) - those carry the client's private set-password link.
def _office_bcc(to):
    return [x.strip() for x in _env("ENG_BCC", "marci@annelabes.com").split(",") if x.strip() and x.strip().lower() != to.lower()]


def _resend(to, subject, body, text, attachments=None, bcc=False):
    live = _env("ENG_LIVE") == "1"
    rcpt = [to] if live else [x.strip() for x in _env("ENG_TEST_TO", "ari@eqoppa.com").split(",") if x.strip()]
    if not live:
        subject = f"[TEST → {to}] " + subject
    msg = {"from": _env("ENG_FROM", "Marci at Anne Labes, Esq. <marci@annelabes.com>"), "to": rcpt,
           "reply_to": _env("ENG_REPLY_TO", "marci@annelabes.com"), "subject": subject, "html": body, "text": text}
    if attachments:
        msg["attachments"] = attachments
    if bcc and live and _office_bcc(to):
        msg["bcc"] = _office_bcc(to)
    req = urllib.request.Request("https://api.resend.com/emails", data=json.dumps(msg).encode(), method="POST",
                                 headers={"Authorization": "Bearer " + _env("RESEND_API_KEY"), "Content-Type": "application/json",
                                          "User-Agent": "annelabes-portal/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            rid = json.loads(r.read() or b"{}").get("id", "")
        return "sent", ("" if live else "TEST to " + ", ".join(rcpt) + " · ") + "resend " + rid
    except urllib.error.HTTPError as ex:
        err = ex.read().decode("utf-8", "replace")[:300]
        # 4xx other than rate limits won't get better by retrying
        return ("queued" if ex.code in (429, 500, 502, 503) else "failed"), f"HTTP {ex.code}: {err}"


# ── 📞 Client calls booked from the client portal (mail_outbox: call_booked_staff / call_booked_client) ──────────
def _ics(p, summary, desc):
    start = datetime.fromisoformat(str(p["slot_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    end = start + timedelta(minutes=int(p.get("minutes") or 10))
    f = lambda d: d.strftime("%Y%m%dT%H%M%SZ")
    esc = lambda t: str(t or "").replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")
    return "\r\n".join(["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Anne Labes Esq//Client calls//EN", "METHOD:PUBLISH",
                         "BEGIN:VEVENT", f"UID:client-call-{p.get('call_id')}@annelabes.com", "DTSTAMP:" + f(datetime.now(timezone.utc)),
                         "DTSTART:" + f(start), "DTEND:" + f(end), "SUMMARY:" + esc(summary), "DESCRIPTION:" + esc(desc),
                         "BEGIN:VALARM", "TRIGGER:-PT10M", "ACTION:DISPLAY", "DESCRIPTION:" + esc(summary), "END:VALARM",
                         "END:VEVENT", "END:VCALENDAR", ""])


def _when(ts):
    d = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(ET)
    day = d.strftime("%A, %B ") + str(d.day)
    return day + " at " + d.strftime("%I:%M %p").lstrip("0") + " (Eastern)"


def build_call_email(kind, p):
    name = (p.get("name") or "").strip()
    esc = html.escape
    if kind == "call_callback_staff":     # none of the call times worked: call them back / leave a voicemail
        who = name or ("Bidder #" + str(p.get("bidder") or ""))
        link = "https://portal.annelabes.com/#client=" + urllib.parse.quote(str(p.get("bidder") or ""))
        phone = str(p.get("phone") or "")
        body = (f"<p><b>{esc(who)}</b> couldn't make any of the call times and asked us to call them back.</p>"
                f"<p>Phone: <a href='tel:{esc(phone, quote=True)}'>{esc(phone)}</a></p>"
                + (f"<p>Good time to reach them: “{esc(p['note'])}”</p>" if p.get("note") else "")
                + "<p>Call when you can, or leave a voicemail. Then click <b>✓ Talked</b> or <b>Left voicemail</b> in the portal's call list.</p>"
                + _button(link, "Open the client's file"))
        text = f"{who} asked for a call back at {phone}." + (f" Good time: {p['note']}." if p.get("note") else "") + " Client file: " + link
        return f"📞 Please call back: {who}", _wrap(body), text, []
    when = _when(p["slot_at"])
    if kind == "call_booked_staff":
        who = name or ("Bidder #" + str(p.get("bidder") or ""))
        link = "https://portal.annelabes.com/#client=" + urllib.parse.quote(str(p.get("bidder") or ""))
        desc = f"Call {who} at {p.get('phone') or '(no phone)'}.\n" + (f"About: {p['note']}\n" if p.get("note") else "") + "Client file: " + link
        subject = f"📞 Client call: {who} — {when}"
        body = (f"<p><b>{esc(who)}</b> booked a {int(p.get('minutes') or 10)}-minute call from the client portal.</p>"
                f"<p>When: <b>{esc(when)}</b><br>Phone: <a href='tel:{esc(str(p.get('phone') or ''), quote=True)}'>{esc(str(p.get('phone') or ''))}</a></p>"
                + (f"<p>About: “{esc(p['note'])}”</p>" if p.get("note") else "")
                + _button(link, "Open the client's file") + "<p style='color:#6b7280;font-size:13px'>The calendar invite is attached.</p>")
        return subject, _wrap(body), desc, [{"filename": "client-call.ics", "content": base64.b64encode(_ics(p, "Call " + who, desc).encode()).decode(),
                                              "content_type": "text/calendar"}]
    first = name.split()[0] if name else ""
    subject = "Your call with our office — " + when
    body = (f"<p>{('Hi ' + esc(first) + ',') if first else 'Hello,'}</p>"
            f"<p>Thank you for scheduling a call. We'll call you on <b>{esc(when)}</b>"
            + (f" at <b>{esc(str(p['phone']))}</b>" if p.get("phone") else "") + ". It takes about 10 minutes, and we'll have your file open.</p>"
            "<p>If you need to change the time, you can cancel it on your portal page and pick a new one, or just reply to this email.</p>")
    text = f"Thank you for scheduling a call. We'll call you on {when}. To change it, cancel it on your portal page or reply to this email."
    return subject, _wrap(body), text, [{"filename": "call.ics", "content": base64.b64encode(_ics(p, "Call with Anne Labes' office", text).encode()).decode(),
                                          "content_type": "text/calendar"}]


def send_call(kind, p):
    to = (p.get("to") or "").strip()
    if not to:
        return "skipped", "no email address"
    subject, body, text, attachments = build_call_email(kind, p)
    return _resend(to, subject, body, text, attachments, bcc=(kind == "call_booked_client"))


# 📅 Client calls onto Marci's Gold Standard calendar (the real estate app on Render). Waits until
# PORTAL_INTEGRATION_SECRET is set here and in Gold Standard (same value); until then the rows just wait in line.
_GS_PAUSE = [0.0]


def send_gs_call(p):
    url = _env("GS_URL", "https://real-estate-app-hr1c.onrender.com").rstrip("/") + "/integrations/portal-call"
    req = urllib.request.Request(url, data=json.dumps(p).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "X-Portal-Secret": _env("PORTAL_INTEGRATION_SECRET"),
                                          "User-Agent": "annelabes-portal/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read() or b"{}")
        return "sent", f"{p.get('action')} → {out.get('calendar_of') or ('removed' if out.get('removed') else 'ok')}"
    except urllib.error.HTTPError as ex:
        err = ex.read().decode("utf-8", "replace")[:200]
        # Gold Standard asleep / restarting, or not configured yet: try again later
        return ("queued" if ex.code in (429, 500, 502, 503, 504) else "failed"), f"HTTP {ex.code}: {err}"
    except urllib.error.URLError as ex:
        return "queued", f"unreachable: {ex.reason}"


def mailer_loop():
    have = lambda k: "yes" if _env(k) else "NO"
    print("[mailer] started (" + ("LIVE" if _env("ENG_LIVE") == "1" else "TEST MODE") + ") — keys: resend " + have("RESEND_API_KEY")
          + ", square " + have("SQUARE_ACCESS_TOKEN") + ", square location " + have("SQUARE_LOCATION_ID")
          + ", square webhook " + have("SQUARE_WEBHOOK_SIGNATURE_KEY"), flush=True)
    while True:
        try:
            if not _env("SUPABASE_SECRET_KEY") or not (_env("RESEND_API_KEY") or _env("SQUARE_ACCESS_TOKEN")):
                time.sleep(60); continue
            kinds = (KINDS + ["paylink_email"] if _env("RESEND_API_KEY") else []) + (["make_paylink"] if _env("SQUARE_ACCESS_TOKEN") else [])
            job = _rpc("engagement_outbox_next", {"p_kinds": kinds})
            if not job and _env("PORTAL_INTEGRATION_SECRET") and time.time() >= _GS_PAUSE[0]:
                g = _rpc("mail_outbox_next", {"p_kinds": ["gs_call"]})
                if g:
                    try:
                        status, detail = send_gs_call(g["payload"] or {})
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("mail_outbox_done", {"p_id": g["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] gs_call #{g['id']}: {status} {detail[:120]}", flush=True)
                    if status == "queued":
                        _GS_PAUSE[0] = time.time() + 300   # Gold Standard not answering: let the emails go on, retry in 5 min
                    else:
                        time.sleep(2); continue
            if not job and _env("RESEND_API_KEY"):
                m = _rpc("mail_outbox_next", {"p_kinds": ["call_booked_staff", "call_booked_client", "call_callback_staff"]})
                if m:
                    try:
                        status, detail = send_call(m["kind"], m["payload"] or {})
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("mail_outbox_done", {"p_id": m["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] {m['kind']} #{m['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2 if status != "queued" else 60); continue
            if not job:
                time.sleep(20); continue
            try:
                if job["kind"] == "make_paylink":
                    status, detail = make_paylink(job["id"], job["engagement"])
                else:
                    status, detail = send(job["kind"], job["engagement"])
            except Exception as ex:
                status, detail = "failed", str(ex)[:300]
            _rpc("engagement_outbox_done", {"p_id": job["id"], "p_status": status, "p_detail": detail})
            print(f"[mailer] {job['kind']} #{job['id']}: {status} {detail[:120]}", flush=True)
            time.sleep(2 if status != "queued" else 60)
        except Exception as ex:
            print(f"[mailer] error: {ex}", flush=True)
            time.sleep(60)
