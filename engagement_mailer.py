"""📨 Client agreement emails (portal.annelabes.com) — sent through Resend from marci@annelabes.com.

A background thread takes one queued email at a time from the database (engagement_outbox_next), sends it
and reports back (engagement_outbox_done):
  agreement_email   — "please review and sign" with the client's private signing link
  signed_copy_email — after signing: their signed agreement as a PDF, and what happens next
Texts and Square payment links are not handled here yet; those rows stay queued.

Render environment:
  RESEND_API_KEY   — needed; without it the thread only waits (nothing is sent)
  ENG_LIVE=1       — send to the real clients. Anything else = TEST MODE: every email goes only to
                     ENG_TEST_TO (default ari@eqoppa.com), with the client's address in the subject.
  ENG_FROM         — default "Marci at Anne Labes, Esq. <marci@annelabes.com>"
  ENG_REPLY_TO     — default marci@annelabes.com
"""
import base64, html, json, os, time, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

SB_URL = "https://uhunhyfgwvoknqnkzlmr.supabase.co"
KINDS = ["agreement_email", "signed_copy_email"]
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
                "<p>After you sign, you'll get a secure link to pay. We start the title work as soon as the fee is paid.</p>")
        text = f"Hi {first},\n\nPlease review and sign your agreement ({n} lien(s), {total}):\n{e['link']}\n\nMarci · Office of Anne Labes, Esq."
        return subject, _wrap(body), text, None
    if kind == "signed_copy_email":
        subject = "Your signed agreement — Anne Labes, Esq."
        if e.get("pay_by") == "bill_later" or e.get("status") == "bill_later":
            nxt = "<p>We're starting your title work now.</p>"
        elif e.get("pay_by") == "check":
            nxt = f"<p><b>Next step:</b> please mail your check for <b>{total}</b>. We start as soon as it arrives.</p>"
        else:
            nxt = (f"<p><b>Next step:</b> payment of <b>{total}</b>. Your secure payment link will appear on your agreement page "
                   f"(and we'll send it to you). We start the title work as soon as it's paid.</p>" + _button(e["link"], "Open my agreement page"))
        body = (f"<p>Hi {html.escape(first)},</p><p>Thank you for signing. Your signed agreement is attached as a PDF for your records.</p>" + nxt)
        text = f"Hi {first},\n\nThank you for signing. Your signed agreement is attached.\n{e['link']}\n\nMarci · Office of Anne Labes, Esq."
        pdf = signed_pdf(e)
        safe = "".join(c for c in (name or "client") if c.isalnum() or c in " -_").strip().replace(" ", "-") or "client"
        return subject, _wrap(body), text, [{"filename": f"Signed-Agreement-{safe}.pdf", "content": base64.b64encode(pdf).decode()}]
    raise ValueError("unknown kind " + kind)


def send(kind, e):
    to = (e.get("email") or "").strip()
    if not to:
        return "skipped", "no email address"
    subject, body, text, attachments = build_email(kind, e)
    live = _env("ENG_LIVE") == "1"
    rcpt = [to] if live else [x.strip() for x in _env("ENG_TEST_TO", "ari@eqoppa.com").split(",") if x.strip()]
    if not live:
        subject = f"[TEST → {to}] " + subject
    msg = {"from": _env("ENG_FROM", "Marci at Anne Labes, Esq. <marci@annelabes.com>"), "to": rcpt,
           "reply_to": _env("ENG_REPLY_TO", "marci@annelabes.com"), "subject": subject, "html": body, "text": text}
    if attachments:
        msg["attachments"] = attachments
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


def mailer_loop():
    print("[mailer] started (" + ("LIVE" if _env("ENG_LIVE") == "1" else "TEST MODE") + ")", flush=True)
    while True:
        try:
            if not _env("RESEND_API_KEY") or not _env("SUPABASE_SECRET_KEY"):
                time.sleep(60); continue
            job = _rpc("engagement_outbox_next", {"p_kinds": KINDS})
            if not job:
                time.sleep(20); continue
            try:
                status, detail = send(job["kind"], job["engagement"])
            except Exception as ex:
                status, detail = "failed", str(ex)[:300]
            _rpc("engagement_outbox_done", {"p_id": job["id"], "p_status": status, "p_detail": detail})
            print(f"[mailer] {job['kind']} #{job['id']}: {status} {detail[:120]}", flush=True)
            time.sleep(2 if status != "queued" else 60)
        except Exception as ex:
            print(f"[mailer] error: {ex}", flush=True)
            time.sleep(60)
