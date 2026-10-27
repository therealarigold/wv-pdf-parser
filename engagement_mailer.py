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
  ENG_FROM         — default "Marci at <ENG_BRAND> <marci@annelabes.com>"
  ENG_REPLY_TO     — default marci@annelabes.com
  ENG_BRAND        — the firm's name on every client email, default "Labes Legal Services LLC" (Ari 10/27; was "Anne Labes, Esq.")
  WEB_SITE_URL     — the public website (FAQ link), default https://www.annelabes.com
"""
import base64, hashlib, hmac, html, json, os, re, threading, time, urllib.request, urllib.error, urllib.parse
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
def signed_html(e, subtitle="WV Tax Lien Title Services"):
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
    if e.get("esign_consent_at"):        # ✍️ the consent step before signing (Ari 10/17)
        rec.insert(2, ("Agreed to sign electronically", _et(e.get("esign_consent_at"))
                       + (" · IP " + esc(e.get("esign_consent_ip")) if e.get("esign_consent_ip") else "")))
    if e.get("doc_hash"):                # 🔒 the document fingerprint (Ari 10/17)
        rec.append(("Document fingerprint (SHA-256)", "<span style='font-family:monospace;word-break:break-all'>" + esc(e.get("doc_hash")) + "</span>"))
    consent = ("<div class='rec'><h4>Consent to use electronic records and signatures (as the client saw it)</h4><div style='white-space:pre-wrap'>"
               + esc(e.get("esign_consent_text")) + "</div></div>") if e.get("esign_consent_text") else ""
    css = ("@page{size:letter;margin:0.8in 0.9in}body{font:11.5pt/1.5 Georgia,'Times New Roman',serif;color:#111;max-width:7in;margin:0 auto}"
           ".lh{text-align:center;border-bottom:2px solid #1e3a8a;padding-bottom:10px;margin-bottom:22px}.lh .n{font-size:20pt;letter-spacing:.5px;color:#1e3a8a}"
           ".lh .s{font:9.5pt Arial,sans-serif;color:#555;margin-top:2px}p{margin:0 0 10px;text-align:justify}h3{font-size:12pt;margin:16px 0 6px}"
           ".sign{margin:14px 0;break-inside:avoid}.sig{width:260px;height:auto;display:block;border-bottom:1px solid #333;margin:2px 0 4px}"
           ".info{border:1px solid #bbb;border-radius:6px;padding:10px 14px;margin:14px 0;break-inside:avoid;background:#fafafa}"
           ".rec{font:9pt/1.45 Arial,sans-serif;color:#333;border:1px solid #1e3a8a;border-radius:6px;padding:10px 14px;margin-top:22px;break-inside:avoid}"
           ".rec h4{margin:0 0 6px;font-size:10pt;color:#1e3a8a}.rec td{padding:1px 10px 1px 0;vertical-align:top}.rec td:first-child{color:#666;white-space:nowrap}")
    return ("<!DOCTYPE html><html><head><meta charset='utf-8'><title>Signed agreement — " + esc(f.get("client_name")) + "</title><style>" + css
            + "</style></head><body><div class='lh'><div class='n'>" + esc(_brand()) + "</div><div class='s'>" + esc(subtitle) + " · anne@annelabes.com</div></div>"
            + "".join(parts)
            + "<div class='rec'><h4>Electronic signature record</h4><table>" + "".join("<tr><td>" + a + "</td><td>" + b + "</td></tr>" for a, b in rec) + "</table>"
            + "<div style='margin-top:6px;color:#666'>Signed electronically on portal.annelabes.com. The client typed their name, drew their signature and ticked "
            + "“I have read this agreement and I agree to it.” The text above is exactly what the client saw and signed."
            + (" The document fingerprint is a code made from everything signed (text, name, signature, time, IP address, device, consent); "
               "if even one letter were changed later, the code would no longer match." if e.get("doc_hash") else "")
            + "</div></div>" + consent + "</body></html>")


def signed_pdf(e, subtitle="WV Tax Lien Title Services"):
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        b = p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            pg = b.new_page()
            pg.set_content(signed_html(e, subtitle), wait_until="load")
            return pg.pdf(format="Letter", print_background=True, margin={"top": "0.8in", "bottom": "0.8in", "left": "0.9in", "right": "0.9in"})
        finally:
            b.close()


# ── the emails ────────────────────────────────────────────────────────────────────────────────
_SURPLUS_BRAND = "Anne Labes, Esq."      # surplus clients contract with Anne herself, not the LLC (Ari 10/27)
_brand_for = threading.local()


def _brand():
    """The firm's name on every client email (ENG_BRAND) - Labes Legal Services LLC since Ari 10/27 (was "Anne Labes, Esq.").
    Surplus emails (send_surplus) say "Anne Labes, Esq." - surplus is Anne's own practice, not the title-search LLC."""
    return getattr(_brand_for, "name", None) or _env("ENG_BRAND", "Labes Legal Services LLC")


def _wrap(inner):
    b = html.escape(_brand())
    return ("<div style=\"font:15px/1.55 -apple-system,'Segoe UI',Arial,sans-serif;color:#1f2937;max-width:560px\">"
            "<div style='font:20px Georgia,serif;color:#1e3a8a;border-bottom:2px solid #1e3a8a;padding-bottom:6px;margin-bottom:16px'>" + b + "</div>"
            + inner +
            "<p style='color:#6b7280;font-size:13px;margin-top:24px'>Marci · " + b + "<br>WV Tax Lien Title Services · reply to this email with any questions</p></div>")


def _button(url, label):
    return ("<p style='margin:22px 0'><a href='" + html.escape(url, quote=True) + "' style='background:#166534;color:#fff;text-decoration:none;"
            "font-weight:700;padding:12px 22px;border-radius:8px;display:inline-block'>" + html.escape(label) + "</a></p>")


# ── 📦 Welcome Packet look (Ari 10/19 polish, approved mockup): bigger type for older readers, one letterhead for the packet
# and "You're all set", phone in every footer. Used only by those two client emails and the office "Paid" heads-up.
_NAVY, _GREEN, _MUTED = "#1e3a8a", "#166534", "#4b5563"
_PHONE, _TEL = "(304) 615-8978", "+13046158978"
_PORTAL_LOGIN = "https://portal.annelabes.com/#client-login"
_ABOUT_US = ("We're a small West Virginia law office in Parkersburg. Real people, real answers &mdash; call any time and "
             "you'll reach someone who knows your file.")        # option B (Ari 10/19) - Anne still to approve the wording


def _letter(inner):
    b = html.escape(_brand())
    return (f"<div style=\"font:17px/1.6 -apple-system,'Segoe UI',Arial,sans-serif;color:#1f2937;max-width:580px\">"
            f"<div style='font:22px Georgia,serif;color:{_NAVY};border-bottom:2px solid {_NAVY};padding-bottom:6px;margin-bottom:18px'>{b}</div>"
            + inner +
            f"<p style='color:#6b7280;font-size:14px;margin-top:26px;border-top:1px solid #e5e7eb;padding-top:12px'>"
            f"Marci &middot; {b} &middot; Parkersburg, West Virginia<br>"
            f"Call <a href='tel:{_TEL}' style='color:#6b7280'>{_PHONE}</a> or just reply to this email.</p></div>")


def _big_button(url, label, color=_GREEN):
    return (f"<p style='margin:18px 0'><a href='{html.escape(url, quote=True)}' style='background:{color};color:#fff;text-decoration:none;"
            f"font-weight:700;font-size:17px;padding:14px 26px;border-radius:10px;display:inline-block'>{html.escape(label)}</a></p>")


def _box(title, inner):
    return (f"<div style='background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:14px 18px;margin:16px 0'>"
            f"<div style='font-weight:700;color:{_NAVY};font-size:17px;margin-bottom:6px'>{title}</div>{inner}</div>")


def _faq(anchor, label):
    site = _env("WEB_SITE_URL", "https://www.annelabes.com").rstrip("/")
    return f"<a href='{html.escape(site, quote=True)}/faq.html{'#' + anchor if anchor else ''}' style='color:{_NAVY}'>{label}</a>"


def _signin_steps(bidder, password_line):
    """3 numbered sign-in steps + locked out + iPhone tip + FAQ links (older readers - Ari 10/19)."""
    bid = html.escape(str(bidder or ""))
    return (f"<ol style='margin:4px 0 0 22px;padding:0;font-size:17px'>"
            f"<li style='margin-bottom:6px'>Go to <a href='{_PORTAL_LOGIN}' style='color:{_NAVY};font-weight:700'>portal.annelabes.com</a></li>"
            f"<li style='margin-bottom:6px'><b>Username:</b> your bidder number" + (f", <b style='font-size:19px'>{bid}</b>" if bid else "") + "</li>"
            f"<li style='margin-bottom:6px'><b>Password:</b> {password_line}</li></ol>"
            f"<div style='font-size:15px;color:{_MUTED};margin-top:10px'>"
            f"&#128274; <b>Locked out?</b> Wait 15 minutes and try again, or call us at <a href='tel:{_TEL}' style='color:{_MUTED}'>{_PHONE}</a>.<br>"
            f"&#128241; <b>On an iPhone?</b> Open the portal in Safari, tap <b>Share</b>, then <b>Add to Home Screen</b> &mdash; it becomes an app icon.<br>"
            f"Help: {_faq('sign-in', 'How do I sign in?')} &middot; {_faq('forgot-password', 'Forgot my password')} &middot; "
            f"{_faq('home-screen', 'Add it to my phone')}</div>")


def _questions():
    return _box("Questions?", f"Most answers are on our {_faq('', 'questions &amp; answers page')}. Or call us at "
                              f"<a href='tel:{_TEL}' style='color:{_NAVY};font-weight:700'>{_PHONE}</a> &mdash; a real person answers.")


def _cert_list(certs):
    li = "".join(f"<li>{html.escape(c)}</li>" for c in certs[:60])
    return f"<ul style='margin:6px 0 0 20px;padding:0;color:{_MUTED};font-size:15px'>{li}</ul>" if li else ""


def _first_name(name):
    """'Mary Smith' -> 'Mary'; names written 'SMITH, MARY' -> 'Mary'; companies -> '' (then 'Hello,')."""
    name = (name or "").strip()
    if not name or any(w in name.upper() for w in (" LLC", " INC", " CORP", "TRUST", "COMPANY", " LTD", " LP", "ENTERPRISES")):
        return ""
    if "," in name:
        rest = name.split(",", 1)[1].strip()
        name = rest or name
    w = name.split()[0]
    return w.title() if w.isupper() else w


def thanks_email(job):
    """📦 Email 2 "You're all set" (client_login_outbox 'thanks'): payment received + receipt (card) or the check's date +
    the choose-your-password button for a new client + the sign-in steps. Never BCC'd (it can hold the password link)."""
    first = _first_name(job.get("name"))
    try: amt = f"${float(job.get('amount')):,.2f}"
    except Exception: amt = ""
    certs = [str(c).strip().title() for c in (job.get("certs") or []) if c and str(c).strip()]
    n = len(certs) or int(job.get("liens") or 0)
    method = str(job.get("method") or "")
    receipt = str(job.get("receipt_url") or "")
    if re.search(r"check", method, re.I):
        paid = f"We received your check for <b>{amt}</b>" + (f" on {html.escape(job['paid_on'])}" if job.get("paid_on") else "") + "."
    else:
        paid = (f"We received your payment{' of <b>' + amt + '</b>' if amt else ''}" + (" by card" if re.search(r"card|square", method, re.I) else "")
                + (f" &mdash; <a href='{html.escape(receipt, quote=True)}' style='color:{_NAVY}'>view or print your receipt</a>." if receipt.startswith("https://") else "."))
    if job.get("token"):
        link = f"https://portal.annelabes.com/set-password.html?t={job['token']}"
        portal = _box("Set up your client portal (one time)",
                      "Tap the button to choose your password:" + _big_button(link, "Choose your password")
                      + f"<div style='font-size:14px;color:{_MUTED};margin-bottom:10px'>This button works once and is good for 7 days. "
                        "If it expires, call us and we'll send a new one.</div>"
                      + "After that, signing in is 3 steps:" + _signin_steps(job.get("bidder"), "the one you just chose"))
    elif job.get("portal_account"):
        portal = _box("Your client portal", "Follow the progress any time:" + _big_button(_PORTAL_LOGIN, "Open my client portal", _NAVY)
                      + _signin_steps(job.get("bidder"), "the one you already use"))
    else:
        portal = _box("Your client portal", "Our office will email you your client portal access shortly &mdash; there you can follow every step.")
    body = (f"<p>Hi{' ' + html.escape(first) if first else ''},</p>"
            f"<p style='font-size:19px'><b>You're all set!</b> &#10003;</p>"
            f"<p>{paid} Our title work has started" + (f" on {n} certificate{'' if n == 1 else 's'}" if n else "") + (":" if certs else ".") + "</p>"
            + _cert_list(certs) + portal + _questions())
    return f"You're all set — payment received | {_brand()}", _letter(body)


def office_paid_email(job, client_email):
    """💰 Heads-up to the office when a client pays (sent right after their "You're all set"). NEVER the password link."""
    name = (job.get("name") or "").strip() or "A client"
    bid = str(job.get("bidder") or "")
    try: amt = f"${float(job.get('amount')):,.2f}"
    except Exception: amt = "?"
    certs = [str(c).strip().title() for c in (job.get("certs") or []) if c and str(c).strip()]
    n = len(certs) or int(job.get("liens") or 0)
    method = str(job.get("method") or "").strip()
    receipt = str(job.get("receipt_url") or "")
    went = ("new client, so it included the choose-your-password button (not copied here, for the client's security)" if job.get("token")
            else "they already have a portal password" if job.get("portal_account")
            else "no portal login in it - the office sends the client portal access")
    body = (f"<p><b>{html.escape(name)}</b> (bidder {html.escape(bid)}) paid <b>{amt}</b>" + (f" by {html.escape(method)}" if method else "") + ".</p>"
            + (f"<p>Title work started on: {html.escape(', '.join(certs))}.</p>" if certs else "")
            + f"<p>&#9989; The <i>You're all set</i> email went to {html.escape(client_email or '?')} &mdash; {went}.</p>"
            + (f"<p>Receipt: <a href='{html.escape(receipt, quote=True)}'>Square receipt</a></p>" if receipt.startswith("https://") else "")
            + _big_button("https://portal.annelabes.com/#client=" + urllib.parse.quote(bid), f"Open {name.split()[0] if name.split() else 'the'}'s file", _NAVY))
    html_ = (f"<div style=\"font:15px/1.55 -apple-system,'Segoe UI',Arial,sans-serif;color:#1f2937;max-width:580px\">{body}</div>")
    subject = f"💰 Paid: {name} — {amt} ({n} certificate{'' if n == 1 else 's'})"
    text = (f"{name} (bidder {bid}) paid {amt}" + (f" by {method}" if method else "") + ".\n"
            + (f"Title work started on: {', '.join(certs)}.\n" if certs else "")
            + f"The You're all set email went to {client_email or '?'} - {went}.\n"
            + (f"Receipt: {receipt}\n" if receipt.startswith("https://") else "")
            + f"Open the file: https://portal.annelabes.com/#client={bid}\n")
    return subject, html_, text


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
        text = f"Hi {first},\n\nPlease review and sign your agreement ({n} lien(s), {total}):\n{e['link']}\n\nMarci · {_brand()}"
        return subject, _wrap(body), text, None
    if kind == "signed_copy_email":
        # 📦 THE WELCOME PACKET (Ari 10/19 - 4 emails became 2; polished 10/19 to the approved mockup): the signed agreement +
        # the Pay button / how to pay + what happens next + the client portal (3 big sign-in steps) + FAQ + about us.
        # Card payers get it once the Square link exists.
        site = _env("WEB_SITE_URL", "https://www.annelabes.com").rstrip("/")
        subject = "Welcome — your signed agreement and next steps | " + _brand()
        hi = _first_name(name)
        certs = [c.strip() for c in (f.get("certificates") or "").replace(";", ",").split(",") if c.strip()]
        order = _box("Your order", f"{n} certificate{'' if n == 1 else 's'} &times; $500 = <b>{total}</b>" + _cert_list(certs))
        bill_later = e.get("pay_by") == "bill_later" or e.get("status") == "bill_later"
        if bill_later:
            pay = _box("Payment", "Nothing to pay right now &mdash; <b>we're starting your title work today.</b>")
        elif e.get("pay_by") == "check":
            pay = _box("Payment", f"Please send your payment of <b>{total}</b> by check, or as arranged with our office. "
                                  f"We start your title work as soon as it arrives. {_faq('pay', 'Where do I send it?')}")
        elif e.get("pay_link_url"):
            pay = _box("Last step: payment", "We start your title work as soon as your payment arrives."
                       + _big_button(e["pay_link_url"], "Pay " + total + " securely")
                       + f"<div style='font-size:15px;color:{_MUTED}'>Payment is handled by Square. Prefer to pay by check? Just reply to this email. "
                         f"More: {_faq('pay', 'How can I pay?')}</div>")
        else:
            pay = _box("Last step: payment", f"We start your title work as soon as the fee of <b>{total}</b> is paid. "
                       "Your secure payment link is on your agreement page." + _big_button(e["link"], "Open my agreement page"))
        nxt = _box("What happens next",
                   "<ol style='margin:4px 0 0 22px;padding:0'><li>We search the county records for each certificate (the title search).</li>"
                   "<li>We prepare your Notice to Redeem and file it with the State Auditor.</li>"
                   "<li>You can follow every step in your client portal &mdash; no need to call.</li></ol>")
        bid = e.get("bidder")
        if e.get("portal_account"):
            portal = _box("Your client portal", "You already have an account. Sign in any time:" + _signin_steps(bid, "the one you already use")
                          + _big_button(_PORTAL_LOGIN, "Open my client portal", _NAVY))
        elif e.get("hold_login"):
            portal = _box("Your client portal", "Our office will email you your client portal access shortly.")
        elif bill_later:
            portal = _box("Your client portal", "We'll email you a green <b>Choose your password</b> button shortly. Then signing in is 3 steps:"
                          + _signin_steps(bid, "the one you choose from that email"))
        else:
            portal = _box("Your client portal", "Once your payment arrives, we'll email you a green <b>Choose your password</b> button. "
                          "Signing in is then 3 steps:" + _signin_steps(bid, "the one you choose from that email"))
        about = f"<p style='font-size:15px;color:{_MUTED};margin-top:18px'><b>About us:</b> {_ABOUT_US}</p>"
        body = (f"<p>Hi{' ' + html.escape(hi) if hi else ''},</p><p>Thank you for choosing us. Your signed agreement is attached as a PDF for your records.</p>"
                + order + pay + nxt + portal + _questions() + about)
        text = (f"Hi{' ' + hi if hi else ''},\n\nThank you for choosing us. Your signed agreement is attached.\n"
                + (f"Pay {total} securely: {e['pay_link_url']}\n" if e.get("pay_link_url") and e.get("pay_by") not in ("check", "bill_later") and not bill_later else "")
                + f"Your agreement page: {e['link']}\nClient portal: portal.annelabes.com - username: your bidder number{(' ' + str(bid)) if bid else ''}\n"
                + f"Questions & answers: {site}/faq.html\n\nMarci · {_brand()} · Parkersburg, West Virginia · {_PHONE}")
        pdf = signed_pdf(e)
        safe = "".join(c for c in (name or "client") if c.isalnum() or c in " -_").strip().replace(" ", "-") or "client"
        return subject, _letter(body), text, [{"filename": f"Signed-Agreement-{safe}.pdf", "content": base64.b64encode(pdf).decode()}]
    if kind == "paylink_email":
        url = e.get("pay_link_url") or e["link"]
        subject = "Payment for your title work — " + total
        body = (f"<p>Hi {html.escape(first)},</p><p>Thank you for signing. The last step is the fee for your title work: "
                f"<b>{n} × $500 = {total}</b>.</p><p><b>Liens:</b> {liens}</p>"
                + _button(url, "Pay " + total + " securely") +
                "<p>Payment is handled by Square. We start the title work as soon as it's paid (the agreement asks for payment "
                "within 7 days of signing). Prefer to pay by check? Just reply to this email.</p>")
        text = f"Hi {first},\n\nPlease pay {total} for your title work here:\n{url}\n\nMarci · {_brand()}"
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
            "payment_note": f"Labes Legal Services agreement {e['id']} · bidder {f.get('bidder', '')}"[:500]}
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
    if p.get("receipt_url"):        # the "You're all set" email links the Square receipt (Ari 10/19)
        try: _rpc("engagement_set_receipt", {"p_id": eng["id"], "p_url": p["receipt_url"]})
        except Exception as ex: print("[square] receipt not kept: " + str(ex)[:120], flush=True)
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


def _resend(to, subject, body, text, attachments=None, bcc=False, reply_to=None, always_bcc=None):
    live = _env("ENG_LIVE") == "1"
    rcpt = [to] if live else [x.strip() for x in _env("ENG_TEST_TO", "ari@eqoppa.com").split(",") if x.strip()]
    if not live:
        subject = f"[TEST → {to}] " + subject
    sender = ("Marci at " + _brand() + " <marci@annelabes.com>") if getattr(_brand_for, "name", None) else _env("ENG_FROM", "Marci at " + _brand() + " <marci@annelabes.com>")
    msg = {"from": sender, "to": rcpt,
           "reply_to": reply_to or _env("ENG_REPLY_TO", "marci@annelabes.com"), "subject": subject, "html": body, "text": text}
    if attachments:
        msg["attachments"] = attachments
    if bcc and live and _office_bcc(to):
        msg["bcc"] = _office_bcc(to)
    if live and always_bcc:                    # e.g. Marci on every letter to the Auditor (Ari 10/19), whatever ENG_BCC says
        msg["bcc"] = list(dict.fromkeys((msg.get("bcc") or []) + [x for x in always_bcc if x.lower() != to.lower()]))
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
    return "\r\n".join(["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Labes Legal Services//Client calls//EN", "METHOD:PUBLISH",
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
    return subject, _wrap(body), text, [{"filename": "call.ics", "content": base64.b64encode(_ics(p, "Call with Labes Legal Services", text).encode()).decode(),
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


def send_gs_call(p, path="/integrations/portal-call"):
    url = _env("GS_URL", "https://real-estate-app-hr1c.onrender.com").rstrip("/") + path
    req = urllib.request.Request(url, data=json.dumps(p).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "X-Portal-Secret": _env("PORTAL_INTEGRATION_SECRET"),
                                          "User-Agent": "annelabes-portal/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.loads(r.read() or b"{}")
        if path.endswith("/portal-diego") and out.get("status"):
            # Diego checks every staff message first (Gold Standard, Ari 10/21): sent / ari (waits for Ari's Yes) / held
            return "sent", f"diego: {out.get('status')}" + (f" - {str(out.get('why'))[:150]}" if out.get("why") else "")
        return "sent", f"{p.get('action')} → {out.get('calendar_of') or ('removed' if out.get('removed') else 'ok')}"
    except urllib.error.HTTPError as ex:
        err = ex.read().decode("utf-8", "replace")[:200]
        # Gold Standard asleep / restarting, or not configured yet: try again later
        return ("queued" if ex.code in (429, 500, 502, 503, 504) else "failed"), f"HTTP {ex.code}: {err}"
    except urllib.error.URLError as ex:
        return "queued", f"unreachable: {ex.reason}"


# 📧 NTR packets to the client (Ari 2026-10-06): one email per client for the certificates staff chose; each
# certificate's NTR + letter + title-search notes joined into one PDF; the text was made by the database
# (_ntr_packet_text, also shown as the preview before Send). Office bcc as for the agreements.
def send_ntr_packet(oid, p):
    import io
    from pypdf import PdfReader, PdfWriter
    to = (p.get("to") or "").strip()
    if not to:
        return "failed", "no email address"
    files = _rpc("ntr_packet_files", {"p_outbox": oid}) or []
    if not files:
        return "failed", "no packets found for this email"
    # Ari 10/17: the NTR stays on our server - the email says to log in to the client portal and download it. Only an
    # email made before that change (its text still says "Attached") gets the PDFs.
    if "Attached" not in (p.get("text") or ""):
        return _resend(to, p.get("subject") or "Your tax lien packet", p.get("html") or "", p.get("text") or "", None, bcc=True)
    atts = []
    for f in files:
        w = PdfWriter()
        for part in (f.get("ntr"), f.get("letter")):
            for page in PdfReader(io.BytesIO(base64.b64decode(part))).pages:
                w.add_page(page)
        buf = io.BytesIO(); w.write(buf)
        atts.append({"filename": f["name"], "content": base64.b64encode(buf.getvalue()).decode()})
    return _resend(to, p.get("subject") or "Your tax lien packet", p.get("html") or "", p.get("text") or "", atts, bcc=True)


# 📬 NTR packet reminder (Ari 2026-10-17): every 2 days while a packet waits to be mailed, until the client presses
# "I mailed it" in the portal (database ntr_reminder_scan). One email per client for all of their waiting packets; no
# attachment (the packet came with the first email and is in the portal).
def build_ntr_reminder(p):
    certs = p.get("certs") or []
    name = (p.get("name") or "").strip()
    first = name.split(" ")[0].title() if name and not any(w in name.upper() for w in ("LLC", "INC", "CORP", "TRUST", "COMPANY", "LTD")) else ""
    hi = ("Hi " + html.escape(first) + ",") if first else "Hello,"
    n = len(certs)
    if n == 1:
        c = certs[0]
        subject = "Reminder: your NTR packet for " + c.get("county", "") + " " + c.get("cert", "") + " is waiting to be mailed"
    else:
        subject = "Reminder: " + str(n) + " NTR packets are waiting to be mailed"
    rows = []
    for c in certs:
        days = c.get("days_left")
        left = (" (" + str(days) + " day" + ("" if days == 1 else "s") + " left)") if isinstance(days, int) else ""
        rows.append("<li style='margin:4px 0'><b>" + html.escape(str(c.get("county", ""))) + " County, Certificate " + html.escape(str(c.get("cert", "")))
                    + "</b><br>Check to the WVSAO: <b>" + _money(c.get("total")) + "</b> · last day to file: "
                    + html.escape(str(c.get("deadline", ""))) + html.escape(left) + "</li>")
    checks = "one check to the WVSAO" if n == 1 else "a separate check to the WVSAO for each certificate (please do not combine them)"
    body = ("<p>" + hi + "</p>"
            "<p>This is a friendly reminder that your Notice to Redeem packet" + ("" if n == 1 else "s are") + (" is" if n == 1 else "")
            + " ready and still need" + ("s" if n == 1 else "") + " to be mailed to the State Auditor:</p>"
            "<ul style='padding-left:20px'>" + "".join(rows) + "</ul>"
            "<p style='margin-bottom:4px'><b>What to do:</b></p><ol style='margin-top:0;padding-left:20px'>"
            "<li>Log in to your client portal and download your packet.</li>"
            "<li>Print all of it.</li>"
            "<li>Write " + checks + ".</li>"
            "<li>Mail the packet and the check" + ("" if n == 1 else "s") + " together to: <b>WVSAO, 1900 Kanawha Blvd E, Building 1, Room W114, Charleston, WV 25305</b></li></ol>"
            "<p>Once it is in the mail, press <b>“I mailed it”</b> in your portal and these reminders stop.</p>"
            + _button(_env("PORTAL_URL", "https://portal.annelabes.com") + "/#client-login", "Log in to your portal") +
            "<p>Already mailed it, or would you like us to mail it for you? Just reply to this email or call us at (304) 615-8978.</p>")
    text = ("Reminder: your NTR packet(s) are ready and still need to be mailed to the State Auditor.\n"
            + "\n".join("- " + str(c.get("county", "")) + " County, Certificate " + str(c.get("cert", "")) + ": check " + _money(c.get("total"))
                         + ", last day to file " + str(c.get("deadline", "")) for c in certs)
            + "\n\n1. Log in to your portal (https://portal.annelabes.com/#client-login) and download your packet.\n2. Print all of it.\n3. Write "
            + checks + ".\n4. Mail the packet and the check(s) to: WVSAO, 1900 Kanawha Blvd E, Building 1, Room W114, Charleston, WV 25305.\n\n"
            "Once it is in the mail, press \"I mailed it\" in your portal and these reminders stop. Questions? Reply or call (304) 615-8978.")
    return ("[TEST] " if p.get("test") else "") + subject, _wrap(body), text


def send_ntr_reminder(p):
    to = (p.get("to") or "").strip()
    if not to:
        return "failed", "no email address"
    if not p.get("certs"):
        return "skipped", "no packets in the reminder"
    subject, body, text = build_ntr_reminder(p)
    return _resend(to, subject, body, text)


# 📣 "Your new client portal is ready" (Ari 2026-10-17, wording approved): one email per client (database portal_invite_scan,
# about 50 a day). Same login button for everyone; saved passwords keep working, "Forgot password?" for the rest.
def build_portal_invite(p):
    name = (p.get("name") or "").strip()
    company = any(w in name.upper() for w in ("LLC", "INC", "CORP", "TRUST", "COMPANY", "LTD", "BANK", "PARTNERS", "HOLDINGS"))
    first = name.split(" ")[0].title() if name and not company else ""
    if "," in name and not company:   # "SMITH, JOHN" -> John
        first = name.split(",")[1].strip().split(" ")[0].title()
    hi = ("Hi " + html.escape(first) + ",") if first else "Hello,"
    bidder = html.escape(str(p.get("bidder") or ""))
    login = _env("PORTAL_URL", "https://portal.annelabes.com") + "/#client-login"
    body = ("<p>" + hi + "</p>"
            "<p>We have built a new client portal for you, and we would love for you to try it.</p>"
            "<p style='margin-bottom:4px'>In your portal you can:</p><ul style='padding-left:20px;margin-top:6px'>"
            "<li style='margin:5px 0'><b>See where every certificate stands</b>, with a progress bar from payment all the way to the State Auditor</li>"
            "<li style='margin:5px 0'><b>Download your Notice to Redeem packets</b>, ready to print and mail</li>"
            "<li style='margin:5px 0'><b>See everything you bought</b> under your bidder number, and what changed since your last visit</li>"
            "<li style='margin:5px 0'><b>Ask us a question</b> any time</li>"
            "<li style='margin:5px 0'><b>Book a short phone call</b> at a time that works for you</li></ul>"
            + _button(login, "Log in to my portal") +
            "<p>Sign in with your bidder number (" + bidder + ") and your password. Don't have a password, or forgot it? On the sign-in page, "
            "press <b>“Forgot password?”</b> and we will email you a link to choose a new one.</p>"
            "<p style='color:#6b7280;font-size:13px'>Tip: on your phone, add the portal to your home screen so it opens with one tap. "
            "<a href='https://www.annelabes.com/faq.html#home-screen'>Here's how</a>.</p>"
            "<p>Questions? Just reply to this email or call us at (304) 615-8978.</p>"
            "<p>Thank you,<br>Marci<br>" + html.escape(_brand()) + "<br>(304) 615-8978</p>")
    text = ((first and "Hi " + first + "," or "Hello,") + "\n\nWe have built a new client portal for you, and we would love for you to try it.\n\n"
            "In your portal you can:\n- See where every certificate stands, with a progress bar from payment all the way to the State Auditor\n"
            "- Download your Notice to Redeem packets, ready to print and mail\n- See everything you bought under your bidder number, and what changed since your last visit\n"
            "- Ask us a question any time\n- Book a short phone call at a time that works for you\n\n"
            "Log in to your portal: " + login + "\n\nSign in with your bidder number (" + str(p.get("bidder") or "") + ") and your password. "
            "Don't have a password, or forgot it? On the sign-in page, press \"Forgot password?\" and we will email you a link to choose a new one.\n\n"
            "Questions? Just reply to this email or call us at (304) 615-8978.\n\nThank you,\nMarci\n" + _brand() + "\n(304) 615-8978")
    return ("[TEST] " if p.get("test") else "") + "Your new client portal is ready", _wrap(body), text


# 📱 Texts through Twilio (Ari 2026-10-17): the toll-free number TWILIO_FROM, an API key (TWILIO_API_KEY + TWILIO_API_SECRET,
# pasted into Render by Ari - never in the code). Who may get a text, the words and the 9 am - 8 pm window are decided by the
# database (sms_outbox). Twilio error 21610 = the person replied STOP -> the database turns their texting off.
def send_sms(to, body):
    to = (to or "").strip()
    if not re.match(r"^\+1\d{10}$", to) or not body:
        return "failed", None, "bad number or empty text", False
    url = "https://api.twilio.com/2010-04-01/Accounts/" + _env("TWILIO_ACCOUNT_SID") + "/Messages.json"
    data = urllib.parse.urlencode({"From": _env("TWILIO_FROM"), "To": to, "Body": body[:600]}).encode()
    auth = base64.b64encode((_env("TWILIO_API_KEY") + ":" + _env("TWILIO_API_SECRET")).encode()).decode()
    req = urllib.request.Request(url, data=data, method="POST", headers={"Authorization": "Basic " + auth,
                                                                         "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            out = json.loads(r.read() or b"{}")
        return "sent", out.get("sid"), "twilio " + str(out.get("status") or ""), False
    except urllib.error.HTTPError as ex:
        try:
            err = json.loads(ex.read() or b"{}")
        except Exception:
            err = {}
        code = err.get("code")
        if ex.code in (429, 500, 502, 503, 504):
            return "queued", None, "twilio busy " + str(ex.code), False
        return "failed", None, "twilio " + str(code) + ": " + str(err.get("message") or ex.code)[:200], code == 21610


# ✉️ A one-off note from the office to one client (mail_outbox office_email: subject + html + text made in the database or
# by Claude on Ari's word). Wrapped like the other client emails, from marci@annelabes.com, office bcc.
def send_office_email(p):
    to = (p.get("to") or "").strip()
    if not to or not p.get("subject") or not p.get("html"):
        return "failed", "to, subject and html are needed"
    return _resend(to, p["subject"], _wrap(p["html"]), p.get("text") or "", bcc=True)


def send_portal_invite(p):
    to = (p.get("to") or "").strip()
    if not to:
        return "failed", "no email address"
    subject, body, text = build_portal_invite(p)
    return _resend(to, subject, body, text)


# 🔄 Redeemed filing (Ari + Anne 2026-10-07): the verified title search letter + notes of a redeemed certificate, to the
# Auditor's title verification box (fz_config redeemed_filing_to; a test address until Ari switches it on). Office bcc.
def send_redeemed_filing(oid, p):
    to = (p.get("to") or "").strip()
    if not to:
        return "failed", "no email address"
    f = _rpc("redeemed_filing_file", {"p_outbox": oid}) or {}
    if not f.get("letter"):
        return "failed", "no letter found for this filing"
    atts = [{"filename": p.get("file") or "Title_Search.pdf", "content": f["letter"]}]
    return _resend(to, p.get("subject") or "Title search", p.get("html") or "", p.get("text") or "", atts, bcc=True,
                   always_bcc=["marci@annelabes.com"])     # Marci sees every letter Fernando sends (Ari 10/19)


# ── ✍️ Surplus agreements (Ari 10/17, mail_outbox): the link to sign, and once signed the PDF into the case's Files +
# the person's own copy. The office gets the hidden copy (bcc) like the title search agreement emails.
SURPLUS_SUB = "Surplus Funds Recovery"


def send_surplus(kind, oid, p):
    _brand_for.name = _SURPLUS_BRAND
    try:
        return _send_surplus(kind, oid, p)
    finally:
        _brand_for.name = None


def _send_surplus(kind, oid, p):
    e = _rpc("surplus_agreement_for_mail", {"p_id": p.get("id")}) or {}
    if not e.get("id"):
        return "failed", "agreement not found"
    f = e.get("fields") or {}
    name = (e.get("client_name") or "").strip()
    first = html.escape(name.split(" ")[0] if name else "there")
    where = html.escape(((f.get("county") or "") + " County").strip())
    to = (e.get("email") or "").strip()
    if not to:
        return "failed", "no email address"
    if kind == "surplus_agreement_email":
        if e.get("status") != "sent":
            return "skipped", "already " + str(e.get("status"))
        n = int(f.get("cert_count") or 1)
        if n > 1:   # one agreement for all of the owner's certificates (Exhibit A, Ari 10/17)
            cts = f.get("counties") or ""
            where_s = cts + (" counties" if " and " in cts else " County")
            subject = "Please review and sign — surplus funds agreement (" + where_s + ", " + str(n) + " properties)"
            about = (f"the surplus funds from the tax sale of {n} properties in {html.escape(where_s)} (all listed in Exhibit A of the agreement)")
        else:
            subject = "Please review and sign — surplus funds agreement (" + ((f.get("county") or "") + " County").strip() + ")"
            about = f"the surplus funds from the tax sale of the property in {where} (certificate {html.escape(f.get('cert_number') or '')})"
        body = (f"<p>Hi {first},</p><p>Thank you for speaking with us about {about}. Please review and sign our agreement online — it takes a few minutes "
                f"on your phone or computer.</p>" + _button(e["link"], "Review and sign")
                + "<p>If you have any questions, just reply to this email.</p>")
        text = f"Hi {name.split(' ')[0] if name else 'there'},\n\nPlease review and sign the surplus funds agreement here:\n{e['link']}\n\nMarci · {_brand()}"
        return _resend(to, subject, _wrap(body), text, None, bcc=True)
    if kind == "surplus_signed":
        pdf = signed_pdf(e, SURPLUS_SUB)
        safe = "".join(c for c in (name or "client") if c.isalnum() or c in " -_").strip().replace(" ", "-") or "client"
        fname = f"Signed-Surplus-Agreement-{safe}.pdf"
        b64 = base64.b64encode(pdf).decode()
        _rpc("surplus_agreement_file_put", {"p_id": e["id"], "p_filename": fname, "p_b64": b64})     # into the case's Files first
        subject = "Your signed agreement — " + _brand()
        body = (f"<p>Hi {first},</p><p>Thank you for signing. Your signed agreement is attached as a PDF for your records. "
                f"Our office will be in touch about the next steps.</p>")
        text = f"Hi {name.split(' ')[0] if name else 'there'},\n\nThank you for signing. Your signed agreement is attached.\n\nMarci · {_brand()}"
        return _resend(to, subject, _wrap(body), text, [{"filename": fname, "content": b64}], bcc=True)
    return "failed", "unknown kind " + kind


# ── 🌐 annelabes.com contact form (Ari 10/17, mail_outbox 'site_contact'): to the office; "Reply" answers the visitor
def send_site_contact(p):
    to = [x.strip() for x in _env("SITE_CONTACT_TO", "marci@annelabes.com").split(",") if x.strip()]
    esc = lambda v: html.escape(str(v or ""))
    rows = [("Name", esc(p.get("name"))), ("Email", esc(p.get("email")) or "—"), ("Phone", esc(p.get("phone")) or "—"), ("Page", esc(p.get("page")) or "/")]
    body = ("<p><b>New message from the website (annelabes.com)</b></p><table style='border-collapse:collapse'>"
            + "".join("<tr><td style='color:#6b7280;padding:2px 12px 2px 0'>" + a + "</td><td>" + b + "</td></tr>" for a, b in rows)
            + "</table><div style='margin-top:12px;padding:12px;border:1px solid #e5e7eb;border-radius:8px;white-space:pre-wrap'>" + esc(p.get("message")) + "</div>"
            + ("<p style='color:#6b7280;font-size:13px'>Press Reply to answer " + esc(p.get("name")) + " directly.</p>" if p.get("email") else
               "<p style='color:#6b7280;font-size:13px'>No email given - call them back.</p>"))
    text = f"Website message from {p.get('name')}\nEmail: {p.get('email') or '-'}\nPhone: {p.get('phone') or '-'}\n\n{p.get('message')}"
    subject = "🌐 Website message — " + str(p.get("name") or "visitor")
    status, detail = "sent", ""
    for t in to:
        status, detail = _resend(t, subject, _wrap(body), text, None, bcc=False, reply_to=p.get("email") or None)
        if status != "sent": break
    return status, detail


# ── 📬 "We saw your new certificates" (Ari 10/17, step 3; mail_outbox 'web_offer', made by web_offer_scan with a daily cap):
# the certificates a client just bought that we have no job for, a link to the order page with them ticked, and a stop link.
def send_web_offer(p):
    o = _rpc("web_offer_for_mail", {"p_id": p.get("id")}) or {}
    if not o.get("id"):
        return "failed", "offer not found"
    if o.get("status") != "queued":
        return "skipped", "offer is " + str(o.get("status"))
    to = (o.get("email") or "").strip()
    if not to:
        _rpc("web_offer_mark", {"p_id": o["id"], "p_status": "failed"}); return "failed", "no email"
    site = _env("WEB_SITE_URL", "https://annelabes-site.netlify.app").rstrip("/")
    certs = o.get("certs") or []
    n = len(certs)
    name = (o.get("name") or "").strip()
    first = html.escape((name.title() if name.isupper() else name) or "there")      # names are often "Last First" - use it whole
    rows = "".join("<tr><td style='padding:3px 12px 3px 0;white-space:nowrap'><b>" + html.escape(c.get("county") or "") + "</b></td><td style='padding:3px 12px 3px 0;white-space:nowrap'>"
                   + html.escape(c.get("cert") or "") + "</td><td style='padding:3px 0;color:#4b5563'>" + html.escape((c.get("property") or "")[:70]) + "</td></tr>" for c in certs[:12])
    more = ("<p style='color:#6b7280'>…and " + str(n - 12) + " more.</p>") if n > 12 else ""
    link = site + "/order.html?offer=" + o["token"]
    stop = site + "/order.html?stop=" + o["token"]
    total = _money(n * 500)
    subject = ("We saw your new certificate" if n == 1 else "We saw your %d new certificates" % n) + " — want us to do the title work?"
    body = (f"<p>Hi {first},</p><p>The State Auditor's list shows you bought {'a new certificate' if n == 1 else str(n) + ' new certificates'}. "
            f"Would you like us to handle the title work, as before? It's $500 per certificate — title search, Notice to Redeem, "
            f"the packet to the State Auditor and the first round of supplemental notices.</p>"
            f"<table style='border-collapse:collapse;font-size:14px;margin:10px 0'>{rows}</table>{more}"
            + _button(link, "Choose and order online") +
            f"<p>The link opens our order page with {'it' if n == 1 else 'them'} ticked — untick any you'd rather keep yourself "
            f"({'$500' if n == 1 else 'all ' + str(n) + ' = ' + total}). You sign online and can pay by card, by check or at the office. "
            f"Or just reply to this email or call us at (304) 615-8978.</p>"
            f"<p style='color:#9ca3af;font-size:12px;margin-top:22px'>You get this because you're a client of {html.escape(_brand())}, "
            f"1507 Saint Mary's Ave, Parkersburg, WV 26101. <a href='{html.escape(stop, quote=True)}' style='color:#9ca3af'>Don't send me these emails</a>.</p>")
    text = (f"Hi {first},\n\nThe State Auditor's list shows you bought {n} new certificate(s):\n"
            + "\n".join("- " + (c.get('county') or '') + " " + (c.get('cert') or '') for c in certs[:30])
            + f"\n\nWant us to do the title work ($500 each)? Choose and order: {link}\n\nOr call (304) 615-8978.\n\n"
            f"{_brand()}, 1507 Saint Mary's Ave, Parkersburg, WV 26101. Stop these emails: {stop}")
    status, detail = _resend(to, subject, _wrap(body), text, None, bcc=True)
    if status in ("sent", "failed"):
        _rpc("web_offer_mark", {"p_id": o["id"], "p_status": status})
    return status, detail


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
                g = _rpc("mail_outbox_next", {"p_kinds": ["gs_call", "gs_diego", "gs_photo"]})
                if g:
                    try:
                        # gs_diego (Ari 10/8): Diego tells a staff member something in the Gold Standard chat
                        # gs_photo (Ari 10/15): a certificate sent to Gold Standard as a photo job
                        status, detail = send_gs_call(g["payload"] or {}, {"gs_diego": "/integrations/portal-diego", "gs_photo": "/integrations/photo-job"}.get(g.get("kind"), "/integrations/portal-call"))
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("mail_outbox_done", {"p_id": g["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] gs_call #{g['id']}: {status} {detail[:120]}", flush=True)
                    if status == "queued":
                        _GS_PAUSE[0] = time.time() + 300   # Gold Standard not answering: let the emails go on, retry in 5 min
                    else:
                        time.sleep(2); continue
            if not job and _env("RESEND_API_KEY"):
                n = _rpc("mail_outbox_next", {"p_kinds": ["ntr_packet"]})
                if n:
                    try:
                        status, detail = send_ntr_packet(n["id"], n["payload"] or {})
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("ntr_packet_done", {"p_outbox": n["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] ntr_packet #{n['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2 if status != "queued" else 60); continue
            if not job and _env("RESEND_API_KEY"):
                n = _rpc("mail_outbox_next", {"p_kinds": ["ntr_reminder", "portal_invite", "office_email"]})
                if n:
                    try:
                        status, detail = {"portal_invite": send_portal_invite, "office_email": send_office_email}.get(n.get("kind"), send_ntr_reminder)(n["payload"] or {})
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("mail_outbox_done", {"p_id": n["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] {n.get('kind')} #{n['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2 if status != "queued" else 60); continue
            if not job and _env("TWILIO_ACCOUNT_SID") and _env("TWILIO_API_KEY") and _env("TWILIO_API_SECRET") and _env("TWILIO_FROM"):
                t = _rpc("sms_outbox_next", {})
                if t:
                    try:
                        status, sid, detail, opted_out = send_sms(t.get("to"), t.get("body"))
                    except Exception as ex:
                        status, sid, detail, opted_out = "failed", None, str(ex)[:300], False
                    _rpc("sms_outbox_done", {"p_id": t["id"], "p_status": status, "p_sid": sid, "p_detail": detail, "p_opted_out": opted_out})
                    print(f"[mailer] sms #{t['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2); continue
            if not job and _env("RESEND_API_KEY"):
                n = _rpc("mail_outbox_next", {"p_kinds": ["redeemed_filing"]})
                if n:
                    try:
                        status, detail = send_redeemed_filing(n["id"], n["payload"] or {})
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("redeemed_filing_done", {"p_outbox": n["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] redeemed_filing #{n['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2 if status != "queued" else 60); continue
            if not job and _env("RESEND_API_KEY"):
                n = _rpc("mail_outbox_next", {"p_kinds": ["surplus_agreement_email", "surplus_signed", "site_contact", "web_offer"]})
                if n:
                    try:
                        status, detail = (send_site_contact(n["payload"] or {}) if n["kind"] == "site_contact"
                                          else send_web_offer(n["payload"] or {}) if n["kind"] == "web_offer"
                                          else send_surplus(n["kind"], n["id"], n["payload"] or {}))
                    except Exception as ex:
                        status, detail = "failed", str(ex)[:300]
                    _rpc("mail_outbox_done", {"p_id": n["id"], "p_status": status, "p_detail": detail})
                    print(f"[mailer] {n['kind']} #{n['id']}: {status} {detail[:120]}", flush=True)
                    time.sleep(2 if status != "queued" else 60); continue
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
