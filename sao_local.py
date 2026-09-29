"""State Auditor letters reader for the office PC (Ari 2026-09-29).

The State Auditor's office wrote that automated reading is fine at a reasonable pace (~30 approval letters an hour) and that we
may read from another computer while the county firewall blocks our server. This runs ONLY the letters reader, one letter at a
time, on the shared pace kept in the database (one State Auditor visit every ~2 minutes for ALL readers together).

Start:  double-click sao_local.bat   (or:  python sao_local.py)
Stop:   close the window (or Ctrl+C)

It needs a file named .env next to this one with the line
    SUPABASE_SECRET_KEY=sb_secret_...
Ari pastes that key himself; it is never committed (.gitignore) and never shown.
"""
import os, sys

here = os.path.dirname(os.path.abspath(__file__))
env = os.path.join(here, ".env")
if os.path.exists(env):
    for line in open(env, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
if not os.environ.get("SUPABASE_SECRET_KEY", "").startswith("sb_secret_"):
    sys.exit("Missing SUPABASE_SECRET_KEY in the .env file next to sao_local.py - see the note at the top of this file.")
os.environ["SAO_LOCAL"] = "1"
sys.path.insert(0, here)

import main  # noqa: E402  (loads the shared code; the web server is not started)

print("State Auditor letters reader - office PC. Shared pace ~30 letters / hour. Close this window to stop.", flush=True)
try:
    main.sao_loop(0)
except KeyboardInterrupt:
    print("stopped", flush=True)
