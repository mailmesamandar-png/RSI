"""Register Telegram webhook using secured Railway environment variables."""
import json
import os
import sys
import urllib.error
import urllib.request

base = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else ""
token = os.getenv("TELEGRAM_BOT_TOKEN", "")
secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
if not base.startswith("https://") or len(token) < 16 or len(secret) < 16:
    print("Telegram webhook configuration missing or invalid", file=sys.stderr)
    sys.exit(1)
url = f"https://api.telegram.org/bot{token}/setWebhook"
payload = json.dumps({"url": base + "/telegram", "secret_token": secret, "allowed_updates": ["message"], "drop_pending_updates": False}).encode()
request = urllib.request.Request(url, payload, headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.load(response)
    print("Telegram webhook registered" if result.get("ok") else "Telegram webhook registration rejected")
except urllib.error.HTTPError as ex:
    print(f"Telegram webhook registration HTTP {ex.code}", file=sys.stderr)
    sys.exit(1)
except Exception:
    print("Telegram webhook registration failed", file=sys.stderr)
    sys.exit(1)
