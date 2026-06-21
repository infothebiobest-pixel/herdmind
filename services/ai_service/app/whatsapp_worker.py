import os
import time
import json
import redis
import logging
import socket

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("whatsapp_worker")

# Clean, stable production connection profile
r_cache = redis.Redis(
    host="herd_redis",
    port=6379,
    db=0,
    socket_timeout=None,
    socket_connect_timeout=5,
    health_check_interval=30,
    retry_on_timeout=True,
)

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM        = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
TWILIO_TO          = os.environ.get("TWILIO_WHATSAPP_TO", "")

_twilio_client = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        from twilio.rest import Client
        _twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        log.info("📱 Native Twilio Rest Client initialized for background worker.")
    except Exception as e:
        log.error(f"Failed to load Twilio module: {e}")

def send_whatsapp(event):
    cow_id = event.get('cow_id')
    disease = event.get('disease')
    risk = event.get('risk', 0.0)
    level = event.get('level')

    body = (
        f"🚨 *HerdMind-X Emergency Alert*\n\n"
        f"• *Livestock ID:* Cow #{cow_id}\n"
        f"• *Condition:* {disease}\n"
        f"• *Risk Index:* {risk:.2f}\n"
        f"• *Severity:* {level}\n\n"
        f"⚠️ *Directive:* Immediate isolation required. Check telemetry dashboard."
    )

    if _twilio_client and TWILIO_TO:
        _twilio_client.messages.create(from_=TWILIO_FROM, to=TWILIO_TO, body=body)
        log.warning(f"📱 Real Twilio WhatsApp Alert Dispatched to {TWILIO_TO}")
        return True

    print("\n" + "="*60)
    print("🤖 [ASYNC MOCK TWILIO SANDBOX] — QUEUED INCIDENT DISPATCHED")
    print(f"📱 Recipient Link: {TWILIO_TO if TWILIO_TO else 'whatsapp:+MOCK_USER'}")
    print("-"*60)
    print(body)
    print("="*60 + "\n")
    return True

def run_worker():
    log.info("📲 WhatsApp Worker engine listening for live alert queue events...")
    while True:
        try:
            # 5-second blocking pull cycle
            result = r_cache.brpop("herd:alert_queue", timeout=5)
            
            if result:
                _, data = result
                event = json.loads(data.decode('utf-8'))
                try:
                    send_whatsapp(event)
                except Exception as e:
                    log.error(f"External API transmission failed: {e}. Requeuing payload.")
                    r_cache.lpush("herd:alert_queue_retry", json.dumps(event))
            else:
                continue
                
        except BaseException as e:
            # 🛡️ THE BULLETPROOF SHIELD: Catch everything, evaluate message context cleanly
            err_msg = str(e).lower()
            # If the error is a socket timeout, has an empty string message, or contains timeout tags -> SILENT SKIP
            if not err_msg or "timeout" in err_msg or "socket" in err_msg:
                continue
                
            log.error(f"Worker loop error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    run_worker()
