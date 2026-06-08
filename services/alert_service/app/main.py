"""HerdMind-X Alert Service v2 — Tiered routing"""
import logging, os
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="HerdMind-X Alert Service v2", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_WA     = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
TWILIO_FROM_SMS    = os.environ.get("TWILIO_SMS_FROM", "")
RECIPIENTS_WA      = [r.strip() for r in os.environ.get("HERDMIND_ALERT_RECIPIENTS","").split(",") if r.strip()]
RECIPIENTS_SMS     = [r.strip() for r in os.environ.get("HERDMIND_SMS_RECIPIENTS","").split(",") if r.strip()]

TIERS = {"CRITICAL": 0.95, "WARNING": 0.80, "WATCH": 0.60}

_twilio = None
def _get_twilio():
    global _twilio
    if _twilio: return _twilio
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN: return None
    from twilio.rest import Client
    _twilio = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _twilio

class AlertRequest(BaseModel):
    cow_id: str
    alert_level: str
    disease: str = "unknown"
    risk_score: float
    temperature: float = 0.0
    rumination: float = 0.0
    activity: float = 0.0
    message: str = ""
    action: str = ""

class AlertResponse(BaseModel):
    sent: bool
    tier: str
    channels: list[str]
    cow_id: str
    timestamp: str
    reason: str = ""

def _get_tier(risk: float, level: str) -> str:
    if risk >= TIERS["CRITICAL"] or level == "CRITICAL": return "CRITICAL"
    if risk >= TIERS["WARNING"]  or level == "WARNING":  return "WARNING"
    if risk >= TIERS["WATCH"]:                           return "WATCH"
    return "NONE"

def _wa_msg(req, tier):
    emoji = "🔴" if tier == "CRITICAL" else "🟡"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"{emoji} HerdMind-X {tier}\nCow:{req.cow_id} Disease:{req.disease.upper()}\nTemp:{req.temperature:.1f}C Rum:{req.rumination:.0f} Act:{req.activity:.0f}\nRisk:{req.risk_score:.3f}\n{req.message}\n{req.action}\n{ts}"

def _sms_msg(req):
    return f"HERDMIND CRITICAL: Cow {req.cow_id} Risk {req.risk_score:.2f} Temp {req.temperature:.1f}C Disease:{req.disease.upper()} ACTION REQUIRED"

def _send_wa(client, recipients, body, cow_id):
    sent = []
    for n in recipients:
        to = f"whatsapp:{n}" if not n.startswith("whatsapp:") else n
        try:
            msg = client.messages.create(from_=TWILIO_FROM_WA, to=to, body=body)
            log.info("WA sent cow=%s sid=%s", cow_id, msg.sid)
            sent.append(f"whatsapp:{n}")
        except Exception as e:
            log.error("WA failed: %s", e)
    return sent

def _send_sms(client, recipients, body, cow_id):
    if not TWILIO_FROM_SMS: return []
    sent = []
    for n in recipients:
        try:
            msg = client.messages.create(from_=TWILIO_FROM_SMS, to=n, body=body)
            log.info("SMS sent cow=%s sid=%s", cow_id, msg.sid)
            sent.append(f"sms:{n}")
        except Exception as e:
            log.error("SMS failed: %s", e)
    return sent

@app.get("/health")
def health():
    return {"service":"alert_service","status":"ok","version":"2.0.0","twilio_ready":bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN),"wa_recipients":len(RECIPIENTS_WA),"sms_recipients":len(RECIPIENTS_SMS),"tiers":TIERS}

@app.post("/notify", response_model=AlertResponse)
def notify(req: AlertRequest):
    ts   = datetime.now(timezone.utc).isoformat()
    tier = _get_tier(req.risk_score, req.alert_level)
    log.info("cow=%s risk=%.3f tier=%s", req.cow_id, req.risk_score, tier)
    if tier in ("NONE","WATCH"):
        return AlertResponse(sent=False, tier=tier, channels=[], cow_id=req.cow_id, timestamp=ts, reason=f"tier={tier}")
    client = _get_twilio()
    if not client:
        return AlertResponse(sent=False, tier=tier, channels=[], cow_id=req.cow_id, timestamp=ts, reason="Twilio not configured")
    channels = []
    if tier in ("WARNING","CRITICAL"):
        channels += _send_wa(client, RECIPIENTS_WA, _wa_msg(req, tier), req.cow_id)
    if tier == "CRITICAL":
        channels += _send_sms(client, RECIPIENTS_SMS, _sms_msg(req), req.cow_id)
    return AlertResponse(sent=len(channels)>0, tier=tier, channels=channels, cow_id=req.cow_id, timestamp=ts)

@app.get("/config")
def config():
    return {"tiers":TIERS,"wa_recipients":len(RECIPIENTS_WA),"sms_recipients":len(RECIPIENTS_SMS),"twilio_configured":bool(TWILIO_ACCOUNT_SID),"channels":["whatsapp","sms"]}
