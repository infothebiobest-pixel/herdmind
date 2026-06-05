"""
HerdMind-X · Alert Service
Owns notification routing — decoupled from Twilio and delivery channels.
ai_service calls this endpoint; this service decides how and where to send.
"""

import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(
    title       = "HerdMind-X · Alert Service",
    description = "Notification routing — WhatsApp, SMS, email, webhook.",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM        = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
RECIPIENTS         = os.environ.get("HERDMIND_ALERT_RECIPIENTS", "").split(",")

NOTIFY_ON_LEVELS   = {"CRITICAL", "WARNING"}

# ---------------------------------------------------------------------------
# Twilio client
# ---------------------------------------------------------------------------

_twilio = None

def _get_twilio():
    global _twilio
    if _twilio:
        return _twilio
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        return None
    from twilio.rest import Client
    _twilio = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _twilio

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class AlertRequest(BaseModel):
    cow_id:      str
    alert_level: str
    disease:     str
    risk_score:  float
    temperature: float
    rumination:  float
    activity:    float
    message:     str = ""
    action:      str = ""

class AlertResponse(BaseModel):
    sent:      bool
    channels:  list[str]
    cow_id:    str
    timestamp: str
    reason:    str = ""

# ---------------------------------------------------------------------------
# Message builder
# ---------------------------------------------------------------------------

def _build_message(req: AlertRequest) -> str:
    emoji = "🔴" if req.alert_level == "CRITICAL" else "🟡"
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"{emoji} *HerdMind-X — {req.alert_level}*\n"
        f"──────────────────────\n"
        f"🐄 Cow ID:     {req.cow_id}\n"
        f"🦠 Disease:    {req.disease.upper()}\n"
        f"🌡️  Temp:       {req.temperature:.1f}°C\n"
        f"💭 Rumination: {req.rumination:.0f} min/day\n"
        f"⚡ Activity:   {req.activity:.0f} units\n"
        f"📊 Risk Score: {req.risk_score:.2f} / 1.00\n"
        f"──────────────────────\n"
        f"⚠️  {req.message}\n"
        f"✅ {req.action}\n"
        f"🕐 {ts}"
    )

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    twilio_ready = bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN)
    return {
        "service":      "alert_service",
        "status":       "ok",
        "twilio_ready": twilio_ready,
        "recipients":   len([r for r in RECIPIENTS if r.strip()]),
    }


@app.post("/notify", response_model=AlertResponse, tags=["alerts"])
def notify(req: AlertRequest):
    """
    Route alert to configured channels.
    Currently: WhatsApp via Twilio.
    Extensible: add SMS, email, webhook blocks below.
    """
    ts = datetime.now(timezone.utc).isoformat()

    if req.alert_level not in NOTIFY_ON_LEVELS:
        log.debug("Skipping notification for level=%s cow_id=%s",
                  req.alert_level, req.cow_id)
        return AlertResponse(
            sent=False, channels=[], cow_id=req.cow_id,
            timestamp=ts, reason=f"level {req.alert_level} below notify threshold"
        )

    client   = _get_twilio()
    channels = []

    if not client:
        log.warning("Twilio not configured — alert not sent. cow_id=%s", req.cow_id)
        return AlertResponse(
            sent=False, channels=[], cow_id=req.cow_id,
            timestamp=ts, reason="Twilio credentials not set"
        )

    body       = _build_message(req)
    recipients = [r.strip() for r in RECIPIENTS if r.strip()]

    for number in recipients:
        to = f"whatsapp:{number}" if not number.startswith("whatsapp:") else number
        try:
            msg = client.messages.create(from_=TWILIO_FROM, to=to, body=body)
            log.info("WhatsApp sent → %s  sid=%s  cow_id=%s",
                     number, msg.sid, req.cow_id)
            channels.append(f"whatsapp:{number}")
        except Exception as exc:
            log.error("WhatsApp failed → %s: %s", number, exc)

    return AlertResponse(
        sent      = len(channels) > 0,
        channels  = channels,
        cow_id    = req.cow_id,
        timestamp = ts,
    )


@app.get("/config", tags=["config"])
def config():
    """Return notification configuration (no secrets)."""
    return {
        "notify_on_levels": list(NOTIFY_ON_LEVELS),
        "recipient_count":  len([r for r in RECIPIENTS if r.strip()]),
        "twilio_configured": bool(TWILIO_ACCOUNT_SID),
        "channels_available": ["whatsapp"],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=False)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=False)
