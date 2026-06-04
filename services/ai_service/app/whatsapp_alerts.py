"""
HerdMind-X · WhatsApp Alert Layer
Sends real-world notifications via Twilio WhatsApp API when risk threshold is breached.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

from twilio.rest import Client

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config  (set these as environment variables — never hardcode in production)
# ---------------------------------------------------------------------------

TWILIO_ACCOUNT_SID  = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM         = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")  # Twilio sandbox number
ALERT_RECIPIENTS    = os.environ.get("HERDMIND_ALERT_RECIPIENTS", "").split(",")       # comma-separated numbers

# Alert level → whether to send
SEND_ON_LEVELS = {"CRITICAL", "WARNING"}

# ---------------------------------------------------------------------------
# Twilio client (lazy init so missing creds don't crash on import)
# ---------------------------------------------------------------------------

_twilio: Client | None = None


def _get_client() -> Client | None:
    global _twilio
    if _twilio:
        return _twilio
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        log.warning("Twilio credentials not set — WhatsApp alerts disabled.")
        return None
    _twilio = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    return _twilio


# ---------------------------------------------------------------------------
# Message builder
# ---------------------------------------------------------------------------

def _build_message(
    cow_id:     Any,
    risk_score: float,
    alert:      dict,
    temperature: float,
    rumination:  float,
    activity:    float,
) -> str:
    level   = alert.get("alert_level", "UNKNOWN")
    message = alert.get("message", "")
    action  = alert.get("action", "")
    ts      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    emoji = "🔴" if level == "CRITICAL" else "🟡"

    return (
        f"{emoji} *HerdMind-X Alert — {level}*\n"
        f"──────────────────────\n"
        f"🐄 Cow ID:      {cow_id}\n"
        f"🌡️  Temp:        {temperature:.1f}°C\n"
        f"💭 Rumination:  {rumination:.0f} min/day\n"
        f"⚡ Activity:    {activity:.0f} units\n"
        f"📊 Risk Score:  {risk_score:.2f} / 1.00\n"
        f"──────────────────────\n"
        f"⚠️  {message}\n"
        f"✅ Action: {action}\n"
        f"🕐 {ts}"
    )


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_whatsapp_alert(
    cow_id:      Any,
    risk_score:  float,
    alert:       dict,
    temperature: float,
    rumination:  float,
    activity:    float,
) -> bool:
    """
    Send a WhatsApp message to all configured recipients.
    Returns True if at least one message was sent successfully.
    """
    alert_level = alert.get("alert_level", "NONE")

    if alert_level not in SEND_ON_LEVELS:
        return False

    client = _get_client()
    if not client:
        return False

    recipients = [r.strip() for r in ALERT_RECIPIENTS if r.strip()]
    if not recipients:
        log.warning("No alert recipients configured — set HERDMIND_ALERT_RECIPIENTS.")
        return False

    body    = _build_message(cow_id, risk_score, alert, temperature, rumination, activity)
    success = False

    for number in recipients:
        to = f"whatsapp:{number}" if not number.startswith("whatsapp:") else number
        try:
            msg = client.messages.create(from_=TWILIO_FROM, to=to, body=body)
            log.info("WhatsApp alert sent → %s  sid=%s  cow_id=%s  level=%s",
                     number, msg.sid, cow_id, alert_level)
            success = True
        except Exception as exc:
            log.error("WhatsApp send failed → %s: %s", number, exc)

    return success