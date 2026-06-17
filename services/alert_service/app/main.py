from fastapi import FastAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import os
import json
import time
import logging
import threading
import redis
import httpx


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("herd_alert_service")


# ============================================================
# APP
# ============================================================

app = FastAPI(title="HerdMind-X Alert Service")

r = None


# ============================================================
# CONFIG
# ============================================================

AI_SERVICE_URL = os.getenv(
    "AI_SERVICE_URL",
    "http://ai_service:8001/ai/analyze"
)

QUEUE_NAME = "herd:queue:raw_anomalies"
ALERT_CHANNEL = "herd:alerts"

HISTORY_KEY = "herd:alerts:history"
DLQ_KEY = "herd:queue:failed_anomalies"

ALERT_COOLDOWN_SEC = 1800
MAX_HISTORICAL_ALERTS = 500

RISK_THRESHOLD = 0.70

INFLUX_URL = os.getenv(
    "INFLUX_URL",
    "http://herd_influx:8086"
)

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "herdmind")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "herd_telemetry")


# ============================================================
# HELPERS
# ============================================================

def cooldown_key(cow_id: str, diagnosis: str):
    return f"cooldown:{cow_id}:{diagnosis.lower().replace(' ', '_')}"


def should_throttle(cow_id: str, diagnosis: str):

    key = cooldown_key(cow_id, diagnosis)

    if r.get(key):
        return True

    r.setex(key, ALERT_COOLDOWN_SEC, "1")
    return False


# ============================================================
# AI
# ============================================================

def query_ai(cow_id, metrics):

    try:

        payload = {
            "cow_id": str(cow_id),
            **metrics
        }

        with httpx.Client(timeout=5) as client:

            res = client.post(
                AI_SERVICE_URL,
                json=payload
            )

            if res.status_code == 200:

                data = res.json()

                return (
                    data.get("disease", "Unknown"),
                    float(data.get("risk_score", 0.5)),
                    data.get("alert_level", "Monitor")
                )

            logger.warning(
                f"AI returned {res.status_code}"
            )

    except Exception as e:

        logger.warning(
            f"AI fallback: {e}"
        )

    return (
        "Unknown",
        0.5,
        "Monitor"
    )


# ============================================================
# TWILIO
# ============================================================

def dispatch_external_notification(
    cow_id,
    diagnosis,
    risk,
    action
):

    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")

    if not sid or not token:

        logger.info(
            "Twilio disabled."
        )
        return

    url = (
        f"https://api.twilio.com/"
        f"2010-04-01/"
        f"Accounts/{sid}/Messages.json"
    )

    payload = {

        "From": os.getenv(
            "TWILIO_WHATSAPP_FROM",
            "whatsapp:+14155238886"
        ),

        "To": (
            "whatsapp:"
            + os.getenv(
                "HERDMIND_ALERT_RECIPIENTS",
                "+923001234567"
            )
        ),

        "Body": (
            f"🚨 HERDMIND-X ALERT\n\n"
            f"Cow: {cow_id}\n"
            f"Diagnosis: {diagnosis}\n"
            f"Risk: {risk:.2f}\n"
            f"Action: {action}"
        ),
    }

    try:

        with httpx.Client(timeout=10) as client:

            res = client.post(
                url,
                auth=(sid, token),
                data=payload
            )

            if res.status_code in [200, 201]:

                logger.info(
                    f"Notification sent → {cow_id}"
                )

            else:

                logger.error(
                    f"Twilio {res.status_code}"
                )

    except Exception as e:

        logger.error(
            f"Notification failed: {e}"
        )


# ============================================================
# INFLUX
# ============================================================

def archive_event(
    cow_id,
    diagnosis,
    risk,
    confidence
):

    if not INFLUX_TOKEN:
        return

    try:

        with InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            timeout=3000
        ) as client:

            point = (
                Point("biological_risk")
                .tag("cow_id", str(cow_id))
                .tag("diagnosis", diagnosis)
                .field(
                    "risk_percentage",
                    risk * 100
                )
                .field(
                    "confidence",
                    confidence
                )
                .time(
                    int(time.time()),
                    WritePrecision.S
                )
            )

            client.write_api(
                write_options=SYNCHRONOUS
            ).write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point
            )

    except Exception as e:

        logger.warning(
            f"Influx bypass: {e}"
        )


# ============================================================
# WORKER
# ============================================================

def process_loop():

    logger.info(
        "Alert worker started."
    )

    while True:

        raw = None

        try:

            item = r.brpop(
                QUEUE_NAME,
                timeout=5
            )

            if not item:
                continue

            _, raw = item

            payload = json.loads(raw)

            cow_id = payload.get("cow_id")

            risk = float(
                payload.get(
                    "risk_score",
                    0
                )
            )

            metrics = payload.get(
                "metrics",
                {}
            )

            if not cow_id:
                continue

            if risk < RISK_THRESHOLD:
                continue

            diagnosis, confidence, action = (
                query_ai(
                    cow_id,
                    metrics
                )
            )

            if should_throttle(
                cow_id,
                diagnosis
            ):
                continue

            event = {

                "type":
                "DISEASE_ALERT",

                "cow_id":
                cow_id,

                "risk_score":
                risk,

                "diagnosis":
                diagnosis,

                "confidence":
                confidence,

                "recommendation":
                action,

                "timestamp":
                time.time()
            }

            event_json = json.dumps(event)

            r.lpush(
                HISTORY_KEY,
                event_json
            )

            r.ltrim(
                HISTORY_KEY,
                0,
                MAX_HISTORICAL_ALERTS
            )

            archive_event(
                cow_id,
                diagnosis,
                risk,
                confidence
            )

            if action in [
                "CRITICAL",
                "WARNING"
            ]:

                dispatch_external_notification(
                    cow_id,
                    diagnosis,
                    risk,
                    action
                )

            r.publish(
                ALERT_CHANNEL,
                event_json
            )

            logger.info(
                f"ALERT → "
                f"{cow_id} "
                f"{diagnosis}"
            )

        except redis.exceptions.RedisError as e:

            logger.error(
                f"Redis error: {e}"
            )

            time.sleep(2)

        except Exception as e:

            logger.error(
                f"Worker error: {e}"
            )

            if raw:

                try:
                    r.lpush(
                        DLQ_KEY,
                        raw
                    )

                except Exception:
                    pass

            time.sleep(2)


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
def startup():

    global r

    for i in range(10):

        try:

            r = redis.Redis(
                host="redis",
                port=6379,
                decode_responses=True,
                socket_timeout=15
            )

            r.ping()

            logger.info(
                "Redis connected."
            )

            break

        except Exception as e:

            logger.warning(
                f"Retry {i+1}/10 → {e}"
            )

            time.sleep(2)

    else:

        raise RuntimeError(
            "Redis unavailable"
        )

    threading.Thread(
        target=process_loop,
        daemon=True
    ).start()


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():

    return {

        "status": "ok",
        "worker": "sync_thread_running"
    }

