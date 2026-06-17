from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import json
import logging
import time
import threading
import httpx
import redis
from fastapi import FastAPI

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("herd_alert_service")

# ================= APP =================
app = FastAPI(title="Herd Alert Service (Streams V2)")

# ================= GLOBAL STATE =================
r = None

# ================= CONFIG =================
AI_SERVICE_URL = "http://ai_service:8001/ai/analyze"
ALERT_COOLDOWN_SEC = 1800  # 30 min
MAX_HISTORICAL_ALERTS = 500

STREAM_NAME = "herd:stream:anomalies"
CONSUMER_GROUP = "alert_service_group"
CONSUMER_NAME = "worker_node_1"

ALERT_CHANNEL = "herd:alerts"
HISTORY_KEY = "herd:alerts:history"


# ================= HELPERS =================
def cooldown_key(cow_id: str) -> str:
    return f"cooldown:global_protection:{cow_id}"

def is_currently_throttled(cow_id: str) -> bool:
    return r.exists(cooldown_key(cow_id))

def activate_cooldown(cow_id: str):
    r.setex(cooldown_key(cow_id), ALERT_COOLDOWN_SEC, "active")


# ================= BATCH AI INFERENCE CONTRACT =================
def query_ai_batch(flat_payloads: list) -> list:
    """
    Sends processed batches back to the AI Service. 
    Falls back gracefully if batch routes are unconfigured.
    """
    results = []
    with httpx.Client(timeout=5.0) as client:
        for item in flat_payloads:
            cow_id = item.get("cow_id")
            try:
                res = client.post(AI_SERVICE_URL, json=item)
                if res.status_code == 200:
                    data = res.json()
                    results.append((cow_id, data.get("disease", "Unknown"), data.get("risk_score", 0.5), data.get("alert_level", "Monitor")))
                else:
                    results.append((cow_id, "Unknown", 0.5, "Monitor"))
            except Exception as e:
                logger.warning(f"Batch routing component fallback for Cow {cow_id}: {e}")
                results.append((cow_id, "Unknown", 0.5, "Monitor"))
    return results


# ================= TWILIO DISPATCH MECHANISM =================
def dispatch_external_notification(cow_id: str, diagnosis: str, risk: float, action: str):
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    whatsapp_from = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
    recipient = os.getenv("HERDMIND_ALERT_RECIPIENTS", "+923001234567")

    if not account_sid or not auth_token or account_sid == "replace_with_sid" or auth_token == "replace_with_token":
        return

    url = f"https://twilio.com{account_sid}/Messages.json"
    message_body = (
        f"🚨 *HERDMIND-X STREAM BATCH ALERT*\n\n"
        f"🔹 *Cow ID:* {cow_id}\n"
        f"🔹 *Diagnosis:* {diagnosis} (Risk: {risk:.2f})\n"
        f"🔹 *Prescription:* {action}\n"
    )
    payload = {"From": whatsapp_from, "To": f"whatsapp:{recipient}", "Body": message_body}

    try:
        with httpx.Client() as client:
            client.post(url, auth=(account_sid, auth_token), data=payload)
    except Exception as e:
        logger.error(f"Twilio failure: {e}")


# ================= STREAM WORKER LOOP =================
def process_stream_loop():
    logger.info(f"🚀 Redis Stream Consumer Group active: {CONSUMER_GROUP} | Node: {CONSUMER_NAME}")

    # Enforce stream group creation state on startup
    try:
        r.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError:
        pass # Group already initialized

    while True:
        try:
            # Batch extraction: Pulls up to 20 unacknowledged message frames simultaneously
            response = r.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {STREAM_NAME: ">"}, count=20, block=2000)
            
            if not response:
                continue

            batch_payloads = []
            message_ids = []
            
            for stream, messages in response:
                for msg_id, payload in messages:
                    message_ids.append(msg_id)
                    cow_id = str(payload.get("cow_id", ""))
                    
                    if not cow_id or is_currently_throttled(cow_id):
                        r.xack(STREAM_NAME, CONSUMER_GROUP, msg_id) # Ack instantly if dropped/throttled
                        continue
                        
                    # Flatten out schema structure variables dynamically
                    metrics = {
                        "temperature": float(payload.get("temperature", 38.5)),
                        "rumination": float(payload.get("rumination", 280)),
                        "activity": float(payload.get("activity", 70))
                    }
                    batch_payloads.append({"cow_id": cow_id, "risk_score": float(payload.get("risk_score", 0.0)), "metrics": metrics})

            if not batch_payloads:
                continue

            # ================= MICRO-BATCH COOLDOWN-AWARE INFERENCE =================
            evaluations = query_ai_batch(batch_payloads)

            for cow_id, diagnosis, confidence, recommendation in evaluations:
                # Lock cooldown protections immediately
                activate_cooldown(cow_id)
                
                event = {
                    "type": "DISEASE_ALERT",
                    "cow_id": cow_id,
                    "diagnosis": diagnosis,
                    "risk_score": confidence,
                    "recommendation": recommendation,
                    "timestamp": time.time()
                }
                event_json = json.dumps(event)

                # Bulk Persistence block
                try:
                    now_ts = time.time()
                    event_id = f"{cow_id}:{int(now_ts * 1000)}"
                    pipe = r.pipeline(transaction=True)
                    pipe.hset(f"herd:event:{event_id}", mapping={"cow_id": cow_id, "event_json": event_json})
                    pipe.expire(f"herd:event:{event_id}", 604800)
                    pipe.lpush(HISTORY_KEY, event_json)
                    pipe.ltrim(HISTORY_KEY, 0, MAX_HISTORICAL_ALERTS - 1)
                    pipe.execute()
                except Exception as e:
                    logger.error(f"Persistence error: {e}")

                if recommendation in ["CRITICAL", "WARNING"]:
                    dispatch_external_notification(cow_id, diagnosis, confidence, recommendation)

                logger.info(f"🔥 Stream Processed: Cow {cow_id} → {diagnosis} ({recommendation})")

            # Acknowledge entire batch group processed successfully
            for m_id in message_ids:
                r.xack(STREAM_NAME, CONSUMER_GROUP, m_id)

        except Exception as e:
            logger.error(f"Streams loop execution core fault: {e}")
            time.sleep(2)


# ================= STARTUP =================
@app.on_event("startup")
def startup():
    global r
    logger.info("Connecting to Redis Streams Cluster...")
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    threading.Thread(target=process_stream_loop, daemon=True).start()

@app.get("/health")
def health():
    return {"status": "ok", "engine": "redis_streams_active"}
