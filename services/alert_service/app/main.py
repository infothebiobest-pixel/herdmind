
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
app = FastAPI(title="Herd Alert Service")

# ================= GLOBAL STATE =================
r = None

# ================= CONFIG =================
AI_SERVICE_URL = "http://ai_service:8001/ai/analyze"
ALERT_COOLDOWN_SEC = 1800  # 30 min
MAX_HISTORICAL_ALERTS = 500

QUEUE_NAME = "herd:queue:raw_anomalies"
ALERT_CHANNEL = "herd:alerts"
HISTORY_KEY = "herd:alerts:history"
DLQ_KEY = "herd:queue:failed_anomalies"


# ================= HELPERS =================
def cooldown_key(cow_id: str, diagnosis: str) -> str:
    return f"cooldown:{cow_id}:{diagnosis.lower().replace(' ', '_')}"


def should_throttle(cow_id: str, diagnosis: str) -> bool:
    key = cooldown_key(cow_id, diagnosis)
    exists = r.get(key)

    if exists:
        return True

    r.setex(key, ALERT_COOLDOWN_SEC, "1")
    return False


# ================= AI CALL =================
def query_ai(cow_id: str, metrics: dict):
    diagnosis = "Unknown"
    confidence = 0.5
    recommendation = "Monitor animal"

    try:
        with httpx.Client(timeout=5.0) as client:
            res = client.post(
                AI_SERVICE_URL,
                json={"cow_id": cow_id, "metrics": metrics},
            )

            if res.status_code == 200:
                data = res.json()
                return (
                    data.get("diagnosis", diagnosis),
                    data.get("confidence", confidence),
                    data.get("recommendation", recommendation),
                )

            logger.warning(f"AI service status {res.status_code} for {cow_id}")

    except Exception as e:
        logger.warning(f"AI fallback for {cow_id}: {e}")

    return diagnosis, confidence, recommendation


# ================= WORKER LOOP =================
def process_loop():
    logger.info("🚀 Pure Sync Alert worker loop engaged and listening.")

    while True:
        raw = None
        try:
            # Clean sync blocking pop from queue
            item = r.brpop(QUEUE_NAME, timeout=5)

            if not item:
                continue

            _, raw = item
            payload = json.loads(raw)

            cow_id = payload.get("cow_id")
            risk = payload.get("risk_score", 0)
            metrics = payload.get("metrics", {})

            if not cow_id:
                logger.warning("Dropped payload: missing cow_id")
                continue

            if risk < 0.7:
                logger.info(f"Filtered low-risk event: cow={cow_id}, risk={risk}")
                continue

            # ================= AI =================
            diagnosis, confidence, recommendation = query_ai(cow_id, metrics)

            # ================= THROTTLE =================
            if should_throttle(cow_id, diagnosis):
                logger.info(f"Throttled: {cow_id} ({diagnosis})")
                continue

            # ================= EVENT =================
            event = {
                "type": "DISEASE_ALERT",
                "cow_id": cow_id,
                "risk_score": risk,
                "diagnosis": diagnosis,
                "confidence": round(confidence, 3),
                "recommendation": recommendation,
                "timestamp": time.time(),
            }

            event_json = json.dumps(event)

            # ================= LAYER 2 PRECOMPUTATION + HISTORY STORE =================
            try:
                now_ts = time.time()
                event_id = f"{cow_id}:{int(now_ts * 1000)}"
                risk_score = float(event.get("risk_score", 0.0))
                pipe = r.pipeline(transaction=True)
                pipe.hset(
                    f"herd:event:{event_id}",
                    mapping={
                        "cow_id": str(cow_id),
                        "timestamp": str(now_ts),
                        "event_json": event_json,
                        "risk_score": str(risk_score),
                        "diagnosis": str(event.get("diagnosis", "")),
                        "type": str(event.get("type", "DISEASE_ALERT")),
                    },
                )
                pipe.expire(f"herd:event:{event_id}", 604800)
                ts_key = f"herd:ts:cow:{cow_id}"
                pipe.zadd(ts_key, {event_id: now_ts})
                pipe.zremrangebyscore(ts_key, 0, now_ts - 604800)
                pipe.expire(ts_key, 604800)
                pipe.hset("herd:matrix:latest_risk", cow_id, risk_score)
                pipe.lpush(HISTORY_KEY, event_json)
                pipe.ltrim(HISTORY_KEY, 0, MAX_HISTORICAL_ALERTS - 1)
                pipe.execute()
                
                # ----------------------------------------------------
                # 5. INFLUXDB LONG-TERM APPEND-ONLY ARCHIVAL SINK
                # ----------------------------------------------------
                try:
                    influx_url = os.getenv("INFLUX_URL", "http://herd_influx:8086")
                    influx_token = os.getenv("INFLUX_TOKEN", "cfuR3oHFeBlAbbiIxas5OcXhTY3CZxkz1_QNkAlVrCu48Y6osB-loG7UcGvP1RlN1lRugY7qsAPgHZiu3JteEA==")
                    influx_org = os.getenv("INFLUX_ORG", "herdmind")
                    influx_bucket = os.getenv("INFLUX_BUCKET", "herd_telemetry")
                    
                    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org, timeout=3000) as client:
                        with client.write_api(write_options=SYNCHRONOUS) as write_api:
                            point = Point("biological_risk") \
                                .tag("cow_id", str(cow_id)) \
                                .tag("diagnosis", str(event.get("diagnosis", "Unknown Anomaly"))) \
                                .field("risk_percentage", float(risk_score) * 100) \
                                .field("confidence", float(event.get("confidence", 0.0))) \
                                .time(int(now_ts), WritePrecision.S)
                            
                            write_api.write(bucket=influx_bucket, org=influx_org, record=point)
                            logger.info(f"💾 [Archival Sink] Logged long-term timeseries event for Cow {cow_id}")
                except Exception as influx_err:
                    logger.warning(f"⚠️ [Archival Sink] Long-term storage bypass: {influx_err}")
                    
            except Exception as e:
                logger.error(f"Layer 2 persistence pipeline failure: {e}")

            # ================= BROADCAST =================
            try:
                r.publish(ALERT_CHANNEL, event_json)
            except Exception as e:
                logger.error(f"Publish failed: {e}")

            logger.info(f"ALERT SENT: {cow_id} → {diagnosis}")

        except redis.exceptions.RedisError as e:
            logger.error(f"Redis link error inside loop: {e}")
            time.sleep(2)

        except Exception as e:
            logger.error(f"Worker processing error: {e}")

            # ================= DLQ =================
            if raw:
                try:
                    r.lpush(DLQ_KEY, raw)
                    logger.warning("Moved corrupt payload to DLQ channel.")
                except Exception as dlq_err:
                    logger.error(f"DLQ failed: {dlq_err}")

            time.sleep(2)


# ================= STARTUP =================
@app.on_event("startup")
def startup():
    global r

    logger.info("Connecting to Redis Core via Sync Driver...")

    for i in range(1, 11):
        try:
            r = redis.Redis(
                host="redis",
                socket_timeout=15.0,
                socket_connect_timeout=10.0,
                socket_keepalive=True,
                port=6379,
                decode_responses=True,
            )
            r.ping()
            logger.info("✅ Redis connected completely.")
            break
        except Exception as e:
            logger.warning(f"Redis connection retry ({i}/10): {e}")
            time.sleep(2)
    else:
        raise RuntimeError("Redis infrastructure communication failure.")

    # Spawn daemon thread worker loop to stay isolated from main server lifecycle
    threading.Thread(target=process_loop, daemon=True).start()


# ================= HEALTH =================
@app.get("/health")
def health():
    return {
        "status": "ok",
        "worker": "sync_thread_running"
    }
