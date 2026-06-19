import os
import json
import time
import asyncio
import logging
import redis.asyncio as redis
from fastapi import FastAPI

# Hardened Absolute Application Import Paths
from app.database.session import AsyncSessionLocal, verify_postgres_connection
from app.database.migrations import run_auto_migrations
from app.database.repositories.cows import CowRepository
from app.database.repositories.alerts import AlertRepository
from app.database.repositories.medical_logs import MedicalLogRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("herd_alert_service")

REDIS_HOST = os.getenv("REDIS_HOST", "herd_redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME = "herd:alerts:stream"
GROUP_NAME = "alert_service_group"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "worker_node_1")
OUTPUT_STREAM = "herd:alerts:stream"
HISTORY_STREAM = "herd:alerts:history"
DLQ_QUEUE = "herd:queue:dead_letters"
ALERT_COOLDOWN_SEC = 1800
MAX_HISTORY = 500

redis_client = None

class CombinedRiskAnomalyAgent:
    TEMP_LIMIT, RUMINATION_LIMIT, SPIKE_THRESHOLD_TEMP = 39.5, 200, 1.2  

    async def analyze(self, event: dict, r_conn) -> dict:
        cow_tag = str(event.get("cow_id", event.get("animal_id", "UNKNOWN")))
        try:
            temp, rum, score = float(event.get("temperature", 38.5)), float(event.get("rumination", 280)), float(event.get("risk_score", 0.0))
        except Exception:
            return {"cow_id": cow_tag, "alert_level": "CORRUPTED", "diagnosis": "malformed_data", "risk_score": 0.0, "temp_delta": 0.0, "recommendation": "Corrupted payload structural error."}

        cache_key = f"herd:matrix:cow:{cow_tag}"
        prev = await r_conn.hgetall(cache_key) or {}
        temp_delta = float(temp) - float(prev["last_temperature"]) if prev.get("last_temperature") else 0.0

        level, diagnosis, recommendation = "NORMAL", "healthy", "No action required."
        if temp >= self.TEMP_LIMIT and rum <= self.RUMINATION_LIMIT:
            level, diagnosis, recommendation = "CRITICAL", "mastitis_or_ketosis", "Immediate veterinary examination required"
        elif temp >= self.TEMP_LIMIT or temp_delta >= self.SPIKE_THRESHOLD_TEMP:
            level, diagnosis, recommendation = "CRITICAL", "hyperthermia", f"CRITICAL TEMP SPIKE: +{temp_delta:.2f}°C rise detected" if temp_delta >= self.SPIKE_THRESHOLD_TEMP else "Check infection markers and udder inflammation"
        elif rum <= self.RUMINATION_LIMIT:
            level, diagnosis, recommendation = "WARNING", "rumination_suppression", "Adjust feeding balance and hydration"
        elif score >= 0.80:
            level, diagnosis, recommendation = "WARNING", "pipeline_anomaly", "Verify sensor calibration and animal ID mapping"

        await r_conn.hset(cache_key, mapping={"last_temperature": str(temp), "last_rumination": str(rum), "updated_at": str(time.time())})
        await r_conn.expire(cache_key, 86400)
        return {"cow_id": cow_tag, "alert_level": level, "diagnosis": diagnosis, "recommendation": recommendation, "risk_score": round(max(score, 0.75) if level != "NORMAL" else score, 3), "temp_delta": round(temp_delta, 3)}

agent = CombinedRiskAnomalyAgent()

async def process_stream():
    logger.info(f"🚀 Consumer Group '{GROUP_NAME}' active on Node: '{CONSUMER_NAME}'")
    try: await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception: pass 

    while True:
        try:
            response = await redis_client.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=3000)
            if not response: continue

            for _, rows in response:
                for msg_id, payload in rows:
                    try:
                        raw = payload.get("data", "{}")
                        event = json.loads(raw)
                        if event.get("event_type") == "AGENT_ALERT":
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        result = await agent.analyze(event, redis_client)
                        if result["alert_level"] in ["NORMAL", "CORRUPTED"]:
                            if result["alert_level"] == "CORRUPTED": await redis_client.rpush(DLQ_QUEUE, raw)
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        cow_tag, disease = result["cow_id"], result["diagnosis"]
                        cd_key = f"cooldown:{cow_tag}:{disease}"
                        if await redis_client.exists(cd_key):
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue
                        await redis_client.setex(cd_key, ALERT_COOLDOWN_SEC, "1")

                        # Repository Transaction Execution Window
                        async with AsyncSessionLocal() as session:
                            async with session.begin():
                                cow_repo = CowRepository(session)
                                alert_repo = AlertRepository(session)
                                med_repo = MedicalLogRepository(session)

                                db_cow = await cow_repo.get_or_create_by_tag(cow_tag)
                                await alert_repo.create_alert(db_cow.id, result["alert_level"], result["risk_score"], str(msg_id))
                                await med_repo.create_log(db_cow.id, disease, result["recommendation"])

                        out = {"event_type": "AGENT_ALERT", "animal_id": cow_tag, "cow_id": cow_tag, "risk_score": result["risk_score"], "alert_level": result["alert_level"], "disease": disease, "message": result["recommendation"], "temp_delta": result["temp_delta"], "timestamp": time.time()}
                        serialized = json.dumps(out)

                        async with redis_client.pipeline(transaction=True) as pipe:
                            pipe.xadd(OUTPUT_STREAM, {"data": serialized})
                            pipe.xadd(HISTORY_STREAM, {"data": serialized}, maxlen=MAX_HISTORY)
                            pipe.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            await pipe.execute()
                        
                        logger.info(f"💾 [PostgreSQL Repositories Parity]: Alert committed for Cow {cow_tag}")

                    except Exception as ex:
                        logger.error(f"❌ Processing error on frame {msg_id}: {ex}")
                        await redis_client.rpush(DLQ_QUEUE, str(payload))
                        await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
        except Exception as e:
            logger.error(f"🚨 Master consumer loop failure: {e}")
            await asyncio.sleep(2)

app = FastAPI(title="HerdMind-X Alert Service (Repositories Core)", version="phase2-repository")

@app.on_event("startup")
async def startup_event():
    global redis_client
    logger.info("Initializing relational engine auto-migrations boot step...")
    await run_auto_migrations()
    if await verify_postgres_connection():
        redis_client = redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
        asyncio.create_task(process_stream())
