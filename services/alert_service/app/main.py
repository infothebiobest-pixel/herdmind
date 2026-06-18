import os
import json
import time
import asyncio
import logging
import redis.asyncio as redis
from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
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
    TEMP_LIMIT = 39.5
    RUMINATION_LIMIT = 200
    SPIKE_THRESHOLD_TEMP = 1.2  

    async def analyze(self, event: dict, r_conn) -> dict:
        cow = str(event.get("cow_id", event.get("animal_id", "UNKNOWN")))

        try:
            temp = float(event.get("temperature", 38.5))
            rum = float(event.get("rumination", 280))
            score = float(event.get("risk_score", 0.0))
        except Exception:
            return {
                "cow_id": cow,
                "alert_level": "CORRUPTED",
                "diagnosis": "malformed_data",
                "risk_score": 0.0,
                "recommendation": "Corrupted pipeline payload frame skipped."
            }

        cache_key = f"herd:matrix:cow:{cow}"
        previous_state = await r_conn.hgetall(cache_key) or {}

        temp_delta = 0.0
        if previous_state.get("last_temperature"):
            try:
                temp_delta = temp - float(previous_state["last_temperature"])
            except Exception:
                temp_delta = 0.0

        level = "NORMAL"
        diagnosis = "healthy"
        recommendation = "No action required."

        if temp >= self.TEMP_LIMIT and rum <= self.RUMINATION_LIMIT:
            level = "CRITICAL"
            diagnosis = "mastitis_or_ketosis"
            recommendation = "Immediate veterinary examination required"

        elif temp >= self.TEMP_LIMIT or temp_delta >= self.SPIKE_THRESHOLD_TEMP:
            level = "CRITICAL"
            diagnosis = "hyperthermia"
            if temp_delta >= self.SPIKE_THRESHOLD_TEMP:
                recommendation = f"CRITICAL TEMP SPIKE: +{temp_delta:.2f}°C sudden rise detected"
            else:
                recommendation = "Check infection markers and udder inflammation"

        elif rum <= self.RUMINATION_LIMIT:
            level = "WARNING"
            diagnosis = "rumination_suppression"
            recommendation = "Adjust feeding balance and hydration"

        elif score >= 0.80:
            level = "WARNING"
            diagnosis = "pipeline_anomaly"
            recommendation = "Verify sensor calibration and animal ID mapping"

        await r_conn.hset(cache_key, mapping={
            "last_temperature": str(temp),
            "last_rumination": str(rum),
            "updated_at": str(time.time())
        })
        await r_conn.expire(cache_key, 86400) 

        return {
            "cow_id": cow,
            "alert_level": level,
            "diagnosis": diagnosis,
            "recommendation": recommendation,
            "risk_score": round(max(score, 0.75) if level != "NORMAL" else score, 3),
            "temp_delta": round(temp_delta, 3)
        }

agent = CombinedRiskAnomalyAgent()

def cooldown_key(cow, diagnosis):
    return f"cooldown:{cow}:{diagnosis}"

async def throttled(cow, diagnosis):
    return await redis_client.exists(cooldown_key(cow, diagnosis))

async def activate(cow, diagnosis):
    await redis_client.setex(cooldown_key(cow, diagnosis), ALERT_COOLDOWN_SEC, "1")

async def process_stream():
    logger.info(f"🚀 Consumer Group '{GROUP_NAME}' active on Node: '{CONSUMER_NAME}'")
    
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass 

    while True:
        try:
            response = await redis_client.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=3000 
            )

            if not response:
                continue

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
                            if result["alert_level"] == "CORRUPTED":
                                await redis_client.rpush(DLQ_QUEUE, raw)
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        cow = result["cow_id"]
                        disease = result["diagnosis"]

                        if await throttled(cow, disease):
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        await activate(cow, disease)

                        out = {
                            "event_type": "AGENT_ALERT",
                            "animal_id": cow,
                            "cow_id": cow,
                            "risk_score": result["risk_score"],
                            "alert_level": result["alert_level"],
                            "disease": disease,
                            "message": result["recommendation"],
                            "temp_delta": result["temp_delta"],
                            "timestamp": time.time()
                        }
                        serialized = json.dumps(out)

                        async with redis_client.pipeline(transaction=True) as pipe:
                            pipe.xadd(OUTPUT_STREAM, {"data": serialized})
                            pipe.xadd(HISTORY_STREAM, {"data": serialized}, maxlen=MAX_HISTORY)
                            pipe.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            await pipe.execute()
                        
                        logger.info(f"🧠 [Agent Memory Match Alert]: Insight logged for Cow: {cow}")

                    except Exception as ex:
                        logger.error(f"❌ Transaction processing fault on frame {msg_id}: {ex}")
                        await redis_client.rpush(DLQ_QUEUE, str(payload))
                        await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)

        except Exception as e:
            logger.error(f"🚨 Master Agent core stream consumer thread exception: {e}")
            await asyncio.sleep(2)

app = FastAPI(
    title="HerdMind-X Alert Service (Hardened Agent)",
    version="phase2"
)

@app.on_event("startup")
async def startup_event():
    global redis_client
    logger.info("Initializing Redis async connections pool via native startup event path...")
    redis_client = redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    asyncio.create_task(process_stream())

@app.on_event("shutdown")
async def shutdown_event():
    if redis_client:
        await redis_client.close()

@app.get("/health")
async def health():
    return {"status": "ok", "service": "alert_service", "phase": 2, "cache_layer": "enabled"}
