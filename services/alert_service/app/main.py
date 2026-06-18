import os
import json
import time
import asyncio
import logging
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("herd_alert_service")

# =====================================================
# CONFIG
# =====================================================
REDIS_HOST = os.getenv("REDIS_HOST", "herd_redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

STREAM_NAME = "herd:alerts:stream"
GROUP_NAME = "alert_service_group"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "worker_node_1")

OUTPUT_STREAM = "herd:alerts:out"
HISTORY_STREAM = "herd:alerts:history"
DLQ_QUEUE = "herd:queue:dead_letters"

ALERT_COOLDOWN_SEC = 1800
MAX_HISTORY = 500

redis_client = None
worker_task = None


# =====================================================
# AGENT
# =====================================================
class CombinedRiskAnomalyAgent:

    TEMP_LIMIT = 39.5
    RUMINATION_LIMIT = 200
    SPIKE_THRESHOLD_TEMP = 1.2

    async def analyze(self, event, r_conn):

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
                "recommendation": "bad payload",
                "temp_delta": 0.0
            }

        cache_key = f"herd:matrix:cow:{cow}"
        prev = await r_conn.hgetall(cache_key) or {}

        temp_delta = 0.0
        if prev.get("last_temperature"):
            try:
                temp_delta = temp - float(prev["last_temperature"])
            except Exception:
                temp_delta = 0.0

        level = "NORMAL"
        diagnosis = "healthy"
        recommendation = "ok"

        if temp >= self.TEMP_LIMIT and rum <= self.RUMINATION_LIMIT:
            level = "CRITICAL"
            diagnosis = "mastitis_ketosis"

        elif temp >= self.TEMP_LIMIT or temp_delta >= self.SPIKE_THRESHOLD_TEMP:
            level = "CRITICAL"
            diagnosis = "hyperthermia"

        elif rum <= self.RUMINATION_LIMIT:
            level = "WARNING"
            diagnosis = "rumination_suppression"

        elif score >= 0.80:
            level = "WARNING"
            diagnosis = "pipeline_anomaly"

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


# =====================================================
# COOLDOWN
# =====================================================
async def throttled(r, cow, diag):
    return await r.exists(f"cooldown:{cow}:{diag}")

async def activate(r, cow, diag):
    await r.setex(f"cooldown:{cow}:{diag}", ALERT_COOLDOWN_SEC, "1")


# =====================================================
# WORKER
# =====================================================
async def process_stream():

    logger.info("🚀 Alert worker started")

    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass

    while True:
        try:

            resp = await redis_client.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=3000
            )

            if not resp:
                continue

            for _, rows in resp:
                for msg_id, payload in rows:

                    try:
                        raw = payload.get("data", "{}")
                        event = json.loads(raw)

                        # 🔥 CRITICAL FILTER
                        if event.get("event_type") == "AGENT_ALERT":
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        # FIXED LINE (YOUR ISSUE)
                        result = await agent.analyze(event, redis_client)

                        if result["alert_level"] == "NORMAL":
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        cow = result["cow_id"]
                        diag = result["diagnosis"]

                        if await throttled(redis_client, cow, diag):
                            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        await activate(redis_client, cow, diag)

                        out = {
                            "event_type": "AGENT_ALERT",
                            "cow_id": cow,
                            "risk_score": result["risk_score"],
                            "alert_level": result["alert_level"],
                            "disease": diag,
                            "message": result["recommendation"],
                            "temp_delta": result["temp_delta"],
                            "timestamp": time.time()
                        }

                        serialized = json.dumps(out)

                        pipe = redis_client.pipeline(transaction=True)
                        pipe.xadd(OUTPUT_STREAM, {"data": serialized})
                        pipe.xadd(HISTORY_STREAM, {"data": serialized})
                        pipe.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        await pipe.execute()

                    except Exception as e:
                        logger.error(f"msg error: {e}")
                        await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)

        except Exception as e:
            logger.error(f"worker loop error: {e}")
            await asyncio.sleep(2)


# =====================================================
# LIFESPAN
# =====================================================
@asynccontextmanager
async def lifespan(app: FastAPI):

    global redis_client, worker_task

    logger.info("Connecting Redis...")

    redis_client = redis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True
    )

    worker_task = asyncio.create_task(process_stream())

    yield

    worker_task.cancel()
    await redis_client.close()


app = FastAPI(
    title="HerdMind-X Alert Service",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    return {"status": "ok", "phase": "stable"}
