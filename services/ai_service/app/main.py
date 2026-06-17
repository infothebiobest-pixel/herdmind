import os
import json
import time
import logging
import numpy as np
import redis
import requests
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import paho.mqtt.client as mqtt

from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier
from app.storage import write_reading, close as close_storage

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("herdmind_ai")

# ================= ENV =================
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "herd/sensors/#"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

RISK_SERVICE_URL = os.getenv("RISK_SERVICE_URL", "http://risk_service:8003/evaluate")

RISK_THRESHOLD = 0.80
CRITICAL_THRESHOLD = 0.90
COOLDOWN_SEC = 300
ALERT_COOLDOWN = {}

# ================= MODELS =================
ai_engine = HerdAnomalyEngine()
disease_classifier = DiseaseClassifier()

# ================= SCHEMA (LOOSENED FOR GATEWAY AGGREGATION) =================
from typing import Union
class SensorPayload(BaseModel):
    cow_id: Union[int, str]  # Changed from int to str to allow both "101" and "COW-101"
    temperature: float = 38.5
    rumination: float = 280.0
    activity: float = 70.0
    milk_yield: float = 20.0
    conductivity: float = 5.0
    flow_rate: float = 2.5
    quarter_delta: float = 0.5
    lying_time: float = 12.0
    heart_rate: float = 70.0

# ================= ALERT QUEUE =================
def send_alert(payload, risk: float, level: str):
    body = {
        "cow_id": str(payload.cow_id),
        "risk_score": risk,
        "metrics": payload.model_dump(),
        "alert_level": level
    }
    try:
        r_client.lpush("herd:queue:raw_anomalies", json.dumps(body))
        log.info(f"📥 Alert queued for cow={payload.cow_id}")
    except Exception as e:
        log.error(f"Redis queue error: {e}")

# ================= MQTT =================
def on_connect(client, userdata, flags, rc):
    log.info(f"MQTT connected rc={rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        p = SensorPayload(**json.loads(msg.payload.decode()))

        X = np.array([[
            p.temperature,
            p.rumination,
            p.activity,
            p.milk_yield,
            p.conductivity,
            p.flow_rate,
            p.quarter_delta,
            p.lying_time,
            p.heart_rate
        ]])

        pred = int(ai_engine.predict(X))
        risk = float(ai_engine.risk_score(X))

        cow_id = str(p.cow_id)
        now = time.time()

        # ================= INTEGRATED DISEASE MODEL =================
        # Extracts string classification label directly from internal method output
        ml_label = str(disease_classifier.predict_disease(X)).upper()

        # ================= RULE ENGINE =================
        if p.temperature > 39.5:
            rule_label = "CRITICAL_HYPERTHERMIA"
        elif p.rumination < 200:
            rule_label = "SEVERE_RUMINATION_DROP"
        else:
            rule_label = "NORMAL"

        # ================= ALERT LOGIC =================
        alert = None
        if (pred == -1 and risk >= RISK_THRESHOLD) or p.conductivity > 8.0:

            level = "CRITICAL" if risk >= CRITICAL_THRESHOLD or p.conductivity > 10 else "WARNING"

            if now - ALERT_COOLDOWN.get(cow_id, 0) > COOLDOWN_SEC:
                send_alert(p, risk, level)
                ALERT_COOLDOWN[cow_id] = now

                alert = {
                    "alert_level": level,
                    "message": f"Risk:{risk:.2f}"
                }

        # ================= RISK SERVICE SYNC =================
        try:
            payload = {
                "cow_id": cow_id,
                "anomaly_score": risk,
                "rule_label": rule_label,
                "ml_label": ml_label
            }

            res = requests.post(RISK_SERVICE_URL, json=payload, timeout=2)

            if res.ok:
                evaluation = res.json()
                action = evaluation.get("action")

                log.info(
                    f"🔮 Cow={cow_id} | Disease={ml_label} | Action={action}"
                )

                if alert:
                    alert["message"] = f"{ml_label} ({risk:.2f}) - {action}"
            else:
                log.warning(f"Risk service error {res.status_code}")

        except Exception as e:
            log.error(f"Risk sync failed: {e}")

        # ================= STORAGE =================
        write_reading(
            cow_id=p.cow_id,
            temperature=p.temperature,
            rumination=p.rumination,
            activity=p.activity,
            prediction=pred,
            risk_score=risk,
            alert=alert
        )

        log.info(f"💾 cow={cow_id} pred={pred} risk={risk:.3f}")

    except Exception as e:
        log.error(f"MQTT processing error: {e}")

# ================= FASTAPI LIFESPAN =================
mqtt_client = mqtt.Client(client_id="herdmind_ai")

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🧠 Starting AI Service...")

    rng = np.random.default_rng(42)

    healthy = rng.normal([38.7,300,70,22,5,2.5,0.4,12,70],
                         [0.2,20,8,2,0.3,0.2,0.1,0.8,4],
                         (600,9))

    sick = rng.normal([40.0,200,40,10,8,1.5,3.0,10,90],
                      [0.4,30,10,3,1,0.3,1.0,1.2,6],
                      (600,9))

    X = np.vstack([healthy, sick])
    y = np.array([0]*600 + [1]*600)

    ai_engine.train(X)
    disease_classifier.train(X, y)

    log.info("✅ Models trained")

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    log.info("✅ MQTT running")

    yield

    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

app = FastAPI(title="HerdMind AI Service", version="5.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ================= CLEAN API ENDPOINTS =================
@app.post("/ai/analyze")
def analyze(payload: SensorPayload):

    X = np.array([[
        payload.temperature,
        payload.rumination,
        payload.activity,
        payload.milk_yield,
        payload.conductivity,
        payload.flow_rate,
        payload.quarter_delta,
        payload.lying_time,
        payload.heart_rate
    ]])

    pred = int(ai_engine.predict(X))
    risk = float(ai_engine.risk_score(X))

    level = (
        "CRITICAL" if risk >= CRITICAL_THRESHOLD else
        "WARNING" if risk >= RISK_THRESHOLD else
        "NORMAL"
    )

    disease = (
        "Mastitis Risk" if payload.conductivity > 7 else
        "Ketosis Risk" if payload.rumination < 200 else
        "Hyperthermia Risk" if payload.temperature > 39.5 else
        "Normal"
    )

    return {
        "cow_id": str(payload.cow_id),
        "prediction": pred,
        "risk_score": round(risk, 4),
        "alert_level": level,
        "disease": disease
    }

@app.get("/health")
def health():
    return {"status": "healthy"}
