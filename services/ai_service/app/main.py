import os
import json
import numpy as np
import requests
import asyncio
import time
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime

from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier, DISEASES
from app.storage import write_reading, query_cow_history, query_herd_summary, query_high_risk_cows, close as close_storage, _query_api, INFLUX_BUCKET

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MQTT_BROKER       = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT         = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC        = "herd/sensors/#"
# Fixed to point to internal container network port 8000 instead of external 8002
ALERT_SERVICE_URL = os.getenv("ALERT_SERVICE_URL", "http://alert_service:8000/notify")

# ---------------------------------------------------------------------------
# ML Engines
# ---------------------------------------------------------------------------
ai_engine          = HerdAnomalyEngine()
disease_classifier = DiseaseClassifier()

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class SensorPayload(BaseModel):
    cow_id:        int
    temperature:   float
    rumination:    float
    activity:      float
    milk_yield:    float = 20.0
    conductivity:  float = 5.0
    flow_rate:     float = 2.5
    quarter_delta: float = 0.5
    lying_time:    float = 12.0
    heart_rate:    float = 70.0

# ---------------------------------------------------------------------------
# Alert dispatcher with retry logic
# ---------------------------------------------------------------------------
def dispatch_alert(payload, risk: float, level: str) -> None:
    alert_request = {
        "cow_id":      str(payload.cow_id),
        "alert_level": level,
        "disease":     "Unspecified Bio-Shift" if risk < 0.85 else "Mastitis Indicators",
        "risk_score":  risk,
        "temperature": payload.temperature,
        "rumination":  payload.rumination,
        "activity":    payload.activity,
        "message":     f"ML model flagged outlier pattern. Risk index: {risk:.2f}",
        "action":      "Inspect cow vitals and isolate from herd immediately.",
    }
    for attempt in range(3):
        try:
            res = requests.post(ALERT_SERVICE_URL, json=alert_request, timeout=3.0)
            if res.status_code == 200:
                print(f"🚨 [Alert Dispatched] Cow {payload.cow_id} tier={level} (Attempt {attempt+1})")
                return
            else:
                print(f"⚠️  [Alert Warning] status={res.status_code} cow={payload.cow_id} (Attempt {attempt+1})")
        except Exception as e:
            wait = 2 ** attempt
            print(f"💥 [Alert Retry {attempt+1}/3] Failed to connect: {e} — retrying in {wait}s")
            if attempt < 2:
                time.sleep(wait)
            else:
                print(f"❌ [Alert Failed] All retries exhausted cow={payload.cow_id}")

# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Connected to MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to stream: {MQTT_TOPIC}")
    else:
        print(f"❌ MQTT Connection failed rc={rc}")

def on_message(client, userdata, msg):
    try:
        payload_data = json.loads(msg.payload.decode("utf-8"))
        payload      = SensorPayload(**payload_data)

        X = np.array([[
            payload.temperature, payload.rumination, payload.activity,
            payload.milk_yield,  payload.conductivity, payload.flow_rate,
            payload.quarter_delta, payload.lying_time, payload.heart_rate
        ]])

        preds  = ai_engine.predict(X)
        scores = ai_engine.risk_score(X)

        prediction = int(preds[0]) if hasattr(preds, "__len__") else int(preds)
        risk       = float(scores[0]) if hasattr(scores, "__len__") else float(scores)

        alert_data = None
        if prediction == -1:
            level      = "CRITICAL" if risk >= 0.75 else "WARNING"
            alert_data = {"alert_level": level, "message": f"Risk index: {risk:.2f}"}
            dispatch_alert(payload, risk, level)

        write_reading(
            cow_id      = payload.cow_id,
            temperature = payload.temperature,
            rumination  = payload.rumination,
            activity    = payload.activity,
            prediction  = prediction,
            risk_score  = risk,
            alert       = alert_data,
        )
        print(f"💾 [Influx Ingested] Cow {payload.cow_id} -> Prediction:{prediction} Risk:{risk:.3f}")

    except Exception as e:
        print(f"💥 Ingestion processing failure: {e}")

mqtt_client = mqtt.Client(client_id="herd_ai_main_v2")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# ---------------------------------------------------------------------------
# Lifespan — bootstrap models + start MQTT
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🧠 Bootstrapping AI Engines with disease-aware baseline...")
    try:
        rng = np.random.default_rng(42)

        healthy  = rng.normal([38.7,300,70,22,5.0,2.5,0.4,12,70], [0.2,20,8,2,0.3,0.2,0.1,0.8,4],   (700,9))
        mastitis = rng.normal([39.8,240,50,14,9.5,1.4,4.2,10,90], [0.3,25,10,3,1.0,0.3,0.8,1.0,6],  (100,9))
        ketosis  = rng.normal([38.1,160,25,11,5.1,2.4,0.5,17,65], [0.2,20,8, 2,0.3,0.2,0.1,1.0,4],  (100,9))
        lameness = rng.normal([38.6,210,22,16,5.2,2.3,0.4,15,78], [0.2,25,8, 2,0.3,0.2,0.1,1.2,5],  (60, 9))
        noise    = rng.normal([42.0,580,260,5,5.0,2.5,0.4,12,70], [0.5,10,15,1,0.2,0.1,0.1,0.5,3],  (40, 9))

        base_all = np.vstack([healthy, mastitis, ketosis, lameness, noise])
        labels   = np.array([0]*700 + [1]*100 + [2]*100 + [3]*60 + [4]*40)

        ai_engine.train(healthy)
        disease_classifier.train(base_all, labels)
        print("✅ Core baseline models initialized!")
    except Exception as ex:
        print(f"🚨 Initialization failed: {ex}")

    print("🔄 Spawning MQTT listener...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"🚨 MQTT failed: {e}")

    yield

    print("🛑 Shutting down...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="HerdMind AI Service", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health_check():
    return "healthy"

@app.post("/predict")
def predict(payload: SensorPayload):
    X      = np.array([[payload.temperature, payload.rumination, payload.activity,
                        payload.milk_yield, payload.conductivity, payload.flow_rate,
                        payload.quarter_delta, payload.lying_time, payload.heart_rate]])
    preds  = ai_engine.predict(X)
    scores = ai_engine.risk_score(X)
    return {"cow_id": payload.cow_id,
            "prediction": int(preds[0]),
            "risk_score": float(scores[0])}

@app.get("/cows/{cow_id}/history")
def get_cow_history(cow_id: int, hours: Optional[int] = 24):
    return query_cow_history(str(cow_id), hours=hours)

@app.get("/cows/{cow_id}/latest")
def get_cow_latest(cow_id: int):
    history = query_cow_history(str(cow_id), hours=24)
    if not history:
        raise HTTPException(status_code=404, detail="No logs found recently")
    return history[-1]

@app.get("/herd/summary")
def get_herd_summary_endpoint(window_hours: Optional[int] = 24):
    return query_herd_summary(hours=window_hours)
