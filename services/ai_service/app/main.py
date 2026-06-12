import os
import json
import time
import logging
import numpy as np
import requests
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import paho.mqtt.client as mqtt

# Production engines and metrics
from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier
from app.storage import (
    write_reading, 
    query_cow_history, 
    query_herd_summary, 
    query_high_risk_cows, 
    close as close_storage, 
    _query_api, 
    INFLUX_BUCKET
)
from app.temporal_engine import (
    rolling_stats, 
    risk_trend, 
    early_warning, 
    herd_early_warnings, 
    cow_timeline
)

log = logging.getLogger("herdmind_main")

MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "herd/sensors/#"
ALERT_URL = os.getenv("ALERT_SERVICE_URL", "http://herd_alert_service:8000/notify")
HERD_IDS = [101, 102, 103, 104, 105]
RISK_THRESHOLD = 0.80
CRITICAL_THRESHOLD = 0.90
COOLDOWN_SEC = 300
ALERT_COOLDOWN = {}

ai_engine = HerdAnomalyEngine()
disease_classifier = DiseaseClassifier()

class SensorPayload(BaseModel):
    cow_id: int
    temperature: float
    rumination: float
    activity: float
    milk_yield: float = 20.0
    conductivity: float = 5.0
    flow_rate: float = 2.5
    quarter_delta: float = 0.5
    lying_time: float = 12.0
    heart_rate: float = 70.0

def send_alert(payload, risk: float, level: str):
    body = {
        "cow_id": str(payload.cow_id), 
        "alert_level": level, 
        "disease": "unknown", 
        "risk_score": risk, 
        "temperature": payload.temperature, 
        "rumination": payload.rumination, 
        "activity": payload.activity, 
        "message": f"Risk:{risk:.2f}", 
        "action": "Inspect immediately"
    }
    for i in range(3):
        try:
            r = requests.post(ALERT_URL, json=body, timeout=3)
            print(f"🚨 ALERT cow={payload.cow_id} level={level} status={r.status_code}")
            return
        except Exception as e:
            wait = 2**i
            print(f"⚠ retry {i+1}/3 {e}")
            if i < 2: 
                time.sleep(wait)

def on_connect(c, u, f, rc):
    print(f"✅ MQTT rc={rc}")
    c.subscribe(MQTT_TOPIC)

def on_message(c, u, msg):
    try:
        p = SensorPayload(**json.loads(msg.payload.decode()))
        X = np.array([[p.temperature, p.rumination, p.activity, p.milk_yield, p.conductivity, p.flow_rate, p.quarter_delta, p.lying_time, p.heart_rate]])
        
        # FIX: Remove [0] tracking arrays since engine returns scalars smoothly
        pred = int(ai_engine.predict(X))
        
        # FIX: Directly use calibrated engine risk values to unlock scores scaling past 0.881
        risk = float(ai_engine.risk_score(X))
        
        cow_id = str(p.cow_id)
        alert = None
        now = time.time()
        if pred == -1 and risk >= RISK_THRESHOLD:
            level = "CRITICAL" if risk >= CRITICAL_THRESHOLD else "WARNING"
            if now - ALERT_COOLDOWN.get(cow_id, 0) > COOLDOWN_SEC:
                send_alert(p, risk, level)
                ALERT_COOLDOWN[cow_id] = now
                alert = {"alert_level": level, "message": f"Risk:{risk:.2f}"}
        write_reading(cow_id=p.cow_id, temperature=p.temperature, rumination=p.rumination, activity=p.activity, prediction=pred, risk_score=risk, alert=alert)
        print(f"💾 cow={p.cow_id} pred={pred} risk={risk:.3f}")
    except Exception as e:
        print(f"💥 MQTT Message Processing Fault: {e}")

mqtt_client = mqtt.Client(client_id="herdmind_v4")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🧠 Booting CLEAN AI service...")
    try:
        rng = np.random.default_rng(42)
        healthy = rng.normal([38.7,300,70,22,5,2.5,0.4,12,70],[0.2,20,8,2,0.3,0.2,0.1,0.8,4],(600,9))
        sick    = rng.normal([40.0,200,40,10,8,1.5,3.0,10,90],[0.4,30,10,3,1,0.3,1.0,1.2,6],(600,9))
        X = np.vstack([healthy, sick])
        y = np.array([0]*600 + [1]*600)
        ai_engine.train(X)
        disease_classifier.train(X, y)
        print("✅ Models trained (balanced)")
    except Exception as e:
        print(f"🚨 TRAIN ERROR: {e}")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        print("✅ MQTT started")
    except Exception as e:
        print(f"🚨 MQTT ERROR: {e}")
    yield
    mqtt_client.loop_start() # Guard fallback loop
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

app = FastAPI(title="HerdMind AI", version="4.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health(): 
    return {"status": "healthy"}

@app.get("/herd/summary")
def summary(window_hours: Optional[int] = 24): 
    return query_herd_summary(hours=window_hours)

@app.get("/herd/at-risk")
def at_risk(threshold: float = 0.8, hours: int = 24):
    c = query_high_risk_cows(threshold=threshold, hours=hours)
    return {"count": len(c), "cows": c}

@app.get("/herd/critical")
def critical():
    c = query_high_risk_cows(threshold=0.9, hours=1)
    return {"count": len(c), "cows": c}

@app.get("/cows/{cow_id}/history")
def history(cow_id: int, hours: int = 24): 
    return query_cow_history(str(cow_id), hours=hours)

@app.get("/cows/{cow_id}/latest")
def latest(cow_id: int):
    h = query_cow_history(str(cow_id), hours=24)
    if not h: 
        raise HTTPException(404, "No data")
    return sorted(h, key=lambda x: x["time"], reverse=True)[0]

@app.get("/alerts/recent")
def alerts(hours: int = 24, limit: int = 20):
    flux = f'from(bucket:"{INFLUX_BUCKET}")|>range(start:-{hours}h)|>filter(fn:(r)=>r._measurement=="cow_reading")|>filter(fn:(r)=>r._field=="risk_score")|>filter(fn:(r)=>r._value>=0.8)|>sort(columns:["_time"],desc:true)|>limit(n:{limit})'
    try:
        tables = _query_api.query(flux)
        return {"count": 0, "alerts": [{"time": r.get_time().isoformat(), "cow_id": r.values.get("cow_id"), "risk_score": round(r.get_value(),4), "alert_level": r.values.get("alert_level","WARNING")} for t in tables for r in t.records]}
    except Exception as e:
        return {"count": 0, "alerts": [], "error": str(e)}

@app.get("/temporal/cow/{cow_id}/trend")
def trend(cow_id: str, hours: int = 3): 
    return risk_trend(cow_id, hours=hours)

# FIX: Patched broken URL bracket layout string syntax error
@app.get("/temporal/cow/{cow_id}/stats/{field}")
def stats(cow_id: str, field: str, hours: int = 6): 
    return rolling_stats(cow_id, field, hours=hours)

@app.get("/temporal/cow/{cow_id}/warning")
def warning(cow_id: str): 
    return early_warning(cow_id)

@app.get("/temporal/cow/{cow_id}/timeline")
def timeline(cow_id: str, hours: int = 24): 
    return cow_timeline(cow_id, hours=hours)

@app.get("/temporal/herd/warnings")
def herd_warn(level: str = "WATCH"):
    r = herd_early_warnings(HERD_IDS, min_level=level)
    return {"count": len(r), "warnings": r}
