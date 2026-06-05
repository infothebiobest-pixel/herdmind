import os
import json
import numpy as np
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import paho.mqtt.client as mqtt
from contextlib import asynccontextmanager
from typing import Optional

# 1. Native Architectural Package Imports
from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier, DISEASES
from app.storage import write_reading, query_cow_history, query_herd_summary, query_high_risk_cows, close as close_storage

# 2. Network Configuration Parameters
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "herd/sensors/#"
ALERT_SERVICE_URL = os.getenv("ALERT_SERVICE_URL", "http://herd_alert_service:8002/notify")

# 3. Instantiate the Prediction Engines
ai_engine = HerdAnomalyEngine()
disease_classifier = DiseaseClassifier()

# 4. Request Pydantic Schema Validation Layer
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

# 5. Define Background MQTT Client Event Logic
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Connected to MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to stream: {MQTT_TOPIC}")
    else:
        print(f"❌ MQTT Connection failed with code: {rc}")

def on_message(client, userdata, msg):
    """Intercepts telemetries, runs ML inference, saves to InfluxDB, and fires out-of-band alerts."""
    try:
        payload_data = json.loads(msg.payload.decode("utf-8"))
        payload = SensorPayload(**payload_data)
        
        # 9-Feature ML Input Matrix
        X = np.array([[
            payload.temperature, payload.rumination, payload.activity,
            payload.milk_yield, payload.conductivity, payload.flow_rate,
            payload.quarter_delta, payload.lying_time, payload.heart_rate
        ]])
        
        preds = ai_engine.predict(X)
        scores = ai_engine.risk_score(X)
        
        prediction = int(preds) if hasattr(preds, "__len__") else int(preds)
        risk = float(scores) if hasattr(scores, "__len__") else float(scores)
        
        alert_data = None
        
        # If ML flags an anomaly, escalate to the Alert Service
        if prediction == -1:
            level = "CRITICAL" if risk >= 0.75 else "WARNING"
            alert_data = {
                "alert_level": level,
                "message": f"Machine learning model flagged anomaly pattern. Risk profile index: {risk:.2f}"
            }
            
            # Formulate the payload expected by the Alert Service AlertRequest schema
            alert_request_payload = {
                "cow_id": str(payload.cow_id),
                "alert_level": level,
                "disease": "Unspecified Bio-Shift" if risk < 0.85 else "Mastitis Indicators",
                "risk_score": risk,
                "temperature": payload.temperature,
                "rumination": payload.rumination,
                "activity": payload.activity,
                "message": alert_data["message"],
                "action": "Inspect cow vitals and isolate from herd immediately."
            }
            
            # Post directly to the Alert Microservice over the internal network fabric
            try:
                res = requests.post(ALERT_SERVICE_URL, json=alert_request_payload, timeout=3.0)
                if res.status_code == 200:
                    print(f"🚨 [Alert Dispatched] Router notified successfully for Cow {payload.cow_id}")
                else:
                    print(f"⚠️ [Alert Failed] Downstream routing microservice rejected post: {res.status_code}")
            except Exception as http_err:
                print(f"💥 [Alert Connection Broken] Could not reach alert_service: {http_err}")
        
        # Record everything into InfluxDB
        write_reading(
            cow_id=payload.cow_id,
            temperature=payload.temperature,
            rumination=payload.rumination,
            activity=payload.activity,
            prediction=prediction,
            risk_score=risk,
            alert=alert_data
        )
        print(f"💾 [Influx Ingested] Cow ID {payload.cow_id} -> Prediction: {prediction}, Risk: {risk:.3f}")
            
    except Exception as e:
        print(f"💥 Ingestion processing failure: {e}")

mqtt_client = mqtt.Client(client_id="herd_ai_service_client")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# 6. Lifespan Application Manager with Automated Self-Training Layer
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🧠 Bootstrapping AI Engines with synthetic training matrices...")
    try:
        np.random.seed(42)
        base_healthy = np.random.normal(
            loc=[38.7, 450.0, 90.0, 22.0, 5.0, 2.6, 0.4, 11.0, 72.0], 
            scale=[0.8, 80.0, 30.0, 5.0, 2.0, 1.0, 0.3, 3.0, 12.0],
            size=(1000, 9)
        )
        ai_engine.train(base_healthy)
        labels = np.zeros(1000, dtype=int)
        disease_classifier.train(base_healthy, labels)
        print("✅ Machine Learning models trained and initialized successfully!")
    except Exception as ex:
        print(f"🚨 Initialization of ML Models failed: {ex}")

    print("🔄 Spawning background thread telemetry engine layers...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()  
    except Exception as e:
        print(f"🚨 Background socket initialization failed to bind: {e}")
        
    yield  
    
    print("🛑 Unbinding system messaging connections...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

app = FastAPI(title="HerdMind AI Service", lifespan=lifespan)

@app.get("/health", response_model=str)
def health_check(): 
    return "healthy"

@app.post("/predict")
def predict(payload: SensorPayload):
    X = np.array([[
        payload.temperature, payload.rumination, payload.activity,
        payload.milk_yield, payload.conductivity, payload.flow_rate,
        payload.quarter_delta, payload.lying_time, payload.heart_rate
    ]])
    preds = ai_engine.predict(X)
    scores = ai_engine.risk_score(X)
    p_val = int(preds) if hasattr(preds, "__len__") else int(preds)
    r_val = float(scores) if hasattr(scores, "__len__") else float(scores)
    return {"cow_id": payload.cow_id, "prediction": p_val, "risk_score": r_val}

@app.get("/cows/{cow_id}/history")
def get_cow_history_endpoint(cow_id: int, hours: Optional[int] = 24):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=False)
