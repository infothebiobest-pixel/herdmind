import os
import json
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import paho.mqtt.client as mqtt
from contextlib import asynccontextmanager
from typing import Optional

# 1. Native Architectural Package Imports (Linking your real local files)
from app.ai.engines.prediction_engine import HerdAnomalyEngine
from app.storage import write_reading, query_cow_history, query_herd_summary, query_high_risk_cows, close as close_storage

# 2. Network Configuration Parameters
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "herd/sensors/#"

# 3. Instantiate the Prediction Engine Core
ai_engine = HerdAnomalyEngine()

# 4. Request Pydantic Schema Validation Layer
class SensorPayload(BaseModel):
    cow_id: int
    temperature: float
    rumination: float
    activity: float

# 5. Define Background MQTT Client Event Logic
def on_connect(client, userdata, flags, rc):
    """Triggers automatically when connecting to the broker container."""
    if rc == 0:
        print(f"✅ Connected to MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to stream: {MQTT_TOPIC}")
    else:
        print(f"❌ MQTT Connection failed with code: {rc}")

def on_message(client, userdata, msg):
    """Intercepts telemetries, calculates risk indices, and records to InfluxDB."""
    try:
        payload_data = json.loads(msg.payload.decode("utf-8"))
        payload = SensorPayload(**payload_data)
        
        X = np.array([[payload.temperature, payload.rumination, payload.activity]])
        prediction = int(ai_engine.predict(X)[0])  
        risk = float(ai_engine.risk_score(X)[0])
        
        # Build alert payload structure if flagged as an anomaly
        alert_data = None
        if prediction == -1:
            alert_data = {
                "alert_level": "CRITICAL",
                "message": "Critical bio shift anomaly detected"
            }
        
        # Write metrics out using your native storage.py InfluxDB logic
        write_reading(
            cow_id=payload.cow_id,
            temperature=payload.temperature,
            rumination=payload.rumination,
            activity=payload.activity,
            prediction=prediction,
            risk_score=risk,
            alert=alert_data
        )
        print(f"💾 [Influx Ingested] Cow ID {payload.cow_id} -> Prediction: {prediction}, Risk: {risk}")

        # Alert bus dispatch if classification flags an anomaly
        if prediction == -1:
            alert_payload = {"cow_id": payload.cow_id, "risk_score": risk, "type": "critical bio shift"}
            client.publish("herd/alerts/critical", json.dumps(alert_payload))
            
    except Exception as e:
        print(f"💥 Ingestion processing failure: {e}")

# Construct the socket handler network client structure
mqtt_client = mqtt.Client(client_id="herd_ai_service_client")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# 6. Modern Lifespan Application Manager Layer
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles thread lifecycle bindings."""
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

# 7. Initialize FastAPI Application Hooked to Lifespan Context
app = FastAPI(title="HerdMind AI Service", lifespan=lifespan)

# 8. Standard System Endpoints
@app.get("/health", response_model=str)
def health_check(): 
    return "healthy"

@app.post("/predict")
def predict(payload: SensorPayload):
    X = np.array([[payload.temperature, payload.rumination, payload.activity]])
    return {
        "cow_id": payload.cow_id,
        "prediction": int(ai_engine.predict(X)[0]),
        "risk_score": float(ai_engine.risk_score(X)[0])
    }

# 9. Time-Window Bounded Analytical Endpoints matching your Gateway config
@app.get("/cows/{cow_id}/history")
def get_cow_history_endpoint(cow_id: int, hours: Optional[int] = 24):
    history = query_cow_history(str(cow_id), hours=hours)
    return history

@app.get("/cows/{cow_id}/latest")
def get_cow_latest(cow_id: int):
    # Fetch latest by looking back 1 hour using history helper
    history = query_cow_history(str(cow_id), hours=1)
    if not history: 
        raise HTTPException(status_code=404, detail="No logs found recently")
    return history[-1]

@app.get("/herd/summary")
def get_herd_summary_endpoint(window_hours: Optional[int] = 24):
    return query_herd_summary(hours=window_hours)

@app.get("/herd/at-risk")
def get_herd_at_risk():
    # Warning metrics threshold context (0.5 to 0.8)
    cows = query_high_risk_cows(threshold=0.5, hours=1)
    return [c["cow_id"] for c in cows if c["risk_score"] < 0.8]

@app.get("/herd/critical")
def get_herd_critical():
    # Critical threshold context (>= 0.8)
    cows = query_high_risk_cows(threshold=0.8, hours=1)
    return [c["cow_id"] for c in cows]

# 10. Main Execution Module Loop Guard
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=False)
