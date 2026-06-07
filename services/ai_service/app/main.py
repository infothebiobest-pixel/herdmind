import os
import json
import os
import numpy as np
import requests
import asyncio
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional
from datetime import datetime

# 1. Native Architectural Package Imports
from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier, DISEASES
from app.storage import write_reading, query_cow_history, query_herd_summary, query_high_risk_cows, close as close_storage, _query_api, INFLUX_BUCKET

# 2. Network Configuration Parameters
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "herd/sensors/#"
ALERT_SERVICE_URL = os.getenv("ALERT_SERVICE_URL", "http://herd_alert_service:8002/notify")

# 3. Instantiate the Prediction Engines globally
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
    """Intercepts telemetries, runs ML inference via the active model, and routes alerts."""
    try:
        payload_data = json.loads(msg.payload.decode("utf-8"))
        payload = SensorPayload(**payload_data)
        
        # 9-Feature Input Matrix matching prediction_engine expectation
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
        if prediction == -1:
            level = "CRITICAL" if risk >= 0.75 else "WARNING"
            alert_data = {
                "alert_level": level,
                "message": f"Retrained ML model flagged outlier pattern. Risk index: {risk:.2f}"
            }
            
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
            
            try:
                res = requests.post(ALERT_SERVICE_URL, json=alert_request_payload, timeout=3.0)
                if res.status_code == 200:
                    print(f"🚨 [Alert Dispatched] Router notified successfully for Cow {payload.cow_id}")
            except Exception as http_err:
                print(f"💥 [Alert Connection Broken] Could not reach alert_service: {http_err}")
        
        # Record into InfluxDB
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

mqtt_client = mqtt.Client(client_id="herd_ai_main_v2")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# 6. ML Retraining Engine Subsystem
async def ml_retraining_worker():
    """Background task loop that re-fits the ML model using real collected history data."""
    while True:
        print("🔄 [ML Pipeline] Scanning InfluxDB for historical training logs...")
        flux = f"""
        from(bucket: "{INFLUX_BUCKET}")

          |> range(start: -30d)
          |> filter(fn: (r) => r._measurement == "cow_reading")
          |> pivot(rowKey: ["_time", "cow_id"], columnKey: ["_field"], valueColumn: "_value")
        """
        try:
            tables = _query_api.query(flux)
            records = []
            for table in tables:
                for record in table.records:
                    v = record.values
                    # Extract full 9-feature dimensions safely with defaults if values skip a window
                    row = [
                        float(v.get("temperature", 38.7)),
                        float(v.get("rumination", 450.0)),
                        float(v.get("activity", 90.0)),
                        float(v.get("milk_yield", 22.0)),
                        float(v.get("conductivity", 5.0)),
                        float(v.get("flow_rate", 2.6)),
                        float(v.get("quarter_delta", 0.4)),
                        float(v.get("lying_time", 11.0)),
                        float(v.get("heart_rate", 72.0))
                    ]
                    records.append(row)
            
            # Require a small critical mass of records to update weights securely
            if len(records) >= 500:
                print(f"📈 [ML Pipeline] Extracted {len(records)} clean historical points. Updating model weights...")
                X_train = np.array(records)
                ai_engine.train(X_train)
                
                labels = np.zeros(len(records), dtype=int)
                disease_classifier.train(X_train, labels)
                print("✨ [ML Pipeline] Retraining complete! Production model weights updated successfully.")
            else:
                print(f"ℹ️ [ML Pipeline] Only {len(records)} points found. Postponing re-fit until threshold (500) is hit.")
        except Exception as query_err:
            print(f"🚨 [ML Pipeline] Historical query failed: {query_err}")
            
        # Run the scan every 12 Hours (43200 seconds)
        await asyncio.sleep(43200)

# 7. Lifespan Application Manager with Bootstrapping and Task Loops
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🧠 Bootstrapping AI Engines with synthetic matrices for startup safety...")
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
        print("✅ Core baseline models initialized completely!")
    except Exception as ex:
        print(f"🚨 Initialization of baseline failed: {ex}")

    print("🔄 Spawning background thread telemetry engine layers...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"🚨 Background socket initialization failed to bind: {e}")
        
    # Launch the ML Retraining background engine thread loop
    # retrain_task = asyncio.create_task(ml_retraining_worker())  # disabled: corrupts model with healthy-only data
    retrain_task = None
        
    yield  
    
    print("🛑 Unbinding system messaging connections...")
    if retrain_task: retrain_task.cancel()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

app = FastAPI(title="HerdMind AI Service (Enterprise)", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
    latest = sorted(history, key=lambda x: x["time"], reverse=True)[0]
    return latest

@app.get("/herd/summary")
def get_herd_summary_endpoint(window_hours: Optional[int] = 24):
    return query_herd_summary(hours=window_hours)



from app.temporal_engine import rolling_stats, risk_trend, early_warning, herd_early_warnings, cow_timeline

HERD_IDS = [101, 102, 103, 104, 105]

@app.get("/temporal/cow/{cow_id}/trend")
def cow_risk_trend(cow_id: str, hours: int = 3):
    return risk_trend(cow_id, hours=hours)

@app.get("/temporal/cow/{cow_id}/stats/{field}")
def cow_field_stats(cow_id: str, field: str, hours: int = 6):
    return rolling_stats(cow_id, field, hours=hours)

@app.get("/temporal/cow/{cow_id}/warning")
def cow_early_warning(cow_id: str):
    return early_warning(cow_id)

@app.get("/temporal/cow/{cow_id}/timeline")
def cow_full_timeline(cow_id: str, hours: int = 24):
    return cow_timeline(cow_id, hours=hours)

@app.get("/temporal/herd/warnings")
def herd_warnings(level: str = "WATCH"):
    results = herd_early_warnings(HERD_IDS, min_level=level)
    return {"min_level": level, "warnings": results, "count": len(results)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8001, reload=False)


@app.get("/herd/at-risk")
def get_at_risk(threshold: float = 0.8, hours: int = 24):
    return {"threshold": threshold, "hours": hours, "count": 0, "cows": query_high_risk_cows(threshold=threshold, hours=hours)}

@app.get("/herd/critical")
def get_critical():
    cows = query_high_risk_cows(threshold=0.9, hours=1)
    return {"count": len(cows), "cows": cows}


@app.get("/alerts/recent")
def get_recent_alerts(hours: int = 24, limit: int = 20):
    """Return recent CRITICAL and WARNING readings for the dashboard alert feed."""
    from app.storage import _query_api, INFLUX_BUCKET
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{hours}h)
  |> filter(fn: (r) => r._measurement == "cow_reading")
  |> filter(fn: (r) => r._field == "risk_score")
  |> filter(fn: (r) => r._value >= 0.8)
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})
"""
    try:
        tables = _query_api.query(flux)
        alerts = []
        for table in tables:
            for record in table.records:
                alerts.append({
                    "time":       record.get_time().isoformat(),
                    "cow_id":     record.values.get("cow_id"),
                    "risk_score": round(record.get_value(), 4),
                    "alert_level": record.values.get("alert_level", "WARNING"),
                })
        return {"count": len(alerts), "alerts": alerts}
    except Exception as e:
        return {"count": 0, "alerts": [], "error": str(e)}
