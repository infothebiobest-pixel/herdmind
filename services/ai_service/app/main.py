import os, json, time, asyncio
import numpy as np, requests
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Optional
from app.ai.engines.prediction_engine import HerdAnomalyEngine, DiseaseClassifier
from app.storage import write_reading, query_cow_history, query_herd_summary, query_high_risk_cows, close as close_storage, _query_api, INFLUX_BUCKET
from app.temporal_engine import rolling_stats, risk_trend, early_warning, herd_early_warnings, cow_timeline

MQTT_BROKER=os.getenv("MQTT_BROKER","mqtt")
MQTT_PORT=int(os.getenv("MQTT_PORT",1883))
MQTT_TOPIC="herd/sensors/#"
ALERT_URL=os.getenv("ALERT_SERVICE_URL","http://herd_alert_service:8000/notify")
HERD_IDS=[101,102,103,104,105]

ai_engine=HerdAnomalyEngine()
disease_classifier=DiseaseClassifier()

class SensorPayload(BaseModel):
    cow_id:int; temperature:float; rumination:float; activity:float
    milk_yield:float=20.0; conductivity:float=5.0; flow_rate:float=2.5
    quarter_delta:float=0.5; lying_time:float=12.0; heart_rate:float=70.0

def dispatch(payload,risk,level):
    body={"cow_id":str(payload.cow_id),"alert_level":level,"disease":"unknown","risk_score":risk,"temperature":payload.temperature,"rumination":payload.rumination,"activity":payload.activity,"message":f"Risk:{risk:.2f}","action":"Inspect immediately"}
    for i in range(3):
        try:
            r=requests.post(ALERT_URL,json=body,timeout=3)
            print(f"🚨 Alert cow={payload.cow_id} tier={level} status={r.status_code}")
            return
        except Exception as e:
            w=2**i; print(f"💥 Retry {i+1}/3 {e} wait={w}s")
            if i<2: time.sleep(w)

def on_connect(c,u,f,rc):
    print(f"✅ MQTT rc={rc}")
    c.subscribe(MQTT_TOPIC)

def on_message(c,u,msg):
    try:
        d=json.loads(msg.payload.decode())
        p=SensorPayload(**d)
        X=np.array([[p.temperature,p.rumination,p.activity,p.milk_yield,p.conductivity,p.flow_rate,p.quarter_delta,p.lying_time,p.heart_rate]])
        pred=int(ai_engine.predict(X)[0])
        risk=float(ai_engine.risk_score(X)[0])
        alert=None
        if pred==-1:
            lvl="CRITICAL" if risk>=0.75 else "WARNING"
            alert={"alert_level":lvl,"message":f"Risk:{risk:.2f}"}
            dispatch(p,risk,lvl)
        write_reading(cow_id=p.cow_id,temperature=p.temperature,rumination=p.rumination,activity=p.activity,prediction=pred,risk_score=risk,alert=alert)
        print(f"💾 cow={p.cow_id} pred={pred} risk={risk:.3f}")
    except Exception as e:
        print(f"💥 {e}")

mqtt_client=mqtt.Client(client_id="herd_ai_v3")
mqtt_client.on_connect=on_connect
mqtt_client.on_message=on_message

@asynccontextmanager
async def lifespan(app):
    print("🧠 Bootstrapping...")
    rng=np.random.default_rng(42)
    h=rng.normal([38.7,300,70,22,5,2.5,0.4,12,70],[0.2,20,8,2,0.3,0.2,0.1,0.8,4],(700,9))
    m=rng.normal([39.8,240,50,14,9.5,1.4,4.2,10,90],[0.3,25,10,3,1,0.3,0.8,1,6],(100,9))
    k=rng.normal([38.1,160,25,11,5.1,2.4,0.5,17,65],[0.2,20,8,2,0.3,0.2,0.1,1,4],(100,9))
    l=rng.normal([38.6,210,22,16,5.2,2.3,0.4,15,78],[0.2,25,8,2,0.3,0.2,0.1,1.2,5],(60,9))
    n=rng.normal([42,580,260,5,5,2.5,0.4,12,70],[0.5,10,15,1,0.2,0.1,0.1,0.5,3],(40,9))
    base=np.vstack([h,m,k,l,n])
    labels=np.array([0]*700+[1]*100+[2]*100+[3]*60+[4]*40)
    ai_engine.train(h)
    disease_classifier.train(base,labels)
    print("✅ Models ready")
    try:
        mqtt_client.connect(MQTT_BROKER,MQTT_PORT,60)
        mqtt_client.loop_start()
        print("✅ MQTT started")
    except Exception as e:
        print(f"🚨 MQTT failed: {e}")
    yield
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    close_storage()

app=FastAPI(title="HerdMind AI",version="2.0",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

@app.get("/health")
def health(): return "healthy"

@app.get("/cows/{cow_id}/history")
def history(cow_id:int,hours:Optional[int]=24): return query_cow_history(str(cow_id),hours=hours)

@app.get("/cows/{cow_id}/latest")
def latest(cow_id:int):
    h=query_cow_history(str(cow_id),hours=24)
    if not h: raise HTTPException(404,"No data")
    return sorted(h,key=lambda x:x["time"],reverse=True)[0]

@app.get("/herd/summary")
def summary(window_hours:Optional[int]=24): return query_herd_summary(hours=window_hours)

@app.get("/herd/at-risk")
def at_risk(threshold:float=0.8,hours:int=24):
    c=query_high_risk_cows(threshold=threshold,hours=hours)
    return {"threshold":threshold,"hours":hours,"count":len(c),"cows":c}

@app.get("/herd/critical")
def critical():
    c=query_high_risk_cows(threshold=0.9,hours=1)
    return {"count":len(c),"cows":c}

@app.get("/alerts/recent")
def alerts(hours:int=24,limit:int=20):
    flux=f'from(bucket:"{INFLUX_BUCKET}")|>range(start:-{hours}h)|>filter(fn:(r)=>r._measurement=="cow_reading")|>filter(fn:(r)=>r._field=="risk_score")|>filter(fn:(r)=>r._value>=0.8)|>group()|>sort(columns:["_time"],desc:true)|>limit(n:{limit})'
    try:
        tables=_query_api.query(flux)
        return {"count":0,"alerts":[{"time":r.get_time().isoformat(),"cow_id":r.values.get("cow_id"),"risk_score":round(r.get_value(),4),"alert_level":r.values.get("alert_level","WARNING")} for t in tables for r in t.records]}
    except Exception as e:
        return {"count":0,"alerts":[],"error":str(e)}

@app.get("/temporal/cow/{cow_id}/trend")
def trend(cow_id:str,hours:int=3): return risk_trend(cow_id,hours=hours)

@app.get("/temporal/cow/{cow_id}/stats/{field}")
def stats(cow_id:str,field:str,hours:int=6): return rolling_stats(cow_id,field,hours=hours)

@app.get("/temporal/cow/{cow_id}/warning")
def warning(cow_id:str): return early_warning(cow_id)

@app.get("/temporal/cow/{cow_id}/timeline")
def timeline(cow_id:str,hours:int=24): return cow_timeline(cow_id,hours=hours)

@app.get("/temporal/herd/warnings")
def herd_warn(level:str="WATCH"):
    r=herd_early_warnings(HERD_IDS,min_level=level)
    return {"min_level":level,"warnings":r,"count":len(r)}
