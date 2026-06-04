"""
HerdMind-X · REST API
FastAPI layer exposing herd intelligence to dashboards, mobile apps, and integrations.
"""

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.storage import (
    query_cow_history,
    query_high_risk_cows,
    query_herd_summary,
)
from app.ai.engines.prediction_engine import (
    HerdAnomalyEngine,
    DiseaseClassifier,
    build_synthetic_training_data,
    FEATURES,
)
from app.alerts.alert_engine import AlertEngine

import numpy as np

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title       = "HerdMind-X API",
    description = "Real-time dairy herd intelligence — anomaly detection, disease classification, alerts.",
    version     = "2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)

# ---------------------------------------------------------------------------
# Models (shared with mqtt_listener — in production inject via DI)
# ---------------------------------------------------------------------------

_anomaly_engine     = HerdAnomalyEngine()
_disease_classifier = DiseaseClassifier()
_alert_engine       = AlertEngine(threshold=0.8)
_models_ready       = False


@app.on_event("startup")
def _startup() -> None:
    global _models_ready
    baseline = np.array([
        [38.5, 300, 70, 22, 5.0, 2.5, 0.4, 12, 70],
        [38.7, 310, 72, 23, 4.8, 2.6, 0.3, 11, 68],
        [38.4, 305, 68, 21, 5.1, 2.4, 0.5, 12, 72],
        [39.0, 290, 65, 20, 5.3, 2.3, 0.4, 13, 74],
        [38.6, 315, 75, 24, 4.9, 2.7, 0.3, 11, 69],
        [38.8, 295, 71, 22, 5.0, 2.5, 0.4, 12, 71],
    ])
    _anomaly_engine.train(baseline)
    X_train, y_train = build_synthetic_training_data()
    _disease_classifier.train(X_train, y_train)
    _models_ready = True
    log.info("Models ready.")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class SensorPayload(BaseModel):
    cow_id:        Any
    temperature:   float
    rumination:    float
    activity:      float
    milk_yield:    float
    conductivity:  float
    flow_rate:     float
    quarter_delta: float
    lying_time:    float
    heart_rate:    float


class PredictionResponse(BaseModel):
    cow_id:          Any
    anomaly:         int
    risk_score:      float
    rule_label:      str
    ml_label:        str | None
    ml_probabilities: dict
    reasons:         list[str]
    alert:           dict


# ---------------------------------------------------------------------------
# Routes — Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    return {"status": "ok", "models_ready": _models_ready}


# ---------------------------------------------------------------------------
# Routes — Predict
# ---------------------------------------------------------------------------

@app.post("/predict", response_model=PredictionResponse, tags=["inference"])
def predict(payload: SensorPayload):
    """Run full 9-feature inference on a single sensor reading."""
    if not _models_ready:
        raise HTTPException(503, "Models not ready yet.")

    row = payload.model_dump()
    X   = np.array([[row[f] for f in FEATURES]])

    anomaly  = int(_anomaly_engine.predict(X)[0])
    risk     = float(_anomaly_engine.risk_score(X)[0])
    disease  = _disease_classifier.predict(row)
    alert    = _alert_engine.evaluate(cow_id=payload.cow_id, risk_score=risk)

    return PredictionResponse(
        cow_id           = payload.cow_id,
        anomaly          = anomaly,
        risk_score       = round(risk, 4),
        rule_label       = disease["rule_label"],
        ml_label         = disease["ml_label"],
        ml_probabilities = disease["ml_probabilities"],
        reasons          = disease["reasons"],
        alert            = alert,
    )


# ---------------------------------------------------------------------------
# Routes — History
# ---------------------------------------------------------------------------

@app.get("/cows/{cow_id}/history", tags=["history"])
def cow_history(
    cow_id: str,
    hours:  int = Query(default=24, ge=1, le=168, description="Lookback window in hours (max 7 days)"),
):
    """Full reading history for a single cow."""
    data = query_cow_history(cow_id, hours=hours)
    if not data:
        raise HTTPException(404, f"No data found for cow_id={cow_id} in last {hours}h.")
    return {"cow_id": cow_id, "hours": hours, "count": len(data), "readings": data}


@app.get("/cows/{cow_id}/latest", tags=["history"])
def cow_latest(cow_id: str):
    """Most recent reading for a single cow."""
    data = query_cow_history(cow_id, hours=24)
    if not data:
        raise HTTPException(404, f"No data found for cow_id={cow_id}.")
    return {"cow_id": cow_id, "latest": data[-1]}


# ---------------------------------------------------------------------------
# Routes — Herd
# ---------------------------------------------------------------------------

@app.get("/herd/summary", tags=["herd"])
def herd_summary(
    hours: int = Query(default=24, ge=1, le=168),
):
    """Aggregate herd health stats."""
    return query_herd_summary(hours=hours)


@app.get("/herd/at-risk", tags=["herd"])
def herd_at_risk(
    threshold: float = Query(default=0.8,  ge=0.0, le=1.0),
    hours:     int   = Query(default=24,   ge=1,   le=168),
):
    """All cows currently above risk threshold."""
    data = query_high_risk_cows(threshold=threshold, hours=hours)
    return {"threshold": threshold, "hours": hours, "count": len(data), "cows": data}


@app.get("/herd/critical", tags=["herd"])
def herd_critical():
    """Shortcut — cows at risk ≥ 0.9 in last hour. For dashboard alerts panel."""
    data = query_high_risk_cows(threshold=0.9, hours=1)
    return {"count": len(data), "cows": data}


# ---------------------------------------------------------------------------
# Routes — Feature intelligence
# ---------------------------------------------------------------------------

@app.get("/model/feature-importance", tags=["model"])
def feature_importance():
    """Random Forest feature importance scores."""
    return _disease_classifier.feature_importance()


@app.get("/model/info", tags=["model"])
def model_info():
    """Model configuration and feature list."""
    return {
        "features":    FEATURES,
        "diseases":    ["healthy", "mastitis", "ketosis", "lameness", "noise"],
        "anomaly":     "IsolationForest(n_estimators=200, contamination=0.1)",
        "classifier":  "RandomForestClassifier(n_estimators=300, max_depth=8)",
    }