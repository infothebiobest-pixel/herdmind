"""
HerdMind-X · Risk Service
Owns risk escalation logic — decoupled from ML engine.
Receives anomaly scores and disease labels, returns structured alert levels.
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(
    title       = "HerdMind-X · Risk Service",
    description = "Risk escalation and alert level classification.",
    version     = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

THRESHOLDS = {
    "CRITICAL": 0.9,
    "WARNING":  0.7,
    "WATCH":    0.5,
}

CRITICAL_DISEASES = {"mastitis", "ketosis"}

ACTION_MAP = {
    "CRITICAL": "Notify vet immediately",
    "WARNING":  "Monitor closely — reassess in 2 hours",
    "WATCH":    "Flag for next routine check",
    "NORMAL":   "No action required",
}

MESSAGE_MAP = {
    "mastitis": "Mastitis indicators detected — check udder and milk quality",
    "ketosis":  "Ketosis indicators detected — check feed intake and energy balance",
    "lameness": "Lameness indicators detected — check hooves and gait",
    "noise":    "Sensor anomaly detected — verify device calibration",
    "healthy":  "All parameters within normal range",
}

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class RiskRequest(BaseModel):
    cow_id:        str
    anomaly_score: float = Field(..., ge=0.0, le=1.0)
    rule_label:    str
    ml_label:      str

class RiskResponse(BaseModel):
    cow_id:      str
    alert_level: str
    disease:     str
    risk_score:  float
    message:     str
    action:      str

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"service": "risk_service", "status": "ok"}

@app.post("/evaluate", response_model=RiskResponse)
def evaluate(req: RiskRequest):
    disease = req.rule_label.lower() if req.rule_label != "healthy" else req.ml_label.lower()
    score = req.anomaly_score

    if disease in CRITICAL_DISEASES or score >= THRESHOLDS["CRITICAL"]:
        level = "CRITICAL"
    elif score >= THRESHOLDS["WARNING"]:
        level = "WARNING"
    elif score >= THRESHOLDS["WATCH"]:
        level = "WATCH"
    else:
        level = "NORMAL"

    return RiskResponse(
        cow_id=req.cow_id,
        alert_level=level,
        disease=disease,
        risk_score=score,
        message=MESSAGE_MAP.get(disease, f"Unspecified anomaly detected: {disease}"),
        action=ACTION_MAP[level]
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8003, reload=False)
