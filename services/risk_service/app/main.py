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

# Disease-specific escalation overrides
# Some diseases warrant immediate CRITICAL regardless of score
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
    ml_label:      str | None = None

class RiskResponse(BaseModel):
    cow_id:      str
    risk_score:  float
    alert_level: str
    disease:     str
    message:     str
    action:      str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    return {"service": "risk_service", "status": "ok"}


@app.post("/evaluate", response_model=RiskResponse, tags=["risk"])
def evaluate(req: RiskRequest):
    """
    Evaluate risk level from anomaly score and disease label.
    Disease-specific rules can escalate to CRITICAL regardless of score.
    """
    disease = req.ml_label or req.rule_label

    # Score-based level
    if req.anomaly_score >= THRESHOLDS["CRITICAL"]:
        level = "CRITICAL"
    elif req.anomaly_score >= THRESHOLDS["WARNING"]:
        level = "WARNING"
    elif req.anomaly_score >= THRESHOLDS["WATCH"]:
        level = "WATCH"
    else:
        level = "NORMAL"

    # Disease override — mastitis/ketosis always CRITICAL if any anomaly
    if disease in CRITICAL_DISEASES and req.anomaly_score >= THRESHOLDS["WATCH"]:
        level = "CRITICAL"

    message = MESSAGE_MAP.get(disease, f"Anomaly detected — disease: {disease}")
    action  = ACTION_MAP[level]

    log.info("cow_id=%-6s  score=%.3f  disease=%-10s  level=%s",
             req.cow_id, req.anomaly_score, disease, level)

    return RiskResponse(
        cow_id      = req.cow_id,
        risk_score  = round(req.anomaly_score, 4),
        alert_level = level,
        disease     = disease,
        message     = message,
        action      = action,
    )


@app.get("/thresholds", tags=["config"])
def get_thresholds():
    """Return current escalation thresholds."""
    return {
        "thresholds":        THRESHOLDS,
        "critical_diseases": list(CRITICAL_DISEASES),
        "action_map":        ACTION_MAP,
    }