"""
HerdMind-X · Prediction Engine v3.1 (PRODUCTION-GRADE CORE)
Fixes:
- Restores missing DiseaseClassifier for app/main.py imports
- Patches 1D/2D array runtime shape verification errors
- Enforces strict 9-feature schema consistency
"""

import logging
import numpy as np
from typing import Dict, Any, Tuple
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONTRACT
# ---------------------------------------------------------------------

FEATURES = [
    "temperature",
    "rumination",
    "activity",
    "milk_yield",
    "conductivity",
    "flow_rate",
    "quarter_delta",
    "lying_time",
    "heart_rate",
]

N_FEATURES = len(FEATURES)
DISEASES = ["healthy", "mastitis", "ketosis", "lameness", "noise"]


# ---------------------------------------------------------------------
# SAFE CONVERTER
# ---------------------------------------------------------------------

def _f(x, default=0.0):
    try:
        if x is None:
            return default
        x = float(x)
        if np.isnan(x) or np.isinf(x):
            return default
        return x
    except Exception:
        return default


# ---------------------------------------------------------------------
# RULE ENGINE (DETERMINISTIC SAFETY LAYER)
# ---------------------------------------------------------------------

def classify_rules(row: Dict[str, Any]):
    scores = {d: 0.0 for d in DISEASES}
    reasons = []

    t   = _f(row.get("temperature", 38.7))
    rum = _f(row.get("rumination", 300))
    act = _f(row.get("activity", 70))
    mly = _f(row.get("milk_yield", 20))
    cnd = _f(row.get("conductivity", 5))
    flw = _f(row.get("flow_rate", 2.5))
    qdl = _f(row.get("quarter_delta", 0.5))
    lie = _f(row.get("lying_time", 12))
    hr  = _f(row.get("heart_rate", 70))

    # noise
    if t > 41.5 or t < 36.0:
        scores["noise"] += 2.5
        reasons.append("temperature anomaly")

    if rum < 20:
        scores["noise"] += 2.0
        reasons.append("rumination sensor failure")

    # mastitis
    if cnd > 8.0:
        scores["mastitis"] += 3.0
    if qdl > 3.0:
        scores["mastitis"] += 2.0
    if flw < 1.5:
        scores["mastitis"] += 1.5

    # ketosis
    if rum < 180:
        scores["ketosis"] += 3.0
    if act < 30:
        scores["ketosis"] += 2.0
    if lie > 16:
        scores["ketosis"] += 2.5

    # lameness
    if act < 25:
        scores["lameness"] += 2.5
    if lie > 14:
        scores["lameness"] += 2.0

    # healthy
    if 38.3 <= t <= 39.3 and 250 <= rum <= 450 and 40 <= act <= 120:
        scores["healthy"] += 5.0

    label = max(scores, key=scores.get)
    conf = scores[label] / (sum(scores.values()) + 1e-9)

    return label, float(round(conf, 3)), reasons


# ---------------------------------------------------------------------
# ANOMALY ENGINE (STABLE + CALIBRATED)
# ---------------------------------------------------------------------

class HerdAnomalyEngine:

    def __init__(self, contamination: float = 0.1):
        self.scaler = StandardScaler()
        self.model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
        )

        self._trained = False
        self._baseline_mean = None
        self._baseline_std = None
        self.X_shape = None

    def train(self, X: np.ndarray):
        X_arr = np.asarray(X)
        if len(X_arr.shape) != 2 or X_arr.shape[1] != N_FEATURES:
            raise ValueError(f"Expected 2D matrix with {N_FEATURES} features, got shape {X_arr.shape}")

        Xs = self.scaler.fit_transform(X_arr)
        self.model.fit(Xs)

        scores = self.model.decision_function(Xs)

        self._baseline_mean = float(np.mean(scores))
        self._baseline_std = float(np.std(scores) + 1e-9)

        self.X_shape = X_arr.shape
        self._trained = True

        log.info(
            "Engine trained | samples=%d | shape=%s | μ=%.5f σ=%.5f",
            len(X_arr), str(X_arr.shape), self._baseline_mean, self._baseline_std
        )

    def predict(self, X: np.ndarray):
        X_arr = np.asarray(X)
        if len(X_arr.shape) == 1:
            X_arr = X_arr.reshape(1, -1)
        self._check(X_arr)
        return self.model.predict(self.scaler.transform(X_arr))

    def risk_score(self, X: np.ndarray):
        X_arr = np.asarray(X)
        if len(X_arr.shape) == 1:
            X_arr = X_arr.reshape(1, -1)
        self._check(X_arr)

        Xs = self.scaler.transform(X_arr)
        raw = self.model.decision_function(Xs)

        # Z-score normalization
        z = (raw - self._baseline_mean) / self._baseline_std

        # stable probability mapping
        risk = 1 / (1 + np.exp(z))

        return np.clip(risk, 0.0, 1.0)

    def _check(self, X: np.ndarray):
        if not self._trained:
            raise RuntimeError("Model not trained")
        if X.shape[-1] != N_FEATURES:
            raise ValueError(f"Expected {N_FEATURES} features, got {X.shape[-1]}")


# ---------------------------------------------------------------------
# DISEASE CLASSIFIER LAYER
# ---------------------------------------------------------------------

class DiseaseClassifier:

    def __init__(self):
        self.model = RandomForestClassifier(n_estimators=150, random_state=42)
        self._trained = False

    def train(self, X: np.ndarray, y: np.ndarray):
        X_arr, y_arr = np.asarray(X), np.asarray(y)
        if X_arr.shape[-1] != N_FEATURES:
            raise ValueError(f"Classifier error. Expected {N_FEATURES} features, got shape {X_arr.shape}")
        
        self.model.fit(X_arr, y_arr)
        self._trained = True
        log.info("DiseaseClassifier balanced matrix training complete.")

    def predict_disease(self, X: np.ndarray) -> str:
        if not self._trained:
            return "unknown"
        X_arr = np.asarray(X)
        if len(X_arr.shape) == 1:
            X_arr = X_arr.reshape(1, -1)
            
        class_idx = int(self.model.predict(X_arr)[0])
        if 0 <= class_idx < len(DISEASES):
            return DISEASES[class_idx]
        return "unknown"
