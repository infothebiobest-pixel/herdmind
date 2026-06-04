"""
HerdMind-X · Prediction Engine v2
9-feature anomaly detection + disease classification.

Features (in order):
    0  temperature     °C
    1  rumination      min/day
    2  activity        units
    3  milk_yield      litres/session
    4  conductivity    mS/cm
    5  flow_rate       L/min
    6  quarter_delta   mS/cm  (max quarter conductivity − min)
    7  lying_time      hrs/day
    8  heart_rate      bpm
"""

import logging
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature spec
# ---------------------------------------------------------------------------

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

DISEASES = ["healthy", "mastitis", "ketosis", "lameness", "noise"]

# Normal ranges for validation
VALID_RANGES = {
    "temperature":   (35.0, 43.0),
    "rumination":    (0.0,  600.0),
    "activity":      (0.0,  300.0),
    "milk_yield":    (0.0,  60.0),
    "conductivity":  (2.0,  20.0),
    "flow_rate":     (0.0,  10.0),
    "quarter_delta": (0.0,  15.0),
    "lying_time":    (0.0,  24.0),
    "heart_rate":    (40.0, 140.0),
}


# ---------------------------------------------------------------------------
# Disease rule engine (fast, explainable, no training needed)
# ---------------------------------------------------------------------------

def classify_rules(row: dict) -> tuple[str, float, list[str]]:
    """
    Rule-based disease classifier.
    Returns (disease_label, confidence, reasons).
    Runs before ML — catches clear-cut cases and filters noise.
    """
    reasons = []
    scores  = {d: 0.0 for d in DISEASES}

    t   = row.get("temperature",   38.7)
    rum = row.get("rumination",    300.0)
    act = row.get("activity",       70.0)
    mly = row.get("milk_yield",     20.0)
    cnd = row.get("conductivity",    5.0)
    flw = row.get("flow_rate",       2.5)
    qdl = row.get("quarter_delta",   0.5)
    lie = row.get("lying_time",     12.0)
    hr  = row.get("heart_rate",     70.0)

    # ── Noise detection (physically impossible values) ──────────────────
    if t > 41.5 or t < 36.5:
        scores["noise"] += 2.0
        reasons.append(f"temp spike ({t:.1f}°C — likely sensor error)")
    if rum > 550 or rum == 0:
        scores["noise"] += 2.0
        reasons.append(f"rumination out of bounds ({rum:.0f} min)")
    if act > 250:
        scores["noise"] += 1.5
        reasons.append(f"activity physically impossible ({act:.0f})")

    # ── Mastitis ─────────────────────────────────────────────────────────
    if t > 39.5:
        scores["mastitis"] += 2.0
        reasons.append(f"elevated temp ({t:.1f}°C)")
    if cnd > 8.0:
        scores["mastitis"] += 3.0
        reasons.append(f"high conductivity ({cnd:.1f} mS/cm — infection marker)")
    if qdl > 3.0:
        scores["mastitis"] += 2.5
        reasons.append(f"quarter differential ({qdl:.1f} mS/cm — subclinical mastitis)")
    if flw < 1.5:
        scores["mastitis"] += 1.5
        reasons.append(f"low flow rate ({flw:.1f} L/min — teat inflammation)")
    if mly < 15.0:
        scores["mastitis"] += 1.0
        reasons.append(f"reduced yield ({mly:.1f} L)")
    if hr > 85:
        scores["mastitis"] += 1.0
        reasons.append(f"elevated heart rate ({hr:.0f} bpm)")

    # ── Ketosis ──────────────────────────────────────────────────────────
    if rum < 180:
        scores["ketosis"] += 3.0
        reasons.append(f"severe rumination drop ({rum:.0f} min/day)")
    if act < 30:
        scores["ketosis"] += 2.0
        reasons.append(f"very low activity ({act:.0f} — lethargy)")
    if lie > 16:
        scores["ketosis"] += 2.5
        reasons.append(f"excessive lying ({lie:.1f} hrs — ketosis pattern)")
    if mly < 12.0:
        scores["ketosis"] += 1.5
        reasons.append(f"significant yield loss ({mly:.1f} L)")
    if t < 38.2:
        scores["ketosis"] += 1.0
        reasons.append(f"sub-normal temp ({t:.1f}°C)")

    # ── Lameness ─────────────────────────────────────────────────────────
    if lie > 14 and act < 40:
        scores["lameness"] += 3.0
        reasons.append(f"high lying + low activity (lying={lie:.1f}h, act={act:.0f})")
    if act < 25:
        scores["lameness"] += 2.0
        reasons.append(f"minimal movement ({act:.0f} units)")
    if rum < 220 and lie > 13:
        scores["lameness"] += 1.5
        reasons.append(f"reluctance to stand (rum={rum:.0f}, lying={lie:.1f}h)")

    # ── Healthy baseline ─────────────────────────────────────────────────
    if (38.3 <= t <= 39.3 and rum >= 250 and 40 <= act <= 120
            and mly >= 18 and cnd <= 6.5 and 8 <= lie <= 14
            and 60 <= hr <= 82):
        scores["healthy"] += 5.0

    # ── Pick winner ──────────────────────────────────────────────────────
    label      = max(scores, key=scores.get)
    total      = sum(scores.values()) or 1.0
    confidence = round(scores[label] / total, 3)

    if scores[label] == 0.0:
        label, confidence = "healthy", 1.0

    return label, confidence, reasons


# ---------------------------------------------------------------------------
# ML anomaly engine (Isolation Forest)
# ---------------------------------------------------------------------------

class HerdAnomalyEngine:
    """Isolation Forest trained on 9-feature healthy baseline."""

    def __init__(self, contamination: float = 0.1):
        self.scaler = StandardScaler()
        self.model  = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=200,
        )
        self._trained = False

    def train(self, X: np.ndarray) -> None:
        assert X.shape[1] == 9, f"Expected 9 features, got {X.shape[1]}"
        Xs = self.scaler.fit_transform(X)
        self.model.fit(Xs)
        self._trained = True
        log.info("HerdAnomalyEngine trained on %d samples × 9 features.", len(X))

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Returns array of 1 (normal) or -1 (anomaly)."""
        self._check_trained()
        return self.model.predict(self.scaler.transform(X))

    def risk_score(self, X: np.ndarray) -> np.ndarray:
        """Returns risk in [0, 1]. Higher = more anomalous."""
        self._check_trained()
        raw = self.model.decision_function(self.scaler.transform(X))
        return np.clip(-raw / (np.abs(raw).max() + 1e-9), 0, 1)

    def _check_trained(self) -> None:
        if not self._trained:
            raise RuntimeError("Model not trained — call train() first.")


# ---------------------------------------------------------------------------
# ML disease classifier (Random Forest, trained on labelled data)
# ---------------------------------------------------------------------------

class DiseaseClassifier:
    """
    Random Forest classifier trained on labelled 9-feature samples.
    Outputs disease label + probability per class.
    Falls back to rule engine if not trained.
    """

    def __init__(self):
        self.scaler   = StandardScaler()
        self.model    = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            random_state=42,
            class_weight="balanced",
        )
        self._trained = False

    def train(self, X: np.ndarray, y: np.ndarray) -> None:
        """
        X: (n_samples, 9)
        y: (n_samples,) — integer labels mapping to DISEASES list
        """
        assert X.shape[1] == 9, f"Expected 9 features, got {X.shape[1]}"
        Xs = self.scaler.fit_transform(X)
        self.model.fit(Xs, y)
        self._trained = True
        log.info("DiseaseClassifier trained on %d labelled samples.", len(X))

    def predict(self, row: dict) -> dict:
        """
        row: dict with 9 feature keys.
        Returns full prediction including rule + ML results.
        """
        # Always run rules — fast, explainable
        rule_label, rule_conf, reasons = classify_rules(row)

        result = {
            "rule_label":      rule_label,
            "rule_confidence": rule_conf,
            "reasons":         reasons,
            "ml_label":        None,
            "ml_probabilities": {},
        }

        if self._trained:
            X  = np.array([[row.get(f, 0.0) for f in FEATURES]])
            Xs = self.scaler.transform(X)
            ml_idx   = int(self.model.predict(Xs)[0])
            ml_proba = self.model.predict_proba(Xs)[0]

            result["ml_label"]        = DISEASES[ml_idx]
            result["ml_probabilities"] = {
                DISEASES[i]: round(float(p), 3)
                for i, p in enumerate(ml_proba)
            }

        return result

    def feature_importance(self) -> dict:
        """Returns feature importance scores (requires trained model)."""
        if not self._trained:
            return {}
        return dict(zip(FEATURES, self.model.feature_importances_.round(4)))


# ---------------------------------------------------------------------------
# Synthetic labelled baseline (for bootstrapping — replace with real data)
# ---------------------------------------------------------------------------

def build_synthetic_training_data() -> tuple[np.ndarray, np.ndarray]:
    """
    Returns (X, y) with ~200 labelled samples across all 5 classes.
    Use only until real farm data is available.

    Label map:
        0 = healthy
        1 = mastitis
        2 = ketosis
        3 = lameness
        4 = noise
    """
    rng = np.random.default_rng(42)

    def jitter(base, std, n):
        return np.clip(base + rng.normal(0, std, (n, len(base))), 0, None)

    # [temp, rum, act, yield, cond, flow, q_delta, lying, hr]
    healthy  = jitter([38.7, 300, 70, 22, 5.0, 2.5, 0.4, 12, 70], [0.2, 20, 8, 2, 0.3, 0.2, 0.1, 0.8, 4], 60)
    mastitis = jitter([39.8, 240, 50, 14, 9.5, 1.4, 4.2, 10, 90], [0.3, 25, 10, 3, 1.0, 0.3, 0.8, 1.0, 6], 50)
    ketosis  = jitter([38.1, 160, 25, 11, 5.1, 2.4, 0.5, 17, 65], [0.2, 20, 8,  2, 0.3, 0.2, 0.1, 1.0, 4], 40)
    lameness = jitter([38.6, 210, 22, 16, 5.2, 2.3, 0.4, 15, 78], [0.2, 25, 8,  2, 0.3, 0.2, 0.1, 1.2, 5], 40)
    noise    = jitter([42.0, 580, 260, 5, 5.0, 2.5, 0.4, 12, 70], [0.5, 10, 15, 1, 0.2, 0.1, 0.1, 0.5, 3], 20)

    X = np.vstack([healthy, mastitis, ketosis, lameness, noise])
    y = np.array([0]*60 + [1]*50 + [2]*40 + [3]*40 + [4]*20)

    return X, y