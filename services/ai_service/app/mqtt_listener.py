"""
HerdMind-X · MQTT Listener v2
9-feature pipeline: ingest → anomaly → disease classify → alert → store → notify.
"""

import json
import logging

import numpy as np
import paho.mqtt.client as mqtt

from app.ai.engines.prediction_engine import (
    HerdAnomalyEngine,
    DiseaseClassifier,
    build_synthetic_training_data,
    FEATURES,
)
from app.alerts.alert_engine import AlertEngine
from app.storage import write_reading, close as close_storage
from app.whatsapp_alerts import send_whatsapp_alert

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BROKER          = "127.0.0.1"
PORT            = 1883
TOPIC           = "herdmind/sensors"
REQUIRED_FIELDS = set(FEATURES)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

anomaly_engine     = HerdAnomalyEngine()
disease_classifier = DiseaseClassifier()
alert_engine       = AlertEngine(threshold=0.8)


def _init_models() -> None:
    # 9-feature healthy baseline for anomaly engine
    baseline = np.array([
        # temp   rum   act  yield  cond  flow  q_dlt  lying   hr
        [38.5,  300,   70,   22,   5.0,  2.5,   0.4,   12,   70],
        [38.7,  310,   72,   23,   4.8,  2.6,   0.3,   11,   68],
        [38.4,  305,   68,   21,   5.1,  2.4,   0.5,   12,   72],
        [39.0,  290,   65,   20,   5.3,  2.3,   0.4,   13,   74],
        [38.6,  315,   75,   24,   4.9,  2.7,   0.3,   11,   69],
        [38.8,  295,   71,   22,   5.0,  2.5,   0.4,   12,   71],
    ])
    anomaly_engine.train(baseline)

    # Disease classifier on synthetic labelled data
    X_train, y_train = build_synthetic_training_data()
    disease_classifier.train(X_train, y_train)

    log.info("Feature importance: %s", disease_classifier.feature_importance())


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------

def on_connect(client: mqtt.Client, userdata, flags, rc: int) -> None:
    if rc == 0:
        client.subscribe(TOPIC)
        log.info("Connected to broker. Subscribed to '%s'.", TOPIC)
    else:
        log.error("Broker connection refused (rc=%d).", rc)


def on_disconnect(client: mqtt.Client, userdata, rc: int) -> None:
    if rc != 0:
        log.warning("Unexpected disconnect (rc=%d). Paho will auto-reconnect.", rc)


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    # --- Parse -----------------------------------------------------------
    try:
        payload: dict = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.error("Invalid payload: %s", exc)
        return

    # --- Validate --------------------------------------------------------
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        log.warning("Payload missing fields %s — skipping. cow_id=%s",
                    missing, payload.get("cow_id"))
        return

    # --- Build feature vector --------------------------------------------
    X = np.array([[payload[f] for f in FEATURES]])

    # --- Anomaly detection -----------------------------------------------
    prediction = int(anomaly_engine.predict(X)[0])
    risk       = float(anomaly_engine.risk_score(X)[0])

    # --- Disease classification ------------------------------------------
    disease    = disease_classifier.predict(payload)
    rule_label = disease["rule_label"]
    ml_label   = disease["ml_label"]
    reasons    = disease["reasons"]

    # --- Alert -----------------------------------------------------------
    alert = alert_engine.evaluate(
        cow_id=payload.get("cow_id"),
        risk_score=risk,
    )

    # --- Log -------------------------------------------------------------
    log.info(
        "cow_id=%-6s  risk=%.3f  anomaly=%+d  rule=%-10s  ml=%-10s  alert=%s",
        payload.get("cow_id"),
        risk,
        prediction,
        rule_label,
        ml_label or "—",
        alert.get("alert_level", "NONE"),
    )
    if reasons:
        log.info("  → reasons: %s", " | ".join(reasons))
    if disease.get("ml_probabilities"):
        log.info("  → ml_proba: %s", disease["ml_probabilities"])

    # --- Persist ---------------------------------------------------------
    write_reading(
        cow_id        = payload.get("cow_id"),
        temperature   = payload["temperature"],
        rumination    = payload["rumination"],
        activity      = payload["activity"],
        prediction    = prediction,
        risk_score    = risk,
        alert         = alert,
    )

    # --- Notify ----------------------------------------------------------
    send_whatsapp_alert(
        cow_id      = payload.get("cow_id"),
        risk_score  = risk,
        alert       = alert,
        temperature = payload["temperature"],
        rumination  = payload["rumination"],
        activity    = payload["activity"],
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def start_mqtt() -> None:
    _init_models()

    client = mqtt.Client()
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    log.info("Connecting to broker %s:%d …", BROKER, PORT)
    client.connect(BROKER, PORT, keepalive=60)

    try:
        client.loop_forever()
    finally:
        close_storage()
        log.info("Storage connection closed.")