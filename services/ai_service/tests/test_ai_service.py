import time
import json
import pytest
import numpy as np
from unittest.mock import MagicMock, patch

from app.ai.engines.prediction_engine import HerdAnomalyEngine
from app.main import (
    SensorPayload, 
    normalize_risk, 
    on_message,
    mqtt_client
)

# Mocked MQTT message container layout matching the paho-mqtt interface signature
class MockMQTTMessage:
    def __init__(self, topic: str, payload: bytes):
        self.topic = topic
        self.payload = payload

# =====================================================================
# CORE MATHEMATICAL FILTER TESTING
# =====================================================================
@pytest.mark.unit
def test_normalize_risk_bounds_and_stability():
    """Asserts that normalization properly shapes and bounds risk outputs."""
    # Test safe parsing of standard raw float vectors
    assert 0.0 <= normalize_risk(0.5) <= 1.0
    
    # Test handling of edge-case corrupt inputs (NaN/Inf)
    assert normalize_risk(float('nan')) == 0.0
    assert normalize_risk(float('inf')) == 0.0

# =====================================================================
# END-TO-END INGESTION SIGNAL TESTING
# =====================================================================
@pytest.mark.integration
@patch('app.main.write_reading')
@patch('app.main.ai_engine')
def test_mqtt_callback_ingestion_pipeline(mock_ai, mock_write):
    """Simulates raw edge sensor packets processing directly through on_message."""
    # Enforce stable mock returns matching real application array extraction layers
    mock_ai.predict.return_value = np.array([-1])
    mock_ai.risk_score.return_value = np.array([0.95]) # Triggers the verified alert state check
    mock_write.return_value = True

    # Build raw synthetic binary payload matching your live edge layout format
    telemetry_data = {
        "cow_id": 999, "temperature": 41.5, "rumination": 80.0, "activity": 10.0,
        "milk_yield": 5.0, "conductivity": 12.0, "flow_rate": 0.5, 
        "quarter_delta": 6.0, "lying_time": 18.0, "heart_rate": 100.0
    }
    raw_payload = json.dumps(telemetry_data).encode('utf-8')
    mock_msg = MockMQTTMessage("herd/sensors/999", raw_payload)

    # Process packet directly through your real, active on_message event loop handler
    on_message(mqtt_client, None, mock_msg)

    # Assert that data moved successfully through internal validation and storage pipelines
    assert mock_write.call_count == 1
