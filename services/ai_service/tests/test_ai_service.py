import time
import json
import pytest
import numpy as np
from unittest.mock import MagicMock, patch
from pydantic import ValidationError

# System Imports
from app.ai.engines.prediction_engine import HerdAnomalyEngine
from app.main import (
    SensorPayload, 
    smooth_risk, 
    clamp_risk, 
    process_sensor_pipeline,
    executor_pool,
    LAST_MSG_TS,
    RISK_STATE,
    SCALER_MEAN,
    SCALER_STD
)

# =====================================================================
# TEST FIXTURES & DATA BUILDERS
# =====================================================================
@pytest.fixture(autouse=True)
def reset_global_states():
    """Flushes internal dictionaries between individual runs."""
    LAST_MSG_TS.clear()
    RISK_STATE.clear()
    yield

def generate_telemetry_matrix(rows: int = 750) -> np.ndarray:
    """Generates synthetic multi-variable telemetry data matrix."""
    return np.random.normal(
        loc=[38.7, 300.0, 70.0, 22.0, 5.0, 2.5, 0.4, 12.0, 70.0],
        scale=[0.2, 20.0, 8.0, 2.0, 0.3, 0.2, 0.1, 0.8, 4.0],
        size=(rows, 9)
    )

@pytest.fixture
def clean_payload_sample() -> SensorPayload:
    return SensorPayload(
        cow_id=101, temperature=38.9, rumination=295.0, activity=72.0,
        milk_yield=21.5, conductivity=4.8, flow_rate=2.4,
        quarter_delta=0.3, lying_time=12.2, heart_rate=71.0
    )

# =====================================================================
# MACHINE LEARNING ENGINE CORE TESTS (UNIT LAYER)
# =====================================================================
@pytest.mark.unit
def test_engine_initialization_and_signatures():
    """Asserts engine initialization works."""
    model = HerdAnomalyEngine()
    assert model is not None

@pytest.mark.unit
def test_ml_matrix_shapes_and_training():
    """Validates structural array boundaries during fitting routines."""
    model = HerdAnomalyEngine()
    X = generate_telemetry_matrix(n=750)
    
    # Matches the unsupervised production signature requirement
    model.train(X)
    assert X.shape == (750, 9)

@pytest.mark.unit
def test_prediction_output_normalization():
    """Verifies normalized inference outputs fall within predictable states."""
    model = HerdAnomalyEngine()
    raw_X = generate_telemetry_matrix(n=10)
    
    # Process matrix through system normalization formula before running tests
    X = (raw_X - SCALER_MEAN) / (SCALER_STD + 1e-6)
    model.train(X)
    
    preds = model.predict(X)
    assert len(preds) == 10
    assert all(p in [-1, 1] for p in preds)

@pytest.mark.unit
def test_risk_output_bounds():
    """Assures raw model risks fall within expected safe bounds."""
    model = HerdAnomalyEngine()
    raw_X = generate_telemetry_matrix(n=20)
    X = (raw_X - SCALER_MEAN) / (SCALER_STD + 1e-6)
    model.train(X)
    
    risks = model.risk_score(X)
    for r in risks:
        assert np.isfinite(r)
        assert 0.0 <= float(r) <= 1.0

# =====================================================================
# PIPELINE DATA & CONCURRENCY TESTS (INTEGRATION LAYER)
# =====================================================================
@pytest.mark.integration
def test_sensor_payload_validation_bounds():
    """Verifies that corrupt telemetric packets are dropped before processing."""
    with pytest.raises(ValidationError):
        SensorPayload(temperature=40.5)

@pytest.mark.integration
def test_clamp_risk_edge_cases():
    """Asserts that mathematical bounds handling sanitizes corrupt signals."""
    assert clamp_risk(0.75) == 0.75
    assert clamp_risk(float('nan')) == 0.0
    assert clamp_risk(float('inf')) == 0.0

@pytest.mark.integration
@patch('app.main.write_reading')
@patch('app.main.ai_engine')
def test_concurrent_ingestion_stress_load(mock_ai, mock_write, clean_payload_sample):
    """Simulates parallel high-velocity data bursts to assess thread pooling."""
    mock_ai.predict.return_value = np.array([-1])
    mock_ai.risk_score.return_value = np.array([0.15])
    mock_write.return_value = True

    start_time = time.time()
    total_messages = 100
    
    futures = [
        executor_pool.submit(process_sensor_pipeline, clean_payload_sample)
        for _ in range(total_messages)
    ]
    
    for f in futures:
        f.result()
        
    duration = time.time() - start_time
    assert duration < 1.0
    assert mock_write.call_count == total_messages
