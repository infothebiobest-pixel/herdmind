import numpy as np
from engines.prediction_engine import HerdAnomalyEngine

engine = HerdAnomalyEngine()

data = np.array([
    [38.5, 300, 70],
    [38.7, 310, 72],
    [41.2, 120, 25],  # abnormal cow
    [38.4, 305, 68],
])

engine.train(data)

print("Predictions:", engine.predict(data))
print("Risk:", engine.risk_score(data))