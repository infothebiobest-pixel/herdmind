import logging, os
from datetime import datetime, timezone
import numpy as np
from influxdb_client import InfluxDBClient

log = logging.getLogger(__name__)
INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "herdmind")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "herd_telemetry")
_client = None
_query_api = None

def _get_query_api():
    global _client, _query_api
    if _query_api:
        return _query_api
    _client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    _query_api = _client.query_api()
    return _query_api

def rolling_stats(cow_id, field, hours=6):
    flux = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -{hours}h) |> filter(fn: (r) => r._measurement == "cow_reading") |> filter(fn: (r) => r.cow_id == "{cow_id}") |> filter(fn: (r) => r._field == "{field}") |> group()'
    try:
        tables = _get_query_api().query(flux)
        values = [r.get_value() for t in tables for r in t.records if r.get_value() is not None]
        if not values:
            return {}
        arr = np.array(values, dtype=float)
        return {"field": field, "cow_id": str(cow_id), "hours": hours, "count": len(arr), "mean": round(float(arr.mean()), 4), "std": round(float(arr.std()), 4), "min": round(float(arr.min()), 4), "max": round(float(arr.max()), 4), "latest": round(float(arr[-1]), 4), "z_score": round(float((arr[-1] - arr.mean()) / (arr.std() + 1e-9)), 3)}
    except Exception as e:
        log.error("rolling_stats failed: %s", e)
        return {}

def risk_trend(cow_id, hours=3):
    flux = f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -{hours}h) |> filter(fn: (r) => r._measurement == "cow_reading") |> filter(fn: (r) => r.cow_id == "{cow_id}") |> filter(fn: (r) => r._field == "risk_score") |> group() |> sort(columns: ["_time"])'
    try:
        tables = _get_query_api().query(flux)
        points = [(r.get_time().timestamp(), r.get_value()) for t in tables for r in t.records if r.get_value() is not None]
        if len(points) < 3:
            return {"cow_id": str(cow_id), "trend": "insufficient_data", "slope": 0.0}
        times = np.array([p[0] for p in points])
        values = np.array([p[1] for p in points], dtype=float)
        times = (times - times[0]) / 60.0
        slope, intercept = np.polyfit(times, values, 1)
        trend = "worsening" if slope > 0.005 else "improving" if slope < -0.005 else "stable"
        return {"cow_id": str(cow_id), "hours": hours, "slope": round(float(slope), 6), "trend": trend, "points": len(points)}
    except Exception as e:
        log.error("risk_trend failed: %s", e)
        return {}

def early_warning(cow_id):
    stats = rolling_stats(cow_id, "risk_score", hours=24)
    trend = risk_trend(cow_id, hours=3)
    temp_s = rolling_stats(cow_id, "temperature", hours=24)
    rum_s = rolling_stats(cow_id, "rumination", hours=24)
    if not stats or not trend:
        return {"cow_id": str(cow_id), "warning": "NO_DATA"}
    z = stats.get("z_score", 0)
    slope = trend.get("slope", 0)
    latest = stats.get("latest", 0)
    worsening = trend.get("trend") == "worsening"
    level = "ALERT" if z > 2.5 and worsening and latest > 0.6 else "ADVISORY" if z > 2.0 and worsening else "WATCH" if z > 1.5 or worsening else "NORMAL"
    reasons = []
    if z > 1.5: reasons.append(f"risk z-score elevated ({z:.2f})")
    if worsening: reasons.append(f"risk trending upward (slope={slope:.4f}/min)")
    if temp_s.get("z_score", 0) > 1.5: reasons.append(f"temperature elevated ({temp_s['latest']:.1f}C)")
    if rum_s and rum_s.get("z_score", 0) < -1.5: reasons.append(f"rumination declining ({rum_s['latest']:.0f} min)")
    return {"cow_id": str(cow_id), "warning_level": level, "risk_z_score": z, "risk_trend": trend.get("trend"), "risk_slope": slope, "latest_risk": latest, "reasons": reasons, "timestamp": datetime.now(timezone.utc).isoformat()}

def herd_early_warnings(cow_ids, min_level="WATCH"):
    level_rank = {"NORMAL": 0, "WATCH": 1, "ADVISORY": 2, "ALERT": 3, "NO_DATA": -1}
    min_rank = level_rank.get(min_level, 1)
    results = []
    for cid in cow_ids:
        w = early_warning(cid)
        if level_rank.get(w.get("warning_level", "NORMAL"), 0) >= min_rank:
            results.append(w)
    results.sort(key=lambda x: level_rank.get(x.get("warning_level", "NORMAL"), 0), reverse=True)
    return results

def cow_timeline(cow_id, hours=24):
    fields = ["risk_score", "temperature", "rumination", "activity", "milk_yield", "conductivity", "heart_rate"]
    return {"cow_id": str(cow_id), "hours": hours, "timestamp": datetime.now(timezone.utc).isoformat(), "fields": {f: rolling_stats(cow_id, f, hours) for f in fields}, "trend": risk_trend(cow_id, min(hours, 6)), "warning": early_warning(cow_id)}
