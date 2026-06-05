"""
HerdMind-X · Storage Layer
Persists sensor readings, risk scores, predictions, and alerts to InfluxDB.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config  (override via environment variables in production)
# ---------------------------------------------------------------------------

INFLUX_URL = "http://herd_influx:8086"
INFLUX_TOKEN  = "cfuR3oHFeBlAbbiIxas5OcXhTY3CZxkz1_QNkAlVrCu48Y6osB-loG7UcGvP1RlN1lRugY7qsAPgHZiu3JteEA=="
INFLUX_ORG    = "herdmind"
INFLUX_BUCKET = "herd_telemetry"

# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
_write_api = _client.write_api(write_options=SYNCHRONOUS)
_query_api = _client.query_api()


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def write_reading(
    cow_id:      Any,
    temperature: float,
    rumination:  float,
    activity:    float,
    prediction:  int,
    risk_score:  float,
    alert:       dict | None = None,
) -> None:
    """
    Write one sensor event to InfluxDB.

    Measurement: cow_reading
    Tags:        cow_id, alert_level
    Fields:      temperature, rumination, activity, prediction, risk_score
    """
    alert_level = (alert or {}).get("alert_level", "NONE")

    point = (
        Point("cow_reading")
        .tag("cow_id",      str(cow_id))
        .tag("alert_level", alert_level)
        .field("temperature", float(temperature))
        .field("rumination",  float(rumination))
        .field("activity",    float(activity))
        .field("prediction",  int(prediction))
        .field("risk_score",  float(risk_score))
        .time(datetime.now(timezone.utc), WritePrecision.S)
    )

    if alert:
        point = point.field("alert_message", alert.get("message", ""))

    try:
        _write_api.write(bucket=INFLUX_BUCKET, record=point)
        log.debug("Wrote reading for cow_id=%s  risk=%.3f  alert=%s",
                  cow_id, risk_score, alert_level)
    except Exception as exc:
        log.error("InfluxDB write failed: %s", exc)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def query_cow_history(cow_id: Any, hours: int = 24) -> list[dict]:
    """Return the last `hours` of readings for a single cow."""
    flux = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "cow_reading")
      |> filter(fn: (r) => r.cow_id == "{cow_id}")
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: false)
    """
    try:
        tables = _query_api.query(flux)
        return [
            {
                "time":        record.get_time().isoformat(),
                "cow_id":      record.values.get("cow_id"),
                "temperature": record.values.get("temperature"),
                "rumination":  record.values.get("rumination"),
                "activity":    record.values.get("activity"),
                "prediction":  record.values.get("prediction"),
                "risk_score":  record.values.get("risk_score"),
                "alert_level": record.values.get("alert_level"),
            }
            for table in tables
            for record in table.records
        ]
    except Exception as exc:
        log.error("InfluxDB query failed: %s", exc)
        return []


def query_high_risk_cows(threshold: float = 0.8, hours: int = 1) -> list[dict]:
    """Return cows whose risk score exceeded `threshold` in the last `hours`."""
    flux = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "cow_reading")
      |> filter(fn: (r) => r._field == "risk_score")
      |> filter(fn: (r) => r._value >= {threshold})
      |> group(columns: ["cow_id"])
      |> last()
    """
    try:
        tables = _query_api.query(flux)
        return [
            {
                "cow_id":     record.values.get("cow_id"),
                "risk_score": record.get_value(),
                "time":       record.get_time().isoformat(),
            }
            for table in tables
            for record in table.records
        ]
    except Exception as exc:
        log.error("InfluxDB query failed: %s", exc)
        return []


def query_herd_summary(hours: int = 24) -> dict:
    """Return aggregate stats across the whole herd for the last `hours`."""
    flux = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "cow_reading")
      |> filter(fn: (r) => r._field == "risk_score")
      |> group()
      |> reduce(
           identity: {{count: 0, total: 0.0, max: 0.0}},
           fn: (r, accumulator) => ({{
             count: accumulator.count + 1,
             total: accumulator.total + r._value,
             max:   if r._value > accumulator.max then r._value else accumulator.max,
           }})
         )
    """
    try:
        tables = _query_api.query(flux)
        for table in tables:
            for record in table.records:
                count = record.values.get("count", 0)
                total = record.values.get("total", 0.0)
                return {
                    "readings":   count,
                    "avg_risk":   round(total / count, 3) if count else 0.0,
                    "max_risk":   round(record.values.get("max", 0.0), 3),
                    "period_hrs": hours,
                }
    except Exception as exc:
        log.error("InfluxDB query failed: %s", exc)
    return {"readings": 0, "avg_risk": 0.0, "max_risk": 0.0, "period_hrs": hours}


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def close() -> None:
    _client.close()
