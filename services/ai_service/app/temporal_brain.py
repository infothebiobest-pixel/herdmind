import os
import time
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

# ==============================
# 🧠 CONFIG
# ==============================
DB_URL = os.getenv("DATABASE_URL", "postgresql://herd:herd123@herd_postgres:5432/herdmind")
r_cache = redis.Redis(host="herd_redis", port=6379, db=0)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("herdmind_brain")

# ==============================
# DB CONNECT
# ==============================
def db():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

# ==============================
# ALERT CONTROL (Redis Latch 2.0 with Fixed Schema Mapping)
# ==============================
def trigger_alert(conn, cow_id, level, disease, risk):
    key = f"alert:latched:{cow_id}"
    try:
        cached = r_cache.get(key)
        if cached:
            try:
                data = json.loads(cached.decode())
                # ESCALATION RULE: allow escalation only (WARNING -> CRITICAL)
                if level != "CRITICAL" or data.get("level") == "CRITICAL":
                    return  # Suppressed!
            except Exception:
                pass

        payload = {
            "level": level,
            "disease": disease,
            "risk": risk,
            "time": time.time()
        }
        r_cache.setex(key, 7200, json.dumps(payload))

        with conn.cursor() as cur:
            # FIX: Mapping 'level' explicitly to your database column 'alert_level'
            cur.execute("""
                INSERT INTO agent_alerts (cow_id, alert_level, disease, risk_score, created_at, acknowledged)
                VALUES (%s, %s, %s, %s, NOW(), FALSE);
            """, (cow_id, level, disease, risk))
        
        log.warning(f"🚨 ALERT REGISTERED: Cow #{cow_id} | {disease} | Level: {level} (Risk: {risk:.3f})")
    except Exception as e:
        log.error(f"❌ Alert dispatch error: {e}")

# ==============================
# EPISODE ENGINE
# ==============================
def upsert_episode(cur, cow_id, condition, risk):
    cur.execute("""
        SELECT episode_id, peak_risk
        FROM herd_health_episodes
        WHERE cow_id = %s AND condition_type = %s AND episode_status = 'ACTIVE'
        LIMIT 1;
    """, (cow_id, condition))
    ep = cur.fetchone()

    if ep:
        new_peak = max(float(ep["peak_risk"]), float(risk))
        cur.execute("""
            UPDATE herd_health_episodes
            SET peak_risk = %s
            WHERE episode_id = %s;
        """, (new_peak, ep["episode_id"]))
    else:
        cur.execute("""
            INSERT INTO herd_health_episodes (cow_id, condition_type, start_time, start_risk, peak_risk, episode_status)
            VALUES (%s, %s, NOW(), %s, %s, 'ACTIVE');
        """, (cow_id, condition, risk, risk))

# ==============================
# CORE BRAIN LOOP
# ==============================
def run_cycle():
    try:
        conn = db()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM herd_feature_recent;")
            rows = cur.fetchall()

            for r in rows:
                cow_id = r["cow_id"]
                milk_drop = r.get("milk_yield_drop_pct") or 0
                temp_off = r.get("temp_baseline_offset") or 0
                act_dev = r.get("activity_deviation_score") or 0
                rum_drop = r.get("rumination_drop_pct") or 0
                risk = r.get("peak_risk_score") or 0.5
                momentum = r.get("risk_momentum") or 0

                # =========================
                # 🧠 MASTITIS ENGINE
                # =========================
                if milk_drop > 15 and temp_off > 0.8 and act_dev > 10:
                    upsert_episode(cur, cow_id, "MASTITIS", risk)
                    trigger_alert(conn, cow_id, "CRITICAL", "MASTITIS", risk)
                    continue

                # =========================
                # 🧠 METABOLIC ENGINE
                # =========================
                if rum_drop > 20 and milk_drop > 10 and momentum > 0.2:
                    upsert_episode(cur, cow_id, "METABOLIC_STRESS", risk)
                    trigger_alert(conn, cow_id, "WARNING", "METABOLIC_STRESS", risk)
                    continue
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"❌ Brain execution loop error: {e}")

# ==============================
# RUN ENGINE
# ==============================
if __name__ == "__main__":
    log.info("🧠 HERDMIND-X CROSS-BOUNDARY BRAIN SYSTEM WORKER ONLINE.")
    while True:
        run_cycle()
        time.sleep(10)
