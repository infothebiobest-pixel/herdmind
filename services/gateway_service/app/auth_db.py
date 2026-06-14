import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("herd_gateway_auth")

DB_PARAMS = {
    "host": "postgres",  # Matches the Docker network service name perfectly
    "user": "herd",
    "password": "herd123",
    "dbname": "herdmind",
    "connect_timeout": 3
}

def wait_for_db():
    """
    Backoff protection gate to absorb container network timing jitter
    """
    logger.info("🔄 [Auth Database] Initiating database gate handshake loop...")
    for attempt in range(1, 21):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.close()
            logger.info("✅ [Auth Database] Database server discovered and verified ready.")
            return True
        except Exception as e:
            logger.warning(f"⚠️ [Auth Database] [Attempt {attempt}/20] Server not ready yet: {e}")
            time.sleep(2)
    raise RuntimeError("🔥 Failed to connect to PostgreSQL database infrastructure.")

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def init_auth_tables():
    # Wait for the storage engine to complete its local file setups first
    wait_for_db()
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'farm_manager' NOT NULL
            );
        """)
        conn.commit()
        logger.info("✅ [Auth Database] User tables synchronized cleanly.")
        
        # Seed an administrative account if table is empty
        cur.execute("SELECT COUNT(*) FROM users;")
        if cur.fetchone()[0] == 0:
            import bcrypt
            pwd = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s);",
                ("admin", pwd, "administrator")
            )
            conn.commit()
            logger.info("👤 [Auth Database] Default seed user profile injected.")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ [Auth Database] Table generation failure: {e}")
    finally:
        cur.close()
        conn.close()

def create_user(username, password, role="farm_manager"):
    import bcrypt
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        pwd_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id;",
            (username, pwd_hash, role)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        return user_id
    except psycopg2.IntegrityError:
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()

def verify_user_credentials(username, password):
    import bcrypt
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT username, password_hash, role FROM users WHERE username = %s;", (username,))
        user = cur.fetchone()
        if user and bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
            return {"username": user["username"], "role": user["role"]}
        return None
    except Exception:
        return None
    finally:
        cur.close()
        conn.close()
