import os
import psycopg2
import bcrypt
import logging

log = logging.getLogger(__name__)

# Connection parameters mapping directly to your herd_postgres credentials configuration
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "herdmind")
DB_USER = os.getenv("POSTGRES_USER", "herd")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "herd123")

def get_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def init_auth_tables():
    """Generates the enterprise user schema table natively on startup if absent."""
    commands = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role VARCHAR(20) DEFAULT 'farmer',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(commands)
        cur.close()
        conn.commit()
        print("📁 [Auth Database] Enterprise user schema table synchronized successfully!")
    except Exception as e:
        print(f"🚨 [Auth Database] Initialization failed to build tables: {e}")
    finally:
        if conn:
            conn.close()

def create_user(username, password, role="farmer"):
    """Hashes the password and records the user credentials to PostgreSQL."""
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id;",
            (username, hashed, role)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return {"id": user_id, "username": username, "role": role}
    except psycopg2.errors.UniqueViolation:
        if conn: conn.rollback()
        return None
    finally:
        if conn: conn.close()

def verify_user_credentials(username, password):
    """Checks the plain text password against the hashed string stored in the database."""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT password_hash, role FROM users WHERE username = %s;", (username,))
        result = cur.fetchone()
        cur.close()
        
        if not result:
            return None
            
        stored_hash, role = result
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
            return {"username": username, "role": role}
    except Exception as e:
        print(f"🚨 [Auth Database] Authentication credential check failed: {e}")
    finally:
        if conn: conn.close()
    return None
