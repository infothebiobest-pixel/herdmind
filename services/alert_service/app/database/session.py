import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

logger = logging.getLogger("herd_alert_service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://herd:herd123@herd_postgres:5432/herdmind")
ASYNC_DB_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1) if DATABASE_URL.startswith("postgresql://") else DATABASE_URL

# Production-pooled database async connection properties
async_engine = create_async_engine(
    ASYNC_DB_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    echo=False
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def verify_postgres_connection() -> bool:
    from sqlalchemy import text
    try:
        async with async_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("📡 [PostgreSQL] Connection pool established and verified successfully.")
        return True
    except Exception as e:
        logger.critical(f"❌ [PostgreSQL] Core database handshake timeout failure: {e}")
        return False
