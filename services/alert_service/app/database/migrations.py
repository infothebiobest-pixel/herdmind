import logging
from app.database.session import async_engine
from app.database.models import Base

logger = logging.getLogger("herd_alert_service")

async def run_auto_migrations():
    logger.info("🛠️ [Auto-Migration] Testing entity models schema maps...")
    try:
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("📋 [Auto-Migration] PostgreSQL schemas compiled successfully.")
    except Exception as e:
        logger.critical(f"❌ [Auto-Migration] Schema synchronization failure: {e}")
        raise e
