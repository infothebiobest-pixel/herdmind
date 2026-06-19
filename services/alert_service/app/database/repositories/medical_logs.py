from sqlalchemy.ext.asyncio import AsyncSession
from app.database.models import MedicalLog

class MedicalLogRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_log(self, cow_id: int, diagnosis: str, recommendation: str, confidence: float = 0.85) -> MedicalLog:
        log_entry = MedicalLog(cow_id=cow_id, diagnosis=diagnosis, recommendation=recommendation, confidence=confidence)
        self.session.add(log_entry)
        return log_entry
