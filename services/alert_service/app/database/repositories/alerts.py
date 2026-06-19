from sqlalchemy.ext.asyncio import AsyncSession
from app.database.models import AgentAlert

class AlertRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_alert(self, cow_id: int, severity: str, risk_score: float, event_id: str) -> AgentAlert:
        alert_record = AgentAlert(cow_id=cow_id, severity=severity, risk_score=risk_score, stream_event_id=event_id)
        self.session.add(alert_record)
        return alert_record
