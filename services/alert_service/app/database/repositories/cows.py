from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.models import Cow

class CowRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_or_create_by_tag(self, external_tag: str) -> Cow:
        query = await self.session.execute(select(Cow).where(Cow.external_tag == external_tag))
        cow_instance = query.scalar_one_or_none()
        if not cow_instance:
            cow_instance = Cow(external_tag=external_tag, herd_group="MAIN_BARN", lifecycle_status="MONITORED")
            self.session.add(cow_instance)
            await self.session.flush()
        return cow_instance
