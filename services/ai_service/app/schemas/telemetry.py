from pydantic import BaseModel

class CowTelemetry(BaseModel):
    cow_id: int
    temperature: float
    activity: float
