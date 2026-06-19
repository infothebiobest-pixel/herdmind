import datetime
from sqlalchemy import String, Integer, Float, Boolean, DateTime, ForeignKey, JSON, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Cow(Base):
    __tablename__ = "cows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    external_tag: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    herd_group: Mapped[str] = mapped_column(String(50), nullable=False, default="GENERAL")
    lifecycle_status: Mapped[str] = mapped_column(String(30), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    medical_logs: Mapped[list["MedicalLog"]] = relationship("MedicalLog", back_populates="cow", cascade="all, delete-orphan")
    agent_alerts: Mapped[list["AgentAlert"]] = relationship("AgentAlert", back_populates="cow", cascade="all, delete-orphan")

class MedicalLog(Base):
    __tablename__ = "medical_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cow_id: Mapped[int] = mapped_column(Integer, ForeignKey("cows.id", ondelete="CASCADE"), nullable=False, index=True)
    diagnosis: Mapped[str] = mapped_column(String(100), nullable=False)
    recommendation: Mapped[str] = mapped_column(String(500), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    recorded_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=True)

    cow: Mapped["Cow"] = relationship("Cow", back_populates="medical_logs")

class AgentAlert(Base):
    __tablename__ = "agent_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cow_id: Mapped[int] = mapped_column(Integer, ForeignKey("cows.id", ondelete="CASCADE"), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    stream_event_id: Mapped[str] = mapped_column(String(50), nullable=True)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now(), index=True)
    dispatched_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)

    cow: Mapped["Cow"] = relationship("Cow", back_populates="agent_alerts")
