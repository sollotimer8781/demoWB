from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.types import JSON

from demowb.db import Base

_JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


class LogisticTariff(Base):
    __tablename__ = "logistic_tariffs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), nullable=False, unique=True)
    base_first_l = Column(Float, nullable=False)
    per_next_l = Column(Float, nullable=False)
    active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<LogisticTariff name={self.name!r} base_first_l={self.base_first_l} per_next_l={self.per_next_l}>"


class ProfitScenario(Base):
    __tablename__ = "scenarios"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(String(255), nullable=True)
    inputs = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    results = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<ProfitScenario name={self.name!r}>"


__all__ = ["LogisticTariff", "ProfitScenario"]
