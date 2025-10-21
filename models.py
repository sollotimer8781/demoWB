from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String
from sqlalchemy.types import JSON

from db import Base


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    nm_id = Column(Integer, nullable=False, unique=True, index=True)
    title = Column(String(512), nullable=False)
    brand = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)
    stock = Column(Integer, nullable=True)
    image_urls = Column(JSON, nullable=False, default=list)
    extra = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<Product nm_id={self.nm_id!r} title={self.title!r}>"
