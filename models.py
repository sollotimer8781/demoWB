from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.types import JSON

from db import Base

_JSON_TYPE = JSON().with_variant(JSONB, "postgresql")


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(128), nullable=True, unique=True, index=True)
    nm_id = Column(Integer, nullable=True, unique=True, index=True)
    title = Column(String(512), nullable=False)
    brand = Column(String(255), nullable=True, index=True)
    category = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)
    stock = Column(Integer, nullable=True)
    barcode = Column(String(128), nullable=True, unique=True)
    is_active = Column(Boolean, nullable=False, default=True)
    image_urls = Column(MutableList.as_mutable(JSON), nullable=False, default=list)
    custom_fields = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<Product sku={self.sku!r} title={self.title!r}>"


class ProductImportLog(Base):
    __tablename__ = "product_import_logs"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String(255), nullable=False)
    status = Column(String(32), nullable=False, default="success")
    rows_processed = Column(Integer, nullable=False, default=0)
    inserted_count = Column(Integer, nullable=False, default=0)
    updated_count = Column(Integer, nullable=False, default=0)
    errors = Column(MutableList.as_mutable(JSON), nullable=False, default=list)
    details = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<ProductImportLog file_name={self.file_name!r} status={self.status!r}>"
