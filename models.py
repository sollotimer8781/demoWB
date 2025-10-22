from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.types import JSON

from demowb.db import Base

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


class ProductItem(Base):
    __tablename__ = "product_items"
    __table_args__ = (
        UniqueConstraint("source", "external_key", "external_key_type", name="uq_product_items_source_key"),
        {"sqlite_autoincrement": True},
    )

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String(64), nullable=False, index=True)
    external_key = Column(String(255), nullable=False, index=True)
    external_key_type = Column(String(64), nullable=False, index=True)
    product_id = Column(String(255), nullable=True)
    offer_id = Column(String(255), nullable=True)
    sku = Column(String(128), nullable=True)
    nm_id = Column(Integer, nullable=True, index=True)
    title = Column(String(512), nullable=True)
    brand = Column(String(255), nullable=True, index=True)
    price = Column(Float, nullable=True)
    stock = Column(Integer, nullable=True)
    image_urls = Column(MutableList.as_mutable(JSON), nullable=False, default=list)
    extra = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<ProductItem source={self.source!r} external_key={self.external_key!r}>"


class Coefficient(Base):
    __tablename__ = "coefficients"
    __table_args__ = (
        CheckConstraint("scope_type IN ('GLOBAL','CATEGORY','PRODUCT')", name="ck_coefficients_scope"),
        CheckConstraint("value_type IN ('TEXT','NUMBER')", name="ck_coefficients_value_type"),
        UniqueConstraint("scope_type", "scope_ref", "name", name="uq_coefficients_scope_key"),
        {"sqlite_autoincrement": True},
    )

    id = Column(Integer, primary_key=True, index=True)
    scope_type = Column(String(32), nullable=False, index=True)
    scope_ref = Column(String(255), nullable=True, index=True)
    name = Column(String(255), nullable=False)
    value = Column(String(255), nullable=False)
    value_type = Column(String(16), nullable=False, default="TEXT")
    unit = Column(String(64), nullable=True)
    extra = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<Coefficient scope_type={self.scope_type!r} name={self.name!r}>"


class PricingRule(Base):
    __tablename__ = "pricing_rules"
    __table_args__ = ({"sqlite_autoincrement": True},)

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    expression = Column(Text, nullable=False)
    priority = Column(Integer, nullable=False, default=0)
    is_enabled = Column(Boolean, nullable=False, default=True)
    extra = Column(MutableDict.as_mutable(_JSON_TYPE), nullable=False, default=dict)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:  # pragma: no cover - representation helper
        return f"<PricingRule name={self.name!r} priority={self.priority}>"
