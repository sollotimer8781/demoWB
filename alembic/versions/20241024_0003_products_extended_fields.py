"""add extended product fields and pricing metadata

Revision ID: 20241024_0003
Revises: 20241023_0002
Create Date: 2024-10-24 00:00:00
"""
from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa


revision = "20241024_0003"
down_revision = "20241023_0002"
branch_labels = None
depends_on = None


def _coerce_mapping(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
        return {}
    return {}


def upgrade() -> None:
    op.add_column("products", sa.Column("seller_sku", sa.String(length=128), nullable=True))
    op.add_column("products", sa.Column("wb_sku", sa.String(length=128), nullable=True))
    op.add_column("products", sa.Column("price_src", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("seller_discount_pct", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("product_cost", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("shipping_cost", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("logistics_back_cost", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("warehouse_coeff", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("weight_kg", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("package_l_cm", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("package_w_cm", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("package_h_cm", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("volume_l", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("stock_wb", sa.Integer(), nullable=True))
    op.add_column("products", sa.Column("stock_seller", sa.Integer(), nullable=True))
    op.add_column("products", sa.Column("turnover_days", sa.Float(), nullable=True))
    op.add_column("products", sa.Column("comments", sa.Text(), nullable=True))

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    column_names = {column["name"] for column in inspector.get_columns("products")}
    if "extra" in column_names:
        rows = list(bind.execute(sa.text("SELECT id, custom_data, extra FROM products WHERE extra IS NOT NULL")))
        for row in rows:
            merged = _coerce_mapping(row.custom_data)
            extra_payload = _coerce_mapping(row.extra)
            if extra_payload:
                merged.update(extra_payload)
                bind.execute(
                    sa.text("UPDATE products SET custom_data = :payload WHERE id = :pid"),
                    {"pid": row.id, "payload": json.dumps(merged, ensure_ascii=False)},
                )
        op.drop_column("products", "extra")


def downgrade() -> None:
    op.drop_column("products", "comments")
    op.drop_column("products", "turnover_days")
    op.drop_column("products", "stock_seller")
    op.drop_column("products", "stock_wb")
    op.drop_column("products", "volume_l")
    op.drop_column("products", "package_h_cm")
    op.drop_column("products", "package_w_cm")
    op.drop_column("products", "package_l_cm")
    op.drop_column("products", "weight_kg")
    op.drop_column("products", "warehouse_coeff")
    op.drop_column("products", "logistics_back_cost")
    op.drop_column("products", "shipping_cost")
    op.drop_column("products", "product_cost")
    op.drop_column("products", "seller_discount_pct")
    op.drop_column("products", "price_src")
    op.drop_column("products", "wb_sku")
    op.drop_column("products", "seller_sku")
