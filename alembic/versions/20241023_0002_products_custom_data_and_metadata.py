"""add custom data column and metadata table

Revision ID: 20241023_0002
Revises: 20241022_0001
Create Date: 2024-10-23 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20241023_0002"
down_revision = "20241022_0001"
branch_labels = None
depends_on = None

json_type = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def upgrade() -> None:
    op.add_column(
        "products",
        sa.Column("custom_data", json_type, nullable=False, server_default=sa.text("'{}'")),
    )
    op.execute("UPDATE products SET custom_data = custom_fields WHERE custom_fields IS NOT NULL")

    op.create_table(
        "product_custom_fields",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False, server_default=sa.text("'string'")),
        sa.Column("default", json_type, nullable=True),
        sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("visible", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("order", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("choices", json_type, nullable=False, server_default=sa.text("'[]'")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("key", name="uq_product_custom_fields_key"),
    )
    op.create_index("ix_product_custom_fields_order", "product_custom_fields", ["order", "key"])

    op.alter_column("products", "custom_data", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_product_custom_fields_order", table_name="product_custom_fields")
    op.drop_table("product_custom_fields")
    op.drop_column("products", "custom_data")
