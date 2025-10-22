"""create products and product import logs tables

Revision ID: 20241022_0001
Revises: 
Create Date: 2024-10-22 00:41:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20241022_0001"
down_revision = None
branch_labels = None
depends_on = None

json_type = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("sku", sa.String(length=128), nullable=True, unique=True, index=True),
        sa.Column("nm_id", sa.Integer(), nullable=True, unique=True, index=True),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("brand", sa.String(length=255), nullable=True, index=True),
        sa.Column("category", sa.String(length=255), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("stock", sa.Integer(), nullable=True),
        sa.Column("barcode", sa.String(length=128), nullable=True, unique=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("image_urls", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("custom_fields", json_type, nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )

    op.create_index("ix_products_title", "products", ["title"])
    op.create_index("ix_products_brand", "products", ["brand"])

    op.create_table(
        "product_import_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'success'")),
        sa.Column("rows_processed", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("inserted_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("errors", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("details", json_type, nullable=False, server_default=sa.text("'{}'")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
    )

    op.create_index("ix_product_import_logs_created_at", "product_import_logs", ["created_at"], descending=True)


def downgrade() -> None:
    op.drop_index("ix_product_import_logs_created_at", table_name="product_import_logs")
    op.drop_table("product_import_logs")
    op.drop_index("ix_products_brand", table_name="products")
    op.drop_index("ix_products_title", table_name="products")
    op.drop_table("products")
