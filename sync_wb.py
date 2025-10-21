import json
import sqlite3
from typing import Dict, List, Tuple

import streamlit as st

from wb_client import WBClient, get_token_from_secrets, normalize_card_to_product

DB_PATH = "/home/engine/project/sqlite.db"
TABLE_NAME = "product_items"


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_key TEXT NOT NULL,
            external_key_type TEXT NOT NULL,
            nm_id INTEGER,
            title TEXT,
            brand TEXT,
            price REAL,
            stock INTEGER,
            image_urls TEXT, -- JSON array
            extra TEXT,      -- JSON object
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, external_key, external_key_type)
        )
        """
    )
    # Trigger to auto update updated_at
    conn.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS {TABLE_NAME}_updated_at_trigger
        AFTER UPDATE ON {TABLE_NAME}
        FOR EACH ROW
        BEGIN
            UPDATE {TABLE_NAME} SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;
        """
    )
    conn.commit()


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def upsert_products_wb(products: List[Dict]) -> Tuple[int, int]:
    inserted = 0
    updated = 0
    with get_connection() as conn:
        init_db(conn)
        for p in products:
            nm_id = p.get("nm_id")
            if nm_id is None:
                # Skip items without nm_id for stable external key
                continue
            title = p.get("title")
            brand = p.get("brand")
            price = p.get("price")
            stock = p.get("stock")
            image_urls = json.dumps(p.get("image_urls") or [])
            extra = json.dumps(p.get("extra") or {})

            # Upsert by (source, external_key, external_key_type)
            params = (
                "WB",
                str(nm_id),
                "WB:nm_id",
                nm_id,
                title,
                brand,
                price,
                stock,
                image_urls,
                extra,
                title,
                brand,
                price,
                stock,
                image_urls,
                extra,
            )
            cur = conn.execute(
                f"""
                INSERT INTO {TABLE_NAME} (
                    source, external_key, external_key_type,
                    nm_id, title, brand, price, stock, image_urls, extra
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, external_key, external_key_type) DO UPDATE SET
                    title = ?,
                    brand = ?,
                    price = ?,
                    stock = ?,
                    image_urls = ?,
                    extra = ?
                """,
                params,
            )
            if cur.rowcount == 1 and cur.lastrowid is not None:
                inserted += 1
            else:
                # SQLite returns -1 for UPSERT updates in some versions; count another way
                # Try to detect whether row existed: check changes()
                # We'll assume if lastrowid is None, it was an update
                updated += 1
        conn.commit()
    return inserted, updated


def sync_wb() -> Tuple[int, int]:
    token = get_token_from_secrets()
    if not token:
        raise RuntimeError("WB_API_TOKEN is not configured in Streamlit secrets")

    client = WBClient(token=token)
    cards = client.fetch_all_cards(limit=100)
    normalized = [normalize_card_to_product(c) for c in cards]

    inserted, updated = upsert_products_wb(normalized)
    return inserted, updated


@st.cache_data(ttl=300)
def load_wb_products_df():
    import pandas as pd
    with get_connection() as conn:
        init_db(conn)
        df = pd.read_sql(
            f"SELECT id, nm_id, title, brand, price, stock, image_urls, extra, created_at, updated_at FROM {TABLE_NAME} WHERE source = ?",
            conn,
            params=("WB",),
        )
    # Parse JSON fields
    def parse_json(value, default):
        try:
            if isinstance(value, str):
                return json.loads(value)
        except Exception:
            pass
        return default

    if not df.empty:
        df["image_urls"] = df["image_urls"].apply(lambda v: parse_json(v, []))
        df["extra"] = df["extra"].apply(lambda v: parse_json(v, {}))
    return df
