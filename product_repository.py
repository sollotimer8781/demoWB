import json
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_PATH = "/home/engine/project/sqlite.db"
TABLE_NAME = "product_items"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def _ensure_columns(conn: sqlite3.Connection, table: str, required: Dict[str, str]) -> None:
    existing_columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    for column, ddl in required.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_key TEXT NOT NULL,
            external_key_type TEXT NOT NULL,
            product_id TEXT,
            offer_id TEXT,
            sku TEXT,
            nm_id INTEGER,
            title TEXT,
            brand TEXT,
            price REAL,
            stock INTEGER,
            image_urls TEXT,
            extra TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, external_key, external_key_type)
        )
        """
    )
    _ensure_columns(
        conn,
        TABLE_NAME,
        {
            "product_id": "TEXT",
            "offer_id": "TEXT",
            "sku": "TEXT",
            "nm_id": "INTEGER",
            "image_urls": "TEXT",
            "extra": "TEXT",
        },
    )
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
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_source_key
        ON {TABLE_NAME} (source, external_key, external_key_type)
        """
    )
    conn.commit()


def _as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    try:
        text = str(value)
    except Exception:
        return None
    text = text.strip()
    return text or None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(" ", "")
        if not stripped:
            return None
        normalized = stripped.replace(",", ".")
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _ensure_list_of_strings(value: Any) -> List[str]:
    if not value:
        return []
    result: List[str] = []
    if isinstance(value, (list, tuple, set)):
        iterable = value
    else:
        iterable = [value]
    for item in iterable:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
            if text:
                result.append(text)
        else:
            try:
                text = str(item)
            except Exception:
                continue
            text = text.strip()
            if text:
                result.append(text)
    return result


def _ensure_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        json.dumps(value)
        return value  # type: ignore[return-value]
    except Exception:
        return {"value": str(value)}


def upsert_products(items: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
    inserted = 0
    updated = 0
    with get_connection() as conn:
        ensure_schema(conn)
        for item in items:
            source = _as_text(item.get("source"))
            external_key = _as_text(item.get("external_key"))
            external_key_type = _as_text(item.get("external_key_type"))
            if not source or not external_key or not external_key_type:
                continue

            product_id = _as_text(item.get("product_id"))
            offer_id = _as_text(item.get("offer_id"))
            sku = _as_text(item.get("sku"))
            nm_id = _as_int(item.get("nm_id"))
            title = item.get("title")
            if title is not None:
                title = str(title)
            brand = item.get("brand")
            if brand is not None:
                brand = str(brand)
            price = _as_float(item.get("price"))
            stock = _as_int(item.get("stock"))
            image_urls = json.dumps(_ensure_list_of_strings(item.get("image_urls")))
            extra = json.dumps(_ensure_json_object(item.get("extra")))

            existing = conn.execute(
                f"SELECT id FROM {TABLE_NAME} WHERE source = ? AND external_key = ? AND external_key_type = ?",
                (source, external_key, external_key_type),
            ).fetchone()

            if existing:
                conn.execute(
                    f"""
                    UPDATE {TABLE_NAME}
                    SET
                        product_id = ?,
                        offer_id = ?,
                        sku = ?,
                        nm_id = ?,
                        title = ?,
                        brand = ?,
                        price = ?,
                        stock = ?,
                        image_urls = ?,
                        extra = ?
                    WHERE id = ?
                    """,
                    (
                        product_id,
                        offer_id,
                        sku,
                        nm_id,
                        title,
                        brand,
                        price,
                        stock,
                        image_urls,
                        extra,
                        existing[0],
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (
                        source,
                        external_key,
                        external_key_type,
                        product_id,
                        offer_id,
                        sku,
                        nm_id,
                        title,
                        brand,
                        price,
                        stock,
                        image_urls,
                        extra
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source,
                        external_key,
                        external_key_type,
                        product_id,
                        offer_id,
                        sku,
                        nm_id,
                        title,
                        brand,
                        price,
                        stock,
                        image_urls,
                        extra,
                    ),
                )
                inserted += 1
        conn.commit()
    return inserted, updated


def _parse_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return default


def load_products_df(source: str):
    import pandas as pd

    with get_connection() as conn:
        ensure_schema(conn)
        df = pd.read_sql(
            f"""
            SELECT
                id,
                source,
                external_key,
                external_key_type,
                product_id,
                offer_id,
                sku,
                nm_id,
                title,
                brand,
                price,
                stock,
                image_urls,
                extra,
                created_at,
                updated_at
            FROM {TABLE_NAME}
            WHERE source = ?
            ORDER BY updated_at DESC, id DESC
            """,
            conn,
            params=(source,),
        )

    if df.empty:
        return df

    df["image_urls"] = df["image_urls"].apply(lambda v: _parse_json(v, []))
    df["extra"] = df["extra"].apply(lambda v: _parse_json(v, {}))
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce")

    if "nm_id" in df.columns:
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce").astype("Int64")

    for col in ("product_id", "offer_id", "sku", "external_key"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: None if v is None or pd.isna(v) else str(v).strip()
            )

    return df
