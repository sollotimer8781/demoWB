from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from product_repository import DB_PATH, ensure_schema as ensure_products_schema

ALLOWED_SCOPE_TYPES = {"GLOBAL", "CATEGORY", "PRODUCT"}
ALLOWED_VALUE_TYPES = {"TEXT", "NUMBER"}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    ensure_products_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS coefficients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_type TEXT NOT NULL CHECK(scope_type IN ('GLOBAL', 'CATEGORY', 'PRODUCT')),
            scope_ref TEXT,
            name TEXT NOT NULL,
            value TEXT NOT NULL,
            value_type TEXT NOT NULL DEFAULT 'TEXT' CHECK(value_type IN ('TEXT', 'NUMBER')),
            unit TEXT,
            extra TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS coefficients_updated_at_trigger
        AFTER UPDATE ON coefficients
        FOR EACH ROW
        BEGIN
            UPDATE coefficients SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_coefficients_scope_name
        ON coefficients (scope_type, IFNULL(scope_ref, ''), name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pricing_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            expression TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            extra TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS pricing_rules_updated_at_trigger
        AFTER UPDATE ON pricing_rules
        FOR EACH ROW
        BEGIN
            UPDATE pricing_rules SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
        END;
        """
    )
    conn.commit()


def _parse_json(value: Any) -> Any:
    if value in (None, "", "null"):
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        if isinstance(parsed, (dict, list)):
            return parsed
        return {}
    return {}


def _dump_json(value: Any) -> str:
    if value in (None, ""):
        return json.dumps({}, ensure_ascii=False)
    if isinstance(value, str):
        value_str = value.strip()
        if not value_str:
            return json.dumps({}, ensure_ascii=False)
        try:
            parsed = json.loads(value_str)
        except json.JSONDecodeError as exc:  # noqa: TRY003
            raise ValueError(f"Некорректный JSON в extra: {exc}") from exc
        return json.dumps(parsed, ensure_ascii=False)
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError as exc:  # noqa: TRY003
        raise ValueError(f"Некорректное значение JSON: {value}") from exc


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "")
        if not text:
            return None
        normalized = text.replace(",", ".")
        try:
            return float(normalized)
        except ValueError as exc:  # noqa: TRY003
            raise ValueError(f"Не удалось преобразовать '{value}' к числу") from exc
    raise ValueError(f"Не удалось преобразовать '{value}' к числу")


def fetch_coefficients() -> List[Dict[str, Any]]:
    with get_connection() as conn:
        ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, scope_type, scope_ref, name, value, value_type, unit, extra, updated_at
            FROM coefficients
            ORDER BY scope_type, name, scope_ref
            """
        ).fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        value_type = (row["value_type"] or "TEXT").upper()
        raw_value: Any = row["value"]
        display_value: Any = raw_value
        if value_type == "NUMBER":
            try:
                display_value = _to_float(raw_value)
            except ValueError:
                display_value = raw_value
        extra_parsed = _parse_json(row["extra"])
        extra_display = ""
        if extra_parsed:
            extra_display = json.dumps(extra_parsed, ensure_ascii=False)
        result.append(
            {
                "id": row["id"],
                "scope_type": row["scope_type"],
                "scope_ref": row["scope_ref"],
                "name": row["name"],
                "value": display_value,
                "value_type": value_type,
                "unit": row["unit"],
                "extra": extra_display,
                "updated_at": row["updated_at"],
            }
        )
    return result


def _prepare_coefficient_payload(record: Dict[str, Any]) -> Tuple[Optional[int], str, Optional[str], str, str, str, Optional[str], str]:
    coeff_id = record.get("id")
    if coeff_id is not None:
        coeff_id = int(coeff_id)

    scope_type_raw = (record.get("scope_type") or "").strip().upper()
    if not scope_type_raw:
        raise ValueError("Поле 'scope_type' обязательно для заполнения")
    if scope_type_raw not in ALLOWED_SCOPE_TYPES:
        raise ValueError(f"Недопустимый scope_type: {scope_type_raw}")

    scope_ref_value = record.get("scope_ref")
    if scope_ref_value is not None:
        scope_ref_value = str(scope_ref_value).strip() or None

    name_raw = (record.get("name") or "").strip()
    if not name_raw:
        raise ValueError("Поле 'name' обязательно для заполнения")

    value_type_raw = (record.get("value_type") or "TEXT").strip().upper()
    if value_type_raw not in ALLOWED_VALUE_TYPES:
        raise ValueError(f"Недопустимый value_type: {value_type_raw}")

    value_raw = record.get("value")
    if value_type_raw == "NUMBER":
        value_float = _to_float(value_raw)
        value_str = f"{value_float}"
    else:
        value_str = str(value_raw or "").strip()
        if not value_str:
            raise ValueError("Поле 'value' обязательно для заполнения")

    unit_value = record.get("unit")
    if unit_value is not None:
        unit_value = str(unit_value).strip() or None

    extra_value = _dump_json(record.get("extra"))

    if scope_type_raw == "PRODUCT" and not scope_ref_value:
        raise ValueError("Для scope_type=PRODUCT необходимо указать scope_ref")

    return coeff_id, scope_type_raw, scope_ref_value, name_raw, value_str, value_type_raw, unit_value, extra_value


def apply_coefficients_changes(*, delete_ids: Sequence[int], upserts: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    if not delete_ids and not upserts:
        return {"inserted": 0, "updated": 0, "deleted": 0}

    inserted = 0
    updated = 0
    deleted = 0

    with get_connection() as conn:
        ensure_schema(conn)
        conn.execute("BEGIN")
        try:
            if delete_ids:
                for delete_id in delete_ids:
                    cur = conn.execute("DELETE FROM coefficients WHERE id = ?", (int(delete_id),))
                    deleted += cur.rowcount

            for record in upserts:
                (
                    coeff_id,
                    scope_type,
                    scope_ref,
                    name,
                    value,
                    value_type,
                    unit,
                    extra,
                ) = _prepare_coefficient_payload(record)

                params: List[Any] = [scope_type, scope_ref or "", name]
                query = (
                    "SELECT id FROM coefficients "
                    "WHERE scope_type = ? AND IFNULL(scope_ref, '') = ? AND name = ?"
                )
                if coeff_id is not None:
                    query += " AND id <> ?"
                    params.append(coeff_id)
                duplicate = conn.execute(query, params).fetchone()
                if duplicate:
                    raise ValueError(
                        "Коэффициент с такими scope_type, scope_ref и name уже существует"
                    )

                if coeff_id is None:
                    conn.execute(
                        """
                        INSERT INTO coefficients (scope_type, scope_ref, name, value, value_type, unit, extra)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (scope_type, scope_ref, name, value, value_type, unit, extra),
                    )
                    inserted += 1
                else:
                    conn.execute(
                        """
                        UPDATE coefficients
                        SET scope_type = ?, scope_ref = ?, name = ?, value = ?, value_type = ?, unit = ?, extra = ?
                        WHERE id = ?
                        """,
                        (scope_type, scope_ref, name, value, value_type, unit, extra, coeff_id),
                    )
                    updated += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return {"inserted": inserted, "updated": updated, "deleted": deleted}


def replace_all_coefficients(records: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    to_insert: List[Dict[str, Any]] = list(records)
    seen_keys: set[Tuple[str, str, str]] = set()
    for record in to_insert:
        (
            _coeff_id,
            scope_type,
            scope_ref,
            name,
            value,
            value_type,
            unit,
            extra,
        ) = _prepare_coefficient_payload(record)
        key = (scope_type, scope_ref or "", name)
        if key in seen_keys:
            raise ValueError("В импортируемых данных есть дубли по scope_type/scope_ref/name")
        seen_keys.add(key)
        record.update(
            {
                "scope_type": scope_type,
                "scope_ref": scope_ref,
                "name": name,
                "value": value,
                "value_type": value_type,
                "unit": unit,
                "extra": extra,
            }
        )

    with get_connection() as conn:
        ensure_schema(conn)
        conn.execute("BEGIN")
        try:
            conn.execute("DELETE FROM coefficients")
            for record in to_insert:
                conn.execute(
                    """
                    INSERT INTO coefficients (scope_type, scope_ref, name, value, value_type, unit, extra)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["scope_type"],
                        record.get("scope_ref"),
                        record["name"],
                        record["value"],
                        record["value_type"],
                        record.get("unit"),
                        record["extra"],
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return {"inserted": len(to_insert), "updated": 0, "deleted": 0}


def list_sources() -> List[str]:
    with get_connection() as conn:
        ensure_schema(conn)
        rows = conn.execute(
            "SELECT DISTINCT source FROM product_items WHERE source IS NOT NULL AND TRIM(source) <> '' ORDER BY source"
        ).fetchall()
    return [row[0] for row in rows if row[0]]


def fetch_distinct_brands(source: str) -> List[str]:
    with get_connection() as conn:
        ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT DISTINCT brand
            FROM product_items
            WHERE source = ? AND brand IS NOT NULL AND TRIM(brand) <> ''
            ORDER BY brand COLLATE NOCASE
            """,
            (source,),
        ).fetchall()
    return [row[0] for row in rows if row[0]]


def extract_categories_from_extra(extra_value: Any) -> List[str]:
    data = extra_value
    if isinstance(extra_value, str):
        extra_value = extra_value.strip()
        if not extra_value:
            return []
        try:
            data = json.loads(extra_value)
        except Exception:
            return []
    if not isinstance(data, dict):
        return []

    categories: List[str] = []
    for key, value in data.items():
        lower = key.lower()
        if "category" in lower or "subject" in lower:
            if isinstance(value, str):
                text = value.strip()
                if text:
                    categories.append(text)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    if isinstance(item, str):
                        text = item.strip()
                        if text:
                            categories.append(text)
    return categories


def fetch_distinct_categories(source: str) -> List[str]:
    with get_connection() as conn:
        ensure_schema(conn)
        rows = conn.execute(
            "SELECT extra FROM product_items WHERE source = ? AND extra IS NOT NULL AND TRIM(extra) <> ''",
            (source,),
        ).fetchall()

    categories: set[str] = set()
    for (extra_value,) in rows:
        for category in extract_categories_from_extra(extra_value):
            categories.add(category)

    return sorted(categories)


def fetch_products_scope_candidates(source: str, limit: int = 500) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, external_key, sku, title, brand
            FROM product_items
            WHERE source = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (source, limit),
        ).fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "external_key": row["external_key"],
                "sku": row["sku"],
                "title": row["title"],
                "brand": row["brand"],
            }
        )
    return result


def extract_categories_from_series(series: Sequence[Any]) -> List[List[str]]:
    extracted: List[List[str]] = []
    for extra_value in series:
        extracted.append(extract_categories_from_extra(extra_value))
    return extracted
