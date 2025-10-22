from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from demowb.db import SessionLocal, session_scope
from models import Coefficient, ProductItem

ALLOWED_SCOPE_TYPES = {"GLOBAL", "CATEGORY", "PRODUCT"}
ALLOWED_VALUE_TYPES = {"TEXT", "NUMBER"}


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


def _normalize_extra(value: Any) -> Dict[str, Any]:
    if value in (None, "", "null"):
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:  # noqa: TRY003
            raise ValueError(f"Некорректный JSON в extra: {exc}") from exc
        if isinstance(parsed, dict):
            return parsed
        return {"value": parsed}
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:  # noqa: TRY003
        raise ValueError(f"Некорректное значение JSON: {value}") from exc
    if isinstance(value, dict):
        return dict(value)
    return {"value": value}


def fetch_coefficients() -> List[Dict[str, Any]]:
    with SessionLocal() as session:
        stmt = select(Coefficient).order_by(Coefficient.scope_type, Coefficient.name, Coefficient.scope_ref)
        rows = session.scalars(stmt).all()

    result: List[Dict[str, Any]] = []
    for row in rows:
        value_type = (row.value_type or "TEXT").upper()
        raw_value: Any = row.value
        display_value: Any = raw_value
        if value_type == "NUMBER" and raw_value is not None:
            try:
                display_value = _to_float(raw_value)
            except ValueError:
                display_value = raw_value
        extra_payload = row.extra or {}
        extra_display = json.dumps(extra_payload, ensure_ascii=False) if extra_payload else ""
        result.append(
            {
                "id": row.id,
                "scope_type": (row.scope_type or "").upper(),
                "scope_ref": row.scope_ref,
                "name": row.name,
                "value": display_value,
                "value_type": value_type,
                "unit": row.unit,
                "extra": extra_display,
                "updated_at": row.updated_at,
            }
        )
    return result


def _prepare_coefficient_payload(record: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
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
        value_value = f"{value_float}"
    else:
        value_value = str(value_raw or "").strip()
        if not value_value:
            raise ValueError("Поле 'value' обязательно для заполнения")

    unit_value = record.get("unit")
    if unit_value is not None:
        unit_value = str(unit_value).strip() or None

    extra_value = _normalize_extra(record.get("extra"))

    if scope_type_raw == "PRODUCT" and not scope_ref_value:
        raise ValueError("Для scope_type=PRODUCT необходимо указать scope_ref")

    payload = {
        "scope_type": scope_type_raw,
        "scope_ref": scope_ref_value,
        "name": name_raw,
        "value": value_value,
        "value_type": value_type_raw,
        "unit": unit_value,
        "extra": extra_value,
    }
    return coeff_id, payload


def _check_duplicate(session: Session, payload: Dict[str, Any], coeff_id: Optional[int]) -> None:
    scope_ref_normalized = payload["scope_ref"] or ""
    stmt = (
        select(Coefficient.id)
        .where(
            Coefficient.scope_type == payload["scope_type"],
            func.coalesce(Coefficient.scope_ref, "") == scope_ref_normalized,
            Coefficient.name == payload["name"],
        )
        .limit(1)
    )
    if coeff_id is not None:
        stmt = stmt.where(Coefficient.id != coeff_id)
    duplicate = session.execute(stmt).scalar_one_or_none()
    if duplicate is not None:
        raise ValueError("Коэффициент с такими scope_type, scope_ref и name уже существует")


def apply_coefficients_changes(*, delete_ids: Sequence[int], upserts: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    if not delete_ids and not upserts:
        return {"inserted": 0, "updated": 0, "deleted": 0}

    inserted = 0
    updated = 0
    deleted = 0
    now = datetime.utcnow()

    delete_ids_int = [int(value) for value in delete_ids]

    with session_scope() as session:
        if delete_ids_int:
            deleted = (
                session.query(Coefficient)
                .filter(Coefficient.id.in_(delete_ids_int))
                .delete(synchronize_session=False)
            )

        for record in upserts:
            coeff_id, payload = _prepare_coefficient_payload(record)
            _check_duplicate(session, payload, coeff_id)
            if coeff_id is None:
                coefficient = Coefficient(**payload, created_at=now, updated_at=now)
                session.add(coefficient)
                inserted += 1
            else:
                coefficient = session.get(Coefficient, coeff_id)
                if coefficient is None:
                    coefficient = Coefficient(id=coeff_id, **payload, created_at=now, updated_at=now)
                    session.add(coefficient)
                    inserted += 1
                else:
                    for key, value in payload.items():
                        setattr(coefficient, key, value)
                    coefficient.updated_at = now
                    updated += 1

    return {"inserted": inserted, "updated": updated, "deleted": deleted}


def replace_all_coefficients(records: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    normalized: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[str, str, str]] = set()

    for record in records:
        coeff_id, payload = _prepare_coefficient_payload(record)
        if coeff_id is not None:
            payload["id"] = coeff_id  # Preserve provided IDs for completeness, though not required
        key = (payload["scope_type"], payload["scope_ref"] or "", payload["name"])
        if key in seen_keys:
            raise ValueError("В импортируемых данных есть дубли по scope_type/scope_ref/name")
        seen_keys.add(key)
        normalized.append(payload)

    with session_scope() as session:
        session.query(Coefficient).delete(synchronize_session=False)
        now = datetime.utcnow()
        for payload in normalized:
            payload = dict(payload)
            payload.pop("id", None)
            session.add(Coefficient(**payload, created_at=now, updated_at=now))

    return {"inserted": len(normalized), "updated": 0, "deleted": 0}


def list_sources() -> List[str]:
    with SessionLocal() as session:
        rows = (
            session.execute(
                select(ProductItem.source)
                .where(
                    ProductItem.source.is_not(None),
                    func.trim(ProductItem.source) != "",
                )
                .distinct()
                .order_by(ProductItem.source)
            )
            .scalars()
            .all()
        )
    return [row for row in rows if row]


def fetch_distinct_brands(source: str) -> List[str]:
    with SessionLocal() as session:
        rows = (
            session.execute(
                select(ProductItem.brand)
                .where(
                    ProductItem.source == source,
                    ProductItem.brand.is_not(None),
                    func.trim(ProductItem.brand) != "",
                )
                .distinct()
                .order_by(ProductItem.brand)
            )
            .scalars()
            .all()
        )
    return [row for row in rows if row]


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
    with SessionLocal() as session:
        extras = (
            session.execute(
                select(ProductItem.extra)
                .where(ProductItem.source == source, ProductItem.extra.is_not(None))
            )
            .scalars()
            .all()
        )

    categories: set[str] = set()
    for extra_value in extras:
        for category in extract_categories_from_extra(extra_value):
            categories.add(category)

    return sorted(categories)


def fetch_products_scope_candidates(source: str, limit: int = 500) -> List[Dict[str, Any]]:
    with SessionLocal() as session:
        stmt = (
            select(ProductItem)
            .where(ProductItem.source == source)
            .order_by(ProductItem.updated_at.desc(), ProductItem.id.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()

    result: List[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "id": row.id,
                "external_key": row.external_key,
                "sku": row.sku,
                "title": row.title,
                "brand": row.brand,
            }
        )
    return result


def extract_categories_from_series(series: Sequence[Any]) -> List[List[str]]:
    extracted: List[List[str]] = []
    for extra_value in series:
        extracted.append(extract_categories_from_extra(extra_value))
    return extracted
