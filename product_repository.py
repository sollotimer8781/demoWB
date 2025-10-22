from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from demowb.db import SessionLocal, session_scope
from models import ProductItem


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
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
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
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            if isinstance(decoded, list):
                value = decoded
        except Exception:
            value = [value]
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    result: List[str] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
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
        return dict(value)
    if value is None:
        return {}
    if isinstance(value, (list, tuple)):
        try:
            json.dumps(value)
        except TypeError:
            return {"value": list(value)}
        return {"value": list(value)}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {"value": text}
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"value": parsed}
        return {"value": parsed}
    try:
        json.dumps(value)
        return value  # type: ignore[return-value]
    except TypeError:
        return {"value": str(value)}


def _normalize_payload(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source = _as_text(item.get("source"))
    external_key = _as_text(item.get("external_key"))
    external_key_type = _as_text(item.get("external_key_type"))
    if not source or not external_key or not external_key_type:
        return None

    payload: Dict[str, Any] = {
        "source": source,
        "external_key": external_key,
        "external_key_type": external_key_type,
        "product_id": _as_text(item.get("product_id")),
        "offer_id": _as_text(item.get("offer_id")),
        "sku": _as_text(item.get("sku")),
        "nm_id": _as_int(item.get("nm_id")),
        "title": _as_text(item.get("title")),
        "brand": _as_text(item.get("brand")),
        "price": _as_float(item.get("price")),
        "stock": _as_int(item.get("stock")),
        "image_urls": _ensure_list_of_strings(item.get("image_urls")),
        "extra": _ensure_json_object(item.get("extra")),
    }
    return payload


def _fetch_existing(session: Session, payload: Dict[str, Any]) -> Optional[ProductItem]:
    stmt = (
        select(ProductItem)
        .where(
            ProductItem.source == payload["source"],
            ProductItem.external_key == payload["external_key"],
            ProductItem.external_key_type == payload["external_key_type"],
        )
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _apply_payload(model: ProductItem, payload: Dict[str, Any], *, timestamp: datetime) -> None:
    model.product_id = payload["product_id"]
    model.offer_id = payload["offer_id"]
    model.sku = payload["sku"]
    model.nm_id = payload["nm_id"]
    model.title = payload["title"]
    model.brand = payload["brand"]
    model.price = payload["price"]
    model.stock = payload["stock"]
    model.image_urls = payload["image_urls"] or []
    model.extra = payload["extra"] or {}
    model.updated_at = timestamp


def upsert_products(items: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
    inserted = 0
    updated = 0
    now = datetime.utcnow()

    normalized_items = [_normalize_payload(item) for item in items]
    normalized_items = [item for item in normalized_items if item is not None]
    if not normalized_items:
        return inserted, updated

    with session_scope() as session:
        for payload in normalized_items:
            assert payload is not None
            existing = _fetch_existing(session, payload)
            if existing is None:
                new_item = ProductItem(**payload, created_at=now, updated_at=now)
                session.add(new_item)
                inserted += 1
            else:
                _apply_payload(existing, payload, timestamp=now)
                updated += 1
    return inserted, updated


def _coerce_extra(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def load_products_df(source: str) -> pd.DataFrame:
    with SessionLocal() as session:
        stmt = (
            select(ProductItem)
            .where(ProductItem.source == source)
            .order_by(ProductItem.updated_at.desc(), ProductItem.id.desc())
        )
        items = session.scalars(stmt).all()

    rows: List[Dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "id": item.id,
                "source": item.source,
                "external_key": item.external_key,
                "external_key_type": item.external_key_type,
                "product_id": item.product_id,
                "offer_id": item.offer_id,
                "sku": item.sku,
                "nm_id": item.nm_id,
                "title": item.title,
                "brand": item.brand,
                "price": item.price,
                "stock": item.stock,
                "image_urls": item.image_urls or [],
                "extra": item.extra or {},
                "created_at": item.created_at.isoformat() if item.created_at else None,
                "updated_at": item.updated_at.isoformat() if item.updated_at else None,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce")
    if "nm_id" in df.columns:
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce").astype("Int64")

    if "image_urls" in df.columns:
        df["image_urls"] = df["image_urls"].apply(_ensure_list_of_strings)
    if "extra" in df.columns:
        df["extra"] = df["extra"].apply(_coerce_extra)

    for column in ("product_id", "offer_id", "sku", "external_key"):
        if column in df.columns:
            df[column] = df[column].apply(_as_text)

    return df
