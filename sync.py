from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import select

from db import SessionLocal, init_db
from models import Product
from wb_client_mock import fetch_products_mock


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped.replace(",", ".")))
        except ValueError:
            return None
    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        stripped = stripped.replace(" ", "").replace(",", ".")
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _ensure_title(value: Any) -> str:
    if value is None:
        return "Untitled product"
    text = str(value).strip()
    return text or "Untitled product"


def _ensure_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_list_of_strings(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        result: List[str] = []
        for item in value:
            if item is None:
                continue
            result.append(str(item))
        return result
    return [str(value)]


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    return {"value": value}


def _load_products(use_mock: bool) -> Iterable[Dict[str, Any]]:
    if use_mock:
        return fetch_products_mock()
    raise ValueError("Real Wildberries client is not configured")


def sync_products(*, use_mock: bool = True) -> Tuple[int, int]:
    init_db()
    items = list(_load_products(use_mock))
    if not items:
        return 0, 0

    inserted = 0
    updated = 0
    session = SessionLocal()
    try:
        now = datetime.utcnow()
        for payload in items:
            if not isinstance(payload, dict):
                continue
            nm_id = _safe_int(payload.get("nm_id"))
            if nm_id is None:
                continue

            title = _ensure_title(payload.get("title"))
            brand = _ensure_optional_str(payload.get("brand"))
            price = _safe_float(payload.get("price"))
            stock = _safe_int(payload.get("stock"))
            image_urls = _ensure_list_of_strings(payload.get("image_urls"))
            extra = _ensure_dict(payload.get("extra"))

            product = session.scalars(select(Product).where(Product.nm_id == nm_id)).first()
            if product is None:
                product = Product(
                    nm_id=nm_id,
                    title=title,
                    brand=brand,
                    price=price,
                    stock=stock,
                    image_urls=image_urls,
                    extra=extra,
                    created_at=now,
                    updated_at=now,
                )
                session.add(product)
                inserted += 1
            else:
                product.title = title
                product.brand = brand
                product.price = price
                product.stock = stock
                product.image_urls = image_urls
                product.extra = extra
                product.updated_at = now
                updated += 1
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return inserted, updated
