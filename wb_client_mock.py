from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

_DATA_DIR = Path(__file__).resolve().parent / "data"
_SAMPLE_FILE = _DATA_DIR / "sample_products.json"


def _ensure_list_of_strings(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
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


def fetch_products_mock() -> List[Dict[str, Any]]:
    if not _SAMPLE_FILE.exists():
        return []
    try:
        with _SAMPLE_FILE.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
    except Exception:
        return []

    products: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return products

    for item in data:
        if not isinstance(item, dict):
            continue
        product = {
            "nm_id": item.get("nm_id"),
            "title": item.get("title"),
            "brand": item.get("brand"),
            "price": item.get("price"),
            "stock": item.get("stock"),
            "image_urls": _ensure_list_of_strings(item.get("image_urls")),
            "extra": _ensure_dict(item.get("extra")),
        }
        products.append(product)
    return products
