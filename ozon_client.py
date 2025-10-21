import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import streamlit as st

DEFAULT_BASE_URL = "https://api-seller.ozon.ru"
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3


def get_credentials_from_secrets() -> Tuple[Optional[str], Optional[str]]:
    try:
        secrets = st.secrets  # type: ignore[attr-defined]
    except Exception:
        return None, None
    client_id = secrets.get("OZON_CLIENT_ID")  # type: ignore[assignment]
    api_key = secrets.get("OZON_API_KEY")  # type: ignore[assignment]
    return (
        str(client_id) if client_id else None,
        str(api_key) if api_key else None,
    )


def _safe_float(value: Any) -> Optional[float]:
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


def _safe_int(value: Any) -> Optional[int]:
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
            if "." in stripped or "," in stripped:
                return int(float(stripped.replace(",", ".")))
            return int(stripped)
        except ValueError:
            return None
    return None


def _collect_images(info_item: Dict[str, Any]) -> List[str]:
    images: List[str] = []
    raw_images = info_item.get("images")
    if isinstance(raw_images, list):
        for img in raw_images:
            if isinstance(img, str) and img.strip():
                images.append(img.strip())
            elif isinstance(img, dict):
                for key in ("url", "file_name", "preview"):
                    val = img.get(key)
                    if isinstance(val, str) and val.strip():
                        images.append(val.strip())
                        break
    primary = info_item.get("primary_image") or info_item.get("image")
    if isinstance(primary, str) and primary.strip():
        if primary.strip() not in images:
            images.insert(0, primary.strip())
    return images


def normalize_product(list_item: Optional[Dict[str, Any]], info_item: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    list_item = list_item or {}
    info_item = info_item or {}

    product_id = list_item.get("product_id") or info_item.get("product_id")
    offer_id = list_item.get("offer_id") or info_item.get("offer_id")
    sku = (
        list_item.get("sku")
        or info_item.get("sku")
        or info_item.get("fbs_sku")
        or info_item.get("fbo_sku")
    )

    external_source = "OZON"
    external_key: Optional[Any] = product_id if product_id is not None else offer_id
    if external_key is None:
        return None
    external_key_type = "OZON:product_id" if product_id is not None else "OZON:offer_id"

    title = (
        info_item.get("name")
        or list_item.get("name")
        or info_item.get("title")
        or info_item.get("product_name")
        or offer_id
        or ""
    )
    if not isinstance(title, str):
        title = str(title)

    brand = (
        info_item.get("brand_name")
        or info_item.get("brand")
        or list_item.get("brand")
    )
    if brand is not None and not isinstance(brand, str):
        brand = str(brand)

    price: Optional[float] = None
    price_container = info_item.get("price")
    if isinstance(price_container, dict):
        for key in ("price", "price_with_discount", "marketing_price", "min_price", "old_price"):
            candidate = _safe_float(price_container.get(key))
            if candidate is not None:
                price = candidate
                break
    if price is None:
        for key in ("price", "min_price", "old_price"):
            candidate = _safe_float(info_item.get(key))
            if candidate is not None:
                price = candidate
                break

    stock_total = 0
    has_stock_data = False
    stocks = info_item.get("stocks")
    if isinstance(stocks, list):
        for entry in stocks:
            if isinstance(entry, dict):
                present = entry.get("present")
                qty = _safe_int(present)
                if qty is not None:
                    stock_total += qty
                    has_stock_data = True
    stock = stock_total if has_stock_data else None

    image_urls = _collect_images(info_item)

    record = {
        "source": external_source,
        "external_key": str(external_key),
        "external_key_type": external_key_type,
        "product_id": str(product_id) if product_id is not None else None,
        "offer_id": str(offer_id) if offer_id is not None else None,
        "sku": str(sku) if sku is not None else None,
        "title": title,
        "brand": brand,
        "price": price,
        "stock": stock,
        "image_urls": image_urls,
        "extra": {
            "list_item": list_item,
            "info_item": info_item,
        },
    }
    return record


class OzonClient:
    def __init__(
        self,
        client_id: str,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        if not client_id or not api_key:
            raise ValueError("Ozon Client-Id and Api-Key are required")
        self.client_id = client_id
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.headers = {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        url = f"{self.base_url}{path}"
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for _ in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    resp = client.request(method, url, headers=self.headers, json=json, params=params)
                if resp.status_code >= 500:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                return resp.status_code, data
            except Exception as exc:  # network error, retry
                last_exc = exc
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
        if last_exc:
            raise last_exc
        return 0, {}

    @staticmethod
    def _extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        items = payload.get("items")
        if isinstance(items, list):
            return items
        result = payload.get("result")
        if isinstance(result, dict):
            nested_items = result.get("items") or result.get("products")
            if isinstance(nested_items, list):
                return nested_items
        return []

    @staticmethod
    def _extract_pagination(payload: Dict[str, Any], full_page: bool) -> Tuple[bool, Optional[str]]:
        candidates: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            candidates.append(payload)
            result = payload.get("result")
            if isinstance(result, dict):
                candidates.append(result)
        has_next_flag: Optional[bool] = None
        next_last_id: Optional[str] = None
        for container in candidates:
            if container.get("has_next") is not None and has_next_flag is None:
                has_next_flag = bool(container.get("has_next"))
            last_id_value = container.get("last_id") or container.get("next_page_id")
            if last_id_value:
                next_last_id = str(last_id_value)
        if next_last_id:
            has_more = full_page if has_next_flag is None else has_next_flag
            if not has_more:
                return False, None
            return True, next_last_id
        if has_next_flag is not None:
            return has_next_flag, None
        return False, None

    def fetch_product_list(self, limit: int = 100, visibility: str = "ALL") -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id: str = ""
        for _ in range(1000):  # safety cap
            payload: Dict[str, Any] = {
                "filter": {"visibility": visibility},
                "limit": limit,
            }
            if last_id:
                payload["last_id"] = last_id
            status, data = self._request_json("POST", "/v2/product/list", json=payload)
            if status != 200:
                break
            batch = self._extract_items(data)
            if not batch:
                break
            items.extend(batch)
            has_next, next_last_id = self._extract_pagination(data, len(batch) >= limit)
            if not has_next or not next_last_id or next_last_id == last_id:
                break
            last_id = next_last_id
        return items

    def fetch_product_info_list(self, limit: int = 100, visibility: str = "ALL") -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_id: str = ""
        for _ in range(1000):
            payload: Dict[str, Any] = {
                "filter": {"visibility": visibility},
                "limit": limit,
            }
            if last_id:
                payload["last_id"] = last_id
            status, data = self._request_json("POST", "/v3/product/info/list", json=payload)
            if status != 200:
                break
            batch = self._extract_items(data)
            if not batch:
                break
            items.extend(batch)
            has_next, next_last_id = self._extract_pagination(data, len(batch) >= limit)
            if not has_next or not next_last_id or next_last_id == last_id:
                break
            last_id = next_last_id
        return items

    def fetch_normalized_products(self, limit: int = 100, visibility: str = "ALL") -> List[Dict[str, Any]]:
        list_items = self.fetch_product_list(limit=limit, visibility=visibility)
        info_items = self.fetch_product_info_list(limit=limit, visibility=visibility)

        info_by_product_id: Dict[str, Dict[str, Any]] = {}
        info_by_offer_id: Dict[str, Dict[str, Any]] = {}
        for info in info_items:
            pid = info.get("product_id")
            if pid is not None:
                info_by_product_id[str(pid)] = info
            offer = info.get("offer_id")
            if offer:
                info_by_offer_id[str(offer)] = info

        normalized: List[Dict[str, Any]] = []
        seen_keys = set()

        for item in list_items:
            pid = item.get("product_id")
            info = None
            if pid is not None:
                info = info_by_product_id.get(str(pid))
            if info is None:
                offer = item.get("offer_id")
                if offer:
                    info = info_by_offer_id.get(str(offer))
            record = normalize_product(item, info)
            if record:
                composite_key = (
                    record["source"],
                    record["external_key"],
                    record["external_key_type"],
                )
                seen_keys.add(composite_key)
                normalized.append(record)

        for info in info_items:
            record = normalize_product({}, info)
            if record:
                composite_key = (
                    record["source"],
                    record["external_key"],
                    record["external_key_type"],
                )
                if composite_key not in seen_keys:
                    normalized.append(record)
                    seen_keys.add(composite_key)

        return normalized
