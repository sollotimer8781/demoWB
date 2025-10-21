import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
import streamlit as st


DEFAULT_BASE_URL = "https://suppliers-api.wildberries.ru"


class WBClient:
    def __init__(self, token: str, timeout: float = 30.0, max_retries: int = 3, base_url: str = DEFAULT_BASE_URL):
        if not token:
            raise ValueError("WB API token is required")
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": self.token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request_json(self, method: str, path: str, json: Optional[dict] = None, params: Optional[dict] = None) -> Tuple[int, dict]:
        url = f"{self.base_url}{path}"
        # Basic retry with exponential backoff
        backoff = 1.0
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    resp = client.request(method, url, headers=self.headers, json=json, params=params)
                if resp.status_code >= 500:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                return resp.status_code, data
            except Exception as e:
                last_exc = e
                time.sleep(backoff)
                backoff *= 2
        if last_exc:
            raise last_exc
        return 0, {}

    def fetch_cards_cursor_v1(self, limit: int = 100, updated_at: Optional[str] = None, nm_id_cursor: Optional[int] = None) -> List[dict]:
        # POST /content/v1/cards/cursor/list
        # Body shape for v1 cursor endpoint
        cursor = {}
        if updated_at:
            cursor["updatedAt"] = updated_at
        if nm_id_cursor:
            cursor["nmID"] = nm_id_cursor
        payload = {
            "sort": {"sortBy": "updateAt", "order": "asc"},
            "supplierID": 0,
            "limit": limit,
            "filter": {},
            "cursor": cursor or None,
        }
        status, data = self._request_json("POST", "/content/v1/cards/cursor/list", json=payload)
        if status != 200:
            return []
        cards = data.get("data", {}).get("cards", []) or []
        return cards

    def fetch_cards_v2(self, limit: int = 100, updated_at: Optional[str] = None, nm_id_cursor: Optional[int] = None) -> Tuple[List[dict], Optional[str], Optional[int]]:
        # POST /content/v2/get/cards/list
        cursor = {}
        if updated_at:
            cursor["updatedAt"] = updated_at
        if nm_id_cursor:
            cursor["nmID"] = nm_id_cursor
        payload = {
            "settings": {
                "cursor": {"limit": limit, **cursor},
                "filter": {},
            }
        }
        status, data = self._request_json("POST", "/content/v2/get/cards/list", json=payload)
        if status != 200:
            return [], None, None
        d = data.get("data", {})
        cards = d.get("cards", []) or []
        next_cursor = d.get("cursor", {})
        return cards, next_cursor.get("updatedAt"), next_cursor.get("nmID")

    def fetch_all_cards(self, limit: int = 100) -> List[dict]:
        # Try v2 first with cursor, fall back to v1 simple list.
        all_cards: List[dict] = []
        updated_at: Optional[str] = None
        nm_id_cursor: Optional[int] = None
        for _ in range(10000):  # safety cap
            cards, updated_at, nm_id_cursor = self.fetch_cards_v2(limit=limit, updated_at=updated_at, nm_id_cursor=nm_id_cursor)
            if not cards:
                break
            all_cards.extend(cards)
            # If less than limit returned, assume end
            if len(cards) < limit:
                break
        # Fallback if nothing fetched
        if not all_cards:
            # Single page v1 (no pagination), some APIs ignore cursor args
            cards = self.fetch_cards_cursor_v1(limit=limit)
            all_cards.extend(cards)
        return all_cards


def get_token_from_secrets() -> Optional[str]:
    try:
        token = st.secrets.get("WB_API_TOKEN")  # type: ignore[attr-defined]
        if token:
            return str(token)
    except Exception:
        pass
    return None


def normalize_card_to_product(card: Dict[str, Any]) -> Dict[str, Any]:
    # nm_id
    nm_id = (
        card.get("nmID")
        or card.get("nmId")
        or card.get("nmid")
        or card.get("nm")
    )
    try:
        nm_id = int(nm_id) if nm_id is not None else None
    except Exception:
        nm_id = None

    # title
    title = (
        card.get("title")
        or card.get("name")
        or card.get("object")
        or card.get("vendorCode")
        or ""
    )
    if not isinstance(title, str):
        title = str(title)

    brand = card.get("brand")
    if brand is not None and not isinstance(brand, str):
        brand = str(brand)

    # Try to compute stock from sizes[].stocks[].qty
    stock_total = 0
    sizes = card.get("sizes") or []
    try:
        for s in sizes:
            for stock in s.get("stocks", []) or []:
                qty = stock.get("qty")
                if qty is not None:
                    stock_total += int(qty)
    except Exception:
        stock_total = 0

    # Price may not be available in content API; try common fields
    price = None
    for k in ("price", "priceU", "basicPrice", "salePriceU"):
        if k in card and card.get(k) is not None:
            try:
                v = float(card.get(k))
                # Many WB prices are in integer cents (*100)
                if k.endswith("U") and v > 1000:
                    v = v / 100.0
                price = v
                break
            except Exception:
                continue

    # images
    image_urls: List[str] = []
    media_files = card.get("mediaFiles") or card.get("photos") or []
    try:
        for url in media_files:
            if not url:
                continue
            if isinstance(url, dict):
                u = url.get("big") or url.get("url") or url.get("img")
            else:
                u = str(url)
            if u and isinstance(u, str):
                image_urls.append(u)
    except Exception:
        image_urls = []

    return {
        "nm_id": nm_id,
        "title": title,
        "brand": brand,
        "price": price,
        "stock": stock_total,
        "image_urls": image_urls,
        "extra": card,
    }
