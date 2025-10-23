from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import requests
from requests import Response, Session
from requests import exceptions as requests_exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:  # Streamlit might be unavailable in certain execution contexts (e.g. pure unit tests)
    import streamlit as st
except Exception:  # pragma: no cover - optional dependency during tests
    st = None  # type: ignore[assignment]


DEFAULT_BASE_URL = "https://suppliers-api.wildberries.ru"
DEFAULT_ENDPOINT_CARDS_CURSOR_V1 = "/content/v1/cards/cursor/list"
DEFAULT_ENDPOINT_CARDS_V2 = "/content/v2/get/cards/list"
DEFAULT_TIMEOUT: Tuple[float, float] = (5.0, 30.0)
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.8


class WBAPIError(RuntimeError):
    """Base exception for Wildberries API related errors."""

    def __init__(self, message: str, *, status_code: Optional[int] = None, url: Optional[str] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url


class WBConfigurationError(WBAPIError):
    """Raised when Wildberries configuration or credentials are missing/invalid."""


@dataclass(frozen=True)
class WBAPIConfig:
    base_url: str
    cards_cursor_v1_endpoint: str
    cards_v2_endpoint: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", _normalize_base_url(self.base_url))
        object.__setattr__(self, "cards_cursor_v1_endpoint", _normalize_endpoint(self.cards_cursor_v1_endpoint))
        object.__setattr__(self, "cards_v2_endpoint", _normalize_endpoint(self.cards_v2_endpoint))

    def build_url(self, endpoint: str) -> str:
        normalized = _normalize_endpoint(endpoint)
        if normalized.startswith("http://") or normalized.startswith("https://"):
            return normalized
        return f"{self.base_url}{normalized}"

    @classmethod
    def load(cls) -> WBAPIConfig:
        base = _resolve_setting("WB_API_BASE") or DEFAULT_BASE_URL
        endpoint_v1 = _resolve_setting("WB_API_ENDPOINT_CARDS_CURSOR_V1") or DEFAULT_ENDPOINT_CARDS_CURSOR_V1
        endpoint_v2 = _resolve_setting("WB_API_ENDPOINT_CARDS_V2") or DEFAULT_ENDPOINT_CARDS_V2
        return cls(
            base_url=base,
            cards_cursor_v1_endpoint=endpoint_v1,
            cards_v2_endpoint=endpoint_v2,
        )


def _normalize_base_url(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise WBConfigurationError("Базовый URL Wildberries API не задан. Укажите WB_API_BASE.")
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"}:
        raise WBConfigurationError("WB_API_BASE должен начинаться с http:// или https://.")
    if not parsed.netloc:
        raise WBConfigurationError("WB_API_BASE должен содержать хост.")
    return cleaned.rstrip("/")


def _normalize_endpoint(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise WBConfigurationError("Эндпоинт Wildberries API не может быть пустым.")
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    if not cleaned.startswith("/"):
        cleaned = "/" + cleaned
    return cleaned


@lru_cache(maxsize=1)
def load_config() -> WBAPIConfig:
    return WBAPIConfig.load()


def _resolve_setting(key: str) -> Optional[str]:
    env_value = os.getenv(key)
    if env_value and env_value.strip():
        return env_value.strip()

    if st is not None:
        for candidate in (key, key.lower()):
            try:
                value = st.secrets.get(candidate)  # type: ignore[attr-defined]
            except Exception:  # pragma: no cover - secrets may be unavailable
                continue
            if value and str(value).strip():
                return str(value).strip()
    return None


def _extract_json(response: Response) -> Dict[str, Any]:
    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError:
        return {}


def _unwrap_exception(exc: BaseException) -> BaseException:
    current = exc
    while True:
        cause = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
        if cause is None:
            return current
        current = cause


def _build_network_error(message: str, *, url: Optional[str] = None) -> WBAPIError:
    return WBAPIError(message, url=url)


def _host_from_url(url: Optional[str]) -> str:
    if not url:
        return "Wildberries"
    parsed = urlparse(url)
    return parsed.netloc or url


class WBClient:
    def __init__(
        self,
        token: str,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_url: Optional[str] = None,
        *,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        session: Optional[Session] = None,
        config: Optional[WBAPIConfig] = None,
    ) -> None:
        token = (token or "").strip()
        if not token:
            raise WBConfigurationError("WB API token отсутствует. Укажите WB_API_TOKEN в окружении или secrets.")
        self.token = token
        self.timeout = self._normalize_timeout(timeout)
        self.max_retries = max(0, int(max_retries))
        self.backoff_factor = max(0.0, float(backoff_factor))

        if config is None:
            config = load_config()
        if base_url:
            config = WBAPIConfig(
                base_url=base_url,
                cards_cursor_v1_endpoint=config.cards_cursor_v1_endpoint,
                cards_v2_endpoint=config.cards_v2_endpoint,
            )
        self.config = config

        self.headers = {
            "Authorization": self.token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self.session = session or self._create_session()

    @staticmethod
    def _normalize_timeout(value: Optional[Union[float, Tuple[float, float]]]) -> Tuple[float, float]:
        if value is None:
            return DEFAULT_TIMEOUT
        if isinstance(value, (int, float)):
            timeout = float(value)
            if timeout <= 0:
                raise WBConfigurationError("Значение таймаута должно быть положительным числом.")
            return timeout, timeout
        if isinstance(value, tuple) and len(value) == 2:
            connect, read = value
            if connect <= 0 or read <= 0:
                raise WBConfigurationError("Таймауты подключения и чтения должны быть положительными.")
            return float(connect), float(read)
        raise WBConfigurationError("Таймаут должен быть числом или кортежем из двух чисел (connect, read).")

    def _create_session(self) -> Session:
        session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            status=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=False,  # retry also on POST requests
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request_json(
        self,
        method: str,
        endpoint: str,
        *,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Dict[str, Any]:
        url = self.config.build_url(endpoint)
        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                headers=self.headers,
                json=json,
                params=params,
                timeout=self.timeout,
            )
        except requests_exceptions.RequestException as exc:
            raise self._to_wb_error(exc, url) from exc

        data = _extract_json(response)

        if response.status_code >= 400:
            raise self._http_error(response, data)

        return data

    def _http_error(self, response: Response, payload: Dict[str, Any]) -> WBAPIError:
        status = response.status_code
        detail_candidates = []
        if isinstance(payload, dict):
            for key in ("errorText", "message", "error_description", "detail", "description"):
                value = payload.get(key)
                if value:
                    detail_candidates.append(str(value))
            errors = payload.get("errors")
            if isinstance(errors, list):
                detail_candidates.extend([str(item) for item in errors if item])
        if not detail_candidates:
            text = response.text.strip()
            if text:
                detail_candidates.append(text[:200])
        detail = ". ".join(dict.fromkeys(detail_candidates))  # deduplicate preserving order
        host = _host_from_url(response.url)
        if status in (401, 403):
            base_message = (
                f"Wildberries API отклонил запрос (код {status}). Проверьте корректность WB_API_TOKEN."
            )
        elif status == 404:
            base_message = (
                f"Эндпоинт Wildberries API не найден (код 404). Проверьте настройки WB_API_ENDPOINT_* и WB_API_BASE."
            )
        else:
            base_message = f"Wildberries API вернул ошибку {status} при обращении к {host}."
        if detail:
            message = f"{base_message} Детали: {detail}."
        else:
            message = base_message
        return WBAPIError(message, status_code=status, url=response.url)

    def _to_wb_error(self, exc: requests_exceptions.RequestException, url: str) -> WBAPIError:
        root = _unwrap_exception(exc)
        host = _host_from_url(url)

        if isinstance(root, socket.gaierror):
            message = (
                f"Не удалось разрешить имя хоста `{host}` для Wildberries API. Проверьте значение WB_API_BASE."
            )
            return _build_network_error(message, url=url)
        if isinstance(exc, requests_exceptions.ConnectTimeout):
            message = f"Таймаут подключения к `{host}` (Wildberries API). Попробуйте позже или увеличьте таймаут."
            return _build_network_error(message, url=url)
        if isinstance(exc, requests_exceptions.ReadTimeout):
            message = f"Таймаут чтения данных от Wildberries API ({host}). Попробуйте повторить запрос позднее."
            return _build_network_error(message, url=url)
        if isinstance(exc, requests_exceptions.SSLError):
            message = f"Ошибка SSL при обращении к `{host}`. Проверьте сертификаты и WB_API_BASE."
            return _build_network_error(message, url=url)

        message = f"Ошибка сети при обращении к Wildberries API ({host}): {exc}."
        return _build_network_error(message, url=url)

    def check_connection(self) -> None:
        """Проверяет доступность API, выполняя минимальный запрос.

        Исключение WBAPIError будет выброшено при любой сетевой или HTTP-ошибке.
        """

        payload = {
            "settings": {
                "cursor": {"limit": 1},
                "filter": {},
            }
        }
        self._request_json("POST", self.config.cards_v2_endpoint, json=payload)

    def fetch_cards_cursor_v1(
        self,
        limit: int = 100,
        updated_at: Optional[str] = None,
        nm_id_cursor: Optional[int] = None,
    ) -> List[dict]:
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
        data = self._request_json("POST", self.config.cards_cursor_v1_endpoint, json=payload)
        cards = data.get("data", {}).get("cards", []) if isinstance(data, dict) else []
        return cards or []

    def fetch_cards_v2(
        self,
        limit: int = 100,
        updated_at: Optional[str] = None,
        nm_id_cursor: Optional[int] = None,
    ) -> Tuple[List[dict], Optional[str], Optional[int]]:
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
        data = self._request_json("POST", self.config.cards_v2_endpoint, json=payload)
        container = data.get("data", {}) if isinstance(data, dict) else {}
        cards = container.get("cards", []) or []
        next_cursor = container.get("cursor", {}) if isinstance(container, dict) else {}
        return cards, next_cursor.get("updatedAt"), next_cursor.get("nmID")

    def fetch_all_cards(self, limit: int = 100) -> List[dict]:
        all_cards: List[dict] = []
        updated_at: Optional[str] = None
        nm_id_cursor: Optional[int] = None
        for _ in range(10000):  # safety cap
            cards, updated_at, nm_id_cursor = self.fetch_cards_v2(
                limit=limit,
                updated_at=updated_at,
                nm_id_cursor=nm_id_cursor,
            )
            if not cards:
                break
            all_cards.extend(cards)
            if len(cards) < limit:
                break
        if not all_cards:
            cards = self.fetch_cards_cursor_v1(limit=limit)
            all_cards.extend(cards)
        return all_cards


def get_token_from_secrets() -> Optional[str]:
    return _resolve_setting("WB_API_TOKEN")


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
