from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Tuple, Union
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


DEFAULT_BASE_URL = "https://marketplace-api.wildberries.ru"
DEFAULT_CONTENT_BASE_URL = "https://content-api.wildberries.ru"
DEFAULT_ENDPOINT_CARDS_CURSOR_V1 = "/content/v1/cards/cursor/list"
DEFAULT_ENDPOINT_CARDS_V2 = "/content/v2/get/cards/list"
DEFAULT_ENDPOINT_PRICES_LIST = "/api/v2/prices"
DEFAULT_ENDPOINT_PRICES_UPDATE = "/api/v2/prices"
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


class _AuthVariant(NamedTuple):
    header: str
    value: str
    label: str


@dataclass(frozen=True)
class WBAPIConfig:
    base_url: str
    content_base_url: str
    cards_cursor_v1_endpoint: str
    cards_v2_endpoint: str
    prices_list_endpoint: str
    prices_update_endpoint: str
    is_legacy_base: bool = False

    def __post_init__(self) -> None:
        base = _normalize_base_url(self.base_url)
        content_base = _normalize_base_url(self.content_base_url)
        object.__setattr__(self, "base_url", base)
        object.__setattr__(self, "content_base_url", content_base)
        object.__setattr__(self, "cards_cursor_v1_endpoint", _normalize_endpoint(self.cards_cursor_v1_endpoint))
        object.__setattr__(self, "cards_v2_endpoint", _normalize_endpoint(self.cards_v2_endpoint))
        object.__setattr__(self, "prices_list_endpoint", _normalize_endpoint(self.prices_list_endpoint))
        object.__setattr__(self, "prices_update_endpoint", _normalize_endpoint(self.prices_update_endpoint))
        object.__setattr__(self, "is_legacy_base", "suppliers-api.wildberries.ru" in base)

    def build_url(self, endpoint: str, *, use_content_api: bool = False) -> str:
        normalized = _normalize_endpoint(endpoint)
        if normalized.startswith("http://") or normalized.startswith("https://"):
            return normalized
        base = self.content_base_url if use_content_api else self.base_url
        return f"{base}{normalized}"

    def legacy_base_warning(self) -> Optional[str]:
        if self.is_legacy_base:
            return (
                "Используется устаревший домен `suppliers-api.wildberries.ru`. "
                "Обновите WB_API_BASE до https://marketplace-api.wildberries.ru."
            )
        return None

    @classmethod
    def load(cls) -> WBAPIConfig:
        base = _resolve_setting("WB_API_BASE") or DEFAULT_BASE_URL
        content_base = (
            _resolve_setting("WB_CONTENT_API_BASE")
            or _resolve_setting("WB_API_CONTENT_BASE")
        )
        if not content_base:
            content_base = DEFAULT_CONTENT_BASE_URL if base == DEFAULT_BASE_URL else base
        endpoint_v1 = _resolve_setting("WB_API_ENDPOINT_CARDS_CURSOR_V1") or DEFAULT_ENDPOINT_CARDS_CURSOR_V1
        endpoint_v2 = _resolve_setting("WB_API_ENDPOINT_CARDS_V2") or DEFAULT_ENDPOINT_CARDS_V2
        endpoint_prices = _resolve_setting("WB_API_ENDPOINT_PRICES") or DEFAULT_ENDPOINT_PRICES_LIST
        endpoint_prices_update = (
            _resolve_setting("WB_API_ENDPOINT_PRICES_UPDATE") or endpoint_prices or DEFAULT_ENDPOINT_PRICES_UPDATE
        )
        return cls(
            base_url=base,
            content_base_url=content_base,
            cards_cursor_v1_endpoint=endpoint_v1,
            cards_v2_endpoint=endpoint_v2,
            prices_list_endpoint=endpoint_prices,
            prices_update_endpoint=endpoint_prices_update,
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


def _resolve_setting(key: str, alt_keys: Optional[Iterable[str]] = None) -> Optional[str]:
    keys: List[str] = []
    for candidate in (key, *(alt_keys or [])):
        if candidate not in keys:
            keys.append(candidate)

    for candidate in keys:
        env_value = os.getenv(candidate)
        if env_value and env_value.strip():
            return env_value.strip()

    if st is None:
        return None

    try:
        secrets_obj = st.secrets  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - secrets may be unavailable
        return None

    section_names = ("wb", "WB", "wildberries", "WILDBERRIES")
    for section_name in section_names:
        try:
            section_value = secrets_obj.get(section_name)
        except Exception:  # pragma: no cover - secrets may be unavailable
            continue
        if not section_value:
            continue
        if hasattr(section_value, "items"):
            section_mapping = dict(section_value.items())  # type: ignore[call-arg]
        elif isinstance(section_value, dict):
            section_mapping = section_value
        else:
            continue
        for candidate in keys:
            for variant in (candidate, candidate.lower()):
                value = section_mapping.get(variant)
                if value and str(value).strip():
                    return str(value).strip()

    for candidate in keys:
        for variant in (candidate, candidate.lower()):
            try:
                value = secrets_obj.get(variant)
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
                content_base_url=config.content_base_url,
                cards_cursor_v1_endpoint=config.cards_cursor_v1_endpoint,
                cards_v2_endpoint=config.cards_v2_endpoint,
                prices_list_endpoint=config.prices_list_endpoint,
                prices_update_endpoint=config.prices_update_endpoint,
            )
        self.config = config

        self.base_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        self._auth_variants = self._prepare_auth_variants(self.token)
        if not self._auth_variants:
            raise WBConfigurationError("Не удалось сформировать заголовки авторизации для Wildberries API.")
        self._active_auth_index: Optional[int] = None
        self._last_success_auth_label: Optional[str] = None

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

    @staticmethod
    def _prepare_auth_variants(token: str) -> List[_AuthVariant]:
        stripped = token.strip()
        if not stripped:
            return []
        has_bearer_prefix = stripped.lower().startswith("bearer ")
        token_without_prefix = stripped[7:].strip() if has_bearer_prefix else stripped
        bearer_value = stripped if has_bearer_prefix else f"Bearer {stripped}"

        candidates: List[Tuple[str, str, str]] = [
            ("Authorization", bearer_value, "Authorization: Bearer"),
        ]
        if token_without_prefix:
            candidates.append(("Authorization", token_without_prefix, "Authorization"))
        candidates.append(("X-Authorization", bearer_value, "X-Authorization: Bearer"))
        if token_without_prefix:
            candidates.append(("X-Authorization", token_without_prefix, "X-Authorization"))

        variants: List[_AuthVariant] = []
        seen: set[Tuple[str, str]] = set()
        for header, value, label in candidates:
            if not value:
                continue
            key = (header.lower(), value)
            if key in seen:
                continue
            seen.add(key)
            variants.append(_AuthVariant(header=header, value=value, label=label))
        return variants

    def _build_headers_for_variant(self, index: int) -> Dict[str, str]:
        variant = self._auth_variants[index]
        headers = dict(self.base_headers)
        headers[variant.header] = variant.value
        return headers

    def _auth_sequence(self) -> List[int]:
        if not self._auth_variants:
            return []
        indices = list(range(len(self._auth_variants)))
        if self._active_auth_index is None:
            return indices
        return [self._active_auth_index] + [idx for idx in indices if idx != self._active_auth_index]

    def get_active_auth_label(self) -> Optional[str]:
        if self._active_auth_index is not None:
            return self._auth_variants[self._active_auth_index].label
        return self._last_success_auth_label

    def _with_legacy_warning(self, message: str) -> str:
        warning = self.config.legacy_base_warning()
        if warning and warning not in message:
            return f"{message} {warning}".strip()
        return message

    def _request_json(
        self,
        method: str,
        endpoint: str,
        *,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        use_content_api: bool = False,
    ) -> Any:
        url = self.config.build_url(endpoint, use_content_api=use_content_api)
        auth_order = self._auth_sequence()
        if not auth_order:
            raise WBConfigurationError("Не сконфигурированы варианты авторизации для Wildberries API.")

        auth_errors: List[Tuple[Response, Any, _AuthVariant]] = []
        for attempt_index, auth_index in enumerate(auth_order):
            headers = self._build_headers_for_variant(auth_index)
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    json=json,
                    params=params,
                    timeout=self.timeout,
                )
            except requests_exceptions.RequestException as exc:
                raise self._to_wb_error(exc, url) from exc

            data = _extract_json(response)

            if response.status_code < 400:
                if auth_index != self._active_auth_index:
                    self._active_auth_index = auth_index
                self._last_success_auth_label = self._auth_variants[auth_index].label
                return data

            if response.status_code in (401, 403):
                auth_errors.append((response, data, self._auth_variants[auth_index]))
                if attempt_index + 1 < len(auth_order):
                    self._active_auth_index = None
                    continue
                attempted_labels = [variant.label for _, _, variant in auth_errors]
                raise self._http_error(
                    response,
                    data,
                    auth_attempts=len(auth_errors),
                    attempted_variants=attempted_labels,
                )

            raise self._http_error(response, data)

        if auth_errors:
            last_response, last_payload, _ = auth_errors[-1]
            attempted_labels = [variant.label for _, _, variant in auth_errors]
            raise self._http_error(
                last_response,
                last_payload,
                auth_attempts=len(auth_errors),
                attempted_variants=attempted_labels,
            )

        raise WBAPIError("Wildberries API не ответил на запрос.", url=url)

    def _http_error(
        self,
        response: Response,
        payload: Any,
        *,
        auth_attempts: Optional[int] = None,
        attempted_variants: Optional[List[str]] = None,
    ) -> WBAPIError:
        status = response.status_code
        detail_candidates: List[str] = []
        if isinstance(payload, dict):
            for key in ("errorText", "message", "error_description", "detail", "description"):
                value = payload.get(key)
                if value:
                    detail_candidates.append(str(value))
            errors = payload.get("errors")
            if isinstance(errors, list):
                detail_candidates.extend([str(item) for item in errors if item])
        elif isinstance(payload, list):
            detail_candidates.extend([str(item) for item in payload[:3] if item])
        if not detail_candidates:
            text = response.text.strip()
            if text:
                detail_candidates.append(text[:300])
        detail = ". ".join(dict.fromkeys(detail_candidates)) if detail_candidates else ""
        host = _host_from_url(response.url)

        if status in (401, 403):
            base_message = (
                f"Wildberries API отклонил запрос (код {status}). Проверьте корректность WB_API_TOKEN."
            )
            if auth_attempts and auth_attempts > 1:
                base_message += " Были опробованы альтернативные заголовки Authorization/X-Authorization."
            if attempted_variants:
                unique_labels = list(dict.fromkeys(attempted_variants))
                base_message += " Попробованные варианты: " + ", ".join(unique_labels) + "."
        elif status == 404:
            base_message = (
                f"Эндпоинт Wildberries API не найден (код 404). Проверьте настройки WB_API_ENDPOINT_* и WB_API_BASE."
            )
        elif status == 429:
            retry_after = response.headers.get("Retry-After") if hasattr(response, "headers") else None
            base_message = "Wildberries API ограничил частоту запросов (код 429). Снизьте скорость обращений."
            if retry_after:
                base_message += f" Повторите запрос через {retry_after} сек."
        else:
            base_message = f"Wildberries API вернул ошибку {status} при обращении к {host}."

        if detail:
            if not base_message.endswith("."):
                base_message = base_message + "."
            message = f"{base_message} Детали: {detail}."
        else:
            message = base_message if base_message.endswith(".") else f"{base_message}."

        message = self._with_legacy_warning(message)
        return WBAPIError(message, status_code=status, url=response.url)

    def _to_wb_error(self, exc: requests_exceptions.RequestException, url: str) -> WBAPIError:
        root = _unwrap_exception(exc)
        host = _host_from_url(url)

        if isinstance(root, socket.gaierror):
            message = (
                f"Не удалось разрешить имя хоста `{host}` для Wildberries API. Проверьте значение WB_API_BASE."
            )
            return _build_network_error(self._with_legacy_warning(message), url=url)
        if isinstance(exc, requests_exceptions.ConnectTimeout):
            message = f"Таймаут подключения к `{host}` (Wildberries API). Попробуйте позже или увеличьте таймаут."
            return _build_network_error(self._with_legacy_warning(message), url=url)
        if isinstance(exc, requests_exceptions.ReadTimeout):
            message = f"Таймаут чтения данных от Wildberries API ({host}). Попробуйте повторить запрос позднее."
            return _build_network_error(self._with_legacy_warning(message), url=url)
        if isinstance(exc, requests_exceptions.SSLError):
            message = f"Ошибка SSL при обращении к `{host}`. Проверьте сертификаты и WB_API_BASE."
            return _build_network_error(self._with_legacy_warning(message), url=url)

        message = f"Ошибка сети при обращении к Wildberries API ({host}): {exc}."
        return _build_network_error(self._with_legacy_warning(message), url=url)

    def check_connection(self) -> Dict[str, Any]:
        """Проверяет доступность API минимальным запросом и возвращает диагностику."""

        payload = {
            "settings": {
                "cursor": {"limit": 1},
                "filter": {},
            }
        }
        data = self._request_json(
            "POST",
            self.config.cards_v2_endpoint,
            json=payload,
            use_content_api=True,
        )
        info: Dict[str, Any] = {
            "status": "ok",
            "base_url": self.config.base_url,
            "content_base_url": self.config.content_base_url,
            "auth_header": self.get_active_auth_label(),
        }
        if isinstance(data, dict):
            info["response_keys"] = list(data.keys())[:5]
        warning = self.config.legacy_base_warning()
        if warning:
            info["warning"] = warning
        return info

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
        data = self._request_json(
            "POST",
            self.config.cards_cursor_v1_endpoint,
            json=payload,
            use_content_api=True,
        )
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
        data = self._request_json(
            "POST",
            self.config.cards_v2_endpoint,
            json=payload,
            use_content_api=True,
        )
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

    def fetch_prices(
        self,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        nm_ids: Optional[Iterable[Union[int, str]]] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if limit is not None:
            params["limit"] = int(limit)
        if offset is not None:
            params["offset"] = int(offset)
        if nm_ids:
            nm_values: List[str] = []
            for nm in nm_ids:
                if nm is None:
                    continue
                try:
                    nm_values.append(str(int(nm)))
                except (TypeError, ValueError) as exc:  # pragma: no cover - defensive programming
                    raise WBConfigurationError(f"Некорректный nmID в запросе цен: {nm}") from exc
            if nm_values:
                params["nmID"] = ",".join(nm_values)
        return self._request_json("GET", self.config.prices_list_endpoint, params=params)

    def update_prices(
        self,
        updates: Iterable[Dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        normalized = [self._normalize_price_update(item) for item in updates if item]
        if not normalized:
            raise WBConfigurationError("Передайте хотя бы одну запись для обновления цен Wildberries.")
        if dry_run:
            return {"dry_run": True, "payload": normalized}
        data = self._request_json("POST", self.config.prices_update_endpoint, json=normalized)
        if isinstance(data, dict):
            return data
        return {"data": data}

    def _normalize_price_update(self, update: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(update, dict):
            raise WBConfigurationError("Каждая запись обновления цены должна быть словарём.")
        nm_raw = (
            update.get("nmId")
            or update.get("nmID")
            or update.get("nm_id")
            or update.get("nm")
        )
        if nm_raw is None:
            raise WBConfigurationError("Отсутствует nmId/nm_id/nmID в записи обновления цены.")
        try:
            nm_id = int(nm_raw)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive programming
            raise WBConfigurationError(f"Некорректный nmId для обновления цены: {nm_raw}") from exc

        price_fields = ("price", "priceRub", "price_rub", "priceU", "price_u")
        price_raw: Optional[Any] = None
        for key in price_fields:
            if key in update and update[key] is not None:
                price_raw = update[key]
                break
        if price_raw is None:
            raise WBConfigurationError(f"Отсутствует поле price/priceU для nmId {nm_id}.")
        try:
            price_value = float(price_raw)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive programming
            raise WBConfigurationError(f"Некорректное значение price для nmId {nm_id}: {price_raw}") from exc
        if price_value <= 0:
            raise WBConfigurationError(f"Цена должна быть положительной для nmId {nm_id}.")
        price_units = int(round(price_value * 100)) if price_value < 1000 else int(round(price_value))

        payload: Dict[str, Any] = {
            key: value
            for key, value in update.items()
            if key not in {"nmId", "nmID", "nm_id", "nm", "price", "priceRub", "price_rub", "priceU", "price_u"}
            and value is not None
        }
        payload["nmId"] = nm_id
        payload["price"] = price_units

        discount = update.get("discount")
        if discount is not None:
            try:
                discount_value = int(discount)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive programming
                raise WBConfigurationError(f"Некорректное значение discount для nmId {nm_id}: {discount}") from exc
            if not 0 <= discount_value <= 99:
                raise WBConfigurationError("Скидка должна быть в диапазоне 0-99%.")
            payload["discount"] = discount_value

        return payload


def get_token_from_secrets() -> Optional[str]:
    return _resolve_setting("WB_API_TOKEN", alt_keys=("WB_API_KEY",))


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
