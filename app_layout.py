from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st
from dotenv import load_dotenv
from sqlalchemy.engine import make_url

from demowb.db import get_database_url, init_db
from demowb.ui import inject_css
from wb_client import WBAPIError, WBClient, WBConfigurationError, get_token_from_secrets

logger = logging.getLogger(__name__)

APP_PAGES = [
    {"path": "streamlit_app.py", "label": "Главная", "icon": "🏠"},
    {"path": "pages/1_Products.py", "label": "Каталог товаров", "icon": "📦"},
    {"path": "pages/Custom_Fields.py", "label": "Пользовательские поля", "icon": "🧩"},
    {"path": "pages/WB_Products.py", "label": "Wildberries", "icon": "🟣"},
]

_APP_NAME = "demoWB — управление маркетплейсами"
_ENV_LOADED = False
_DATABASE_READY_KEY = "_app_database_ready"
_DATABASE_MESSAGE_KEY = "_app_database_message"
_WB_STATUS_KEY = "_app_wb_connection_status"
_CSS_PATH = Path(__file__).resolve().parent / "demowb" / "styles.css"


def _load_environment() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    project_root = Path(__file__).resolve().parent
    load_dotenv(override=False)
    load_dotenv(dotenv_path=project_root / ".env", override=False)
    _ENV_LOADED = True


def _ensure_database_ready() -> None:
    if st.session_state.get(_DATABASE_READY_KEY):
        return
    try:
        init_db()
    except Exception as exc:  # noqa: BLE001 - surface error in UI for visibility
        logger.exception("Database initialization failed")
        st.session_state[_DATABASE_MESSAGE_KEY] = (
            "error",
            f"Не удалось инициализировать базу данных: {exc}",
        )
        st.error(f"Не удалось инициализировать базу данных: {exc}")
        raise
    st.session_state[_DATABASE_READY_KEY] = True
    st.session_state[_DATABASE_MESSAGE_KEY] = ("success", "База данных готова.")


def _describe_database(url: str) -> str:
    try:
        url_obj = make_url(url)
    except Exception:  # pragma: no cover - safeguard for unparsable URLs
        return url

    backend = url_obj.get_backend_name()
    if backend == "sqlite":
        database = url_obj.database or ":memory:"
        name = Path(database).name if database not in {":memory:", None} else database
        return f"SQLite ({name})"

    host = url_obj.host or "localhost"
    db_name = url_obj.database or ""
    port = f":{url_obj.port}" if url_obj.port else ""
    return f"{backend}://{host}{port}/{db_name}".rstrip("/")


def _render_message(kind: str, message: str) -> None:
    if kind == "success":
        st.success(message)
    elif kind == "warning":
        st.warning(message)
    elif kind == "error":
        st.error(message)
    else:
        st.info(message)


def _render_sidebar() -> None:
    with st.sidebar:
        st.subheader("⚙️ База данных")
        db_message: Optional[Tuple[str, str]] = st.session_state.get(_DATABASE_MESSAGE_KEY)
        if st.button("Инициализировать БД", key="sidebar_init_db", use_container_width=True):
            try:
                init_db()
            except Exception as exc:  # noqa: BLE001 - surface error in UI for visibility
                logger.exception("Manual database initialization failed")
                db_message = (
                    "error",
                    f"Не удалось инициализировать базу данных: {exc}",
                )
                st.session_state[_DATABASE_READY_KEY] = False
            else:
                db_message = ("success", "База данных готова.")
                st.session_state[_DATABASE_READY_KEY] = True
            st.session_state[_DATABASE_MESSAGE_KEY] = db_message
        if not db_message:
            if st.session_state.get(_DATABASE_READY_KEY):
                db_message = ("success", "База данных готова.")
            else:
                db_message = ("warning", "База данных ещё не инициализирована.")
        if db_message:
            kind, text = db_message
            _render_message(kind, text)

        st.divider()
        st.subheader("🟣 Wildberries")
        token = get_token_from_secrets()
        if token:
            st.caption("WB_API_TOKEN найден в конфигурации.")
        else:
            st.caption("WB_API_TOKEN не найден. Укажите токен в секретах или переменных окружения.")
        wb_status: Optional[Tuple[str, str]] = st.session_state.get(_WB_STATUS_KEY)
        if st.button("Проверить соединение", key="sidebar_check_wb", use_container_width=True):
            if not token:
                wb_status = (
                    "error",
                    "WB_API_TOKEN отсутствует. Добавьте токен в .streamlit/secrets.toml или переменные окружения.",
                )
            else:
                try:
                    client = WBClient(token=token)
                    client.check_connection()
                except (WBConfigurationError, WBAPIError) as exc:
                    wb_status = ("error", str(exc))
                except Exception as exc:  # noqa: BLE001 - показываем неожиданную ошибку
                    logger.exception("Unexpected Wildberries connection failure")
                    wb_status = ("error", f"Не удалось проверить соединение с Wildberries: {exc}")
                else:
                    wb_status = ("success", "Соединение с Wildberries установлено.")
            st.session_state[_WB_STATUS_KEY] = wb_status
        wb_status = st.session_state.get(_WB_STATUS_KEY)
        if wb_status:
            kind, text = wb_status
            _render_message(kind, text)
        elif token:
            st.info("Нажмите «Проверить соединение», чтобы убедиться в доступности Wildberries API.")
        else:
            st.warning("Добавьте токен, чтобы проверить соединение с Wildberries.")


def get_database_status_message() -> Optional[Tuple[str, str]]:
    message = st.session_state.get(_DATABASE_MESSAGE_KEY)
    if message:
        return tuple(message)
    if st.session_state.get(_DATABASE_READY_KEY):
        return ("success", "База данных готова.")
    return None


def get_wb_connection_status() -> Optional[Tuple[str, str]]:
    status = st.session_state.get(_WB_STATUS_KEY)
    if status:
        return tuple(status)
    return None


def render_app_header() -> None:
    database_url = get_database_url()
    description = _describe_database(database_url)
    st.markdown(
        f"""
        <div class="demowb-app-header">
            <div class="demowb-app-header__title">{_APP_NAME}</div>
            <div class="demowb-app-header__subtitle">База данных: {description}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_navigation(current_page: str) -> None:
    nav_cols = st.columns(len(APP_PAGES))
    for idx, page in enumerate(APP_PAGES):
        with nav_cols[idx]:
            st.page_link(
                page["path"],
                label=page["label"],
                icon=page.get("icon"),
                disabled=page["path"] == current_page,
            )


def initialize_page(
    *,
    page_title: str,
    page_icon: str,
    current_page: str,
    description: Optional[str] = None,
    show_title: bool = True,
    inject_theme: bool = True,
) -> None:
    _load_environment()
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")
    if inject_theme:
        css_applied = inject_css(_CSS_PATH)
        if not css_applied:
            logger.warning("Не удалось применить стили из %s", _CSS_PATH)
    _ensure_database_ready()
    _render_sidebar()
    render_app_header()
    render_navigation(current_page)
    st.divider()
    if show_title:
        st.markdown(f"## {page_icon} {page_title}")
        if description:
            st.caption(description)
    elif description:
        st.caption(description)
    st.write("")
