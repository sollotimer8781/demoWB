from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import streamlit as st
from dotenv import load_dotenv
from sqlalchemy.engine import make_url

from demowb.db import get_database_url, init_db

logger = logging.getLogger(__name__)

APP_PAGES = [
    {"path": "streamlit_app.py", "label": "Главная", "icon": "🏠"},
    {"path": "pages/1_Products.py", "label": "Каталог товаров", "icon": "📦"},
    {"path": "pages/2_Profit_Calculator.py", "label": "Калькулятор рентабельности", "icon": "🧮"},
    {"path": "pages/2_OZON_Products.py", "label": "Ozon", "icon": "🛒"},
    {"path": "pages/WB_Products.py", "label": "Wildberries", "icon": "🟣"},
    {"path": "pages/SBIS_Products.py", "label": "SBIS", "icon": "📄"},
    {"path": "pages/Data_Workspace.py", "label": "Data Workspace", "icon": "📊"},
]

_APP_NAME = "demoWB — управление маркетплейсами"
_ENV_LOADED = False
_DATABASE_READY_KEY = "_app_database_ready"


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
        st.error(f"Не удалось инициализировать базу данных: {exc}")
        raise
    st.session_state[_DATABASE_READY_KEY] = True


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


def render_app_header() -> None:
    database_url = get_database_url()
    description = _describe_database(database_url)
    st.markdown(
        f"""
        <div style="padding:1.1rem 1.4rem;border-radius:0.75rem;background:linear-gradient(90deg,#f4f5ff,#ffffff);">
            <div style="font-size:1.1rem;font-weight:600;color:#353E7C;">{_APP_NAME}</div>
            <div style="font-size:0.9rem;color:#4C5BD5;">База данных: {description}</div>
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
) -> None:
    _load_environment()
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout="wide")
    _ensure_database_ready()
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
