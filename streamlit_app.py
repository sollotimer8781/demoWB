from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import streamlit as st
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from app_layout import APP_PAGES, get_database_status_message, get_wb_connection_status, initialize_page
from demowb.db import SessionLocal, get_database_url
from demowb.ui import inject_css
from models import Product, ProductImportLog

CSS_PATH = Path(__file__).resolve().parent / "demowb" / "styles.css"

initialize_page(
    page_title="Центр управления",
    page_icon="🛍️",
    current_page="streamlit_app.py",
    show_title=False,
    description="Единая точка входа Streamlit для работы с ассортиментом маркетплейсов.",
    inject_theme=False,
)

css_applied = inject_css(CSS_PATH)
if not css_applied:
    st.warning("Кастомные стили темы не были применены.")

st.markdown(
    """
    ### 🛍️ Добро пожаловать!
    Используйте навигацию выше, чтобы перейти к работе с каталогом и синхронизации Wildberries.
    Экспериментальные разделы временно скрыты, чтобы ускорить запуск WB-функций.
    На этой странице собраны ключевые статусы и рекомендации для старта.
    """
)


def _load_catalog_metrics() -> Optional[Dict[str, int]]:
    try:
        with SessionLocal() as session:
            total_products = session.scalar(select(func.count(Product.id))) or 0
            import_logs = session.scalar(select(func.count(ProductImportLog.id))) or 0
        return {"products": total_products, "imports": import_logs}
    except SQLAlchemyError as exc:  # pragma: no cover - runtime safeguard
        st.warning(f"Не удалось получить статистику каталога: {exc}")
        return None


def _resolve_setting(key: str) -> Optional[str]:
    try:
        secret_value = st.secrets.get(key)  # type: ignore[attr-defined]
    except Exception:
        secret_value = None
    if secret_value:
        return str(secret_value)
    env_value = os.getenv(key)
    if env_value:
        return env_value
    return None


def _format_database_label(url: str) -> str:
    try:
        url_obj = make_url(url)
    except Exception:  # pragma: no cover - unparsable URLs fall back to raw value
        return url
    backend = url_obj.get_backend_name()
    if backend == "sqlite":
        database = url_obj.database or ":memory:"
        name = database if database == ":memory:" else Path(database).name
        return f"SQLite ({name})"
    host = url_obj.host or "localhost"
    port = f":{url_obj.port}" if url_obj.port else ""
    db_name = url_obj.database or ""
    return f"{backend}://{host}{port}/{db_name}".rstrip("/")


def _render_status_message(kind: str, message: str) -> None:
    if kind == "success":
        st.success(message)
    elif kind == "warning":
        st.warning(message)
    elif kind == "error":
        st.error(message)
    else:
        st.info(message)


metrics = _load_catalog_metrics()
if metrics:
    col_products, col_imports = st.columns(2)
    col_products.metric("Товаров в каталоге", f"{metrics['products']:,}".replace(",", " " ))
    col_imports.metric("Импортов товаров", f"{metrics['imports']:,}".replace(",", " " ))

st.divider()

st.subheader("📋 Статусы интеграции")
status_cols = st.columns(2)
with status_cols[0]:
    st.markdown("#### База данных")
    db_status = get_database_status_message()
    if db_status:
        kind, message = db_status
        _render_status_message(kind, message)
    else:
        st.info("Статус появится после инициализации базы данных.")
with status_cols[1]:
    st.markdown("#### Wildberries API")
    wb_status = get_wb_connection_status()
    if wb_status:
        kind, message = wb_status
        _render_status_message(kind, message)
    else:
        token_present = bool(_resolve_setting("WB_API_TOKEN"))
        if not token_present:
            st.warning("WB_API_TOKEN не найден. Добавьте токен в .streamlit/secrets.toml или переменные окружения.")
        else:
            st.info("Используйте кнопку «Проверить соединение» в боковой панели, чтобы получить статус API.")
st.divider()

st.subheader("⚙️ Конфигурация и окружение")

database_url = get_database_url()
database_label = _format_database_label(database_url)
if database_url.startswith("sqlite"):
    st.info(
        "Используется встроенная SQLite-база `db.sqlite3`. Чтобы подключить PostgreSQL или другую СУБД," 
        " укажите переменную `DATABASE_URL` в `.env` или `.streamlit/secrets.toml`."
    )
else:
    st.success(f"Подключено внешнее хранилище: `{database_label}`")

token_present = bool(_resolve_setting("WB_API_TOKEN"))
if token_present:
    st.success("WB_API_TOKEN найден. Синхронизация с Wildberries доступна.")
else:
    st.warning(
        "WB_API_TOKEN не найден. Добавьте токен в `.streamlit/secrets.toml` или переменные окружения для работы с Wildberries."
    )

with st.expander("Как настроить .env и секреты", expanded=not token_present):
    st.markdown(
        """
        1. Скопируйте шаблон: `cp .streamlit/secrets.toml.example .streamlit/secrets.toml`.
        2. При необходимости укажите `DATABASE_URL` для подключения внешней БД (PostgreSQL, MySQL и др.).
        3. Добавьте `WB_API_TOKEN` — личный токен продавца из кабинета Wildberries.
        4. Перезапустите Streamlit после изменений.
        """
    )
    st.code(
        """# .env
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/marketplace
""",
        language="toml",
    )
    st.code(
        """# .streamlit/secrets.toml
WB_API_TOKEN = "your_wb_api_token"
# DATABASE_URL = "postgresql+psycopg2://user:password@host:5432/marketplace"
""",
        language="toml",
    )

st.divider()

st.subheader("🚀 Быстрый старт")
st.markdown("Выполните smoke-тест локально, чтобы убедиться в работоспособности приложения:")
st.code("streamlit run streamlit_app.py", language="bash")

st.markdown("""После запуска вы сможете перейти на нужную страницу через навигацию или ссылку ниже:""")
links_cols = st.columns(3)
for idx, page in enumerate(APP_PAGES[1:]):
    col = links_cols[idx % 3]
    with col:
        st.page_link(page["path"], label=page["label"], icon=page.get("icon"))

st.caption("Если вы разворачиваете приложение в облаке (Streamlit Cloud, Render, Railway), убедитесь, что" " переменные окружения и секреты заданы в настройках платформы.")
