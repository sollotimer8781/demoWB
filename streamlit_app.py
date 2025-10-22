from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import streamlit as st
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from app_layout import APP_PAGES, initialize_page
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
    Используйте навигацию выше, чтобы перейти к управлению каталогом товаров, синхронизации с маркетплейсами
    или рабочему месту коэффициентов. На этой странице собраны ключевые статусы и рекомендации для запуска.
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


metrics = _load_catalog_metrics()
if metrics:
    col_products, col_imports = st.columns(2)
    col_products.metric("Товаров в каталоге", f"{metrics['products']:,}".replace(",", " " ))
    col_imports.metric("Импортов товаров", f"{metrics['imports']:,}".replace(",", " " ))

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

settings = {
    "DATABASE_URL": bool(_resolve_setting("DATABASE_URL")),
    "OZON_CLIENT_ID": bool(_resolve_setting("OZON_CLIENT_ID")),
    "OZON_API_KEY": bool(_resolve_setting("OZON_API_KEY")),
    "WB_API_TOKEN": bool(_resolve_setting("WB_API_TOKEN")),
}

missing_settings = [key for key, configured in settings.items() if not configured and key != "DATABASE_URL"]
if missing_settings:
    st.warning(
        "Не заполнены ключевые параметры: " + ", ".join(missing_settings) +
        ". Добавьте их в `.streamlit/secrets.toml` или через сигналы окружения."
    )
else:
    st.success("API ключи настроены.")

with st.expander("Как настроить .env и секреты", expanded=bool(missing_settings)):
    st.markdown(
        """
        1. Скопируйте шаблоны: `cp .streamlit/secrets.toml.example .streamlit/secrets.toml`.
        2. Укажите `DATABASE_URL`, если требуется внешняя БД (PostgreSQL, MySQL и т.д.).
        3. Добавьте ключи `OZON_CLIENT_ID`, `OZON_API_KEY`, `WB_API_TOKEN` для работы страниц маркетплейсов.
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
DATABASE_URL = "postgresql+psycopg2://user:password@host:5432/marketplace"
OZON_CLIENT_ID = "your_client_id"
OZON_API_KEY = "your_api_key"
WB_API_TOKEN = "your_wb_api_token"
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
