from typing import List

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from app_layout import initialize_page
from sync_wb import load_wb_products_df, sync_wb
from wb_client import WBAPIError, WBConfigurationError, get_token_from_secrets

initialize_page(
    page_title="Wildberries Products",
    page_icon="🟣",
    current_page="pages/WB_Products.py",
    description="Просмотр ассортимента и синхронизация с Wildberries API",
)

token = get_token_from_secrets()

# Controls
col_sync, col_refresh = st.columns([1, 1])
with col_sync:
    do_sync = st.button("Sync now", type="primary", use_container_width=True)
with col_refresh:
    refresh = st.button("Refresh", use_container_width=True)

if do_sync:
    status = st.status("Синхронизация с Wildberries", expanded=True)
    if not token:
        status.update(
            label="WB_API_TOKEN отсутствует",
            state="error",
            expanded=True,
        )
        status.write(
            "Добавьте токен в `.streamlit/secrets.toml` или переменные окружения, чтобы синхронизировать товары."
        )
    else:
        status.write("Получаем карточки Wildberries…")
        try:
            inserted, updated = sync_wb()
        except WBConfigurationError as exc:
            status.update(label="Ошибка конфигурации", state="error", expanded=True)
            status.write(str(exc))
        except WBAPIError as exc:
            status.update(label="Ошибка синхронизации", state="error", expanded=True)
            status.write(str(exc))
        except Exception as exc:  # noqa: BLE001
            status.update(label="Неожиданная ошибка", state="error", expanded=True)
            status.write(str(exc))
        else:
            load_wb_products_df.clear()
            status.update(label="Синхронизация завершена", state="complete", expanded=True)
            status.write(f"Добавлено: {inserted}, обновлено: {updated}.")
            st.success(f"Синхронизация завершена. Добавлено: {inserted}, обновлено: {updated}.")

if refresh:
    load_wb_products_df.clear()

st.caption("Статус соединения и управление базой данных расположены в боковой панели.")

# Data loading (cached)
try:
    df = load_wb_products_df()
except SQLAlchemyError as exc:  # noqa: BLE001
    df = None
    load_wb_products_df.clear()
    st.error(f"Не удалось обратиться к базе данных Wildberries: {exc}")
    st.info("Используйте кнопку «Инициализировать БД» в боковой панели и обновите страницу после завершения.")
    st.stop()
except Exception as exc:  # noqa: BLE001
    df = None
    st.error(f"Произошла ошибка при загрузке данных Wildberries: {exc}")
    st.stop()

if df is None or df.empty:
    if not token:
        st.info(
            "WB_API_TOKEN не найден. Добавьте токен в `.streamlit/secrets.toml` или переменные окружения."
            " Следите за статусом в боковой панели приложения."
        )
    else:
        st.info("Нет данных для отображения. Нажмите «Sync now», чтобы загрузить ассортимент из Wildberries.")
    st.stop()

# Filters
with st.expander("Фильтры и поиск", expanded=True):
    q = st.text_input("Поиск по названию/бренду/ID", value="")
    brands: List[str] = sorted([b for b in df["brand"].dropna().unique().tolist() if isinstance(b, str)])
    selected_brands = st.multiselect("Бренды", options=brands, default=[])
    min_stock = st.number_input("Мин. остаток", min_value=0, value=0, step=1)

fdf = df.copy()
if q:
    q_low = q.strip().lower()
    fdf = fdf[
        fdf.apply(
            lambda r: (
                (str(r.get("title", "")).lower().find(q_low) != -1)
                or (str(r.get("brand", "")).lower().find(q_low) != -1)
                or (q_low.isdigit() and str(r.get("nm_id", "")) == q_low)
            ),
            axis=1,
        )
    ]
if selected_brands:
    fdf = fdf[fdf["brand"].isin(selected_brands)]
if min_stock:
    fdf = fdf[(fdf["stock"].fillna(0) >= min_stock)]

# Display table
if not fdf.empty:
    # Prepare first image column
    def first_img(lst):
        try:
            if isinstance(lst, list) and lst:
                return lst[0]
        except Exception:
            pass
        return None

    fdf = fdf.copy()
    fdf["image"] = fdf["image_urls"].apply(first_img)

    # Show interactive table
    try:
        st.dataframe(
            fdf[["nm_id", "title", "brand", "price", "stock", "image"]],
            use_container_width=True,
            column_config={
                "image": st.column_config.ImageColumn("Image", help="Первая картинка", width="small"),
            },
            hide_index=True,
        )
    except Exception:
        # Fallback without image column type
        st.dataframe(
            fdf[["nm_id", "title", "brand", "price", "stock", "image"]],
            use_container_width=True,
            hide_index=True,
        )

    # Optional: detailed view with images and extra JSON
    with st.expander("Детали и JSON (extra)"):
        max_rows = st.number_input("Max rows to preview", min_value=1, max_value=min(1000, len(fdf)), value=min(50, len(fdf)))
        subset = fdf.head(int(max_rows))
        for _, row in subset.iterrows():
            with st.expander(f"{row.get('nm_id')} — {row.get('title')}"):
                imgs = row.get("image_urls") or []
                if imgs:
                    st.image(imgs, width=120)
                st.json(row.get("extra", {}))
else:
    st.info("Нет записей после применения фильтров.")
