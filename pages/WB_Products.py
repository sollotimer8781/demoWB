import json
import os
from typing import List

import pandas as pd
import streamlit as st

from sync_wb import load_wb_products_df, sync_wb
from wb_client import get_token_from_secrets

st.set_page_config(page_title="WB Products", layout="wide")

st.title("WB Products")

with st.sidebar:
    st.header("Wildberries API")
    token = get_token_from_secrets()
    if token:
        st.success("WB_API_TOKEN найден в secrets")
    else:
        st.warning("WB_API_TOKEN не найден. См. инструкции ниже на странице")

# Controls
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    do_sync = st.button("Sync now", type="primary", use_container_width=True)
with col2:
    refresh = st.button("Refresh", use_container_width=True)

if do_sync:
    if not token:
        st.error("WB_API_TOKEN отсутствует в secrets. Синхронизация невозможна.")
    else:
        with st.spinner("Синхронизация с Wildberries..."):
            try:
                inserted, updated = sync_wb()
                # clear cache to show fresh data
                load_wb_products_df.clear()
                st.success(f"Синхронизация завершена. Добавлено: {inserted}, обновлено: {updated}.")
            except Exception as e:
                st.error(f"Ошибка синхронизации: {e}")

if refresh:
    load_wb_products_df.clear()

# Data loading (cached)
df = load_wb_products_df()

if df.empty:
    if not token:
        st.info(
            "Не найден WB_API_TOKEN. Добавьте секрет в .streamlit/secrets.toml:\n\n"
            "[secrets]\nWB_API_TOKEN='ваш_токен_из_WB'\n\n"
            "Либо установите переменную окружения STREAMLIT_SECRETS с соответствующим JSON.")
    else:
        st.info("Нет данных для отображения. Нажмите 'Sync now' для загрузки товаров из WB.")
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
