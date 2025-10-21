from __future__ import annotations

from typing import List

import pandas as pd
import streamlit as st
from sqlalchemy import func, or_

from db import SessionLocal, init_db
from models import Product
from sync import sync_products

st.set_page_config(page_title="Products", layout="wide")
st.title("Products (Mock)")

init_db()

with SessionLocal() as session:
    existing_count = session.query(Product).count()

if existing_count == 0:
    with st.spinner("Загрузка мок-данных Wildberries..."):
        inserted, _ = sync_products()
    if inserted:
        st.success(f"Добавлено {inserted} товаров из мок-данных.")
    else:
        st.warning("Не удалось загрузить мок-данные. Проверьте файл data/sample_products.json.")

controls_container = st.container()
with controls_container:
    col_sync, col_search = st.columns([1, 3])
    with col_sync:
        sync_clicked = st.button("Sync now (mock)", type="primary", use_container_width=True)
    with col_search:
        search_query = st.text_input(
            "Поиск по названию или бренду",
            placeholder="Например, hoodie или MockSport",
        ).strip()

if sync_clicked:
    with st.spinner("Синхронизация мок-данных..."):
        inserted, updated = sync_products()
    if inserted or updated:
        st.success(f"Синхронизация завершена. Добавлено: {inserted}, обновлено: {updated}.")
    else:
        st.info("Данные уже актуальны.")


def load_products(query: str) -> List[Product]:
    with SessionLocal() as session:
        stmt = session.query(Product)
        if query:
            pattern = f"%{query.lower()}%"
            conditions = [func.lower(Product.title).like(pattern)]
            conditions.append(func.lower(Product.brand).like(pattern))
            if query.isdigit():
                try:
                    nm_id_value = int(query)
                    conditions.append(Product.nm_id == nm_id_value)
                except ValueError:
                    pass
            stmt = stmt.filter(or_(*conditions))
        stmt = stmt.order_by(Product.updated_at.desc(), Product.id.desc())
        return stmt.all()


products = load_products(search_query)

if not products:
    st.info("Нет данных для отображения. Нажмите 'Sync now (mock)', чтобы загрузить товары из мок-данных.")
    st.stop()

rows = []
for product in products:
    image_urls = product.image_urls or []
    if isinstance(image_urls, str):
        image_urls = [image_urls]
    extra = product.extra or {}
    rows.append(
        {
            "nm_id": product.nm_id,
            "title": product.title,
            "brand": product.brand,
            "price": product.price,
            "stock": product.stock,
            "image_urls": image_urls,
            "extra": extra,
            "created_at": product.created_at,
            "updated_at": product.updated_at,
        }
    )

products_df = pd.DataFrame(rows)
products_df["first_image"] = products_df["image_urls"].apply(
    lambda imgs: imgs[0] if isinstance(imgs, list) and imgs else None
)

try:
    st.dataframe(
        products_df[["nm_id", "title", "brand", "price", "stock", "first_image"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "first_image": st.column_config.ImageColumn(
                "Image",
                help="Первая картинка из списка",
                width="small",
            )
        },
    )
except Exception:
    st.dataframe(
        products_df[["nm_id", "title", "brand", "price", "stock", "first_image"]],
        use_container_width=True,
        hide_index=True,
    )

st.markdown("### Подробности по товарам")
max_rows = st.number_input(
    "Сколько товаров показать в подробном списке",
    min_value=1,
    max_value=len(products_df),
    value=min(len(products_df), 20),
)

for _, row in products_df.head(int(max_rows)).iterrows():
    header = f"{row['nm_id']} — {row['title']}"
    with st.expander(header):
        images = row.get("image_urls") or []
        if isinstance(images, list) and images:
            st.image(images, width=160)
        st.json(row.get("extra") or {})
