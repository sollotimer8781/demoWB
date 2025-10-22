from typing import List, Optional

import streamlit as st

from app_layout import initialize_page
from ozon_client import get_credentials_from_secrets
from sync_ozon import load_ozon_products_df, sync_ozon

initialize_page(
    page_title="Ozon Products",
    page_icon="🛒",
    current_page="pages/2_OZON_Products.py",
    description="Просмотр и синхронизация ассортимента из Ozon Seller API",
)

client_id, api_key = get_credentials_from_secrets()

with st.sidebar:
    st.header("Ozon Seller API")
    if client_id and api_key:
        st.success("Найдены OZON_CLIENT_ID и OZON_API_KEY в secrets")
    else:
        if not client_id and not api_key:
            st.warning("Секреты OZON_CLIENT_ID и OZON_API_KEY не найдены")
        elif not client_id:
            st.warning("Отсутствует OZON_CLIENT_ID")
        else:
            st.warning("Отсутствует OZON_API_KEY")
    st.caption(
        "При отсутствии ключей добавьте их в .streamlit/secrets.toml или в переменную окружения"
    )

col_sync, col_refresh, _ = st.columns([1, 1, 2])
with col_sync:
    do_sync = st.button("Sync now", type="primary", use_container_width=True)
with col_refresh:
    refresh = st.button("Refresh", use_container_width=True)

if do_sync:
    if not (client_id and api_key):
        st.error("Для синхронизации нужны OZON_CLIENT_ID и OZON_API_KEY в Streamlit secrets")
    else:
        with st.spinner("Синхронизация с Ozon Seller API..."):
            try:
                inserted, updated = sync_ozon()
                load_ozon_products_df.clear()
                st.success(f"Синхронизация завершена. Добавлено: {inserted}, обновлено: {updated}.")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Ошибка синхронизации: {exc}")

if refresh:
    load_ozon_products_df.clear()

try:
    df = load_ozon_products_df()
except Exception as exc:  # noqa: BLE001
    st.error(f"Ошибка чтения из базы: {exc}")
    st.stop()

if df.empty:
    if not (client_id and api_key):
        st.info(
            "Добавьте OZON_CLIENT_ID и OZON_API_KEY в .streamlit/secrets.toml:\n\n"
            "[secrets]\n"
            "OZON_CLIENT_ID='ваш_client_id'\n"
            "OZON_API_KEY='ваш_api_key'\n\n"
            "После добавления секретов перезапустите приложение и нажмите 'Sync now'."
        )
    else:
        st.info("Нет данных для отображения. Нажмите 'Sync now', чтобы загрузить товары из Ozon.")
    st.stop()

st.caption(f"Всего товаров: {len(df)}")

with st.expander("Фильтры и поиск", expanded=True):
    search_query = st.text_input(
        "Поиск по названию, бренду, product_id, offer_id или SKU", value=""
    )
    brands: List[str] = sorted(
        {str(b) for b in df["brand"].dropna().tolist() if isinstance(b, str) and b.strip()}
    )
    selected_brands = st.multiselect("Бренды", options=brands, default=[])
    min_stock = st.number_input("Минимальный остаток", min_value=0, value=0, step=1)
    only_with_price = st.checkbox("Только с ценой", value=False)

filtered = df.copy()
if search_query:
    q = search_query.strip().lower()
    if q:
        filtered = filtered[
            filtered.apply(
                lambda row: any(
                    str(row.get(field, "")).lower().find(q) != -1
                    for field in ("title", "brand", "product_id", "offer_id", "sku")
                ),
                axis=1,
            )
        ]
if selected_brands:
    filtered = filtered[filtered["brand"].isin(selected_brands)]
if min_stock:
    filtered = filtered[(filtered["stock"].fillna(0) >= min_stock)]
if only_with_price:
    filtered = filtered[filtered["price"].notna()]

if filtered.empty:
    st.info("Нет записей после применения фильтров.")
    st.stop()

filtered = filtered.copy()


def _first_image(urls: Optional[List[str]]) -> Optional[str]:
    if not urls:
        return None
    for url in urls:
        if isinstance(url, str) and url.strip():
            return url.strip()
    return None


filtered["image"] = filtered["image_urls"].apply(_first_image)

columns_to_show = [
    col
    for col in ["product_id", "offer_id", "sku", "title", "brand", "price", "stock", "image"]
    if col in filtered.columns
]

try:
    st.dataframe(
        filtered[columns_to_show],
        use_container_width=True,
        hide_index=True,
        column_config={
            "image": st.column_config.ImageColumn("Image", help="Первая картинка", width="small"),
        },
    )
except Exception:
    st.dataframe(filtered[columns_to_show], use_container_width=True, hide_index=True)

with st.expander("Детали и JSON (extra)"):
    max_rows = st.number_input(
        "Сколько строк показать", min_value=1, max_value=min(200, len(filtered)), value=min(50, len(filtered))
    )
    subset = filtered.head(int(max_rows))
    for _, row in subset.iterrows():
        header_parts = [row.get("title") or "Без названия"]
        if row.get("product_id"):
            header_parts.append(f"product_id: {row.get('product_id')}")
        if row.get("offer_id"):
            header_parts.append(f"offer_id: {row.get('offer_id')}")
        with st.expander(" — ".join(header_parts)):
            imgs = row.get("image_urls") or []
            if isinstance(imgs, list) and imgs:
                st.image([img for img in imgs if isinstance(img, str)], width=140)
            extra = row.get("extra", {})
            st.json(extra if isinstance(extra, dict) else {})
