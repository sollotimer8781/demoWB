from typing import Dict, List, Tuple

import streamlit as st

from product_repository import load_products_df, upsert_products
from wb_client import (
    WBClient,
    WBConfigurationError,
    get_token_from_secrets,
    normalize_card_to_product,
)


def upsert_products_wb(products: List[Dict]) -> Tuple[int, int]:
    records: List[Dict] = []
    for p in products:
        nm_id = p.get("nm_id")
        if nm_id is None:
            continue
        record = {
            "source": "WB",
            "external_key": str(nm_id),
            "external_key_type": "WB:nm_id",
            "nm_id": nm_id,
            "title": p.get("title"),
            "brand": p.get("brand"),
            "price": p.get("price"),
            "stock": p.get("stock"),
            "image_urls": p.get("image_urls"),
            "extra": p.get("extra"),
        }
        records.append(record)
    if not records:
        return 0, 0
    return upsert_products(records)


def sync_wb() -> Tuple[int, int]:
    token = get_token_from_secrets()
    if not token:
        raise WBConfigurationError(
            "WB_API_TOKEN отсутствует. Добавьте ключ в .streamlit/secrets.toml или переменные окружения."
        )

    client = WBClient(token=token)
    cards = client.fetch_all_cards(limit=100)
    normalized = [normalize_card_to_product(c) for c in cards]

    inserted, updated = upsert_products_wb(normalized)
    return inserted, updated


@st.cache_data(ttl=300)
def load_wb_products_df():
    df = load_products_df("WB")
    return df
