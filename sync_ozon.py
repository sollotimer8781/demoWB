from typing import Tuple

import streamlit as st

from ozon_client import OzonClient, get_credentials_from_secrets
from product_repository import load_products_df, upsert_products


def sync_ozon(limit: int = 100) -> Tuple[int, int]:
    client_id, api_key = get_credentials_from_secrets()
    if not client_id or not api_key:
        raise RuntimeError("OZON_CLIENT_ID and OZON_API_KEY must be configured in Streamlit secrets")

    client = OzonClient(client_id=client_id, api_key=api_key)
    products = client.fetch_normalized_products(limit=limit)
    if not products:
        return 0, 0
    inserted, updated = upsert_products(products)
    return inserted, updated


@st.cache_data(ttl=300)
def load_ozon_products_df():
    return load_products_df("OZON")
