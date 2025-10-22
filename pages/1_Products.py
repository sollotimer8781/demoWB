from __future__ import annotations

import io
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Sequence, Set

import pandas as pd
import streamlit as st
from sqlalchemy import func, select

from app_layout import initialize_page
from demowb.db import SessionLocal
from models import Product
from product_service import (
    CUSTOM_PREFIX,
    CustomFieldDefinition,
    ProductFilters,
    available_aliases,
    bulk_update_field,
    export_products_dataframe,
    fetch_import_logs,
    get_available_brands,
    guess_import_column,
    import_products_from_dataframe,
    load_custom_field_definitions,
    load_products_dataframe,
    sanitize_custom_field_key,
    save_products_from_dataframe,
)
from sync import sync_products


TEMPLATE_SAMPLE_ROWS = [
    {
        "sku": "WB-10001",
        "seller_sku": "SELLER-10001",
        "wb_sku": "WB-10001-01",
        "nm_id": 10001,
        "title": "Футболка мужская базовая",
        "brand": "MockWear",
        "category": "Одежда",
        "price_src": 1299.0,
        "seller_discount_pct": 10.0,
        "price": 1169.1,
        "price_final": 1169.1,
        "stock": 45,
        "stock_wb": 30,
        "stock_seller": 15,
        "barcode": "1112223334445",
        "is_active": True,
        "product_cost": 620.0,
        "shipping_cost": 45.0,
        "logistics_back_cost": 20.0,
        "warehouse_coeff": 35.0,
        "turnover_days": 12.0,
        "weight_kg": 0.35,
        "package_l_cm": 34.0,
        "package_w_cm": 24.0,
        "package_h_cm": 4.0,
        "volume_l": 3.264,
        "comments": "Базовая позиция",
        "custom_data": {"commission_pct": 15, "tax_pct": 6},
        "color": "Черный",
        "size": "M",
        "season": "SS24",
    },
    {
        "sku": "WB-10002",
        "seller_sku": "SELLER-10002",
        "wb_sku": "WB-10002-01",
        "nm_id": 10002,
        "title": "Худи оверсайз женское",
        "brand": "MockWear",
        "category": "Одежда",
        "price_src": 2499.0,
        "seller_discount_pct": 15.0,
        "price": 2124.15,
        "price_final": 2124.15,
        "stock": 30,
        "stock_wb": 18,
        "stock_seller": 12,
        "barcode": "1112223334446",
        "is_active": True,
        "product_cost": 980.0,
        "shipping_cost": 65.0,
        "logistics_back_cost": 28.0,
        "warehouse_coeff": 42.0,
        "turnover_days": 18.0,
        "weight_kg": 0.58,
        "package_l_cm": 40.0,
        "package_w_cm": 28.0,
        "package_h_cm": 8.0,
        "volume_l": 8.96,
        "comments": "Сезонная коллекция",
        "custom_data": {"commission_pct": 18, "tax_pct": 6},
        "color": "Лавандовый",
        "size": "S",
        "season": "AW24",
    },
    {
        "sku": "WB-10003",
        "seller_sku": "SELLER-10003",
        "wb_sku": "WB-10003-01",
        "nm_id": 10003,
        "title": "Рюкзак городской",
        "brand": "Urban Mock",
        "category": "Аксессуары",
        "price_src": 1899.0,
        "seller_discount_pct": 12.0,
        "price": 1671.12,
        "price_final": 1671.12,
        "stock": 18,
        "stock_wb": 10,
        "stock_seller": 8,
        "barcode": "1112223334447",
        "is_active": True,
        "product_cost": 780.0,
        "shipping_cost": 55.0,
        "logistics_back_cost": 25.0,
        "warehouse_coeff": 38.0,
        "turnover_days": 21.0,
        "weight_kg": 0.74,
        "package_l_cm": 45.0,
        "package_w_cm": 32.0,
        "package_h_cm": 12.0,
        "volume_l": 17.28,
        "comments": "Влагостойкий материал",
        "custom_data": {"commission_pct": 17, "tax_pct": 6},
        "color": "Графит",
        "capacity_l": 18,
    },
]


def _build_template_dataframe(custom_fields: Sequence[CustomFieldDefinition]) -> pd.DataFrame:
    rows = [row.copy() for row in TEMPLATE_SAMPLE_ROWS]
    for row in rows:
        for field in custom_fields:
            if field.field_type == "choice" and field.choices:
                row[field.key] = field.choices[0]
            elif field.default is not None:
                row[field.key] = field.default
            elif field.field_type == "boolean":
                row[field.key] = False
            else:
                row[field.key] = ""
    return pd.DataFrame(rows)


def _read_uploaded_file(uploaded_file) -> pd.DataFrame:
    data = uploaded_file.read()
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data), engine="openpyxl")
    raise ValueError("Поддерживаются только файлы CSV и Excel")


def _ensure_session_defaults(custom_fields: Sequence[CustomFieldDefinition]) -> None:
    st.session_state.setdefault("products_search", "")
    st.session_state.setdefault("products_brand", "Все бренды")
    st.session_state.setdefault("products_active_only", False)
    st.session_state.setdefault("products_import_df", None)
    st.session_state.setdefault("products_import_filename", None)

    all_keys = [field.key for field in custom_fields]
    default_visible = [field.key for field in custom_fields if field.visible]

    st.session_state.setdefault("products_all_custom_fields", all_keys)
    st.session_state.setdefault("products_visible_custom_fields", default_visible or all_keys)

    st.session_state["products_all_custom_fields"] = all_keys
    visible = [key for key in st.session_state["products_visible_custom_fields"] if key in all_keys]
    if not visible and all_keys:
        visible = default_visible or all_keys
    st.session_state["products_visible_custom_fields"] = visible


def _coerce_active(value: object) -> bool:
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return True
    except Exception:
        pass
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return True
        if text in {"0", "false", "no", "n", "off", "нет"}:
            return False
        if text in {"1", "true", "yes", "y", "on", "да"}:
            return True
    return bool(value)


def _prepare_editor_dataframe(
    df: pd.DataFrame,
    custom_fields: Mapping[str, CustomFieldDefinition],
    visible_keys: Sequence[str],
) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    prepared = df.copy()

    for column in ("id", "nm_id"):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce").astype("Int64")

    if "stock" in prepared.columns:
        prepared["stock"] = pd.to_numeric(prepared["stock"], errors="coerce").astype("Int64")
    if "price" in prepared.columns:
        prepared["price"] = pd.to_numeric(prepared["price"], errors="coerce")

    numeric_float_columns = [
        "price_src",
        "seller_discount_pct",
        "price_final",
        "product_cost",
        "shipping_cost",
        "logistics_back_cost",
        "warehouse_coeff",
        "turnover_days",
        "weight_kg",
        "package_l_cm",
        "package_w_cm",
        "package_h_cm",
        "volume_l",
        "commission",
        "tax",
        "margin",
        "margin_percent",
    ]
    for column in numeric_float_columns:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    for column in ("stock_wb", "stock_seller"):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce").astype("Int64")

    if "is_active" in prepared.columns:
        prepared["is_active"] = prepared["is_active"].apply(_coerce_active).astype(bool)

    for column in ("sku", "seller_sku", "wb_sku", "title", "brand", "category", "barcode", "comments"):
        if column in prepared.columns:
            prepared[column] = prepared[column].astype("string").fillna("")

    if "custom_data" in prepared.columns:
        prepared["custom_data"] = prepared["custom_data"].astype("string").fillna("{}")

    for column in ("created_at", "updated_at"):
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce")

    for key in visible_keys:
        definition = custom_fields.get(key)
        if not definition:
            continue
        column_name = definition.column_name
        if column_name not in prepared.columns:
            continue
        if definition.field_type == "number":
            prepared[column_name] = pd.to_numeric(prepared[column_name], errors="coerce")
        elif definition.field_type == "boolean":
            prepared[column_name] = prepared[column_name].apply(lambda v: bool(v) if not pd.isna(v) else False)
        elif definition.field_type == "date":
            prepared[column_name] = pd.to_datetime(prepared[column_name], errors="coerce")
        else:
            prepared[column_name] = prepared[column_name].astype("string").fillna("")

    return prepared


def _build_column_config(definition: CustomFieldDefinition):
    try:
        if definition.field_type == "number":
            return st.column_config.NumberColumn(definition.name, format="%.2f")
        if definition.field_type == "boolean":
            return st.column_config.CheckboxColumn(definition.name)
        if definition.field_type == "date":
            return st.column_config.DateColumn(definition.name, format="YYYY-MM-DD")
        if definition.field_type == "choice" and definition.choices:
            try:
                return st.column_config.SelectboxColumn(
                    definition.name,
                    options=definition.choices,
                    required=False,
                )
            except Exception:
                return st.column_config.TextColumn(definition.name)
        return st.column_config.TextColumn(definition.name)
    except Exception:
        return st.column_config.TextColumn(definition.name)


initialize_page(
    page_title="Управление товарами",
    page_icon="📦",
    current_page="pages/1_Products.py",
    description="Каталог с импортом, экспортом и пользовательскими полями",
)

with SessionLocal() as session:
    custom_field_defs = load_custom_field_definitions(session)
    available_brands = get_available_brands(session)
    total_products = session.scalar(select(func.count(Product.id))) or 0

_ensure_session_defaults(custom_field_defs)

custom_field_map: Dict[str, CustomFieldDefinition] = {field.key: field for field in custom_field_defs}
ordered_keys = [field.key for field in custom_field_defs]
all_custom_fields = [key for key in ordered_keys if key in st.session_state["products_all_custom_fields"]]
st.session_state["products_all_custom_fields"] = all_custom_fields
visible_custom_fields = [key for key in ordered_keys if key in st.session_state["products_visible_custom_fields"]]
if not visible_custom_fields and ordered_keys:
    visible_custom_fields = [field.key for field in custom_field_defs if field.visible] or ordered_keys
    st.session_state["products_visible_custom_fields"] = visible_custom_fields

visible_custom_fields = st.session_state["products_visible_custom_fields"]

with st.sidebar:
    st.subheader("Фильтры")
    search_value = st.text_input(
        "Поиск по названию, бренду, SKU или NM ID",
        value=st.session_state["products_search"],
    ).strip()
    st.session_state["products_search"] = search_value

    brand_options = ["Все бренды"] + available_brands
    default_brand = st.session_state.get("products_brand", "Все бренды")
    if default_brand not in brand_options:
        default_brand = "Все бренды"
    brand_value = st.selectbox(
        "Бренд",
        options=brand_options,
        index=brand_options.index(default_brand),
    )
    st.session_state["products_brand"] = brand_value

    active_only = st.checkbox(
        "Только активные",
        value=st.session_state.get("products_active_only", False),
    )
    st.session_state["products_active_only"] = active_only

    st.markdown("---")
    with st.expander("Пользовательские колонки", expanded=False):
        st.caption("Настройте отображение полей. Для создания и редактирования полей перейдите на страницу управления.")
        st.page_link("pages/Custom_Fields.py", label="Управление пользовательскими полями", icon="🛠️")
        selected_fields = st.multiselect(
            "Отображать в таблице",
            options=ordered_keys,
            default=visible_custom_fields,
            format_func=lambda key: f"{custom_field_map[key].name} ({key})" if key in custom_field_map else key,
            key="products_visible_custom_fields_selector",
        )
        selected_set = set(selected_fields)
        ordered_selected = [key for key in ordered_keys if key in selected_set]
        st.session_state["products_visible_custom_fields"] = ordered_selected

        col_hide, col_show = st.columns(2)
        with col_hide:
            if st.button("Скрыть все", key="products_hide_all_custom"):
                st.session_state["products_visible_custom_fields"] = []
        with col_show:
            if st.button("Показать все", key="products_show_all_custom"):
                st.session_state["products_visible_custom_fields"] = ordered_keys

filters = ProductFilters(
    search=search_value or None,
    brand=None if brand_value == "Все бренды" else brand_value,
    active_only=active_only,
)
visible_custom_fields = st.session_state["products_visible_custom_fields"]

with SessionLocal() as session:
    products_df, _ = load_products_dataframe(session, filters, custom_field_defs, visible_custom_fields)

products_df = _prepare_editor_dataframe(products_df, custom_field_map, visible_custom_fields)
selection_count = len(products_df)

catalog_tab, import_tab, export_tab, logs_tab = st.tabs([
    "Каталог",
    "Импорт",
    "Экспорт",
    "Журнал импортов",
])

with catalog_tab:
    st.subheader("Редактирование и просмотр товаров")
    metrics_cols = st.columns(3)
    metrics_cols[0].metric("Всего в базе", total_products)
    metrics_cols[1].metric("В выборке", selection_count)
    metrics_cols[2].metric("Отображаемых custom полей", len(visible_custom_fields))

    if st.button("Загрузить тестовые данные (WB mock)", key="products_sync_mock"):
        with st.spinner("Загрузка мок-данных..."):
            inserted, updated = sync_products()
        if inserted or updated:
            st.success(f"Импорт завершён. Добавлено: {inserted}, обновлено: {updated}.")
        else:
            st.info("Данные уже актуальны.")
        st.experimental_rerun()

    if products_df.empty:
        st.info("Нет данных для отображения. Загрузите их через импорт или используйте мок-данные.")
    else:
        column_config = {
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "sku": st.column_config.TextColumn("SKU"),
            "seller_sku": st.column_config.TextColumn("Артикул продавца"),
            "wb_sku": st.column_config.TextColumn("Артикул WB"),
            "nm_id": st.column_config.NumberColumn("NM ID", step=1),
            "title": st.column_config.TextColumn("Название"),
            "brand": st.column_config.TextColumn("Бренд"),
            "category": st.column_config.TextColumn("Категория"),
            "price_src": st.column_config.NumberColumn("Цена на витрине", format="%.2f ₽", step=1.0),
            "seller_discount_pct": st.column_config.NumberColumn("Скидка продавца, %", format="%.2f %", step=0.5),
            "price": st.column_config.NumberColumn(
                "Итоговая цена",
                format="%.2f ₽",
                disabled=True,
                help="Рассчитывается из цены на витрине и скидки",
            ),
            "price_final": st.column_config.NumberColumn("Цена со скидкой", format="%.2f ₽", disabled=True),
            "stock": st.column_config.NumberColumn("Остаток", step=1, help="Количество на складе"),
            "stock_wb": st.column_config.NumberColumn("Остаток WB", step=1),
            "stock_seller": st.column_config.NumberColumn("Остаток продавца", step=1),
            "turnover_days": st.column_config.NumberColumn("Оборачиваемость, дни", format="%.1f"),
            "product_cost": st.column_config.NumberColumn("Себестоимость", format="%.2f ₽", step=1.0),
            "shipping_cost": st.column_config.NumberColumn("Доставка до склада", format="%.2f ₽", step=1.0),
            "logistics_back_cost": st.column_config.NumberColumn("Логистика возврата", format="%.2f ₽", step=1.0),
            "warehouse_coeff": st.column_config.NumberColumn("Коэфф. склада", format="%.2f ₽", step=1.0),
            "commission": st.column_config.NumberColumn("Комиссия", format="%.2f ₽", disabled=True),
            "tax": st.column_config.NumberColumn("Налог", format="%.2f ₽", disabled=True),
            "margin": st.column_config.NumberColumn("Маржа", format="%.2f ₽", disabled=True),
            "margin_percent": st.column_config.NumberColumn("Маржа, %", format="%.2f %", disabled=True),
            "weight_kg": st.column_config.NumberColumn("Вес, кг", format="%.3f", step=0.01),
            "package_l_cm": st.column_config.NumberColumn("Длина упаковки, см", format="%.1f", step=0.5),
            "package_w_cm": st.column_config.NumberColumn("Ширина упаковки, см", format="%.1f", step=0.5),
            "package_h_cm": st.column_config.NumberColumn("Высота упаковки, см", format="%.1f", step=0.5),
            "volume_l": st.column_config.NumberColumn("Объём, л", format="%.3f", step=0.1),
            "barcode": st.column_config.TextColumn("Штрихкод"),
            "comments": st.column_config.TextColumn("Комментарии"),
            "custom_data": st.column_config.CodeColumn("custom_data (JSON)", language="json"),
            "is_active": st.column_config.CheckboxColumn("Активен"),
            "created_at": st.column_config.DatetimeColumn("Создано", disabled=True, format="YYYY-MM-DD HH:mm"),
            "updated_at": st.column_config.DatetimeColumn("Обновлено", disabled=True, format="YYYY-MM-DD HH:mm"),
        }
        for custom_key in visible_custom_fields:
            definition = custom_field_map.get(custom_key)
            if not definition:
                continue
            column_config[definition.column_name] = _build_column_config(definition)

        editable_df = st.data_editor(
            products_df,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config=column_config,
            key="products_editor",
        )

        if st.button("Сохранить изменения", type="primary", key="products_save"):
            with SessionLocal() as session:
                definitions_for_save = load_custom_field_definitions(session)
                _, original_products = load_products_dataframe(session, filters, definitions_for_save, visible_custom_fields)
                save_result = save_products_from_dataframe(
                    session,
                    editable_df,
                    original_products,
                    definitions_for_save,
                    visible_custom_fields,
                )
            if save_result.errors:
                for message in save_result.errors:
                    st.error(message)
            else:
                st.success(
                    f"Изменения сохранены. Добавлено: {save_result.inserted}, обновлено: {save_result.updated}, удалено: {save_result.deleted}."
                )
                st.experimental_rerun()

        with st.expander("Массовые правки", expanded=False):
            available_ids = [
                int(value)
                for value in products_df["id"].dropna().astype(int).tolist()
                if value is not None
            ]
            selected_ids = st.multiselect(
                "Выберите товары",
                options=available_ids,
                default=[],
                key="products_bulk_ids",
            )
            field_labels: Dict[str, str] = {
                "title": "Название",
                "brand": "Бренд",
                "category": "Категория",
                "price_src": "Цена на витрине",
                "seller_discount_pct": "Скидка продавца, %",
                "product_cost": "Себестоимость",
                "shipping_cost": "Доставка до склада",
                "logistics_back_cost": "Логистика возврата",
                "warehouse_coeff": "Коэфф. склада",
                "stock": "Остаток общий",
                "stock_wb": "Остаток WB",
                "stock_seller": "Остаток продавца",
                "turnover_days": "Оборачиваемость, дни",
                "weight_kg": "Вес, кг",
                "package_l_cm": "Длина упаковки, см",
                "package_w_cm": "Ширина упаковки, см",
                "package_h_cm": "Высота упаковки, см",
                "volume_l": "Объём, л",
                "barcode": "Штрихкод",
                "comments": "Комментарии",
                "is_active": "Активен",
                "sku": "SKU",
                "seller_sku": "Артикул продавца",
                "wb_sku": "Артикул WB",
                "nm_id": "NM ID",
            }
            custom_labels = {
                f"{CUSTOM_PREFIX}{field.key}": f"Custom · {field.name} ({field.key})"
                for field in custom_field_defs
            }
            field_labels.update(custom_labels)
            field_choice = st.selectbox(
                "Поле для изменения",
                options=list(field_labels.keys()),
                format_func=lambda key: field_labels[key],
                key="products_bulk_field",
            )

            is_custom = field_choice.startswith(CUSTOM_PREFIX)
            field_name = field_choice[len(CUSTOM_PREFIX) :] if is_custom else field_choice
            definition_for_bulk = custom_field_map.get(field_name) if is_custom else None

            clear_value = st.checkbox("Очистить значение", value=False, key="products_bulk_clear")

            value_to_apply: Optional[object]
            if clear_value:
                value_to_apply = None
            elif is_custom and definition_for_bulk:
                if definition_for_bulk.field_type == "number":
                    value_to_apply = st.number_input(
                        "Введите значение",
                        value=0.0,
                        step=0.5,
                        format="%.2f",
                        key=f"products_bulk_custom_number_{field_name}",
                    )
                elif definition_for_bulk.field_type == "boolean":
                    value_to_apply = st.selectbox(
                        "Статус",
                        options=[True, False],
                        format_func=lambda v: "Истина" if v else "Ложь",
                        key=f"products_bulk_custom_bool_{field_name}",
                    )
                elif definition_for_bulk.field_type == "date":
                    selected_date = st.date_input(
                        "Дата",
                        key=f"products_bulk_custom_date_{field_name}",
                    )
                    value_to_apply = selected_date.isoformat() if selected_date else None
                elif definition_for_bulk.field_type == "choice" and definition_for_bulk.choices:
                    value_to_apply = st.selectbox(
                        "Выберите значение",
                        options=definition_for_bulk.choices,
                        key=f"products_bulk_custom_choice_{field_name}",
                    )
                else:
                    value_to_apply = st.text_input(
                        "Введите значение",
                        value="",
                        key=f"products_bulk_custom_text_{field_name}",
                    )
            elif field_name in {"price_src", "product_cost", "shipping_cost", "logistics_back_cost", "warehouse_coeff"}:
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0.0,
                    step=1.0,
                    format="%.2f",
                    key=f"products_bulk_currency_{field_name}",
                )
            elif field_name in {"seller_discount_pct"}:
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0.0,
                    step=0.5,
                    format="%.2f",
                    key=f"products_bulk_percent_{field_name}",
                )
            elif field_name in {"turnover_days", "package_l_cm", "package_w_cm", "package_h_cm"}:
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0.0,
                    step=0.5,
                    format="%.1f",
                    key=f"products_bulk_float_{field_name}",
                )
            elif field_name in {"weight_kg", "volume_l"}:
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0.0,
                    step=0.1,
                    format="%.3f",
                    key=f"products_bulk_precision_{field_name}",
                )
            elif field_name in {"stock", "stock_wb", "stock_seller", "nm_id"}:
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0,
                    step=1,
                    key=f"products_bulk_int_{field_name}",
                )
            elif field_name == "is_active":
                value_to_apply = st.selectbox(
                    "Статус",
                    options=[True, False],
                    format_func=lambda v: "Активен" if v else "Скрыт",
                    key="products_bulk_active_value",
                )
            elif field_name == "comments":
                value_to_apply = st.text_area(
                    "Введите значение",
                    value="",
                    key="products_bulk_comments_value",
                )
            else:
                value_to_apply = st.text_input(
                    "Введите значение",
                    value="",
                    key="products_bulk_text_value",
                )

            if st.button("Применить массовое изменение", key="products_bulk_apply"):
                if not selected_ids:
                    st.warning("Выберите хотя бы одну запись для изменения.")
                else:
                    with SessionLocal() as session:
                        updated_count, error_message = bulk_update_field(
                            session,
                            selected_ids,
                            field=field_name,
                            value=value_to_apply,
                            is_custom=is_custom,
                            custom_definitions=custom_field_map,
                        )
                    if error_message:
                        st.error(error_message)
                    else:
                        st.success(f"Обновлено записей: {updated_count}.")
                        st.experimental_rerun()

with import_tab:
    st.subheader("Импорт товаров из Excel или CSV")

    template_df = _build_template_dataframe(custom_field_defs)
    csv_template = template_df.to_csv(index=False).encode("utf-8")
    excel_buffer = io.BytesIO()
    template_df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)

    col_csv, col_excel = st.columns(2)
    with col_csv:
        st.download_button(
            "Скачать шаблон CSV",
            data=csv_template,
            file_name="products_template.csv",
            mime="text/csv",
        )
    with col_excel:
        st.download_button(
            "Скачать шаблон Excel",
            data=excel_buffer.getvalue(),
            file_name="products_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    uploaded_file = st.file_uploader(
        "Загрузите файл",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=False,
        key="products_import_uploader",
    )
    if uploaded_file is not None:
        try:
            import_df = _read_uploaded_file(uploaded_file)
            st.session_state["products_import_df"] = import_df
            st.session_state["products_import_filename"] = uploaded_file.name
            st.success(f"Файл {uploaded_file.name} загружен. Найдено строк: {len(import_df)}")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Не удалось прочитать файл: {exc}")

    import_df = st.session_state.get("products_import_df")
    import_filename = st.session_state.get("products_import_filename") or "uploaded_file"

    if import_df is not None and not import_df.empty:
        st.markdown("### Предпросмотр данных (первые 20 строк)")
        st.dataframe(import_df.head(20), use_container_width=True)

        columns = [str(col) for col in import_df.columns]
        sentinel = "— не использовать —"

        with st.form("products_import_form"):
            key_target = st.selectbox(
                "Уникальный идентификатор",
                options=["sku", "nm_id"],
                format_func=lambda value: "SKU" if value == "sku" else "NM ID",
                key="products_import_key_target",
            )
            default_key_column = columns.index(key_target) if key_target in columns else 0
            key_column = st.selectbox(
                "Колонка с уникальным идентификатором",
                options=columns,
                index=default_key_column,
                key="products_import_key_column",
            )

            field_mapping: Dict[str, Optional[str]] = {}
            used_columns: Set[str] = set()
            if key_column in columns:
                used_columns.add(key_column)
            field_labels = [
                ("sku", "SKU"),
                ("seller_sku", "Артикул продавца"),
                ("wb_sku", "Артикул WB"),
                ("nm_id", "NM ID"),
                ("title", "Название *"),
                ("brand", "Бренд"),
                ("category", "Категория"),
                ("price_src", "Цена на витрине"),
                ("seller_discount_pct", "Скидка продавца, %"),
                ("price", "Цена (legacy)"),
                ("price_final", "Цена со скидкой (расчет)"),
                ("product_cost", "Себестоимость"),
                ("shipping_cost", "Доставка до склада"),
                ("logistics_back_cost", "Логистика возврата"),
                ("warehouse_coeff", "Коэфф. склада"),
                ("stock", "Остаток общий"),
                ("stock_wb", "Остаток WB"),
                ("stock_seller", "Остаток продавца"),
                ("turnover_days", "Оборачиваемость, дни"),
                ("weight_kg", "Вес, кг"),
                ("package_l_cm", "Длина упаковки, см"),
                ("package_w_cm", "Ширина упаковки, см"),
                ("package_h_cm", "Высота упаковки, см"),
                ("volume_l", "Объём, л"),
                ("barcode", "Штрихкод"),
                ("comments", "Комментарии"),
                ("is_active", "Активен"),
                ("custom_data", "Доп. данные (JSON)"),
            ]
            for field, label in field_labels:
                options = [sentinel] + columns
                default_value: Optional[str] = None
                if field == key_target:
                    default_value = key_column
                else:
                    guessed = guess_import_column(field, columns)
                    if guessed and guessed not in used_columns:
                        default_value = guessed
                default_index = options.index(default_value) if default_value in options else 0
                selected_column = st.selectbox(
                    label,
                    options=options,
                    index=default_index,
                    key=f"products_import_map_{field}",
                    help="Варианты: " + ", ".join(available_aliases(field)),
                )
                if selected_column == sentinel:
                    field_mapping[field] = None
                else:
                    field_mapping[field] = selected_column
                    used_columns.add(selected_column)

            field_mapping[key_target] = key_column
            mapped_columns = {column for column in field_mapping.values() if column}
            candidate_custom_columns = [col for col in columns if col not in mapped_columns]

            custom_field_mapping: Dict[str, str] = {}
            if custom_field_defs:
                custom_options = [field.key for field in custom_field_defs]
                default_preselect = [
                    col
                    for col in candidate_custom_columns
                    if sanitize_custom_field_key(col) in custom_field_map
                ] or candidate_custom_columns
                selected_custom_columns = st.multiselect(
                    "Колонки для custom_fields",
                    options=columns,
                    default=default_preselect,
                    key="products_import_custom_columns",
                )
                for column_name in selected_custom_columns:
                    sanitized_column = sanitize_custom_field_key(column_name)
                    default_key = sanitized_column if sanitized_column in custom_field_map else None
                    field_key = st.selectbox(
                        f"Поле custom_fields для '{column_name}'",
                        options=custom_options,
                        index=custom_options.index(default_key) if default_key in custom_options else 0,
                        format_func=lambda key: f"{custom_field_map[key].name} ({key})" if key in custom_field_map else key,
                        key=f"products_import_custom_key_{column_name}",
                    )
                    custom_field_mapping[column_name] = field_key
            else:
                st.info("Пользовательские поля отсутствуют. Создайте их перед сопоставлением колонок.")

            submit_import = st.form_submit_button("Импортировать данные", type="primary")

        if submit_import:
            if not field_mapping.get("title"):
                st.error("Необходимо выбрать колонку с названием товара.")
            else:
                duplicates = {
                    key for key in custom_field_mapping.values() if list(custom_field_mapping.values()).count(key) > 1
                }
                if duplicates:
                    st.error(f"Повторяющиеся ключи custom_fields: {', '.join(sorted(duplicates))}")
                else:
                    try:
                        with SessionLocal() as session:
                            definitions_for_import = load_custom_field_definitions(session)
                            import_result = import_products_from_dataframe(
                                session,
                                import_df,
                                key_column=key_column,
                                key_target=key_target,
                                field_mapping=field_mapping,
                                custom_field_mapping=custom_field_mapping,
                                file_name=import_filename,
                                field_definitions=definitions_for_import,
                            )
                        if import_result.errors:
                            st.warning(
                                f"Импорт завершён с сообщениями. Добавлено: {import_result.inserted}, обновлено: {import_result.updated}."
                            )
                            for message in import_result.errors[:20]:
                                st.info(f"• {message}")
                            if len(import_result.errors) > 20:
                                st.info(f"… и ещё {len(import_result.errors) - 20} сообщений")
                        else:
                            st.success(
                                f"Импорт завершён. Добавлено: {import_result.inserted}, обновлено: {import_result.updated}."
                            )
                        st.experimental_rerun()
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Ошибка при импорте: {exc}")
    else:
        st.info("Загрузите файл или используйте шаблон для подготовки данных к импорту.")

with export_tab:
    st.subheader("Экспорт товаров в файл")
    export_search = st.text_input(
        "Поиск для экспорта",
        value=st.session_state.get("products_export_search", ""),
        key="products_export_search",
    ).strip()
    export_brand_options = ["Все бренды"] + available_brands
    export_brand_default = st.session_state.get("products_export_brand", "Все бренды")
    if export_brand_default not in export_brand_options:
        export_brand_default = "Все бренды"
    export_brand = st.selectbox(
        "Бренд",
        options=export_brand_options,
        index=export_brand_options.index(export_brand_default),
        key="products_export_brand",
    )
    export_active_only = st.checkbox(
        "Экспортировать только активные товары",
        value=st.session_state.get("products_export_active", False),
        key="products_export_active",
    )
    default_export_fields = st.session_state.get("products_export_custom_fields", ordered_keys)
    export_custom_fields = st.multiselect(
        "Поля custom_fields",
        options=ordered_keys,
        default=default_export_fields if default_export_fields else ordered_keys,
        format_func=lambda key: f"{custom_field_map[key].name} ({key})" if key in custom_field_map else key,
        key="products_export_custom_fields",
    )
    export_format = st.selectbox(
        "Формат файла",
        options=["CSV", "Excel"],
        index=["CSV", "Excel"].index(st.session_state.get("products_export_format", "CSV")),
        key="products_export_format",
    )

    if st.button("Сформировать файл", key="products_export_generate"):
        export_filters = ProductFilters(
            search=export_search or None,
            brand=None if export_brand == "Все бренды" else export_brand,
            active_only=export_active_only,
        )
        with SessionLocal() as session:
            export_df = export_products_dataframe(session, export_filters, custom_field_defs, export_custom_fields)
        if export_df.empty:
            st.info("Нет данных для экспорта по заданным фильтрам.")
        else:
            st.dataframe(export_df.head(20), use_container_width=True)
            if export_format == "CSV":
                data_bytes = export_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Скачать CSV",
                    data=data_bytes,
                    file_name=f"products_export_{datetime.utcnow():%Y%m%d_%H%M%S}.csv",
                    mime="text/csv",
                )
            else:
                buffer = io.BytesIO()
                export_df.to_excel(buffer, index=False)
                buffer.seek(0)
                st.download_button(
                    "Скачать Excel",
                    data=buffer.getvalue(),
                    file_name=f"products_export_{datetime.utcnow():%Y%m%d_%H%M%S}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

with logs_tab:
    st.subheader("Журнал импортов")
    with SessionLocal() as session:
        logs_df = fetch_import_logs(session, limit=50)
    if logs_df.empty:
        st.info("Импортов ещё не было.")
    else:
        st.dataframe(logs_df, use_container_width=True, hide_index=True)
        csv_logs = logs_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Скачать журнал CSV",
            data=csv_logs,
            file_name="product_import_logs.csv",
            mime="text/csv",
        )
