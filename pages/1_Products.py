from __future__ import annotations

import io
import json
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import pandas as pd
import streamlit as st
from sqlalchemy import func, select

from app_layout import initialize_page
from demowb.db import SessionLocal
from models import Product
from product_service import (
    CUSTOM_PREFIX,
    ProductFilters,
    bulk_update_field,
    collect_custom_field_keys,
    export_products_dataframe,
    fetch_import_logs,
    get_available_brands,
    import_products_from_dataframe,
    load_products_dataframe,
    save_products_from_dataframe,
)
from sync import sync_products


TEMPLATE_SAMPLE_ROWS = [
    {
        "sku": "WB-10001",
        "nm_id": 10001,
        "title": "Футболка мужская базовая",
        "brand": "MockWear",
        "category": "Одежда",
        "price": 1299.0,
        "stock": 45,
        "barcode": "1112223334445",
        "is_active": True,
        "color": "Черный",
        "size": "M",
        "season": "SS24",
    },
    {
        "sku": "WB-10002",
        "nm_id": 10002,
        "title": "Худи оверсайз женское",
        "brand": "MockWear",
        "category": "Одежда",
        "price": 2499.0,
        "stock": 30,
        "barcode": "1112223334446",
        "is_active": True,
        "color": "Лавандовый",
        "size": "S",
        "season": "AW24",
    },
    {
        "sku": "WB-10003",
        "nm_id": 10003,
        "title": "Рюкзак городской",
        "brand": "Urban Mock",
        "category": "Аксессуары",
        "price": 1899.0,
        "stock": 18,
        "barcode": "1112223334447",
        "is_active": True,
        "color": "Графит",
        "capacity_l": 18,
    },
]


def _sanitize_custom_key(raw: str) -> Optional[str]:
    if not raw:
        return None
    key = raw.strip().lower()
    if not key:
        return None
    normalized_chars: List[str] = []
    for char in key:
        if char.isalnum():
            normalized_chars.append(char)
        elif char in {" ", "-", "_"}:
            normalized_chars.append("_")
    normalized = "".join(normalized_chars).strip("_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized or None


def _build_template_dataframe() -> pd.DataFrame:
    return pd.DataFrame(TEMPLATE_SAMPLE_ROWS)


def _read_uploaded_file(uploaded_file) -> pd.DataFrame:
    data = uploaded_file.read()
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data), engine="openpyxl")
    raise ValueError("Поддерживаются только файлы CSV и Excel")


def _ensure_session_defaults(all_custom_fields: List[str]) -> None:
    st.session_state.setdefault("products_search", "")
    st.session_state.setdefault("products_brand", "Все бренды")
    st.session_state.setdefault("products_active_only", False)
    st.session_state.setdefault("products_visible_custom_fields", list(all_custom_fields))
    st.session_state.setdefault("products_all_custom_fields", list(all_custom_fields))
    st.session_state.setdefault("products_import_df", None)
    st.session_state.setdefault("products_import_filename", None)

    current_visible = st.session_state["products_visible_custom_fields"]
    merged_visible = sorted(set(current_visible) | set(all_custom_fields))
    st.session_state["products_visible_custom_fields"] = merged_visible

    current_all = st.session_state["products_all_custom_fields"]
    merged_all = sorted(set(current_all) | set(all_custom_fields))
    st.session_state["products_all_custom_fields"] = merged_all


def _normalize_custom_cell(value: object) -> object:
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return ""
    except Exception:
        pass
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:  # noqa: BLE001
            return str(value)
    return value


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


def _prepare_editor_dataframe(df: pd.DataFrame, custom_fields: Sequence[str]) -> pd.DataFrame:
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

    if "is_active" in prepared.columns:
        prepared["is_active"] = prepared["is_active"].apply(_coerce_active).astype(bool)

    for column in ("sku", "title", "brand", "category", "barcode"):
        if column in prepared.columns:
            prepared[column] = prepared[column].astype("string").fillna("")

    for column in ("created_at", "updated_at"):
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce")

    for custom_key in custom_fields:
        column_name = f"{CUSTOM_PREFIX}{custom_key}"
        if column_name in prepared.columns:
            prepared[column_name] = prepared[column_name].apply(_normalize_custom_cell)

    return prepared


initialize_page(
    page_title="Управление товарами",
    page_icon="📦",
    current_page="pages/1_Products.py",
    description="Каталог с импортом, экспортом и пользовательскими полями",
)

with SessionLocal() as session:
    custom_keys_db = collect_custom_field_keys(session)
    available_brands = get_available_brands(session)
    total_products = session.scalar(select(func.count(Product.id))) or 0

_ensure_session_defaults(custom_keys_db)

all_custom_fields = st.session_state["products_all_custom_fields"]
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
        st.caption("Колонки из custom_fields можно добавлять и скрывать для отображения в таблице.")
        selected_fields = st.multiselect(
            "Отображать в таблице",
            options=st.session_state["products_all_custom_fields"],
            default=st.session_state["products_visible_custom_fields"],
            key="products_visible_custom_fields_selector",
        )
        st.session_state["products_visible_custom_fields"] = sorted(selected_fields)

        new_field_raw = st.text_input(
            "Название новой колонки",
            placeholder="например, color",
            key="products_new_custom_field",
        )
        if st.button("Добавить колонку", key="products_add_custom_field"):
            sanitized = _sanitize_custom_key(new_field_raw)
            if not sanitized:
                st.warning("Введите корректное название колонки (латиница, цифры, дефис или подчёркивание).")
            else:
                all_fields = set(st.session_state["products_all_custom_fields"])
                if sanitized not in all_fields:
                    all_fields.add(sanitized)
                    st.session_state["products_all_custom_fields"] = sorted(all_fields)
                visible_fields = set(st.session_state["products_visible_custom_fields"])
                if sanitized not in visible_fields:
                    visible_fields.add(sanitized)
                    st.session_state["products_visible_custom_fields"] = sorted(visible_fields)
                    st.success(f"Колонка '{sanitized}' добавлена и отображена.")
                else:
                    st.info("Такая колонка уже отображается.")

        col_hide, col_show = st.columns(2)
        with col_hide:
            if st.button("Скрыть все", key="products_hide_all_custom"):
                st.session_state["products_visible_custom_fields"] = []
        with col_show:
            if st.button("Показать все", key="products_show_all_custom"):
                st.session_state["products_visible_custom_fields"] = sorted(
                    st.session_state["products_all_custom_fields"]
                )

filters = ProductFilters(
    search=search_value or None,
    brand=None if brand_value == "Все бренды" else brand_value,
    active_only=active_only,
)
visible_custom_fields = st.session_state["products_visible_custom_fields"]

with SessionLocal() as session:
    products_df, _ = load_products_dataframe(session, filters, visible_custom_fields)

products_df = _prepare_editor_dataframe(products_df, visible_custom_fields)
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
            "nm_id": st.column_config.NumberColumn("NM ID", step=1),
            "title": st.column_config.TextColumn("Название"),
            "brand": st.column_config.TextColumn("Бренд"),
            "category": st.column_config.TextColumn("Категория"),
            "price": st.column_config.NumberColumn("Цена", format="%.2f", step=0.5, help="Цена в рублях"),
            "stock": st.column_config.NumberColumn("Остаток", step=1, help="Количество на складе"),
            "barcode": st.column_config.TextColumn("Штрихкод"),
            "is_active": st.column_config.CheckboxColumn("Активен"),
            "created_at": st.column_config.DatetimeColumn("Создано", disabled=True, format="YYYY-MM-DD HH:mm"),
            "updated_at": st.column_config.DatetimeColumn("Обновлено", disabled=True, format="YYYY-MM-DD HH:mm"),
        }
        for custom_key in visible_custom_fields:
            column_name = f"{CUSTOM_PREFIX}{custom_key}"
            column_config[column_name] = st.column_config.TextColumn(custom_key)

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
                _, original_products = load_products_dataframe(session, filters, visible_custom_fields)
                save_result = save_products_from_dataframe(
                    session,
                    editable_df,
                    original_products,
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
                "price": "Цена",
                "stock": "Остаток",
                "barcode": "Штрихкод",
                "is_active": "Активен",
                "sku": "SKU",
                "nm_id": "NM ID",
            }
            custom_labels = {
                f"{CUSTOM_PREFIX}{key}": f"Custom · {key}"
                for key in st.session_state["products_all_custom_fields"]
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

            clear_value = st.checkbox("Очистить значение", value=False, key="products_bulk_clear")

            value_to_apply: Optional[object]
            if clear_value:
                value_to_apply = None
            elif field_name == "price":
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0.0,
                    step=0.5,
                    format="%.2f",
                    key="products_bulk_price_value",
                )
            elif field_name == "stock":
                value_to_apply = st.number_input(
                    "Введите значение",
                    value=0,
                    step=1,
                    key="products_bulk_stock_value",
                )
            elif field_name == "is_active":
                value_to_apply = st.selectbox(
                    "Статус",
                    options=[True, False],
                    format_func=lambda v: "Активен" if v else "Скрыт",
                    key="products_bulk_active_value",
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
                        )
                    if error_message:
                        st.error(error_message)
                    else:
                        st.success(f"Обновлено записей: {updated_count}.")
                        st.experimental_rerun()

with import_tab:
    st.subheader("Импорт товаров из Excel или CSV")

    template_df = _build_template_dataframe()
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
            field_labels = [
                ("title", "Название *"),
                ("brand", "Бренд"),
                ("category", "Категория"),
                ("price", "Цена"),
                ("stock", "Остаток"),
                ("barcode", "Штрихкод"),
                ("is_active", "Активен"),
                ("sku", "SKU"),
                ("nm_id", "NM ID"),
            ]
            for field, label in field_labels:
                options = [sentinel] + columns
                default_value = None
                if field == key_target:
                    default_value = key_column
                elif field in columns:
                    default_value = field
                default_index = options.index(default_value) if default_value in options else 0
                selected_column = st.selectbox(
                    label,
                    options=options,
                    index=default_index,
                    key=f"products_import_map_{field}",
                )
                field_mapping[field] = None if selected_column == sentinel else selected_column

            field_mapping[key_target] = key_column
            mapped_columns = {column for column in field_mapping.values() if column}
            candidate_custom_columns = [col for col in columns if col not in mapped_columns]
            selected_custom_columns = st.multiselect(
                "Колонки для custom_fields",
                options=columns,
                default=candidate_custom_columns,
                key="products_import_custom_columns",
            )

            custom_field_mapping: Dict[str, str] = {}
            for column_name in selected_custom_columns:
                sanitized_default = _sanitize_custom_key(column_name) or column_name.lower()
                custom_key_value = st.text_input(
                    f"Поле custom_fields для '{column_name}'",
                    value=sanitized_default,
                    key=f"products_import_custom_key_{column_name}",
                )
                sanitized_key = _sanitize_custom_key(custom_key_value) or _sanitize_custom_key(column_name)
                if sanitized_key:
                    custom_field_mapping[column_name] = sanitized_key

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
                            import_result = import_products_from_dataframe(
                                session,
                                import_df,
                                key_column=key_column,
                                key_target=key_target,
                                field_mapping=field_mapping,
                                custom_field_mapping=custom_field_mapping,
                                file_name=import_filename,
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
    export_custom_fields = st.multiselect(
        "Поля custom_fields",
        options=st.session_state["products_all_custom_fields"],
        default=st.session_state["products_all_custom_fields"],
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
            export_df = export_products_dataframe(session, export_filters, export_custom_fields)
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
