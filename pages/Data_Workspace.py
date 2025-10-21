from __future__ import annotations

import io
import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from data_workspace_repository import (
    ALLOWED_SCOPE_TYPES,
    ALLOWED_VALUE_TYPES,
    apply_coefficients_changes,
    extract_categories_from_extra,
    fetch_coefficients,
    fetch_distinct_brands,
    fetch_distinct_categories,
    fetch_products_scope_candidates,
    list_sources,
    replace_all_coefficients,
)
from product_repository import load_products_df

st.set_page_config(page_title="Data Workspace", layout="wide")
st.title("Data Workspace")

COEFFICIENT_COLUMNS = [
    "id",
    "scope_type",
    "scope_ref",
    "name",
    "value",
    "value_type",
    "unit",
    "extra",
    "updated_at",
]
_DEFAULT_SCOPE_ORDER = ["GLOBAL", "CATEGORY", "PRODUCT"]
SCOPE_TYPE_OPTIONS = [scope for scope in _DEFAULT_SCOPE_ORDER if scope in ALLOWED_SCOPE_TYPES]
VALUE_TYPE_OPTIONS = [value for value in ("NUMBER", "TEXT") if value in ALLOWED_VALUE_TYPES]


def _is_na(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except Exception:  # noqa: BLE001
        return False


def _normalize_editor_row(row: Dict[str, Any], *, skip_if_blank: bool = True) -> Optional[Dict[str, Any]]:
    raw_id = row.get("id")
    coeff_id: Optional[int]
    if _is_na(raw_id):
        coeff_id = None
    else:
        try:
            coeff_id = int(raw_id)
        except Exception:  # noqa: BLE001
            coeff_id = None

    def _to_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)

    present_fields = []
    for field in ("scope_type", "scope_ref", "name", "value", "unit", "extra"):
        if field == "extra":
            raw_extra = row.get(field)
            if isinstance(raw_extra, dict) and raw_extra:
                present_fields.append(field)
            elif isinstance(raw_extra, str) and raw_extra.strip():
                present_fields.append(field)
            elif raw_extra not in (None, "") and not _is_na(raw_extra):
                present_fields.append(field)
        else:
            val = row.get(field)
            if not _is_na(val) and _to_text(val).strip():
                present_fields.append(field)

    if skip_if_blank and coeff_id is None and not present_fields:
        return None

    scope_type_raw = _to_text(row.get("scope_type")).strip().upper()
    scope_ref_raw = _to_text(row.get("scope_ref")).strip()
    name_raw = _to_text(row.get("name")).strip()
    value_type_raw = _to_text(row.get("value_type")).strip().upper()
    if not value_type_raw:
        if isinstance(row.get("value"), (int, float)) and not _is_na(row.get("value")):
            value_type_raw = "NUMBER"
        else:
            value_type_raw = "TEXT"
    value_raw = row.get("value")
    unit_raw = _to_text(row.get("unit")).strip()
    extra_value = row.get("extra")
    if isinstance(extra_value, dict):
        extra_normalized: Any = extra_value
    else:
        extra_normalized = _to_text(extra_value).strip()

    normalized: Dict[str, Any] = {
        "id": coeff_id,
        "scope_type": scope_type_raw,
        "scope_ref": scope_ref_raw,
        "name": name_raw,
        "value": value_raw,
        "value_type": value_type_raw,
        "unit": unit_raw,
        "extra": extra_normalized,
    }

    return normalized


def _ensure_no_duplicates(records: Sequence[Dict[str, Any]]) -> None:
    seen: Dict[Tuple[str, str, str], int] = {}
    for record in records:
        scope_type = (record.get("scope_type") or "").upper()
        scope_ref = (record.get("scope_ref") or "").strip()
        name = (record.get("name") or "").strip()
        key = (scope_type, scope_ref, name)
        existing_id = record.get("id")
        previous_id = seen.get(key)
        if previous_id is None:
            seen[key] = existing_id or -1
        else:
            if existing_id is None or existing_id != previous_id:
                raise ValueError(
                    "Дублирующиеся строки с одинаковыми значениями scope_type/scope_ref/name"
                )


def _parse_extra_text(extra_text: str) -> Dict[str, Any]:
    if not extra_text or not extra_text.strip():
        return {}
    try:
        parsed = json.loads(extra_text)
    except json.JSONDecodeError as exc:  # noqa: TRY003
        raise ValueError(f"Некорректный JSON в поле extra: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Доп. параметры (extra) должны быть JSON-объектом")
    return parsed


@st.cache_data(ttl=60)
def load_coefficients_table() -> pd.DataFrame:
    records = fetch_coefficients()
    if not records:
        return pd.DataFrame(columns=COEFFICIENT_COLUMNS)

    df = pd.DataFrame(records)
    df = df.reindex(columns=COEFFICIENT_COLUMNS)
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    if "scope_type" in df.columns:
        df["scope_type"] = df["scope_type"].fillna("GLOBAL").apply(lambda x: str(x).upper())
    if "value_type" in df.columns:
        df["value_type"] = df["value_type"].fillna("NUMBER").apply(lambda x: str(x).upper())
    if "scope_ref" in df.columns:
        df["scope_ref"] = df["scope_ref"].fillna("")
    if "unit" in df.columns:
        df["unit"] = df["unit"].fillna("")
    if "extra" in df.columns:
        df["extra"] = df["extra"].fillna("")
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    return df


@st.cache_data(ttl=120)
def load_sources() -> List[str]:
    return list_sources()


@st.cache_data(ttl=120)
def load_brands(source: str) -> List[str]:
    return fetch_distinct_brands(source)


@st.cache_data(ttl=120)
def load_categories(source: str) -> List[str]:
    return fetch_distinct_categories(source)


@st.cache_data(ttl=120)
def load_products(source: str) -> pd.DataFrame:
    df = load_products_df(source)
    if df.empty:
        return df
    df = df.copy()
    if "extra" in df.columns:
        df["categories"] = df["extra"].apply(extract_categories_from_extra)
    else:
        df["categories"] = [[] for _ in range(len(df))]
    return df


@st.cache_data(ttl=120)
def load_product_scope_candidates(source: str) -> List[Dict[str, Any]]:
    return fetch_products_scope_candidates(source)


with_tabs = st.tabs(["Coefficients", "Preview"])
coeff_tab, preview_tab = with_tabs


with coeff_tab:
    st.subheader("Редактор коэффициентов")
    coefficients_df = load_coefficients_table().copy()
    total_coefficients = len(coefficients_df)
    st.caption(f"Всего коэффициентов: {total_coefficients}")

    editor_df = coefficients_df.copy()
    editor_df = editor_df.astype({"id": "Int64"}) if not editor_df.empty else editor_df

    edited_df = st.data_editor(
        editor_df,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        disabled={"id": True, "updated_at": True},
        column_config={
            "id": st.column_config.NumberColumn("ID", format="%d"),
            "scope_type": st.column_config.SelectboxColumn(
                "Scope type", options=SCOPE_TYPE_OPTIONS, help="GLOBAL — для всех, CATEGORY — для категорий/брендов, PRODUCT — для отдельных товаров"
            ),
            "scope_ref": st.column_config.TextColumn("Scope ref", help="Значение связанной сущности. Для PRODUCT используйте формат source|external_key"),
            "name": st.column_config.TextColumn("Name", help="Название коэффициента"),
            "value": st.column_config.TextColumn("Value", help="Значение коэффициента"),
            "value_type": st.column_config.SelectboxColumn(
                "Value type", options=VALUE_TYPE_OPTIONS, help="NUMBER — числовое значение, TEXT — текст"
            ),
            "unit": st.column_config.TextColumn("Unit", help="Единица измерения (например, %, ₽)"),
            "extra": st.column_config.CodeColumn("Extra", language="json", help="Дополнительные параметры в формате JSON"),
            "updated_at": st.column_config.DatetimeColumn("Updated", format="YYYY-MM-DD HH:mm"),
        },
        key="coefficients_editor",
    )

    save_col, refresh_col = st.columns([1, 1])
    with save_col:
        if st.button("Сохранить изменения", type="primary", use_container_width=True):
            try:
                original_df = coefficients_df.copy()
                edited_df_copy = edited_df.copy() if isinstance(edited_df, pd.DataFrame) else pd.DataFrame(edited_df)
                normalized_records: List[Dict[str, Any]] = []
                edited_ids: set[int] = set()
                for row in edited_df_copy.to_dict(orient="records"):
                    normalized = _normalize_editor_row(row)
                    if normalized is None:
                        continue
                    if normalized.get("id") is not None:
                        edited_ids.add(int(normalized["id"]))
                    normalized_records.append(normalized)
                _ensure_no_duplicates(normalized_records)
                original_ids = (
                    original_df["id"].dropna().astype(int).tolist() if "id" in original_df.columns else []
                )
                delete_ids = [id_ for id_ in original_ids if id_ not in edited_ids]
                summary = apply_coefficients_changes(delete_ids=delete_ids, upserts=normalized_records)
                load_coefficients_table.clear()
                st.success(
                    f"Сохранено. Добавлено: {summary['inserted']}, обновлено: {summary['updated']}, удалено: {summary['deleted']}"
                )
                st.experimental_rerun()
            except ValueError as exc:  # noqa: BLE001
                st.error(str(exc))
            except Exception as exc:  # noqa: BLE001
                st.error(f"Не удалось сохранить изменения: {exc}")
    with refresh_col:
        if st.button("Отменить изменения", use_container_width=True):
            load_coefficients_table.clear()
            st.experimental_rerun()

    st.divider()
    st.subheader("Быстрое добавление коэффициента")

    sources = load_sources()
    with st.expander("Создать коэффициент", expanded=False):
        with st.form("quick_add_form", clear_on_submit=True):
            scope_type_choice = st.selectbox(
                "Тип охвата",
                options=SCOPE_TYPE_OPTIONS,
                format_func=lambda value: {
                    "GLOBAL": "GLOBAL — применяется ко всем товарам",
                    "CATEGORY": "CATEGORY — по категории или бренду",
                    "PRODUCT": "PRODUCT — отдельные товары",
                }.get(value, value),
            )
            name_input = st.text_input("Название коэффициента", max_chars=120)
            value_type_choice = st.selectbox(
                "Тип значения",
                options=VALUE_TYPE_OPTIONS,
                index=0,
                format_func=lambda value: {"NUMBER": "NUMBER — числовое", "TEXT": "TEXT — текст"}.get(value, value),
            )
            if value_type_choice == "NUMBER":
                value_input = st.number_input("Значение", value=0.0, step=0.1)
            else:
                value_input = st.text_input("Значение", value="")
            unit_input = st.text_input("Единица измерения", value="", help="Например, %, ₽")
            extra_input = st.text_area("Доп. параметры (JSON)", value="", height=120)

            scope_refs: List[str] = []
            extra_meta: Dict[str, Any] = {}
            if scope_type_choice == "GLOBAL":
                scope_refs = [""]
            elif not sources:
                st.info("Нет данных о товарах для настройки привязок.")
            else:
                scope_source = st.selectbox(
                    "Источник данных",
                    options=sources,
                    index=0,
                    key="quick_add_source",
                )
                extra_meta["source"] = scope_source
                if scope_type_choice == "CATEGORY":
                    scope_mode = st.radio(
                        "Привязка",
                        options=["category", "brand"],
                        format_func=lambda value: "По категории" if value == "category" else "По бренду",
                        horizontal=True,
                    )
                    options_list = load_categories(scope_source) if scope_mode == "category" else load_brands(scope_source)
                    scope_refs = st.multiselect(
                        "Выберите значения",
                        options=options_list,
                        help="Значения, к которым будет применён коэффициент",
                    )
                    extra_meta["scope_kind"] = scope_mode
                else:  # PRODUCT
                    candidates = load_product_scope_candidates(scope_source)
                    options = []
                    option_map: Dict[str, str] = {}
                    for item in candidates:
                        ext_key = item.get("external_key") or ""
                        label_parts = [ext_key]
                        title = item.get("title")
                        if title:
                            label_parts.append(str(title)[:60])
                        sku = item.get("sku")
                        if sku:
                            label_parts.append(f"SKU: {sku}")
                        brand = item.get("brand")
                        if brand:
                            label_parts.append(f"Brand: {brand}")
                        label = " | ".join([part for part in label_parts if part])
                        if not label:
                            continue
                        option_map[label] = ext_key
                        options.append(label)
                    selected_labels = st.multiselect(
                        "Выберите товары",
                        options=options,
                        help="Можно искать по external_key, названию, SKU",
                    )
                    scope_refs = []
                    for label in selected_labels:
                        ext_key = option_map.get(label)
                        if ext_key:
                            scope_refs.append(f"{scope_source}|{ext_key}")
            submit_quick_add = st.form_submit_button("Создать", type="primary")

            if submit_quick_add:
                try:
                    if not name_input.strip():
                        raise ValueError("Укажите название коэффициента")
                    if scope_type_choice != "GLOBAL" and not scope_refs:
                        raise ValueError("Не выбраны значения для привязки")
                    user_extra = _parse_extra_text(extra_input)
                    new_records: List[Dict[str, Any]] = []
                    targets = scope_refs if scope_refs else [""]
                    for scope_ref in targets:
                        record_extra = {**extra_meta, **user_extra}
                        if scope_type_choice == "PRODUCT" and scope_ref:
                            parts = scope_ref.split("|", 1)
                            if len(parts) == 2:
                                record_extra.setdefault("external_key", parts[1])
                                record_extra.setdefault("source", parts[0])
                        new_records.append(
                            {
                                "scope_type": scope_type_choice,
                                "scope_ref": scope_ref,
                                "name": name_input,
                                "value": value_input,
                                "value_type": value_type_choice,
                                "unit": unit_input,
                                "extra": record_extra,
                            }
                        )
                    _ensure_no_duplicates(new_records)
                    summary = apply_coefficients_changes(delete_ids=[], upserts=new_records)
                    load_coefficients_table.clear()
                    st.success(
                        f"Создано коэффициентов: {summary['inserted'] if summary['inserted'] else len(new_records)}"
                    )
                    st.experimental_rerun()
                except ValueError as exc:  # noqa: BLE001
                    st.error(str(exc))
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Не удалось создать коэффициенты: {exc}")

    st.divider()
    st.subheader("Импорт и экспорт")
    export_df = coefficients_df.copy()
    if not export_df.empty and "updated_at" in export_df.columns:
        export_df["updated_at"] = export_df["updated_at"].astype(str)

    def _to_excel_bytes(dataframe: pd.DataFrame) -> bytes:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            dataframe.to_excel(writer, index=False, sheet_name="coefficients")
        buffer.seek(0)
        return buffer.getvalue()

    col_export, col_import = st.columns(2)
    with col_export:
        st.download_button(
            "Экспорт в CSV",
            data=export_df.to_csv(index=False).encode("utf-8"),
            file_name="coefficients.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.download_button(
            "Экспорт в XLSX",
            data=_to_excel_bytes(export_df),
            file_name="coefficients.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with col_import:
        uploaded = st.file_uploader("Импортировать коэффициенты", type=["csv", "xlsx"])
        replace_existing = st.checkbox(
            "Очистить текущие коэффициенты перед импортом", value=False, help="При включении текущие записи будут удалены"
        )
        if uploaded is not None:
            try:
                if uploaded.name.endswith(".csv"):
                    import_df = pd.read_csv(uploaded)
                else:
                    import_df = pd.read_excel(uploaded)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Не удалось прочитать файл: {exc}")
            else:
                missing_required = [
                    col
                    for col in ("scope_type", "name", "value")
                    if col not in import_df.columns
                ]
                if missing_required:
                    st.error(
                        "В файле отсутствуют обязательные столбцы: " + ", ".join(missing_required)
                    )
                else:
                    st.dataframe(import_df.head(20), use_container_width=True, hide_index=True)
                    if st.button("Применить импорт", type="primary"):
                        try:
                            normalized_records: List[Dict[str, Any]] = []
                            for row in import_df.to_dict(orient="records"):
                                normalized = _normalize_editor_row(row, skip_if_blank=True)
                                if normalized is None:
                                    continue
                                if replace_existing:
                                    normalized["id"] = None
                                normalized_records.append(normalized)
                            if not normalized_records:
                                raise ValueError("Нет данных для импорта")
                            _ensure_no_duplicates(normalized_records)
                            if replace_existing:
                                summary = replace_all_coefficients(normalized_records)
                            else:
                                summary = apply_coefficients_changes(delete_ids=[], upserts=normalized_records)
                            load_coefficients_table.clear()
                            st.success(
                                f"Импорт завершён. Добавлено: {summary['inserted']}, обновлено: {summary['updated']}"
                            )
                            st.experimental_rerun()
                        except ValueError as exc:  # noqa: BLE001
                            st.error(str(exc))
                        except Exception as exc:  # noqa: BLE001
                            st.error(f"Ошибка импорта: {exc}")


def _parse_extra_value(extra_value: Any) -> Dict[str, Any]:
    if not extra_value:
        return {}
    if isinstance(extra_value, dict):
        return extra_value
    if isinstance(extra_value, str):
        try:
            parsed = json.loads(extra_value)
        except Exception:  # noqa: BLE001
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coefficient_applies(coefficient: Dict[str, Any], product: pd.Series) -> bool:
    scope_type = (coefficient.get("scope_type") or "").upper()
    scope_ref = (coefficient.get("scope_ref") or "").strip()
    extra_meta = _parse_extra_value(coefficient.get("extra"))

    if scope_type == "GLOBAL":
        return True

    if scope_type == "CATEGORY":
        if not scope_ref:
            return False
        scope_kind = (extra_meta.get("scope_kind") or "category").lower()
        if scope_kind == "brand":
            brand = (product.get("brand") or "").strip()
            return bool(brand and brand.lower() == scope_ref.lower())
        categories = product.get("categories") or []
        categories_lower = {str(cat).lower() for cat in categories if cat}
        return scope_ref.lower() in categories_lower

    if scope_type == "PRODUCT":
        if not scope_ref:
            return False
        source = (product.get("source") or "").strip()
        external_key = (product.get("external_key") or "").strip()
        sku = (product.get("sku") or "").strip()
        candidates = []
        if source and external_key:
            candidates.append(f"{source}|{external_key}")
        if external_key:
            candidates.append(external_key)
        if sku:
            candidates.append(sku)
        return any(scope_ref.lower() == candidate.lower() for candidate in candidates)

    return False


def _to_float_safely(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "")
        if not text:
            return None
        normalized = text.replace(",", ".")
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _format_applied_description(coefficient: Dict[str, Any]) -> str:
    name = coefficient.get("name") or ""
    unit = (coefficient.get("unit") or "").strip()
    value = coefficient.get("value")
    value_type = (coefficient.get("value_type") or "TEXT").upper()
    scope_type = (coefficient.get("scope_type") or "").upper()
    if value_type == "NUMBER":
        number_value = _to_float_safely(value)
        if number_value is None:
            return f"{name} ({scope_type})"
        if unit == "%":
            return f"{name}: {number_value}% ({scope_type})"
        unit_suffix = f" {unit}" if unit else ""
        return f"{name}: {number_value}{unit_suffix} ({scope_type})"
    return f"{name}: {value} ({scope_type})"


def _compute_preview(products_df: pd.DataFrame, coefficients: List[Dict[str, Any]]) -> pd.DataFrame:
    if products_df.empty:
        return pd.DataFrame()

    prepared_coefficients: List[Dict[str, Any]] = []
    for coef in coefficients:
        prepared = dict(coef)
        prepared["value_type"] = (prepared.get("value_type") or "TEXT").upper()
        prepared_coefficients.append(prepared)

    rows: List[Dict[str, Any]] = []
    for _, product in products_df.iterrows():
        applied_coeffs: List[Dict[str, Any]] = []
        percent_adjustment = 0.0
        absolute_adjustment = 0.0
        text_notes: List[str] = []
        for coef in prepared_coefficients:
            if not _coefficient_applies(coef, product):
                continue
            applied_coeffs.append(coef)
            if (coef.get("value_type") or "TEXT").upper() == "NUMBER":
                numeric_value = _to_float_safely(coef.get("value"))
                if numeric_value is None:
                    continue
                unit = (coef.get("unit") or "").strip()
                if unit == "%":
                    percent_adjustment += numeric_value
                else:
                    absolute_adjustment += numeric_value
            else:
                text_notes.append(f"{coef.get('name')}: {coef.get('value')}")

        base_price = _to_float_safely(product.get("price")) or 0.0
        price_with_percent = base_price * (1 + percent_adjustment / 100) if percent_adjustment else base_price
        final_price = price_with_percent + absolute_adjustment
        if final_price < 0:
            final_price = 0.0
        margin_value = final_price - base_price
        margin_percent = (margin_value / base_price * 100) if base_price else None

        rows.append(
            {
                "source": product.get("source"),
                "external_key": product.get("external_key"),
                "title": product.get("title"),
                "brand": product.get("brand"),
                "base_price": round(base_price, 2),
                "percent_adjustment": round(percent_adjustment, 2),
                "absolute_adjustment": round(absolute_adjustment, 2),
                "price_after_coefficients": round(final_price, 2),
                "margin": round(margin_value, 2),
                "margin_percent": round(margin_percent, 2) if margin_percent is not None else None,
                "applied_coefficients": ", ".join(_format_applied_description(c) for c in applied_coeffs)
                if applied_coeffs
                else "-",
                "notes": "; ".join(text_notes) if text_notes else "",
            }
        )

    return pd.DataFrame(rows)


with preview_tab:
    st.subheader("Предпросмотр расчётов")
    coefficients_for_preview = load_coefficients_table().to_dict(orient="records")
    if not coefficients_for_preview:
        st.info("Коэффициенты ещё не заданы. Добавьте хотя бы один коэффициент для предпросмотра.")
    sources = load_sources()
    if not sources:
        st.info("Нет данных о товарах. Загрузите товары на других страницах приложения.")
        st.stop()

    source = st.selectbox("Источник", options=sources)
    products_df = load_products(source)
    if products_df.empty:
        st.info("Нет товаров для выбранного источника.")
        st.stop()

    brands = sorted(b for b in products_df["brand"].dropna().unique().tolist() if isinstance(b, str))
    available_categories: List[str] = sorted({cat for cats in products_df.get("categories", []) for cat in (cats or [])})

    with st.expander("Фильтры", expanded=True):
        search_query = st.text_input("Поиск по названию, external_key или SKU", value="")
        selected_brands = st.multiselect("Бренды", options=brands)
        selected_categories = st.multiselect("Категории", options=available_categories)
        product_options = []
        product_map: Dict[str, str] = {}
        for _, item in products_df.iterrows():
            ext_key = item.get("external_key") or ""
            if not ext_key:
                continue
            label_parts = [ext_key]
            if item.get("title"):
                label_parts.append(str(item.get("title"))[:60])
            product_label = " | ".join(label_parts)
            product_options.append(product_label)
            product_map[product_label] = ext_key
        selected_products = st.multiselect(
            "Выбор товаров",
            options=product_options,
            help="По умолчанию используется фильтрация по брендам и категориям",
        )
        max_rows = st.number_input("Максимум товаров для предпросмотра", min_value=1, max_value=500, value=50)

    filtered_df = products_df.copy()
    if search_query:
        query = search_query.strip().lower()
        if query:
            filtered_df = filtered_df[
                filtered_df.apply(
                    lambda row: any(
                        query in str(row.get(field, "")).lower()
                        for field in ("title", "external_key", "sku")
                    ),
                    axis=1,
                )
            ]
    if selected_brands:
        filtered_df = filtered_df[filtered_df["brand"].isin(selected_brands)]
    if selected_categories:
        selected_set = {cat.lower() for cat in selected_categories}
        filtered_df = filtered_df[
            filtered_df["categories"].apply(
                lambda cats: bool({str(cat).lower() for cat in cats or []} & selected_set)
            )
        ]
    if selected_products:
        selected_keys = {product_map[label] for label in selected_products if label in product_map}
        filtered_df = filtered_df[filtered_df["external_key"].isin(selected_keys)]

    if filtered_df.empty:
        st.warning("Нет товаров после применения фильтров.")
        st.stop()

    limited_df = filtered_df.head(int(max_rows)).copy()
    preview_df = _compute_preview(limited_df, coefficients_for_preview)

    if preview_df.empty:
        st.warning("Для выбранных товаров и коэффициентов нет данных для отображения.")
    else:
        st.caption(f"Показано товаров: {len(preview_df)}")
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "base_price": st.column_config.NumberColumn("Базовая цена", format="%.2f"),
                "percent_adjustment": st.column_config.NumberColumn("% изменение", format="%.2f"),
                "absolute_adjustment": st.column_config.NumberColumn("Абс. изменение", format="%.2f"),
                "price_after_coefficients": st.column_config.NumberColumn("Цена с коэффициентами", format="%.2f"),
                "margin": st.column_config.NumberColumn("Δ цена", format="%.2f"),
                "margin_percent": st.column_config.NumberColumn("Δ %", format="%.2f"),
            },
        )
        csv_preview = preview_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Скачать предпросмотр (CSV)",
            data=csv_preview,
            file_name="preview.csv",
            mime="text/csv",
        )
