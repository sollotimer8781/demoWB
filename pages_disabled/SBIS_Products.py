import io
import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import select

from app_layout import initialize_page
from demowb.db import SessionLocal
from models import ProductItem
from product_repository import load_products_df, upsert_products


initialize_page(
    page_title="SBIS Products",
    page_icon="📄",
    current_page="pages/SBIS_Products.py",
    description="Импорт ассортимента SBIS и загрузка в локальное хранилище",
)


def _list_sample_files() -> List[Path]:
    base_dir = Path("/home/engine/project/data/sbis")
    if not base_dir.exists() or not base_dir.is_dir():
        return []
    files: List[Path] = []
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        files.extend(sorted(base_dir.glob(pattern)))
    return sorted({f.resolve() for f in files})


def _read_dataframe_from_bytes(data: bytes, filename: str) -> pd.DataFrame:
    name = filename.lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data), engine="openpyxl")
    raise ValueError("Поддерживаются только файлы с расширениями .csv, .xlsx или .xls")


def _guess_column(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lowered = {col.lower(): col for col in columns}
    for candidate in candidates:
        key = candidate.lower()
        if key in lowered:
            return lowered[key]
    return None


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8").strip()
        except Exception:  # noqa: BLE001
            decoded = value.decode("utf-8", "ignore").strip()
        return decoded or None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:  # noqa: BLE001
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    if hasattr(value, "item") and not isinstance(value, (datetime, date)):
        try:
            return _normalize_value(value.item())  # type: ignore[call-arg]
        except Exception:  # noqa: BLE001
            pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple, set)):
        normalized = [_normalize_value(v) for v in value]
        return [v for v in normalized if v is not None]
    return value


def _split_image_values(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        urls: List[str] = []
        for item in raw:
            urls.extend(_split_image_values(item))
        return urls
    text: Optional[str]
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001
            text = None
    else:
        text = str(raw)
    if text is None:
        return []
    cleaned = text.strip()
    if not cleaned:
        return []
    if cleaned.startswith("[") and cleaned.endswith("]"):
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, list):
                return _split_image_values(parsed)
        except Exception:  # noqa: BLE001
            pass
    parts = re.split(r"[;\n,]+", cleaned)
    result = []
    for part in parts:
        url = part.strip()
        if url:
            result.append(url)
    return result


FIELD_LABELS = {
    "title": "Название (title)",
    "brand": "Бренд (brand)",
    "price": "Цена (price)",
    "stock": "Остаток (stock)",
    "product_id": "Product ID",
    "offer_id": "Offer ID",
    "sku": "SKU",
    "nm_id": "NM ID",
}
FIELD_ORDER = ["title", "brand", "price", "stock", "product_id", "offer_id", "sku", "nm_id"]
MANDATORY_MAPPING_FIELDS = {"title"}
MANDATORY_ROW_FIELDS = {"title"}
NUMERIC_FLOAT_FIELDS = {"price"}
NUMERIC_INT_FIELDS = {"stock", "nm_id"}
TEXT_FIELDS = {"title", "brand", "product_id", "offer_id", "sku"}
SAMPLE_NOT_SELECTED = "— не выбран —"
FIELD_NOT_USED_OPTION = "— не использовать —"
MAX_LOG_ENTRIES = 200


def _append_import_log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    logs = st.session_state.setdefault("sbis_import_log", [])
    logs.append(f"[{timestamp}] {message}")
    if len(logs) > MAX_LOG_ENTRIES:
        del logs[:-MAX_LOG_ENTRIES]


def _reset_import_state() -> None:
    for key in [
        "sbis_import_df",
        "sbis_import_source_name",
        "sbis_key_column",
        "sbis_external_key_type",
        "sbis_image_columns",
    ]:
        st.session_state.pop(key, None)
    for key in list(st.session_state.keys()):
        if key.startswith("sbis_map_"):
            st.session_state.pop(key)
    if "sbis_sample_select" in st.session_state:
        st.session_state["sbis_sample_select"] = SAMPLE_NOT_SELECTED


def _coerce_float_value(value: Any) -> Tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None, "имеет недопустимое числовое значение"
        return result, None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None, None
        normalized = stripped.replace(" ", "").replace(",", ".")
        try:
            result = float(normalized)
        except ValueError:
            return None, f"значение «{value}» не удалось преобразовать в число"
        if math.isnan(result) or math.isinf(result):
            return None, "имеет недопустимое числовое значение"
        return result, None
    return None, f"значение {value!r} не удалось преобразовать в число"


def _coerce_int_value(value: Any) -> Tuple[Optional[int], Optional[str]]:
    if value is None:
        return None, None
    if isinstance(value, bool):
        return int(value), None
    if isinstance(value, int):
        return value, None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None, "имеет недопустимое целое значение"
        if value.is_integer():
            return int(value), None
        return None, f"значение {value} не является целым числом"
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None, None
        normalized = stripped.replace(" ", "")
        try:
            float_value = float(normalized.replace(",", "."))
        except ValueError:
            return None, f"значение «{value}» не удалось преобразовать в целое число"
        if not float_value.is_integer():
            return None, f"значение «{value}» не является целым числом"
        return int(float_value), None
    return None, f"значение {value!r} не удалось преобразовать в целое число"


def _cast_field_value(field: str, value: Any) -> Tuple[Any, Optional[str]]:
    normalized = _normalize_value(value)
    if normalized is None:
        return None, None
    if field in NUMERIC_FLOAT_FIELDS:
        return _coerce_float_value(normalized)
    if field in NUMERIC_INT_FIELDS:
        return _coerce_int_value(normalized)
    if field in TEXT_FIELDS:
        text = str(normalized).strip()
        return (text or None), None
    return normalized, None


def _prepare_records(
    df: pd.DataFrame,
    key_column: str,
    external_key_type: str,
    field_mapping: Dict[str, Optional[str]],
    image_columns: Sequence[str],
) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    records: List[Dict[str, Any]] = []
    errors: List[str] = []
    duplicate_keys: List[str] = []
    seen_keys: Dict[str, int] = {}

    used_columns = {key_column}
    used_columns.update(col for col in field_mapping.values() if col)
    used_columns.update(image_columns)

    missing_columns = [col for col in used_columns if col not in df.columns]
    if missing_columns:
        errors.append(
            "В таблице отсутствуют необходимые столбцы: "
            + ", ".join(sorted(missing_columns))
        )
        return [], errors, []

    for idx, row in df.iterrows():
        row_number = idx + 2
        raw_key = row.get(key_column)
        normalized_key = _normalize_value(raw_key)
        if normalized_key is None:
            errors.append(f"Строка {row_number}: внешний ключ пустой")
            continue
        external_key = str(normalized_key).strip()
        if not external_key:
            errors.append(f"Строка {row_number}: внешний ключ пустой")
            continue

        seen_count = seen_keys.get(external_key, 0)
        if seen_count:
            duplicate_keys.append(external_key)
            seen_keys[external_key] = seen_count + 1
            continue
        seen_keys[external_key] = 1

        record: Dict[str, Any] = {
            "source": "SBIS",
            "external_key": external_key,
            "external_key_type": external_key_type.strip() or "SBIS:code",
        }

        for field, column in field_mapping.items():
            if not column or column not in df.columns:
                continue
            value = row.get(column)
            converted, error_message = _cast_field_value(field, value)
            if error_message:
                label = FIELD_LABELS.get(field, field)
                errors.append(f"Строка {row_number}: {label} — {error_message}")
                continue
            if converted is None:
                continue
            record[field] = converted

        missing_required_values = [
            FIELD_LABELS.get(field, field)
            for field in MANDATORY_ROW_FIELDS
            if field_mapping.get(field) and not record.get(field)
        ]
        if missing_required_values:
            errors.append(
                f"Строка {row_number}: отсутствуют значения для обязательных полей: {', '.join(missing_required_values)}"
            )
            continue

        if image_columns:
            urls: List[str] = []
            for column in image_columns:
                urls.extend(_split_image_values(row.get(column)))
            deduped: List[str] = []
            for url in urls:
                if url not in deduped:
                    deduped.append(url)
            if deduped:
                record["image_urls"] = deduped

        extra: Dict[str, Any] = {}
        for column in df.columns:
            if column in used_columns:
                continue
            normalized_extra = _normalize_value(row.get(column))
            if normalized_extra is None:
                continue
            extra[column] = normalized_extra
        if extra:
            record["extra"] = extra

        records.append(record)

    return records, errors, duplicate_keys


def _simulate_upsert(records: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    if not records:
        return 0, 0

    inserted = 0
    updated = 0
    with SessionLocal() as session:
        for item in records:
            source = item.get("source")
            external_key = item.get("external_key")
            external_key_type = item.get("external_key_type")
            if not (source and external_key and external_key_type):
                continue
            existing = session.execute(
                select(ProductItem.id)
                .where(
                    ProductItem.source == source,
                    ProductItem.external_key == external_key,
                    ProductItem.external_key_type == external_key_type,
                )
                .limit(1)
            ).scalar_one_or_none()
            if existing is None:
                inserted += 1
            else:
                updated += 1
    return inserted, updated


@st.cache_data(ttl=300)
def load_sbis_products_df():
    return load_products_df("SBIS")


if "sbis_import_feedback" not in st.session_state:
    st.session_state["sbis_import_feedback"] = None
if "sbis_import_log" not in st.session_state:
    st.session_state["sbis_import_log"] = []


with st.sidebar:
    st.header("SBIS API (подготовка)")
    st.caption(
        "Импорт через Excel/CSV доступен сейчас. Интеграция по API будет добавлена позже — интерфейс уже подготовлен к использованию ключей."
    )
    import_log = st.session_state.get("sbis_import_log") or []
    if import_log:
        st.subheader("Лог импорта")
        st.caption("Последние события импорта")
        for entry in import_log[-20:]:
            st.write(f"• {entry}")
        if st.button("Очистить лог", key="sbis_clear_log"):
            st.session_state["sbis_import_log"] = []


with st.expander("Import from Excel/CSV", expanded=False):
    uploaded = st.file_uploader(
        "Загрузите файл .xlsx или .csv", type=["csv", "xlsx", "xls"], accept_multiple_files=False
    )

    sample_files = _list_sample_files()
    sample_df: Optional[pd.DataFrame] = None
    sample_source_name: Optional[str] = None
    if sample_files:
        st.markdown("**Примеры из репозитория**")
        selected_sample = st.selectbox(
            "Выберите пример", options=[SAMPLE_NOT_SELECTED] + [str(p.relative_to(Path("/home/engine/project"))) for p in sample_files], key="sbis_sample_select"
        )
        if selected_sample != SAMPLE_NOT_SELECTED:
            if st.button("Загрузить пример из репозитория", key="sbis_load_sample"):
                path = Path("/home/engine/project") / selected_sample
                try:
                    sample_bytes = path.read_bytes()
                    sample_df = _read_dataframe_from_bytes(sample_bytes, path.name)
                    sample_source_name = path.name
                    st.session_state["sbis_import_feedback"] = None
                    st.success(f"Пример {path.name} загружен.")
                except FileNotFoundError:
                    st.error("Файл не найден в репозитории. Проверьте, что он существует.")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Не удалось прочитать файл: {exc}")

    current_df: Optional[pd.DataFrame] = st.session_state.get("sbis_import_df")
    current_source_name: Optional[str] = st.session_state.get("sbis_import_source_name")

    if uploaded is not None:
        try:
            file_bytes = uploaded.read()
            dataframe = _read_dataframe_from_bytes(file_bytes, uploaded.name)
            st.session_state["sbis_import_df"] = dataframe
            st.session_state["sbis_import_source_name"] = uploaded.name
            st.session_state["sbis_import_feedback"] = None
            current_df = dataframe
            current_source_name = uploaded.name
            st.success(f"Файл {uploaded.name} успешно прочитан.")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Ошибка чтения файла: {exc}")

    if sample_df is not None:
        st.session_state["sbis_import_df"] = sample_df
        st.session_state["sbis_import_source_name"] = sample_source_name
        current_df = sample_df
        current_source_name = sample_source_name

    feedback = st.session_state.get("sbis_import_feedback")
    if isinstance(feedback, dict):
        status = feedback.get("status")
        message = feedback.get("message")
        details = feedback.get("details")
        if status == "success" and message:
            st.success(message)
        elif status == "info" and message:
            st.info(message)
        elif status == "error" and message:
            st.error(message)
        elif message:
            st.write(message)
        if details:
            st.caption(details)

    if current_df is not None and not current_df.empty:
        st.caption(f"Выбранный файл: {current_source_name}")
        st.dataframe(current_df.head(30), use_container_width=True)

        columns = list(current_df.columns)
        if not columns:
            st.warning("В файле не обнаружены колонки для обработки.")
        else:
            guess_key = _guess_column(
                columns,
                [
                    "external_key",
                    "sbis_code",
                    "code",
                    "article",
                    "Артикул",
                    "код",
                    "id",
                ],
            )
            key_column = st.selectbox(
                "Ключевой столбец (external_key)",
                options=columns,
                index=columns.index(guess_key) if guess_key in columns else 0,
                key="sbis_key_column",
            )
            default_key_type = "SBIS:code"
            key_type = st.text_input(
                "Тип внешнего ключа (external_key_type)", value=default_key_type, key="sbis_external_key_type"
            ).strip() or default_key_type

            guess_map = {
                "title": _guess_column(columns, ["title", "name", "название", "наименование"]),
                "brand": _guess_column(columns, ["brand", "бренд"]),
                "price": _guess_column(columns, ["price", "цена"]),
                "stock": _guess_column(columns, ["stock", "остаток", "quantity", "qty", "количество"]),
                "product_id": _guess_column(columns, ["product_id", "product id", "id товара"]),
                "offer_id": _guess_column(columns, ["offer_id", "offer", "офер"]),
                "sku": _guess_column(columns, ["sku", "артикул", "sku_code"]),
                "nm_id": _guess_column(columns, ["nm_id", "nm id", "nmid"]),
            }

            mapping: Dict[str, Optional[str]] = {}
            for field in FIELD_ORDER:
                label = FIELD_LABELS[field]
                options = [FIELD_NOT_USED_OPTION] + columns
                default_value = guess_map.get(field)
                default_index = options.index(default_value) if default_value in columns else 0
                selected = st.selectbox(label, options=options, index=default_index, key=f"sbis_map_{field}")
                mapping[field] = None if selected == FIELD_NOT_USED_OPTION else selected

            default_image_columns = [col for col in columns if col.lower() in {"image", "image_url", "image_urls", "photo"}]
            image_columns = st.multiselect(
                "Колонки с URL изображений", options=columns, default=default_image_columns, key="sbis_image_columns"
            )

            duplicates_mask = current_df.duplicated(subset=[key_column], keep=False)
            duplicate_rows = current_df[duplicates_mask]
            if not duplicate_rows.empty:
                st.warning(
                    "Обнаружены дубли по ключу. В импорт будут включены только первые вхождения каждого ключа."
                )
                st.dataframe(duplicate_rows[[key_column]].head(100), use_container_width=True)

            missing_mapping_fields = [
                FIELD_LABELS[field]
                for field in MANDATORY_MAPPING_FIELDS
                if not mapping.get(field)
            ]

            records: List[Dict[str, Any]] = []
            errors: List[str] = []
            duplicate_keys: List[str] = []
            if missing_mapping_fields:
                st.error(
                    "Укажите столбцы для обязательных полей: "
                    + ", ".join(missing_mapping_fields)
                )
            else:
                records, errors, duplicate_keys = _prepare_records(
                    current_df,
                    key_column=key_column,
                    external_key_type=key_type,
                    field_mapping=mapping,
                    image_columns=image_columns,
                )

            st.caption(f"Подготовлено записей: {len(records)}")

            if errors:
                st.error("Обнаружены проблемы с данными. Проверьте строки ниже.")
                for err in errors[:20]:
                    st.write(f"- {err}")
                if len(errors) > 20:
                    st.write(f"… и ещё {len(errors) - 20} строк")
            if duplicate_keys:
                st.warning(
                    f"Обнаружено повторяющихся ключей: {len(duplicate_keys)}. Будут использованы первые вхождения."
                )

            preview_count = min(20, len(records))
            if preview_count:
                st.markdown("**Предпросмотр нормализованных данных (первые строки)**")
                try:
                    st.dataframe(pd.DataFrame(records).head(preview_count), use_container_width=True)
                except Exception:  # noqa: BLE001
                    st.write(records[:preview_count])
            elif not missing_mapping_fields and not errors and not duplicate_keys:
                st.info("После настройки маппинга данные будут показаны здесь для проверки.")

            col_validate, col_import = st.columns(2)
            with col_validate:
                do_dry_run = st.button("Dry-run (валидация)", use_container_width=True)
            with col_import:
                do_import = st.button("Импортировать", type="primary", use_container_width=True)

            if do_dry_run or do_import:
                should_rerun = False
                if missing_mapping_fields:
                    message_text = (
                        "Укажите столбцы для обязательных полей: "
                        + ", ".join(missing_mapping_fields)
                    )
                    st.session_state["sbis_import_feedback"] = {
                        "status": "error",
                        "message": message_text,
                    }
                    _append_import_log(f"Импорт остановлен: не заполнены поля ({', '.join(missing_mapping_fields)})")
                elif not records:
                    st.session_state["sbis_import_feedback"] = {
                        "status": "error",
                        "message": "Нет данных для импорта после обработки. Проверьте настройки маппинга.",
                    }
                    _append_import_log("Импорт остановлен: подготовленные данные отсутствуют")
                else:
                    try:
                        inserted, updated = (
                            _simulate_upsert(records) if do_dry_run else upsert_products(records)
                        )
                        details_lines = [f"Всего подготовлено записей: {len(records)}"]
                        if errors:
                            details_lines.append(f"Предупреждений: {len(errors)}")
                        if duplicate_keys:
                            details_lines.append(f"Повторы ключей: {len(duplicate_keys)}")
                        details_text = " | ".join(details_lines)
                        if do_dry_run:
                            message_type = "info"
                            message = f"Dry-run: добавлено {inserted}, обновлено {updated}."
                        else:
                            message_type = "success"
                            message = f"Импорт завершён: добавлено {inserted}, обновлено {updated}."
                        st.session_state["sbis_import_feedback"] = {
                            "status": message_type,
                            "message": message,
                            "details": details_text,
                        }
                        log_message = (
                            f"{'Dry-run' if do_dry_run else 'Импорт'}: подготовлено {len(records)}, добавлено {inserted}, обновлено {updated}"
                        )
                        if errors:
                            log_message += f", предупреждений: {len(errors)}"
                        if duplicate_keys:
                            log_message += f", повторов: {len(duplicate_keys)}"
                        _append_import_log(log_message)
                        if errors:
                            for err in errors[:3]:
                                _append_import_log(f"⚠️ {err}")
                            if len(errors) > 3:
                                _append_import_log(f"… и ещё {len(errors) - 3} предупреждений")
                        if duplicate_keys:
                            preview_duplicates = ", ".join(duplicate_keys[:5])
                            suffix = "…" if len(duplicate_keys) > 5 else ""
                            _append_import_log(
                                f"Повторяющиеся ключи: {preview_duplicates}{suffix}"
                            )
                        if do_import:
                            _reset_import_state()
                            load_sbis_products_df.clear()
                            should_rerun = True
                    except Exception as exc:  # noqa: BLE001
                        error_message = f"Не удалось обработать данные: {exc}"
                        st.session_state["sbis_import_feedback"] = {
                            "status": "error",
                            "message": error_message,
                        }
                        _append_import_log(
                            f"Ошибка {'dry-run' if do_dry_run else 'импорта'}: {exc}"
                        )
                if should_rerun:
                    st.rerun()

    elif current_df is not None and current_df.empty:
        st.warning("Файл не содержит данных для импорта.")
    else:
        if not feedback:
            st.info("Загрузите файл для начала импорта данных SBIS.")


col_refresh, _, _ = st.columns([1, 1, 3])
with col_refresh:
    if st.button("Refresh данные SBIS", use_container_width=True):
        load_sbis_products_df.clear()

try:
    df_sbis = load_sbis_products_df()
except Exception as exc:  # noqa: BLE001
    st.error(f"Ошибка чтения данных SBIS из базы: {exc}")
    st.stop()

if df_sbis.empty:
    st.info("Нет данных SBIS в хранилище. Импортируйте их через Excel/CSV.")
    st.stop()

with st.expander("Фильтры", expanded=True):
    search = st.text_input("Поиск по названию, бренду или ключу", value="")
    key_types = sorted({kt for kt in df_sbis.get("external_key_type", []).dropna().unique().tolist() if isinstance(kt, str)})
    selected_key_types = st.multiselect("Тип внешнего ключа", options=key_types, default=[])
    brands = sorted({b for b in df_sbis.get("brand", []).dropna().unique().tolist() if isinstance(b, str)})
    selected_brands = st.multiselect("Бренды", options=brands, default=[])
    min_stock = st.number_input("Мин. остаток", min_value=0, value=0, step=1)
    only_with_price = st.checkbox("Только с ценой", value=False)

filtered = df_sbis.copy()
if search:
    q = search.strip().lower()
    if q:
        filtered = filtered[
            filtered.apply(
                lambda row: any(
                    str(row.get(field, "")).lower().find(q) != -1 for field in ("title", "brand", "external_key", "sku")
                ),
                axis=1,
            )
        ]
if selected_key_types:
    filtered = filtered[filtered["external_key_type"].isin(selected_key_types)]
if selected_brands:
    filtered = filtered[filtered["brand"].isin(selected_brands)]
if min_stock:
    filtered = filtered[(filtered["stock"].fillna(0) >= min_stock)]
if only_with_price:
    filtered = filtered[filtered["price"].notna()]

if filtered.empty:
    st.info("Нет записей после применения фильтров.")
    st.stop()

columns_to_show = [
    col
    for col in ["external_key", "external_key_type", "title", "brand", "price", "stock", "product_id", "offer_id", "sku"]
    if col in filtered.columns
]

st.caption(f"Записей после фильтрации: {len(filtered)}")

st.dataframe(filtered[columns_to_show], use_container_width=True, hide_index=True)

with st.expander("Детали записей SBIS"):
    count = st.number_input(
        "Сколько строк показать", min_value=1, max_value=min(200, len(filtered)), value=min(25, len(filtered))
    )
    subset = filtered.head(int(count))
    for _, row in subset.iterrows():
        header_parts = [row.get("title") or "Без названия"]
        header_parts.append(f"key: {row.get('external_key')}")
        with st.expander(" — ".join(part for part in header_parts if part)):
            extra = row.get("extra") or {}
            if isinstance(extra, dict):
                st.json(extra)
            else:
                st.write(extra)
