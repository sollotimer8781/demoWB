import io
import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from product_repository import ensure_schema, get_connection, load_products_df, upsert_products


st.set_page_config(page_title="SBIS Products", layout="wide")

st.title("SBIS Products")


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
    for col in field_mapping.values():
        if col:
            used_columns.add(col)
    for col in image_columns:
        used_columns.add(col)

    for idx, row in df.iterrows():
        raw_key = row.get(key_column)
        normalized_key = _normalize_value(raw_key)
        if normalized_key is None:
            errors.append(f"Строка {idx + 2}: внешний ключ пустой")
            continue
        external_key = str(normalized_key).strip()
        if not external_key:
            errors.append(f"Строка {idx + 2}: внешний ключ пустой")
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
            if not column:
                continue
            value = row.get(column)
            normalized = _normalize_value(value)
            if normalized is None:
                continue
            if field in {"title", "brand", "product_id", "offer_id", "sku"}:
                text = str(normalized).strip()
                if text:
                    record[field] = text
                continue
            record[field] = normalized

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
            normalized = _normalize_value(row.get(column))
            if normalized is None:
                continue
            extra[column] = normalized
        if extra:
            record["extra"] = extra

        records.append(record)

    return records, errors, duplicate_keys


def _simulate_upsert(records: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
    if not records:
        return 0, 0
    with get_connection() as conn:
        ensure_schema(conn)
        cursor = conn.cursor()
        inserted = 0
        updated = 0
        for item in records:
            row = cursor.execute(
                "SELECT 1 FROM product_items WHERE source = ? AND external_key = ? AND external_key_type = ?",
                (item["source"], item["external_key"], item["external_key_type"]),
            ).fetchone()
            if row:
                updated += 1
            else:
                inserted += 1
    return inserted, updated


@st.cache_data(ttl=300)
def load_sbis_products_df():
    return load_products_df("SBIS")


if "sbis_import_feedback" not in st.session_state:
    st.session_state["sbis_import_feedback"] = None


with st.sidebar:
    st.header("SBIS API (подготовка)")
    st.caption(
        "Импорт через Excel/CSV доступен сейчас. Интеграция по API будет добавлена позже — интерфейс уже подготовлен к использованию ключей."
    )


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
            "Выберите пример", options=["— не выбран —"] + [str(p.relative_to(Path("/home/engine/project"))) for p in sample_files], key="sbis_sample_select"
        )
        if selected_sample != "— не выбран —":
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

            field_labels = {
                "title": "Название (title)",
                "brand": "Бренд (brand)",
                "price": "Цена (price)",
                "stock": "Остаток (stock)",
                "product_id": "Product ID",
                "offer_id": "Offer ID",
                "sku": "SKU",
                "nm_id": "NM ID",
            }
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

            sentinel = "— не использовать —"
            mapping: Dict[str, Optional[str]] = {}
            for field, label in field_labels.items():
                options = [sentinel] + columns
                default_value = guess_map.get(field)
                default_index = options.index(default_value) if default_value in columns else 0
                selected = st.selectbox(label, options=options, index=default_index, key=f"sbis_map_{field}")
                mapping[field] = None if selected == sentinel else selected

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

            field_mapping = {k: v for k, v in mapping.items() if v}
            records, errors, duplicates = _prepare_records(
                current_df,
                key_column=key_column,
                external_key_type=key_type,
                field_mapping=mapping,
                image_columns=image_columns,
            )

            st.caption(f"Подготовлено записей: {len(records)}")

            if st.session_state.get("sbis_import_feedback"):
                feedback = st.session_state["sbis_import_feedback"]
                status = feedback.get("status") if isinstance(feedback, dict) else None
                message = feedback.get("message") if isinstance(feedback, dict) else None
                details = feedback.get("details") if isinstance(feedback, dict) else None
                if status == "success":
                    st.success(message)
                elif status == "info":
                    st.info(message)
                elif status == "error":
                    st.error(message)
                if details:
                    st.caption(details)

            if errors:
                st.error("Проблемы с данными:")
                for err in errors[:20]:
                    st.write(f"- {err}")
                if len(errors) > 20:
                    st.write(f"… и ещё {len(errors) - 20} записей")
            if duplicates:
                st.warning(
                    f"Обнаружено повторяющихся ключей: {len(duplicates)}. Будут использованы первые вхождения."
                )

            preview_count = min(20, len(records))
            if preview_count:
                st.markdown("**Предпросмотр нормализованных данных (первые строки)**")
                try:
                    st.dataframe(pd.DataFrame(records).head(preview_count), use_container_width=True)
                except Exception:  # noqa: BLE001
                    st.write(records[:preview_count])

            col_validate, col_import = st.columns(2)
            with col_validate:
                do_dry_run = st.button("Dry-run (валидация)", use_container_width=True)
            with col_import:
                do_import = st.button("Импортировать", type="primary", use_container_width=True)

            if do_dry_run or do_import:
                if not records:
                    st.session_state["sbis_import_feedback"] = {
                        "status": "error",
                        "message": "Нет данных для импорта после обработки. Проверьте настройки маппинга.",
                    }
                else:
                    try:
                        to_process = records
                        inserted, updated = _simulate_upsert(to_process) if do_dry_run else upsert_products(to_process)
                        message_type = "info" if do_dry_run else "success"
                        action_word = "(dry-run)" if do_dry_run else "Импорт завершён"
                        message = f"{action_word}: добавлено {inserted}, обновлено {updated}."
                        st.session_state["sbis_import_feedback"] = {
                            "status": message_type,
                            "message": message,
                            "details": f"Всего подготовлено записей: {len(records)}",
                        }
                        if do_import:
                            load_sbis_products_df.clear()
                    except Exception as exc:  # noqa: BLE001
                        st.session_state["sbis_import_feedback"] = {
                            "status": "error",
                            "message": f"Не удалось обработать данные: {exc}",
                        }
                st.experimental_rerun()

    elif current_df is not None and current_df.empty:
        st.warning("Файл не содержит данных для импорта.")
    else:
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
