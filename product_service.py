from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.orm import Session

from models import Product, ProductCustomField, ProductImportLog

CUSTOM_PREFIX = "custom::"
BASE_COLUMNS = [
    "id",
    "sku",
    "nm_id",
    "title",
    "brand",
    "category",
    "price",
    "stock",
    "barcode",
    "is_active",
]
READ_ONLY_COLUMNS = ["created_at", "updated_at"]
CUSTOM_FIELD_TYPES = {"string", "number", "boolean", "date", "choice"}


@dataclass
class ProductFilters:
    search: Optional[str] = None
    brand: Optional[str] = None
    active_only: bool = False


@dataclass
class SaveResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    errors: List[str] = field(default_factory=list)


@dataclass
class ImportResult:
    inserted: int
    updated: int
    errors: List[str]
    log: ProductImportLog


@dataclass(frozen=True)
class CustomFieldDefinition:
    key: str
    name: str
    field_type: str
    default: Optional[object]
    required: bool
    visible: bool
    order: int
    choices: List[str]

    @property
    def column_name(self) -> str:
        return f"{CUSTOM_PREFIX}{self.key}"


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return True
    except Exception:
        pass
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _normalize_str(value: object) -> Optional[str]:
    if _is_empty(value):
        return None
    text = str(value).strip()
    return text or None


def _normalize_float(value: object) -> Optional[float]:
    if _is_empty(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "")
        if not text:
            return None
        text = text.replace(",", ".")
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _normalize_int(value: object) -> Optional[int]:
    if _is_empty(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        text = text.replace(",", ".")
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _normalize_bool(value: object) -> Optional[bool]:
    if _is_empty(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        if text in {"1", "true", "yes", "y", "да", "истина", "on"}:
            return True
        if text in {"0", "false", "no", "n", "нет", "ложь", "off"}:
            return False
    return None


def _normalize_generic(value: object) -> Optional[object]:
    if _is_empty(value):
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    return text or None


def _normalize_date(value: object) -> Optional[str]:
    if _is_empty(value):
        return None
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if timestamp is None or pd.isna(timestamp):  # type: ignore[arg-type]
        return None
    return timestamp.date().isoformat()


def _normalize_choices(values: Sequence[object]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        candidate = _normalize_str(value)
        if candidate and candidate not in normalized:
            normalized.append(candidate)
    return normalized


def _normalize_custom_value(definition: CustomFieldDefinition, value: object) -> Optional[object]:
    field_type = definition.field_type
    if field_type == "string":
        return _normalize_str(value)
    if field_type == "number":
        return _normalize_float(value)
    if field_type == "boolean":
        return _normalize_bool(value)
    if field_type == "date":
        return _normalize_date(value)
    if field_type == "choice":
        candidate = _normalize_str(value)
        if candidate is None:
            return None
        if not definition.choices:
            return candidate
        for option in definition.choices:
            if candidate == option or candidate.lower() == option.lower():
                return option
        return None
    return _normalize_generic(value)


def _format_custom_value_for_display(definition: CustomFieldDefinition, value: object) -> object:
    if value is None:
        return None
    if definition.field_type == "boolean":
        return bool(value)
    if definition.field_type == "number":
        try:
            return float(value)
        except Exception:
            return None
    if definition.field_type == "date":
        try:
            timestamp = pd.to_datetime(value, errors="coerce")
        except Exception:
            return None
        if timestamp is None or pd.isna(timestamp):  # type: ignore[arg-type]
            return None
        return timestamp
    if definition.field_type in {"choice", "string"}:
        return _normalize_str(value) or ""
    return value


def _definitions_map(definitions: Sequence[CustomFieldDefinition]) -> Dict[str, CustomFieldDefinition]:
    return {definition.key: definition for definition in definitions}


def _merge_custom_payload(product: Product) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if isinstance(product.custom_fields, dict):
        payload.update(product.custom_fields)
    if isinstance(product.custom_data, dict):
        payload.update(product.custom_data)
    return payload


def _sync_product_custom_payload(
    product: Product,
    normalized_data: Mapping[str, object],
    *,
    recognized_keys: Sequence[str],
    keys_to_remove: Optional[Sequence[str]] = None,
) -> None:
    sanitized = {key: value for key, value in normalized_data.items() if value is not None}
    product.custom_data = sanitized

    legacy_payload = dict(product.custom_fields or {})
    if keys_to_remove:
        for key in keys_to_remove:
            legacy_payload.pop(key, None)
    for key in recognized_keys:
        if key not in sanitized and key in legacy_payload:
            legacy_payload.pop(key, None)
    legacy_payload.update(sanitized)
    product.custom_fields = legacy_payload


def sanitize_custom_field_key(raw: str) -> Optional[str]:
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


def load_custom_field_definitions(session: Session) -> List[CustomFieldDefinition]:
    stmt = select(ProductCustomField).order_by(ProductCustomField.order.asc(), ProductCustomField.name.asc())
    records = session.scalars(stmt).all()

    definitions: List[CustomFieldDefinition] = []
    for record in records:
        field_type = record.field_type or "string"
        if field_type not in CUSTOM_FIELD_TYPES:
            field_type = "string"
        choices = record.choices or []
        choices_list = _normalize_choices(choices if isinstance(choices, (list, tuple)) else [choices])
        base_definition = CustomFieldDefinition(
            key=record.key,
            name=record.name or record.key,
            field_type=field_type,
            default=None,
            required=bool(record.required),
            visible=bool(record.visible),
            order=record.order or 0,
            choices=choices_list,
        )
        default_value = _normalize_custom_value(base_definition, record.default_value)
        definitions.append(
            CustomFieldDefinition(
                key=base_definition.key,
                name=base_definition.name,
                field_type=base_definition.field_type,
                default=default_value,
                required=base_definition.required,
                visible=base_definition.visible,
                order=base_definition.order,
                choices=base_definition.choices,
            )
        )
    return definitions


def collect_custom_field_keys(session: Session) -> List[str]:
    return [definition.key for definition in load_custom_field_definitions(session)]


def get_available_brands(session: Session) -> List[str]:
    rows = (
        session.scalars(
            select(Product.brand)
            .where(Product.brand.is_not(None))
            .distinct()
            .order_by(Product.brand)
        ).all()
    )
    return [row for row in rows if row]


def load_products_dataframe(
    session: Session,
    filters: ProductFilters,
    field_definitions: Sequence[CustomFieldDefinition],
    visible_keys: Optional[Sequence[str]] = None,
) -> Tuple[pd.DataFrame, List[Product]]:
    definitions_map = _definitions_map(field_definitions)
    if visible_keys is None:
        visible_keys = [definition.key for definition in field_definitions if definition.visible]

    stmt = select(Product)
    if filters.search:
        pattern = f"%{filters.search.lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(Product.title).like(pattern),
                func.lower(Product.brand).like(pattern),
                func.lower(Product.sku).like(pattern),
                cast(Product.nm_id, String).like(pattern),
            )
        )
    if filters.brand:
        stmt = stmt.where(Product.brand == filters.brand)
    if filters.active_only:
        stmt = stmt.where(Product.is_active.is_(True))
    stmt = stmt.order_by(Product.updated_at.desc(), Product.id.desc())

    products = session.scalars(stmt).all()
    rows: List[Dict[str, object]] = []
    for product in products:
        row: Dict[str, object] = {
            "id": product.id,
            "sku": product.sku,
            "nm_id": product.nm_id,
            "title": product.title,
            "brand": product.brand,
            "category": product.category,
            "price": product.price,
            "stock": product.stock,
            "barcode": product.barcode,
            "is_active": product.is_active,
            "created_at": product.created_at.isoformat() if product.created_at else None,
            "updated_at": product.updated_at.isoformat() if product.updated_at else None,
        }
        combined_payload = _merge_custom_payload(product)
        for key in visible_keys:
            definition = definitions_map.get(key)
            if not definition:
                continue
            column_name = definition.column_name
            raw_value = combined_payload.get(key)
            if raw_value is None and definition.default is not None:
                value = definition.default
            else:
                value = _normalize_custom_value(definition, raw_value)
                if value is None and raw_value is not None:
                    value = raw_value
            row[column_name] = _format_custom_value_for_display(definition, value)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df, products

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").astype("Int64")
    if "nm_id" in df.columns:
        df["nm_id"] = pd.to_numeric(df["nm_id"], errors="coerce").astype("Int64")
    for column in ("sku", "title", "brand", "category", "barcode"):
        if column in df.columns:
            df[column] = df[column].astype("string").fillna("")
    for column in ("created_at", "updated_at"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    for key in visible_keys:
        definition = definitions_map.get(key)
        if not definition:
            continue
        column_name = definition.column_name
        if column_name not in df.columns:
            continue
        if definition.field_type == "number":
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
        elif definition.field_type == "boolean":
            df[column_name] = df[column_name].apply(lambda v: bool(v) if not _is_empty(v) else False)
        elif definition.field_type == "date":
            df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
        else:
            df[column_name] = df[column_name].astype("string").fillna("")

    df = df.reset_index(drop=True)
    return df, products


def save_products_from_dataframe(
    session: Session,
    edited_df: pd.DataFrame,
    original_products: Sequence[Product],
    field_definitions: Sequence[CustomFieldDefinition],
    visible_keys: Sequence[str],
) -> SaveResult:
    result = SaveResult()
    if edited_df is None:
        result.errors.append("Нет данных для сохранения.")
        return result

    definitions_map = _definitions_map(field_definitions)
    recognized_keys = list(definitions_map.keys())

    original_map = {product.id: product for product in original_products if product.id is not None}
    edited_records = []
    if not edited_df.empty:
        edited_records = edited_df.to_dict(orient="records")

    seen_skus: Dict[str, int] = {}
    seen_nm_ids: Dict[int, int] = {}
    for idx, row in enumerate(edited_records, start=1):
        sku_value = _normalize_str(row.get("sku"))
        if sku_value:
            if sku_value in seen_skus:
                result.errors.append(f"Строка {idx}: дублирующий SKU {sku_value}.")
            seen_skus[sku_value] = seen_skus.get(sku_value, 0) + 1
        nm_id_value = _normalize_int(row.get("nm_id"))
        if nm_id_value is not None:
            if nm_id_value in seen_nm_ids:
                result.errors.append(f"Строка {idx}: дублирующий NM ID {nm_id_value}.")
            seen_nm_ids[nm_id_value] = seen_nm_ids.get(nm_id_value, 0) + 1

    if result.errors:
        return result

    processed_ids: set[int] = set()
    now = datetime.utcnow()

    for idx, row in enumerate(edited_records, start=1):
        product_id = _normalize_int(row.get("id"))
        sku = _normalize_str(row.get("sku"))
        title = _normalize_str(row.get("title"))
        if not title:
            result.errors.append(f"Строка {idx}: не заполнено обязательное поле 'Название'.")
            continue
        nm_id_value = _normalize_int(row.get("nm_id"))
        brand = _normalize_str(row.get("brand"))
        category = _normalize_str(row.get("category"))
        price = _normalize_float(row.get("price"))
        stock = _normalize_int(row.get("stock"))
        barcode = _normalize_str(row.get("barcode"))
        is_active = _normalize_bool(row.get("is_active"))
        if is_active is None:
            is_active = True

        custom_updates: Dict[str, Optional[object]] = {}
        for key in visible_keys:
            definition = definitions_map.get(key)
            if not definition:
                continue
            column_name = definition.column_name
            if column_name not in row:
                continue
            raw_value = row.get(column_name)
            normalized_value = _normalize_custom_value(definition, raw_value)
            if normalized_value is None and not _is_empty(raw_value):
                result.errors.append(
                    f"Строка {idx}: значение '{raw_value}' не соответствует типу поля '{definition.name}'."
                )
                continue
            if definition.required and normalized_value is None and definition.default is None:
                result.errors.append(f"Строка {idx}: поле '{definition.name}' обязательно для заполнения.")
                continue
            if normalized_value is None:
                custom_updates[key] = None
            else:
                custom_updates[key] = normalized_value

        if result.errors:
            continue

        if product_id and product_id in original_map:
            product = original_map[product_id]
            processed_ids.add(product_id)
            product.sku = sku
            product.nm_id = nm_id_value
            product.title = title
            product.brand = brand
            product.category = category
            product.price = price
            product.stock = stock
            product.barcode = barcode
            product.is_active = bool(is_active)
            custom_payload = dict(product.custom_data or {})
            for key, value in custom_updates.items():
                if value is None:
                    custom_payload.pop(key, None)
                else:
                    custom_payload[key] = value
            _sync_product_custom_payload(product, custom_payload, recognized_keys, keys_to_remove=custom_updates.keys())
            product.updated_at = now
            result.updated += 1
        else:
            custom_payload = {
                key: value
                for key, value in custom_updates.items()
                if value is not None
            }
            new_product = Product(
                sku=sku,
                nm_id=nm_id_value,
                title=title,
                brand=brand,
                category=category,
                price=price,
                stock=stock,
                barcode=barcode,
                is_active=bool(is_active),
                created_at=now,
                updated_at=now,
            )
            _sync_product_custom_payload(new_product, custom_payload, recognized_keys, keys_to_remove=custom_updates.keys())
            session.add(new_product)
            result.inserted += 1

    if result.errors:
        session.rollback()
        result.inserted = result.updated = result.deleted = 0
        return result

    original_ids = {product.id for product in original_products if product.id is not None}
    edited_ids = {pid for pid in processed_ids if pid is not None}
    if edited_df is not None and not edited_df.empty:
        for row in edited_records:
            pid = _normalize_int(row.get("id"))
            if pid is not None:
                edited_ids.add(pid)
    to_delete = original_ids - edited_ids
    for pid in to_delete:
        product = original_map.get(pid)
        if product is not None:
            session.delete(product)
            result.deleted += 1

    try:
        session.commit()
    except Exception as exc:  # pragma: no cover - runtime safety
        session.rollback()
        result.errors.append(f"Ошибка сохранения: {exc}")
        result.inserted = result.updated = result.deleted = 0
    return result


def import_products_from_dataframe(
    session: Session,
    dataframe: pd.DataFrame,
    *,
    key_column: str,
    key_target: str,
    field_mapping: Dict[str, Optional[str]],
    custom_field_mapping: Mapping[str, str],
    file_name: str,
    field_definitions: Sequence[CustomFieldDefinition],
) -> ImportResult:
    definitions_map = _definitions_map(field_definitions)
    recognized_keys = list(definitions_map.keys())

    cleaned_df = dataframe.copy()
    cleaned_df.columns = [str(col) for col in cleaned_df.columns]
    total_rows = len(cleaned_df)
    errors: List[str] = []

    if key_column not in cleaned_df.columns:
        raise ValueError("Выбранный ключевой столбец отсутствует в данных.")

    valid_rows: List[Tuple[int, Dict[str, object], Dict[str, object]]] = []
    seen_keys: Dict[str, int] = {}
    for idx, row in cleaned_df.iterrows():
        raw_key = row.get(key_column)
        if key_target == "nm_id":
            key_value_int = _normalize_int(raw_key)
            if key_value_int is None:
                errors.append(f"Строка {idx + 2}: некорректный NM ID")
                continue
            key_value = str(key_value_int)
        else:
            key_value_str = _normalize_str(raw_key)
            if not key_value_str:
                errors.append(f"Строка {idx + 2}: ключевой столбец пустой")
                continue
            key_value = key_value_str
        if key_value in seen_keys:
            errors.append(f"Строка {idx + 2}: обнаружен дубликат ключа {key_value}")
            continue
        seen_keys[key_value] = 1

        payload: Dict[str, object] = {}
        for model_field, column_name in field_mapping.items():
            if not column_name:
                continue
            value = row.get(column_name)
            if model_field in {"sku", "title", "brand", "category", "barcode"}:
                payload[model_field] = _normalize_str(value)
            elif model_field == "price":
                payload[model_field] = _normalize_float(value)
            elif model_field == "stock":
                payload[model_field] = _normalize_int(value)
            elif model_field == "is_active":
                bool_value = _normalize_bool(value)
                payload[model_field] = True if bool_value is None else bool_value
            elif model_field == "nm_id":
                payload[model_field] = _normalize_int(value)
            else:
                payload[model_field] = _normalize_generic(value)

        if "title" not in payload or not payload.get("title"):
            errors.append(f"Строка {idx + 2}: не заполнено поле 'Название'")
            continue

        custom_payload: Dict[str, object] = {}
        for column_name, custom_key in custom_field_mapping.items():
            definition = definitions_map.get(custom_key)
            if not definition:
                errors.append(f"Колонка '{column_name}': неизвестное пользовательское поле '{custom_key}'.")
                continue
            value = row.get(column_name)
            normalized = _normalize_custom_value(definition, value)
            if normalized is None:
                if definition.required and definition.default is None:
                    errors.append(
                        f"Строка {idx + 2}: поле '{definition.name}' обязательно и не заполнено."
                    )
                elif not _is_empty(value):
                    errors.append(
                        f"Строка {idx + 2}: значение '{value}' не соответствует типу поля '{definition.name}'."
                    )
                continue
            custom_payload[custom_key] = normalized

        valid_rows.append((idx, payload, custom_payload))

    key_values = [
        _normalize_int(row.get(key_column)) if key_target == "nm_id" else _normalize_str(row.get(key_column))
        for _, row in cleaned_df.iterrows()
    ]
    key_values = [value for value in key_values if value is not None]

    existing_map: Dict[str, Product] = {}
    if key_values:
        if key_target == "nm_id":
            stmt = select(Product).where(Product.nm_id.in_(key_values))
        else:
            stmt = select(Product).where(Product.sku.in_(key_values))
        for product in session.scalars(stmt):
            if key_target == "nm_id" and product.nm_id is not None:
                existing_map[str(product.nm_id)] = product
            elif key_target == "sku" and product.sku:
                existing_map[product.sku] = product

    operations: List[Tuple[str, Product, Dict[str, object], Dict[str, object]]] = []
    for idx, payload, custom_payload in valid_rows:
        key_value = payload.get("nm_id") if key_target == "nm_id" else payload.get("sku")
        key_str = str(key_value) if key_value is not None else str(cleaned_df.iloc[idx][key_column])
        existing = existing_map.get(key_str)
        if existing:
            operations.append(("update", existing, payload, custom_payload))
        else:
            operations.append(("create", Product(), payload, custom_payload))

    inserted = sum(1 for op in operations if op[0] == "create")
    updated = sum(1 for op in operations if op[0] == "update")

    success = False
    try:
        now = datetime.utcnow()
        for action, product, payload, custom_payload in operations:
            if action == "create":
                product = Product(
                    sku=payload.get("sku"),
                    nm_id=payload.get("nm_id"),
                    title=payload.get("title"),
                    brand=payload.get("brand"),
                    category=payload.get("category"),
                    price=payload.get("price"),
                    stock=payload.get("stock"),
                    barcode=payload.get("barcode"),
                    is_active=payload.get("is_active", True) if payload.get("is_active") is not None else True,
                    created_at=now,
                    updated_at=now,
                )
                _sync_product_custom_payload(product, custom_payload, recognized_keys, keys_to_remove=custom_payload.keys())
                session.add(product)
            else:
                for key, value in payload.items():
                    if key == "nm_id" and value is None:
                        continue
                    setattr(product, key, value)
                merged_custom = dict(product.custom_data or {})
                for key, value in custom_payload.items():
                    merged_value = value
                    if merged_value is None and key in merged_custom:
                        merged_custom.pop(key, None)
                    elif merged_value is not None:
                        merged_custom[key] = merged_value
                _sync_product_custom_payload(product, merged_custom, recognized_keys, keys_to_remove=custom_payload.keys())
                product.updated_at = now
        session.commit()
        success = True
    except Exception as exc:  # pragma: no cover - runtime safety
        session.rollback()
        errors.append(f"Ошибка при сохранении данных: {exc}")
        inserted = updated = 0
    finally:
        details = {
            "key_column": key_column,
            "key_target": key_target,
            "field_mapping": field_mapping,
            "custom_fields": dict(custom_field_mapping),
        }
        status = "success" if success and not errors else ("partial" if success else "failed")
        log = ProductImportLog(
            file_name=file_name,
            status=status,
            rows_processed=total_rows,
            inserted_count=inserted,
            updated_count=updated,
            errors=errors,
            details=details,
            created_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
        )
        session.add(log)
        session.commit()

    return ImportResult(inserted=inserted, updated=updated, errors=errors, log=log)


def export_products_dataframe(
    session: Session,
    filters: ProductFilters,
    field_definitions: Sequence[CustomFieldDefinition],
    export_keys: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    df, _ = load_products_dataframe(session, filters, field_definitions, export_keys)
    if df.empty:
        return df
    definitions_map = _definitions_map(field_definitions)
    keys = export_keys or [definition.key for definition in field_definitions if definition.visible]
    rename_map: Dict[str, str] = {}
    seen_labels: set[str] = set()
    for key in keys:
        definition = definitions_map.get(key)
        if not definition:
            continue
        column_name = definition.column_name
        label = definition.name or definition.key
        if label in seen_labels:
            label = f"{label} ({definition.key})"
        seen_labels.add(label)
        rename_map[column_name] = label
    export_df = df.copy()
    export_df = export_df.rename(columns=rename_map)
    return export_df


def fetch_import_logs(session: Session, limit: int = 20) -> pd.DataFrame:
    stmt = select(ProductImportLog).order_by(ProductImportLog.created_at.desc()).limit(limit)
    logs = session.scalars(stmt).all()
    rows: List[Dict[str, object]] = []
    for log in logs:
        rows.append(
            {
                "id": log.id,
                "file_name": log.file_name,
                "status": log.status,
                "rows_processed": log.rows_processed,
                "inserted_count": log.inserted_count,
                "updated_count": log.updated_count,
                "errors": "\n".join(log.errors or []),
                "created_at": log.created_at.isoformat() if log.created_at else None,
            }
        )
    return pd.DataFrame(rows)


def bulk_update_field(
    session: Session,
    product_ids: Iterable[int],
    *,
    field: str,
    value: object,
    is_custom: bool,
    custom_definitions: Optional[Mapping[str, CustomFieldDefinition]] = None,
) -> Tuple[int, Optional[str]]:
    ids = [pid for pid in product_ids if pid is not None]
    if not ids:
        return 0, "Не выбрано ни одной записи."

    definitions_map: Dict[str, CustomFieldDefinition] = {}
    if is_custom:
        if custom_definitions is not None:
            definitions_map = dict(custom_definitions)
        else:
            definitions_map = _definitions_map(load_custom_field_definitions(session))
        definition = definitions_map.get(field)
        if not definition:
            return 0, "Пользовательское поле не найдено."
        normalized_value = _normalize_custom_value(definition, value)
        if normalized_value is None and not _is_empty(value):
            return 0, f"Значение не соответствует типу поля '{definition.name}'."
        recognized_keys = list(definitions_map.keys())
    else:
        recognized_keys = []
        definition = None
        normalized_value = None

    if not is_custom:
        if field in {"sku", "title", "brand", "category", "barcode"}:
            normalized_value = _normalize_str(value)
        elif field == "price":
            normalized_value = _normalize_float(value)
        elif field == "stock":
            normalized_value = _normalize_int(value)
        elif field == "is_active":
            normalized_bool = _normalize_bool(value)
            normalized_value = True if normalized_bool is None else normalized_bool
        elif field == "nm_id":
            normalized_value = _normalize_int(value)
        else:
            normalized_value = _normalize_generic(value)

    try:
        products = session.scalars(select(Product).where(Product.id.in_(ids))).all()
        now = datetime.utcnow()
        for product in products:
            if is_custom and definition is not None:
                custom_payload = dict(product.custom_data or {})
                if normalized_value is None:
                    custom_payload.pop(field, None)
                else:
                    custom_payload[field] = normalized_value
                _sync_product_custom_payload(product, custom_payload, recognized_keys, keys_to_remove=[field])
            else:
                setattr(product, field, normalized_value)
            product.updated_at = now
        session.commit()
        return len(products), None
    except Exception as exc:  # pragma: no cover - runtime safety
        session.rollback()
        return 0, f"Ошибка при массовом обновлении: {exc}"


def _apply_definition_to_products(
    session: Session,
    definition: CustomFieldDefinition,
    *,
    old_key: Optional[str] = None,
    fill_missing_default: bool = False,
) -> None:
    definitions = load_custom_field_definitions(session)
    definitions_map = _definitions_map(definitions)
    recognized_keys = list(definitions_map.keys())
    target_definition = definitions_map.get(definition.key)
    if target_definition is None:
        return

    products = session.scalars(select(Product)).all()
    for product in products:
        custom_payload = dict(product.custom_data or {})
        if old_key and old_key != definition.key:
            if old_key in custom_payload and definition.key not in custom_payload:
                custom_payload[definition.key] = custom_payload.pop(old_key)
            else:
                custom_payload.pop(old_key, None)
        raw_value = custom_payload.get(definition.key)
        normalized_value = _normalize_custom_value(target_definition, raw_value)
        if normalized_value is None and fill_missing_default and target_definition.default is not None:
            normalized_value = target_definition.default
        if normalized_value is None:
            custom_payload.pop(definition.key, None)
        else:
            custom_payload[definition.key] = normalized_value
        keys_to_remove = [definition.key]
        if old_key and old_key != definition.key:
            keys_to_remove.append(old_key)
        _sync_product_custom_payload(product, custom_payload, recognized_keys, keys_to_remove=keys_to_remove)
    session.flush()


def _remove_custom_field_from_products(session: Session, key: str) -> None:
    definitions = load_custom_field_definitions(session)
    definitions_map = _definitions_map(definitions)
    recognized_keys = list(definitions_map.keys())

    products = session.scalars(select(Product)).all()
    for product in products:
        custom_payload = dict(product.custom_data or {})
        custom_payload.pop(key, None)
        _sync_product_custom_payload(product, custom_payload, recognized_keys, keys_to_remove=[key])
    session.flush()


def save_custom_field_definition(
    session: Session,
    *,
    original_key: Optional[str],
    key: str,
    name: str,
    field_type: str,
    default: Optional[object],
    required: bool,
    visible: bool,
    order: int,
    choices: Sequence[object],
) -> Tuple[Optional[ProductCustomField], List[str]]:
    errors: List[str] = []
    sanitized_key = sanitize_custom_field_key(key)
    if not sanitized_key:
        errors.append("Ключ поля должен содержать только латиницу, цифры, дефис или подчёркивание.")
    field_type_normalized = (field_type or "string").lower()
    if field_type_normalized not in CUSTOM_FIELD_TYPES:
        errors.append("Недопустимый тип пользовательского поля.")
    choices_list = _normalize_choices(choices) if field_type_normalized == "choice" else []
    if field_type_normalized == "choice" and not choices_list:
        errors.append("Для типа 'choice' необходимо указать варианты значений.")

    if errors:
        return None, errors

    name_normalized = name.strip() if name else sanitized_key
    order_value = order if isinstance(order, int) else 0

    existing: Optional[ProductCustomField]
    if original_key:
        existing = session.scalar(select(ProductCustomField).where(ProductCustomField.key == original_key))
        if existing is None:
            return None, ["Исходное поле не найдено."]
        if sanitized_key != original_key:
            duplicate = session.scalar(select(ProductCustomField).where(ProductCustomField.key == sanitized_key))
            if duplicate:
                return None, ["Поле с таким ключом уже существует."]
    else:
        existing = session.scalar(select(ProductCustomField).where(ProductCustomField.key == sanitized_key))
        if existing:
            return None, ["Поле с таким ключом уже существует."]

    definition_stub = CustomFieldDefinition(
        key=sanitized_key,
        name=name_normalized,
        field_type=field_type_normalized,
        default=None,
        required=required,
        visible=visible,
        order=order_value,
        choices=choices_list,
    )
    default_value = _normalize_custom_value(definition_stub, default)
    if default is not None and default_value is None:
        errors.append("Значение по умолчанию не соответствует выбранному типу.")
        return None, errors

    now = datetime.utcnow()
    if existing is None:
        existing = ProductCustomField(
            key=sanitized_key,
            name=name_normalized,
            field_type=field_type_normalized,
            default_value=default_value,
            required=required,
            visible=visible,
            order=order_value,
            choices=choices_list,
            created_at=now,
            updated_at=now,
        )
        session.add(existing)
    else:
        existing.key = sanitized_key
        existing.name = name_normalized
        existing.field_type = field_type_normalized
        existing.default_value = default_value
        existing.required = required
        existing.visible = visible
        existing.order = order_value
        existing.choices = choices_list
        existing.updated_at = now

    session.flush()

    definition_after = CustomFieldDefinition(
        key=sanitized_key,
        name=name_normalized,
        field_type=field_type_normalized,
        default=default_value,
        required=required,
        visible=visible,
        order=order_value,
        choices=choices_list,
    )
    _apply_definition_to_products(
        session,
        definition_after,
        old_key=original_key if original_key != sanitized_key else None,
        fill_missing_default=default_value is not None and original_key is None,
    )
    session.commit()
    return existing, []


def delete_custom_field_definition(session: Session, key: str) -> Optional[str]:
    sanitized_key = sanitize_custom_field_key(key)
    if not sanitized_key:
        return "Некорректный ключ поля."
    record = session.scalar(select(ProductCustomField).where(ProductCustomField.key == sanitized_key))
    if record is None:
        return "Поле с указанным ключом не найдено."
    session.delete(record)
    session.flush()
    _remove_custom_field_from_products(session, sanitized_key)
    session.commit()
    return None
