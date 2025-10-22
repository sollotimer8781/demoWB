from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.orm import Session

from models import Product, ProductImportLog

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


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return True
    except Exception:  # pragma: no cover - pandas-specific edge cases
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


def collect_custom_field_keys(session: Session) -> List[str]:
    keys: set[str] = set()
    rows = session.scalars(select(Product.custom_fields)).all()
    for payload in rows:
        if isinstance(payload, dict):
            keys.update(payload.keys())
    return sorted(keys)


def get_available_brands(session: Session) -> List[str]:
    rows = session.scalars(select(Product.brand).where(Product.brand.is_not(None)).distinct().order_by(Product.brand)).all()
    return [row for row in rows if row]


def load_products_dataframe(
    session: Session,
    filters: ProductFilters,
    visible_custom_fields: Sequence[str],
) -> Tuple[pd.DataFrame, List[Product]]:
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
        custom = product.custom_fields or {}
        for key in visible_custom_fields:
            column_name = f"{CUSTOM_PREFIX}{key}"
            row[column_name] = custom.get(key)
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.reset_index(drop=True)
    return df, products


def save_products_from_dataframe(
    session: Session,
    edited_df: pd.DataFrame,
    original_products: Sequence[Product],
    visible_custom_fields: Sequence[str],
) -> SaveResult:
    result = SaveResult()
    if edited_df is None:
        result.errors.append("Нет данных для сохранения.")
        return result

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
        product_id = row.get("id")
        product_id = _normalize_int(product_id)
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

        custom_updates: Dict[str, object] = {}
        for key in visible_custom_fields:
            column_name = f"{CUSTOM_PREFIX}{key}"
            if column_name in row:
                custom_value = _normalize_generic(row.get(column_name))
                if custom_value is None:
                    custom_updates[key] = None
                else:
                    custom_updates[key] = custom_value

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
            existing_custom = dict(product.custom_fields or {})
            for key, value in custom_updates.items():
                if value is None:
                    existing_custom.pop(key, None)
                else:
                    existing_custom[key] = value
            product.custom_fields = existing_custom
            product.updated_at = now
            result.updated += 1
        else:
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
                custom_fields={
                    key: value
                    for key, value in custom_updates.items()
                    if value is not None
                },
                created_at=now,
                updated_at=now,
            )
            session.add(new_product)
            result.inserted += 1

    if result.errors:
        session.rollback()
        result.inserted = result.updated = result.deleted = 0
        return result

    # Handle deletions
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
) -> ImportResult:
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
            value = row.get(column_name)
            normalized = _normalize_generic(value)
            if normalized is not None:
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
                    custom_fields=custom_payload,
                    created_at=now,
                    updated_at=now,
                )
                session.add(product)
            else:
                for key, value in payload.items():
                    if key == "nm_id" and value is None:
                        continue
                    setattr(product, key, value)
                merged_custom = dict(product.custom_fields or {})
                for key, value in custom_payload.items():
                    merged_custom[key] = value
                product.custom_fields = merged_custom
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
    custom_fields: Sequence[str],
) -> pd.DataFrame:
    df, _ = load_products_dataframe(session, filters, custom_fields)
    if df.empty:
        return df
    export_df = df.copy()
    rename_map = {f"{CUSTOM_PREFIX}{key}": key for key in custom_fields}
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
) -> Tuple[int, Optional[str]]:
    ids = [pid for pid in product_ids if pid is not None]
    if not ids:
        return 0, "Не выбрано ни одной записи."

    normalized_value: Optional[object]
    if is_custom:
        normalized_value = _normalize_generic(value)
    elif field in {"sku", "title", "brand", "category", "barcode"}:
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
            if is_custom:
                custom_data = dict(product.custom_fields or {})
                if normalized_value is None:
                    custom_data.pop(field, None)
                else:
                    custom_data[field] = normalized_value
                product.custom_fields = custom_data
            else:
                setattr(product, field, normalized_value)
            product.updated_at = now
        session.commit()
        return len(products), None
    except Exception as exc:  # pragma: no cover - runtime safety
        session.rollback()
        return 0, f"Ошибка при массовом обновлении: {exc}"
