from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import pandas as pd
import streamlit as st

from app_layout import initialize_page
from demowb.db import SessionLocal
from product_service import (
    CustomFieldDefinition,
    delete_custom_field_definition,
    load_custom_field_definitions,
    sanitize_custom_field_key,
    save_custom_field_definition,
)

FIELD_TYPE_LABELS = {
    "string": "Строка",
    "number": "Число",
    "boolean": "Логическое",
    "date": "Дата",
    "choice": "Список вариантов",
}


def _coerce_date(value: Optional[object]) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            pass
        try:
            parsed = pd.to_datetime(value, errors="coerce")
            if parsed is not None and not pd.isna(parsed):
                return parsed.date()
        except Exception:  # noqa: BLE001 - fallback handled below
            pass
    return datetime.utcnow().date()


def _render_default_input(
    *,
    field_type: str,
    current_default: Optional[object],
    choices: List[str],
    key_prefix: str,
) -> Optional[object]:
    use_default = st.checkbox(
        "Задать значение по умолчанию",
        value=current_default is not None,
        key=f"{key_prefix}_use_default",
    )
    if not use_default:
        return None

    if field_type == "number":
        number_default = float(current_default) if isinstance(current_default, (int, float)) else 0.0
        return st.number_input(
            "Значение по умолчанию",
            value=number_default,
            step=0.5,
            format="%.2f",
            key=f"{key_prefix}_default_number",
        )

    if field_type == "boolean":
        bool_default = bool(current_default) if current_default is not None else True
        return st.selectbox(
            "Значение по умолчанию",
            options=[True, False],
            index=0 if bool_default else 1,
            format_func=lambda v: "Истина" if v else "Ложь",
            key=f"{key_prefix}_default_bool",
        )

    if field_type == "date":
        selected_date = st.date_input(
            "Дата по умолчанию",
            value=_coerce_date(current_default),
            key=f"{key_prefix}_default_date",
        )
        return selected_date.isoformat()

    if field_type == "choice":
        if not choices:
            st.info("Добавьте варианты, чтобы выбрать значение по умолчанию.")
            return None
        current = str(current_default) if isinstance(current_default, str) else choices[0]
        if current not in choices:
            current = choices[0]
        return st.selectbox(
            "Значение по умолчанию",
            options=choices,
            index=choices.index(current),
            key=f"{key_prefix}_default_choice",
        )

    # string and fallback
    text_value = st.text_input(
        "Значение по умолчанию",
        value=str(current_default) if current_default is not None else "",
        key=f"{key_prefix}_default_text",
    ).strip()
    return text_value or None


initialize_page(
    page_title="Пользовательские поля товаров",
    page_icon="🧩",
    current_page="pages/Custom_Fields.py",
    description="Управление метаданными пользовательских полей и значениями по умолчанию.",
)

with SessionLocal() as session:
    custom_fields = load_custom_field_definitions(session)

st.caption(
    "Пользовательские поля позволяют расширять карточку товара без миграций."
    " Здесь вы можете создавать новые поля, управлять отображением и типами данных."
)

if custom_fields:
    st.subheader("Существующие поля")
    overview_rows = []
    for field in custom_fields:
        overview_rows.append(
            {
                "Ключ": field.key,
                "Название": field.name,
                "Тип": FIELD_TYPE_LABELS.get(field.field_type, field.field_type),
                "Обязательное": "Да" if field.required else "Нет",
                "Видимое": "Да" if field.visible else "Нет",
                "Порядок": field.order,
                "По умолчанию": field.default,
                "Варианты": ", ".join(field.choices) if field.choices else "—",
            }
        )
    st.dataframe(pd.DataFrame(overview_rows), use_container_width=True, hide_index=True)
else:
    st.info("Пока не создано ни одного пользовательского поля.")

st.divider()

st.subheader("Добавить новое поле")
with st.form("custom_field_create_form"):
    new_key = st.text_input("Ключ", placeholder="например, color")
    new_name = st.text_input("Название", placeholder="Цвет")
    new_type = st.selectbox(
        "Тип",
        options=list(FIELD_TYPE_LABELS.keys()),
        format_func=lambda value: FIELD_TYPE_LABELS[value],
        index=0,
    )
    new_order = st.number_input("Порядок", value=(len(custom_fields) + 1) * 10, step=1, min_value=0)
    new_required = st.checkbox("Обязательное поле", value=False)
    new_visible = st.checkbox("Отображать по умолчанию", value=True)

    choices_value: List[str] = []
    if new_type == "choice":
        raw_choices = st.text_area(
            "Варианты (каждый с новой строки)",
            placeholder="XS\nS\nM\nL",
        )
        choices_value = [line.strip() for line in raw_choices.splitlines() if line.strip()]
    default_value = _render_default_input(
        field_type=new_type,
        current_default=None,
        choices=choices_value,
        key_prefix="create",
    )

    create_submit = st.form_submit_button("Создать поле", type="primary")

if create_submit:
    with SessionLocal() as session:
        record, errors = save_custom_field_definition(
            session,
            original_key=None,
            key=new_key,
            name=new_name or new_key,
            field_type=new_type,
            default=default_value,
            required=new_required,
            visible=new_visible,
            order=int(new_order),
            choices=choices_value,
        )
    if errors:
        for message in errors:
            st.error(message)
    else:
        st.success(f"Поле '{record.name}' создано.")
        st.experimental_rerun()

st.divider()

st.subheader("Редактирование полей")
if not custom_fields:
    st.info("Создайте хотя бы одно поле, чтобы приступить к настройке.")
else:
    for field in custom_fields:
        expander_label = f"{field.name} ({field.key})"
        with st.expander(expander_label, expanded=False):
            col_form, col_actions = st.columns([3, 1])
            with col_form:
                updated_type = st.selectbox(
                    "Тип",
                    options=list(FIELD_TYPE_LABELS.keys()),
                    index=list(FIELD_TYPE_LABELS.keys()).index(field.field_type),
                    format_func=lambda value: FIELD_TYPE_LABELS[value],
                    key=f"edit_type_{field.key}",
                )

                with st.form(f"edit_field_form_{field.key}"):
                    edited_key = st.text_input("Ключ", value=field.key, key=f"edit_key_{field.key}")
                    edited_name = st.text_input("Название", value=field.name, key=f"edit_name_{field.key}")
                    edited_order = st.number_input(
                        "Порядок",
                        value=field.order,
                        step=1,
                        min_value=0,
                        key=f"edit_order_{field.key}",
                    )
                    edited_required = st.checkbox(
                        "Обязательное поле",
                        value=field.required,
                        key=f"edit_required_{field.key}",
                    )
                    edited_visible = st.checkbox(
                        "Отображать по умолчанию",
                        value=field.visible,
                        key=f"edit_visible_{field.key}",
                    )

                    choices_value = field.choices
                    if updated_type == "choice":
                        raw_choices = st.text_area(
                            "Варианты (каждый с новой строки)",
                            value="\n".join(field.choices),
                            key=f"edit_choices_{field.key}",
                        )
                        choices_value = [line.strip() for line in raw_choices.splitlines() if line.strip()]
                    else:
                        st.markdown(
                            "<small>Варианты доступны только для типа 'Список вариантов'.</small>",
                            unsafe_allow_html=True,
                        )
                        choices_value = []

                    default_value = _render_default_input(
                        field_type=updated_type,
                        current_default=field.default,
                        choices=choices_value,
                        key_prefix=f"edit_{field.key}",
                    )

                    submit_edit = st.form_submit_button("Сохранить изменения", type="primary")

            with col_actions:
                st.markdown("\n")
                if st.button("Удалить поле", key=f"delete_button_{field.key}"):
                    with SessionLocal() as session:
                        error = delete_custom_field_definition(session, field.key)
                    if error:
                        st.error(error)
                    else:
                        st.success(f"Поле '{field.name}' удалено.")
                        st.experimental_rerun()

            if submit_edit:
                with SessionLocal() as session:
                    record, errors = save_custom_field_definition(
                        session,
                        original_key=field.key,
                        key=edited_key,
                        name=edited_name or field.name,
                        field_type=updated_type,
                        default=default_value,
                        required=edited_required,
                        visible=edited_visible,
                        order=int(edited_order),
                        choices=choices_value,
                    )
                if errors:
                    for message in errors:
                        st.error(message)
                else:
                    st.success(f"Поле '{record.name}' обновлено.")
                    st.experimental_rerun()

st.caption(
    "🔒 Изменения применяются сразу после сохранения. Поля участвуют в импорте/экспорте и отображаются"
    " на странице каталога согласно указанному порядку."
)
