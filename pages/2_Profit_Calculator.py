from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy.exc import IntegrityError

from app_layout import initialize_page
from demowb.analytics import (
    LogisticTariffData,
    ProfitComputation,
    ProfitInput,
    calculate_profit,
    fetch_logistic_tariffs,
    fetch_profit_scenarios,
    generate_discount_sensitivity,
    generate_price_sensitivity,
    get_profit_scenario,
    save_profit_scenario,
    scenario_to_dict,
)
from demowb.db import session_scope
from demowb.models import LogisticTariff

initialize_page(
    page_title="Калькулятор рентабельности",
    page_icon="🧮",
    current_page="pages/2_Profit_Calculator.py",
    description="Расчёт маржинальности единицы и партии для Wildberries и других маркетплейсов",
)

SESSION_DEFAULTS: Dict[str, object] = {
    "profit_price_src": 1000.0,
    "profit_seller_discount": 10.0,
    "profit_spp": 5.0,
    "profit_wb_fee": 15.0,
    "profit_tax_rate": 6.0,
    "profit_logistics_to": 20.0,
    "profit_logistics_back": 15.0,
    "profit_label": 20.0,
    "profit_package": 30.0,
    "profit_shipping": 25.0,
    "profit_storage": 10.0,
    "profit_product_cost": 400.0,
    "profit_volume_manual": 1.0,
    "profit_qty": 100.0,
    "profit_buyout_rate": 70.0,
    "profit_tax_base": "revenue",
    "profit_length_cm": 0.0,
    "profit_width_cm": 0.0,
    "profit_height_cm": 0.0,
    "profit_use_derived_volume": False,
    "profit_tariff_index": 0,
    "profit_scenario_name": "",
    "profit_scenario_description": "",
    "profit_active_scenario_id": None,
    "profit_selected_scenario_id": None,
}
for key, default_value in SESSION_DEFAULTS.items():
    st.session_state.setdefault(key, default_value)


def _format_number(value: float, *, precision: int = 2, suffix: str = "") -> str:
    formatted = f"{value:,.{precision}f}"
    formatted = formatted.replace(",", " ").replace(".", ",")
    return f"{formatted}{suffix}"


def format_currency(value: float) -> str:
    return _format_number(value, precision=2, suffix=" ₽")


def format_percent(value: Optional[float], *, precision: int = 2) -> str:
    if value is None:
        return "—"
    return _format_number(value, precision=precision, suffix=" %")


def format_units(value: float, *, precision: int = 2, unit: str = "шт") -> str:
    return _format_number(value, precision=precision, suffix=f" {unit}")


@st.cache_data(ttl=300)
def load_tariff_catalog() -> List[Dict[str, object]]:
    with session_scope() as session:
        records = fetch_logistic_tariffs(session, only_active=False)
    return [record.as_dict() for record in records]


def load_scenarios(limit: Optional[int] = 50) -> List[Dict[str, object]]:
    with session_scope() as session:
        scenarios = fetch_profit_scenarios(session, limit=limit)
    return [scenario_to_dict(item) for item in scenarios]


def _calculate_volume_from_dimensions(length_cm: float, width_cm: float, height_cm: float) -> float:
    if length_cm <= 0 or width_cm <= 0 or height_cm <= 0:
        return 0.0
    cubic_cm = length_cm * width_cm * height_cm
    return cubic_cm / 1000.0


def _build_tariff_options() -> Tuple[List[LogisticTariffData], List[Dict[str, object]]]:
    tariff_records = load_tariff_catalog()
    active_records = [record for record in tariff_records if record.get("active")]
    tariff_options = [LogisticTariffData.from_payload(record) for record in active_records]
    return tariff_options, tariff_records


TAX_BASE_OPTIONS: Dict[str, str] = {
    "revenue": "С выручки",
    "profit": "С прибыли",
    "none": "Не учитывать",
}


def _tariff_label(tariff: LogisticTariffData) -> str:
    base = format_currency(tariff.base_first_l)
    extra = format_currency(tariff.per_next_l)
    return f"{tariff.name} — до 1 л: {base}, далее: {extra} за литр"


input_col, result_col = st.columns([1.15, 0.85])

with st.sidebar:
    st.subheader("Тарифы логистики")
    tariffs, tariff_records = _build_tariff_options()
    if not tariffs:
        st.warning("Нет активных тарифов. Добавьте тариф, чтобы использовать калькулятор.")

    if tariff_records:
        tariff_df = pd.DataFrame(tariff_records)
        tariff_df = tariff_df.rename(
            columns={
                "name": "Название",
                "base_first_l": "До 1 л",
                "per_next_l": "За каждый литр",
                "active": "Активен",
            }
        )
        display_df = tariff_df[["Название", "До 1 л", "За каждый литр", "Активен"]].copy()
        display_df["До 1 л"] = display_df["До 1 л"].apply(format_currency)
        display_df["За каждый литр"] = display_df["За каждый литр"].apply(format_currency)
        st.dataframe(display_df, hide_index=True, use_container_width=True)

    with st.expander("Добавить тариф", expanded=False):
        with st.form("add_tariff_form", clear_on_submit=True):
            name_value = st.text_input("Название тарифа", max_chars=120)
            base_value = st.number_input("Базовый тариф до 1 л, ₽", min_value=0.0, value=60.0, step=1.0)
            extra_value = st.number_input("За каждый следующий литр, ₽", min_value=0.0, value=30.0, step=1.0)
            active_value = st.checkbox("Активен", value=True)
            submit_tariff = st.form_submit_button("Сохранить тариф", use_container_width=True)
            if submit_tariff:
                if not name_value.strip():
                    st.warning("Укажите название тарифа")
                else:
                    try:
                        with session_scope() as session:
                            session.add(
                                LogisticTariff(
                                    name=name_value.strip(),
                                    base_first_l=float(base_value),
                                    per_next_l=float(extra_value),
                                    active=bool(active_value),
                                )
                            )
                        load_tariff_catalog.clear()
                        st.success("Тариф сохранён")
                        st.experimental_rerun()
                    except IntegrityError:
                        st.error("Тариф с таким названием уже существует")
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Не удалось сохранить тариф: {exc}")

with input_col:
    st.markdown("### Параметры товара")

    price_src = st.number_input(
        "Цена на витрине, ₽",
        min_value=0.0,
        step=10.0,
        value=st.session_state["profit_price_src"],
        key="profit_price_src",
    )
    seller_discount = st.number_input(
        "Скидка продавца, %",
        min_value=0.0,
        max_value=100.0,
        step=0.5,
        value=st.session_state["profit_seller_discount"],
        key="profit_seller_discount",
    )
    spp = st.number_input(
        "SPP, %",
        min_value=0.0,
        max_value=100.0,
        step=0.5,
        value=st.session_state["profit_spp"],
        key="profit_spp",
    )
    wb_fee = st.number_input(
        "Комиссия маркетплейса, %",
        min_value=0.0,
        max_value=100.0,
        step=0.5,
        value=st.session_state["profit_wb_fee"],
        key="profit_wb_fee",
    )
    tax_base_choice = st.selectbox(
        "Налог считать",
        options=list(TAX_BASE_OPTIONS.keys()),
        format_func=lambda value: TAX_BASE_OPTIONS.get(value, value),
        key="profit_tax_base",
    )
    tax_rate = st.number_input(
        "Ставка налога, %",
        min_value=0.0,
        max_value=100.0,
        step=0.5,
        value=st.session_state["profit_tax_rate"],
        key="profit_tax_rate",
    )

    st.markdown("### Издержки")

    product_cost = st.number_input(
        "Себестоимость товара, ₽",
        min_value=0.0,
        step=10.0,
        value=st.session_state["profit_product_cost"],
        key="profit_product_cost",
    )
    label_cost = st.number_input(
        "Маркировка, ₽",
        min_value=0.0,
        step=5.0,
        value=st.session_state["profit_label"],
        key="profit_label",
    )
    package_cost = st.number_input(
        "Упаковка, ₽",
        min_value=0.0,
        step=5.0,
        value=st.session_state["profit_package"],
        key="profit_package",
    )
    shipping_cost = st.number_input(
        "Доставка до склада, ₽",
        min_value=0.0,
        step=5.0,
        value=st.session_state["profit_shipping"],
        key="profit_shipping",
    )
    storage_cost = st.number_input(
        "Хранение, ₽",
        min_value=0.0,
        step=1.0,
        value=st.session_state["profit_storage"],
        key="profit_storage",
    )
    logistics_to_cost = st.number_input(
        "Доставка на склад WB, ₽",
        min_value=0.0,
        step=5.0,
        value=st.session_state["profit_logistics_to"],
        key="profit_logistics_to",
    )
    logistics_back_cost = st.number_input(
        "Логистика возврата, ₽",
        min_value=0.0,
        step=5.0,
        value=st.session_state["profit_logistics_back"],
        key="profit_logistics_back",
    )

    st.markdown("### Габариты")
    volume_manual = st.number_input(
        "Объём, л",
        min_value=0.0,
        step=0.1,
        value=st.session_state["profit_volume_manual"],
        key="profit_volume_manual",
    )
    with st.expander("Рассчитать объём по габаритам, см", expanded=False):
        length_cm = st.number_input(
            "Длина, см",
            min_value=0.0,
            step=0.5,
            value=st.session_state["profit_length_cm"],
            key="profit_length_cm",
        )
        width_cm = st.number_input(
            "Ширина, см",
            min_value=0.0,
            step=0.5,
            value=st.session_state["profit_width_cm"],
            key="profit_width_cm",
        )
        height_cm = st.number_input(
            "Высота, см",
            min_value=0.0,
            step=0.5,
            value=st.session_state["profit_height_cm"],
            key="profit_height_cm",
        )
        derived_volume = _calculate_volume_from_dimensions(length_cm, width_cm, height_cm)
        st.caption(f"Расчётный объём: {format_units(derived_volume, precision=3, unit='л')}")
        use_derived = st.checkbox(
            "Использовать расчётный объём",
            value=st.session_state["profit_use_derived_volume"],
            key="profit_use_derived_volume",
        )
    actual_volume = derived_volume if use_derived and derived_volume > 0 else volume_manual
    st.caption(f"Используемый объём: {format_units(actual_volume, precision=3, unit='л')}")

    st.markdown("### Партия")
    qty = st.number_input(
        "Размер партии, шт",
        min_value=0.0,
        step=10.0,
        value=st.session_state["profit_qty"],
        key="profit_qty",
    )
    buyout_rate = st.number_input(
        "Выкуп, %",
        min_value=0.0,
        max_value=100.0,
        step=1.0,
        value=st.session_state["profit_buyout_rate"],
        key="profit_buyout_rate",
    )

if tariffs:
    stored_index = int(st.session_state.get("profit_tariff_index", 0))
    clamped_index = min(max(stored_index, 0), len(tariffs) - 1)
    if stored_index != clamped_index:
        st.session_state["profit_tariff_index"] = clamped_index
    selected_index = st.selectbox(
        "Тариф логистики",
        options=list(range(len(tariffs))),
        format_func=lambda idx: _tariff_label(tariffs[idx]),
        key="profit_tariff_index",
    )
    selected_tariff = tariffs[selected_index]
else:
    selected_tariff = LogisticTariffData(id=None, name="Без тарифа", base_first_l=0.0, per_next_l=0.0, active=True)

profit_input = ProfitInput(
    price_src=float(price_src),
    seller_discount=float(seller_discount),
    spp=float(spp),
    wb_fee=float(wb_fee),
    tax_rate=float(tax_rate),
    logistics_to=float(logistics_to_cost),
    logistics_back=float(logistics_back_cost),
    label=float(label_cost),
    package=float(package_cost),
    shipping=float(shipping_cost),
    storage=float(storage_cost),
    product_cost=float(product_cost),
    volume_l=float(actual_volume),
    qty=float(qty),
    buyout_rate=float(buyout_rate),
    tax_base=str(tax_base_choice),
)

result: ProfitComputation = calculate_profit(profit_input, selected_tariff)

with result_col:
    st.markdown("### На единицу")
    metric_cols = st.columns(3)
    metric_cols[0].metric("Итоговая цена", format_currency(result.unit.price_final))
    metric_cols[1].metric(
        "Маржа",
        format_currency(result.unit.margin),
        delta=format_percent(result.unit.margin_percent),
    )
    metric_cols[2].metric("Себестоимость", format_currency(result.unit.cost_total))

    st.caption(
        f"Комиссия: {format_currency(result.unit.commission)} · Налог: {format_currency(result.unit.tax)}"
    )

    breakdown_order = [
        "product_cost",
        "label",
        "package",
        "shipping",
        "storage",
        "logistic_tariff",
        "logistics_to",
        "commission",
        "tax",
    ]
    breakdown_titles = {
        "product_cost": "Себестоимость товара",
        "label": "Маркировка",
        "package": "Упаковка",
        "shipping": "Доставка до склада",
        "storage": "Хранение",
        "logistic_tariff": "Доставка покупателю (тариф)",
        "logistics_to": "Доставка на склад",
        "commission": "Комиссия маркетплейса",
        "tax": "Налог",
    }

    breakdown_rows = []
    for key in breakdown_order:
        value = result.unit.breakdown.get(key)
        if value is None:
            continue
        breakdown_rows.append({"Статья": breakdown_titles.get(key, key), "Сумма": float(value)})
    if breakdown_rows:
        breakdown_df = pd.DataFrame(breakdown_rows)
        st.dataframe(
            breakdown_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Статья": st.column_config.TextColumn("Статья затрат"),
                "Сумма": st.column_config.NumberColumn("Сумма, ₽", format="%.2f ₽"),
            },
        )

    st.markdown("### На партию")
    batch_cols = st.columns(3)
    batch_cols[0].metric("Продано", format_units(result.batch.sold, precision=0))
    batch_cols[1].metric("Возвраты", format_units(result.batch.returns, precision=0))
    batch_cols[2].metric("ROI", format_percent(result.batch.roi_percent, precision=2))

    st.caption(
        f"Прибыль партии: {format_currency(result.batch.batch_profit)} · Затраты на возвраты: {format_currency(result.batch.cost_returns)}"
    )

    st.markdown("### Логистика")
    st.info(
        f"Тариф: {format_currency(result.logistics.tariff_cost)} | "
        f"Доставка на склад: {format_currency(result.logistics.logistics_to)} | "
        f"Шагов по тарифу: {result.logistics.steps}"
    )

st.divider()

st.markdown("### Чувствительность")

price_min = max(result.inputs.price_src * 0.5, 0.0)
price_max = result.inputs.price_src * 1.5 + 1
price_range = st.slider(
    "Диапазон цены, ₽",
    min_value=int(price_min),
    max_value=int(max(price_max, price_min + 10)),
    value=(int(result.inputs.price_src * 0.8), int(result.inputs.price_src * 1.2)),
    step=10,
)
price_points = [
    price_range[0] + i * (price_range[1] - price_range[0]) / 9 for i in range(10)
]
price_sensitivity = generate_price_sensitivity(profit_input, selected_tariff, price_points)
price_df = pd.DataFrame(price_sensitivity)
st.line_chart(price_df.set_index("price_src"), use_container_width=True)

st.caption("Маржа и рентабельность при изменении витринной цены")

discount_range = st.slider(
    "Скидка продавца, %",
    min_value=0,
    max_value=100,
    value=(0, int(min(100, max(result.inputs.seller_discount * 2, 10)))),
    step=1,
)
discount_points = list(range(discount_range[0], discount_range[1] + 1))
discount_sensitivity = generate_discount_sensitivity(profit_input, selected_tariff, discount_points)
discount_df = pd.DataFrame(discount_sensitivity)
st.line_chart(discount_df.set_index("seller_discount"), use_container_width=True)

st.caption("Маржа при изменении скидки продавца")

st.divider()

st.markdown("### Сценарии")

scenario_cols = st.columns([2, 1, 1])
scenario_name = scenario_cols[0].text_input(
    "Название сценария",
    value=st.session_state.get("profit_scenario_name", ""),
    key="profit_scenario_name",
)
scenario_description = scenario_cols[1].text_input(
    "Комментарий",
    value=st.session_state.get("profit_scenario_description", ""),
    key="profit_scenario_description",
)
new_scenario = scenario_cols[2].button("Новый сценарий", use_container_width=True)
if new_scenario:
    st.session_state.update(
        {
            "profit_active_scenario_id": None,
            "profit_scenario_name": "",
            "profit_scenario_description": "",
        }
    )
    st.experimental_rerun()

save_col, load_col = st.columns([1, 1])

if save_col.button("Сохранить сценарий", type="primary", use_container_width=True):
    try:
        with session_scope() as session:
            saved = save_profit_scenario(
                session,
                name=scenario_name,
                description=scenario_description,
                computation=result,
                scenario_id=st.session_state.get("profit_active_scenario_id"),
            )
        st.session_state["profit_active_scenario_id"] = saved.id
        st.session_state["profit_scenario_name"] = saved.name
        st.session_state["profit_scenario_description"] = saved.description or ""
        st.success("Сценарий сохранён")
        st.experimental_rerun()
    except ValueError as exc:
        st.warning(str(exc))
    except Exception as exc:  # noqa: BLE001
        st.error(f"Не удалось сохранить сценарий: {exc}")

scenarios_data = load_scenarios()
scenario_options_map = {
    item["id"]: f"{item['name']} ({item['updated_at'] or item['created_at'] or '—'})" for item in scenarios_data
}
selected_scenario_id = st.selectbox(
    "Загруженные сценарии",
    options=[None, *scenario_options_map.keys()],
    format_func=lambda value: "—" if value is None else scenario_options_map.get(value, "—"),
    key="profit_selected_scenario_id",
)

if load_col.button("Загрузить сценарий", use_container_width=True, disabled=selected_scenario_id is None):
    if selected_scenario_id is not None:
        with session_scope() as session:
            scenario = get_profit_scenario(session, selected_scenario_id)
        if scenario is None:
            st.warning("Сценарий не найден")
        else:
            data = scenario_to_dict(scenario)
            inputs_data = data.get("inputs", {})
            mapping = {
                "profit_price_src": inputs_data.get("price_src"),
                "profit_seller_discount": inputs_data.get("seller_discount"),
                "profit_spp": inputs_data.get("spp"),
                "profit_wb_fee": inputs_data.get("wb_fee"),
                "profit_tax_rate": inputs_data.get("tax_rate"),
                "profit_logistics_to": inputs_data.get("logistics_to"),
                "profit_logistics_back": inputs_data.get("logistics_back"),
                "profit_label": inputs_data.get("label"),
                "profit_package": inputs_data.get("package"),
                "profit_shipping": inputs_data.get("shipping"),
                "profit_storage": inputs_data.get("storage"),
                "profit_product_cost": inputs_data.get("product_cost"),
                "profit_volume_manual": inputs_data.get("volume_l"),
                "profit_qty": inputs_data.get("qty"),
                "profit_buyout_rate": inputs_data.get("buyout_rate"),
            }
            st.session_state["profit_tax_base"] = inputs_data.get("tax_base", "revenue")
            st.session_state["profit_use_derived_volume"] = False
            st.session_state["profit_length_cm"] = 0.0
            st.session_state["profit_width_cm"] = 0.0
            st.session_state["profit_height_cm"] = 0.0
            for state_key, value in mapping.items():
                if value is None:
                    continue
                st.session_state[state_key] = float(value)
            tariff_data = (data.get("results", {}) or {}).get("tariff")
            if isinstance(tariff_data, dict) and tariffs:
                selected_id = tariff_data.get("id")
                for idx, tariff in enumerate(tariffs):
                    if tariff.id == selected_id:
                        st.session_state["profit_tariff_index"] = idx
                        break
            st.session_state["profit_scenario_name"] = data.get("name") or ""
            st.session_state["profit_scenario_description"] = data.get("description") or ""
            st.session_state["profit_active_scenario_id"] = data.get("id")
            st.experimental_rerun()

if scenarios_data:
    scenarios_table = pd.DataFrame(
        [
            {
                "ID": item["id"],
                "Название": item["name"],
                "Комментарий": item.get("description") or "",
                "Обновлено": item.get("updated_at") or item.get("created_at"),
            }
            for item in scenarios_data
        ]
    )
    st.dataframe(scenarios_table, hide_index=True, use_container_width=True)
else:
    st.info("Сохранённых сценариев пока нет. Заполните параметры и нажмите 'Сохранить сценарий'.")
