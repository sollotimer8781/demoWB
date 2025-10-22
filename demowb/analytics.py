from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from demowb.models import LogisticTariff, ProfitScenario

NumberLike = float | int | str | None
TaxBase = str


def _to_float(value: NumberLike, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(value)
    try:
        if isinstance(value, str):
            normalized = value.strip().replace(" ", "")
            if not normalized:
                return default
            candidate = float(normalized.replace(",", "."))
        else:
            candidate = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(candidate):
        return default
    return candidate


@dataclass(frozen=True)
class ProfitInput:
    price_src: float
    seller_discount: float
    spp: float
    wb_fee: float
    tax_rate: float
    logistics_to: float
    logistics_back: float
    label: float
    package: float
    shipping: float
    storage: float
    product_cost: float
    volume_l: float
    qty: float
    buyout_rate: float
    tax_base: TaxBase = "revenue"

    @staticmethod
    def from_payload(payload: Dict[str, NumberLike | str]) -> "ProfitInput":
        return ProfitInput(
            price_src=_to_float(payload.get("price_src"), 0.0),
            seller_discount=_to_float(payload.get("seller_discount"), 0.0),
            spp=_to_float(payload.get("spp"), 0.0),
            wb_fee=_to_float(payload.get("wb_fee"), 0.0),
            tax_rate=_to_float(payload.get("tax_rate"), 0.0),
            logistics_to=_to_float(payload.get("logistics_to"), 0.0),
            logistics_back=_to_float(payload.get("logistics_back"), 0.0),
            label=_to_float(payload.get("label"), 0.0),
            package=_to_float(payload.get("package"), 0.0),
            shipping=_to_float(payload.get("shipping"), 0.0),
            storage=_to_float(payload.get("storage"), 0.0),
            product_cost=_to_float(payload.get("product_cost"), 0.0),
            volume_l=max(_to_float(payload.get("volume_l"), 0.0), 0.0),
            qty=max(_to_float(payload.get("qty"), 0.0), 0.0),
            buyout_rate=max(_to_float(payload.get("buyout_rate"), 0.0), 0.0),
            tax_base=str(payload.get("tax_base", "revenue") or "revenue"),
        )

    def as_dict(self) -> Dict[str, float | str]:
        mapping = asdict(self)
        mapping["tax_base"] = self.tax_base
        return mapping


@dataclass(frozen=True)
class LogisticTariffData:
    id: Optional[int]
    name: str
    base_first_l: float
    per_next_l: float
    active: bool = True

    @classmethod
    def from_payload(cls, payload: Dict[str, NumberLike | str]) -> "LogisticTariffData":
        raw_id = payload.get("id")
        tariff_id: Optional[int]
        try:
            tariff_id = int(raw_id) if raw_id is not None else None
        except (TypeError, ValueError):
            tariff_id = None
        return cls(
            id=tariff_id,
            name=str(payload.get("name") or ""),
            base_first_l=max(_to_float(payload.get("base_first_l"), 0.0), 0.0),
            per_next_l=max(_to_float(payload.get("per_next_l"), 0.0), 0.0),
            active=bool(payload.get("active", True)),
        )

    def as_dict(self) -> Dict[str, float | str | int | None | bool]:
        return {
            "id": self.id,
            "name": self.name,
            "base_first_l": self.base_first_l,
            "per_next_l": self.per_next_l,
            "active": self.active,
        }


@dataclass(frozen=True)
class LogisticBreakdown:
    volume_l: float
    tariff_cost: float
    forward_cost: float
    logistics_to: float
    logistics_back: float
    extra_liters: float
    steps: int

    def as_dict(self) -> Dict[str, float | int]:
        return {
            "volume_l": self.volume_l,
            "tariff_cost": self.tariff_cost,
            "forward_cost": self.forward_cost,
            "logistics_to": self.logistics_to,
            "logistics_back": self.logistics_back,
            "extra_liters": self.extra_liters,
            "steps": self.steps,
        }


@dataclass(frozen=True)
class UnitMetrics:
    price_final: float
    commission: float
    tax: float
    tax_base: TaxBase
    tax_rate: float
    cost_before_tax: float
    cost_total: float
    margin_before_tax: float
    margin: float
    margin_percent: float
    breakdown: Dict[str, float]

    def as_dict(self) -> Dict[str, float | str | Dict[str, float]]:
        return {
            "price_final": self.price_final,
            "commission": self.commission,
            "tax": self.tax,
            "tax_base": self.tax_base,
            "tax_rate": self.tax_rate,
            "cost_before_tax": self.cost_before_tax,
            "cost_total": self.cost_total,
            "margin_before_tax": self.margin_before_tax,
            "margin": self.margin,
            "margin_percent": self.margin_percent,
            "breakdown": self.breakdown,
        }


@dataclass(frozen=True)
class BatchMetrics:
    qty: float
    buyout_rate: float
    sold: float
    returns: float
    profit_sold: float
    cost_returns: float
    batch_profit: float
    roi: Optional[float]
    roi_percent: Optional[float]
    investment: float
    return_unit_cost: float

    def as_dict(self) -> Dict[str, float | Optional[float]]:
        return {
            "qty": self.qty,
            "buyout_rate": self.buyout_rate,
            "sold": self.sold,
            "returns": self.returns,
            "profit_sold": self.profit_sold,
            "cost_returns": self.cost_returns,
            "batch_profit": self.batch_profit,
            "roi": self.roi,
            "roi_percent": self.roi_percent,
            "investment": self.investment,
            "return_unit_cost": self.return_unit_cost,
        }


@dataclass(frozen=True)
class ProfitComputation:
    inputs: ProfitInput
    tariff: LogisticTariffData
    logistics: LogisticBreakdown
    unit: UnitMetrics
    batch: BatchMetrics

    def as_dict(self) -> Dict[str, object]:
        return {
            "inputs": self.inputs.as_dict(),
            "tariff": self.tariff.as_dict(),
            "logistics": self.logistics.as_dict(),
            "unit": self.unit.as_dict(),
            "batch": self.batch.as_dict(),
        }


def _normalize_tax_base(tax_base: str) -> TaxBase:
    normalized = (tax_base or "revenue").strip().lower()
    if normalized not in {"revenue", "profit", "none"}:
        return "revenue"
    return normalized


def calculate_logistic_cost(volume_l: float, *, base_first_l: float, per_next_l: float) -> LogisticBreakdown:
    safe_volume = max(volume_l, 0.0)
    extra_liters = max(safe_volume - 1.0, 0.0)
    steps = int(math.ceil(extra_liters)) if extra_liters > 0 else 0
    tariff_cost = max(base_first_l, 0.0)
    if steps > 0:
        tariff_cost += steps * max(per_next_l, 0.0)
    forward_cost = tariff_cost
    return LogisticBreakdown(
        volume_l=safe_volume,
        tariff_cost=tariff_cost,
        forward_cost=forward_cost,
        logistics_to=0.0,
        logistics_back=0.0,
        extra_liters=extra_liters,
        steps=steps,
    )


def calculate_profit(inputs: ProfitInput, tariff: LogisticTariffData) -> ProfitComputation:
    tax_base = _normalize_tax_base(inputs.tax_base)

    price_final = inputs.price_src * (1 - inputs.seller_discount / 100.0) * (1 - inputs.spp / 100.0)
    price_final = max(price_final, 0.0)

    logistic = calculate_logistic_cost(
        inputs.volume_l, base_first_l=tariff.base_first_l, per_next_l=tariff.per_next_l
    )
    logistic = replace(
        logistic,
        logistics_to=inputs.logistics_to,
        logistics_back=inputs.logistics_back,
        forward_cost=logistic.tariff_cost + inputs.logistics_to,
    )

    commission = price_final * (inputs.wb_fee / 100.0)
    commission = max(commission, 0.0)

    cost_components = {
        "product_cost": inputs.product_cost,
        "label": inputs.label,
        "package": inputs.package,
        "shipping": inputs.shipping,
        "storage": inputs.storage,
        "logistic_tariff": logistic.tariff_cost,
        "logistics_to": inputs.logistics_to,
        "commission": commission,
    }

    cost_before_tax = sum(max(value, 0.0) for value in cost_components.values())
    margin_before_tax = price_final - cost_before_tax

    tax_rate_fraction = inputs.tax_rate / 100.0
    if tax_base == "revenue":
        tax_amount = price_final * tax_rate_fraction
    elif tax_base == "profit":
        taxable = max(margin_before_tax, 0.0)
        tax_amount = taxable * tax_rate_fraction
    else:
        tax_amount = 0.0

    cost_components["tax"] = tax_amount
    cost_total = cost_before_tax + tax_amount
    margin = price_final - cost_total
    margin_percent = (margin / price_final * 100.0) if price_final else 0.0

    unit_metrics = UnitMetrics(
        price_final=price_final,
        commission=commission,
        tax=tax_amount,
        tax_base=tax_base,
        tax_rate=inputs.tax_rate,
        cost_before_tax=cost_before_tax,
        cost_total=cost_total,
        margin_before_tax=margin_before_tax,
        margin=margin,
        margin_percent=margin_percent,
        breakdown={key: max(value, 0.0) for key, value in cost_components.items()},
    )

    sold = inputs.qty * (inputs.buyout_rate / 100.0)
    returns = max(inputs.qty - sold, 0.0)

    profit_sold = sold * margin
    return_unit_cost = max(
        logistic.forward_cost + inputs.logistics_back + inputs.label + inputs.package,
        0.0,
    )
    cost_returns = returns * return_unit_cost
    batch_profit = profit_sold - cost_returns
    investment = inputs.qty * inputs.product_cost
    roi = batch_profit / investment if investment > 0 else None
    roi_percent = roi * 100.0 if roi is not None else None

    batch_metrics = BatchMetrics(
        qty=inputs.qty,
        buyout_rate=inputs.buyout_rate,
        sold=sold,
        returns=returns,
        profit_sold=profit_sold,
        cost_returns=cost_returns,
        batch_profit=batch_profit,
        roi=roi,
        roi_percent=roi_percent,
        investment=investment,
        return_unit_cost=return_unit_cost,
    )

    return ProfitComputation(
        inputs=inputs,
        tariff=tariff,
        logistics=logistic,
        unit=unit_metrics,
        batch=batch_metrics,
    )


def fetch_logistic_tariffs(session: Session, *, only_active: bool = True) -> List[LogisticTariffData]:
    stmt = select(LogisticTariff).order_by(LogisticTariff.name)
    if only_active:
        stmt = stmt.where(LogisticTariff.active.is_(True))
    tariffs = session.execute(stmt).scalars().all()
    result: List[LogisticTariffData] = []
    for tariff in tariffs:
        name_value = (tariff.name or "").strip()
        if not name_value:
            tariff_id = getattr(tariff, "id", None)
            name_value = f"Тариф #{tariff_id}" if tariff_id is not None else "Без названия"
        result.append(
            LogisticTariffData(
                id=getattr(tariff, "id", None),
                name=name_value,
                base_first_l=max(_to_float(getattr(tariff, "base_first_l", 0.0), 0.0), 0.0),
                per_next_l=max(_to_float(getattr(tariff, "per_next_l", 0.0), 0.0), 0.0),
                active=bool(getattr(tariff, "active", True)),
            )
        )
    return result


def scenario_to_dict(scenario: ProfitScenario) -> Dict[str, object]:
    def _iso(dt: Optional[datetime]) -> Optional[str]:
        if isinstance(dt, datetime):
            return dt.isoformat()
        return None

    inputs = scenario.inputs if isinstance(scenario.inputs, dict) else {}
    results = scenario.results if isinstance(scenario.results, dict) else {}
    return {
        "id": getattr(scenario, "id", None),
        "name": getattr(scenario, "name", ""),
        "description": getattr(scenario, "description", None),
        "inputs": dict(inputs),
        "results": dict(results),
        "created_at": _iso(getattr(scenario, "created_at", None)),
        "updated_at": _iso(getattr(scenario, "updated_at", None)),
    }


def fetch_profit_scenarios(session: Session, *, limit: Optional[int] = 50) -> List[ProfitScenario]:
    stmt = select(ProfitScenario).order_by(ProfitScenario.updated_at.desc().nullslast(), ProfitScenario.id.desc())
    if limit is not None:
        stmt = stmt.limit(limit)
    return session.execute(stmt).scalars().all()


def get_profit_scenario(session: Session, scenario_id: int) -> Optional[ProfitScenario]:
    stmt = select(ProfitScenario).where(ProfitScenario.id == scenario_id)
    return session.execute(stmt).scalar_one_or_none()


def save_profit_scenario(
    session: Session,
    *,
    name: str,
    computation: ProfitComputation,
    description: Optional[str] = None,
    scenario_id: Optional[int] = None,
) -> ProfitScenario:
    cleaned_name = (name or "").strip()
    if not cleaned_name:
        raise ValueError("Укажите название сценария")

    desc_value = (description or "").strip() or None

    data_inputs = computation.inputs.as_dict()
    data_results = {
        "unit": computation.unit.as_dict(),
        "batch": computation.batch.as_dict(),
        "logistics": computation.logistics.as_dict(),
        "tariff": computation.tariff.as_dict(),
    }

    if scenario_id is not None:
        stmt = select(ProfitScenario).where(ProfitScenario.id == scenario_id)
    else:
        stmt = select(ProfitScenario).where(ProfitScenario.name == cleaned_name)
    existing = session.execute(stmt).scalar_one_or_none()

    if existing:
        existing.name = cleaned_name
        existing.description = desc_value
        existing.inputs = data_inputs
        existing.results = data_results
        scenario = existing
    else:
        scenario = ProfitScenario(
            name=cleaned_name,
            description=desc_value,
            inputs=data_inputs,
            results=data_results,
        )
        session.add(scenario)

    session.flush()
    return scenario


def generate_price_sensitivity(
    inputs: ProfitInput,
    tariff: LogisticTariffData,
    price_points: Sequence[float],
) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    for price in price_points:
        scenario_inputs = replace(inputs, price_src=max(price, 0.0))
        computed = calculate_profit(scenario_inputs, tariff)
        results.append(
            {
                "price_src": scenario_inputs.price_src,
                "margin": computed.unit.margin,
                "margin_percent": computed.unit.margin_percent,
            }
        )
    return results


def generate_discount_sensitivity(
    inputs: ProfitInput,
    tariff: LogisticTariffData,
    discount_points: Iterable[float],
) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    for discount in discount_points:
        scenario_inputs = replace(inputs, seller_discount=max(discount, 0.0))
        computed = calculate_profit(scenario_inputs, tariff)
        results.append(
            {
                "seller_discount": scenario_inputs.seller_discount,
                "margin": computed.unit.margin,
                "margin_percent": computed.unit.margin_percent,
            }
        )
    return results


__all__ = [
    "ProfitInput",
    "LogisticTariffData",
    "LogisticBreakdown",
    "UnitMetrics",
    "BatchMetrics",
    "ProfitComputation",
    "calculate_logistic_cost",
    "calculate_profit",
    "fetch_logistic_tariffs",
    "scenario_to_dict",
    "fetch_profit_scenarios",
    "get_profit_scenario",
    "save_profit_scenario",
    "generate_price_sensitivity",
    "generate_discount_sensitivity",
]
