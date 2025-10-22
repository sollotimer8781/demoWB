from __future__ import annotations

import unittest

from demowb.analytics import (
    LogisticTariffData,
    ProfitInput,
    calculate_logistic_cost,
    calculate_profit,
)


def _rounded(value: float, ndigits: int = 2) -> float:
    return round(float(value), ndigits)


class ProfitAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:  # noqa: D401 - part of unittest API
        """Prepare reusable fixtures."""
        self.default_tariff = LogisticTariffData(id=1, name="Стандарт", base_first_l=60.0, per_next_l=35.0)

    def test_calculate_profit_with_revenue_tax(self) -> None:
        inputs = ProfitInput(
            price_src=1000.0,
            seller_discount=10.0,
            spp=5.0,
            wb_fee=15.0,
            tax_rate=6.0,
            logistics_to=15.0,
            logistics_back=12.0,
            label=20.0,
            package=30.0,
            shipping=25.0,
            storage=10.0,
            product_cost=400.0,
            volume_l=1.7,
            qty=100.0,
            buyout_rate=70.0,
            tax_base="revenue",
        )

        result = calculate_profit(inputs, self.default_tariff)

        self.assertAlmostEqual(result.unit.price_final, 855.0, places=2)
        self.assertAlmostEqual(result.unit.commission, 128.25, places=2)
        self.assertAlmostEqual(result.unit.tax, 51.3, places=2)
        self.assertAlmostEqual(result.unit.cost_total, 774.55, places=2)
        self.assertAlmostEqual(result.unit.margin, 80.45, places=2)
        self.assertAlmostEqual(result.unit.margin_percent, 9.4093567, places=4)

        self.assertAlmostEqual(result.logistics.tariff_cost, 95.0, places=2)
        self.assertAlmostEqual(result.logistics.forward_cost, 110.0, places=2)

        self.assertAlmostEqual(result.batch.sold, 70.0, places=2)
        self.assertAlmostEqual(result.batch.returns, 30.0, places=2)
        self.assertAlmostEqual(result.batch.profit_sold, 5631.5, places=2)
        self.assertAlmostEqual(result.batch.cost_returns, 5160.0, places=2)
        self.assertAlmostEqual(result.batch.batch_profit, 471.5, places=2)
        self.assertIsNotNone(result.batch.roi)
        self.assertAlmostEqual(result.batch.roi or 0.0, 0.0117875, places=6)
        self.assertAlmostEqual(result.batch.roi_percent or 0.0, 1.17875, places=5)
        self.assertAlmostEqual(result.batch.return_unit_cost, 172.0, places=2)

    def test_calculate_profit_with_profit_tax(self) -> None:
        inputs = ProfitInput(
            price_src=1200.0,
            seller_discount=5.0,
            spp=5.0,
            wb_fee=10.0,
            tax_rate=7.0,
            logistics_to=20.0,
            logistics_back=10.0,
            label=15.0,
            package=25.0,
            shipping=20.0,
            storage=8.0,
            product_cost=350.0,
            volume_l=0.9,
            qty=80.0,
            buyout_rate=60.0,
            tax_base="profit",
        )

        result = calculate_profit(inputs, self.default_tariff)

        margin_before_tax = result.unit.margin_before_tax
        expected_tax = round(max(margin_before_tax, 0.0) * 0.07, 6)
        self.assertAlmostEqual(result.unit.tax, expected_tax, places=4)
        self.assertGreater(result.unit.margin, 0.0)
        self.assertGreater(result.unit.margin_percent, 0.0)
        self.assertAlmostEqual(result.logistics.tariff_cost, 60.0, places=2)
        self.assertAlmostEqual(result.logistics.forward_cost, 80.0, places=2)

    def test_profit_tax_not_applied_on_negative_margin(self) -> None:
        inputs = ProfitInput(
            price_src=500.0,
            seller_discount=0.0,
            spp=0.0,
            wb_fee=10.0,
            tax_rate=10.0,
            logistics_to=20.0,
            logistics_back=5.0,
            label=10.0,
            package=12.0,
            shipping=5.0,
            storage=3.0,
            product_cost=600.0,
            volume_l=1.2,
            qty=50.0,
            buyout_rate=50.0,
            tax_base="profit",
        )

        result = calculate_profit(inputs, self.default_tariff)
        self.assertLess(result.unit.margin_before_tax, 0.0)
        self.assertEqual(_rounded(result.unit.tax, 6), 0.0)
        self.assertLess(result.unit.margin, 0.0)

    def test_logistic_cost_steps(self) -> None:
        breakdown = calculate_logistic_cost(2.4, base_first_l=40.0, per_next_l=18.0)
        self.assertEqual(breakdown.steps, 2)
        self.assertAlmostEqual(breakdown.tariff_cost, 76.0, places=2)
        self.assertAlmostEqual(breakdown.forward_cost, 76.0, places=2)

        breakdown_small = calculate_logistic_cost(0.8, base_first_l=40.0, per_next_l=18.0)
        self.assertEqual(breakdown_small.steps, 0)
        self.assertAlmostEqual(breakdown_small.tariff_cost, 40.0, places=2)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
