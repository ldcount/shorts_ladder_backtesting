from __future__ import annotations

import unittest
from datetime import datetime, timezone

from data.bybit_client import BybitExecution
from new import _derive_managed_short_opened_at


def _build_execution(
    *,
    order_link_id: str,
    side: str,
    exec_qty: float,
    exec_time: datetime,
) -> BybitExecution:
    return BybitExecution(
        symbol="ENJUSDT",
        order_id=f"order-{order_link_id}",
        order_link_id=order_link_id,
        side=side,
        exec_price=1.0,
        exec_qty=exec_qty,
        exec_time=exec_time,
        is_maker=side.lower() == "sell",
    )


class ManagedShortOpenTimeTests(unittest.TestCase):
    def test_derive_open_time_for_multi_fill_position(self) -> None:
        executions = [
            _build_execution(
                order_link_id="cxent_260408113000_0fa538_2",
                side="Sell",
                exec_qty=100.0,
                exec_time=datetime(2026, 4, 8, 16, 10, tzinfo=timezone.utc),
            ),
            _build_execution(
                order_link_id="cxent_260408113000_0fa538_1",
                side="Sell",
                exec_qty=50.0,
                exec_time=datetime(2026, 4, 8, 16, 5, tzinfo=timezone.utc),
            ),
        ]

        opened_at = _derive_managed_short_opened_at(
            executions=executions,
            current_position_size=150.0,
        )

        self.assertEqual(opened_at, datetime(2026, 4, 8, 16, 5, tzinfo=timezone.utc))

    def test_derive_open_time_ignores_closed_prior_cycle(self) -> None:
        executions = [
            _build_execution(
                order_link_id="cxent_260408100000_0fa538_2",
                side="Sell",
                exec_qty=60.0,
                exec_time=datetime(2026, 4, 8, 15, 5, tzinfo=timezone.utc),
            ),
            _build_execution(
                order_link_id="cxent_260408100000_0fa538_1",
                side="Sell",
                exec_qty=60.0,
                exec_time=datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc),
            ),
            _build_execution(
                order_link_id="cxtp2_260407180000_0fa538_tp2",
                side="Buy",
                exec_qty=100.0,
                exec_time=datetime(2026, 4, 7, 18, 0, tzinfo=timezone.utc),
            ),
            _build_execution(
                order_link_id="cxent_260407100000_0fa538_2",
                side="Sell",
                exec_qty=50.0,
                exec_time=datetime(2026, 4, 7, 10, 5, tzinfo=timezone.utc),
            ),
            _build_execution(
                order_link_id="cxent_260407100000_0fa538_1",
                side="Sell",
                exec_qty=50.0,
                exec_time=datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc),
            ),
        ]

        opened_at = _derive_managed_short_opened_at(
            executions=executions,
            current_position_size=120.0,
        )

        self.assertEqual(opened_at, datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
