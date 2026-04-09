from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock

from core.models import PositionState
from data.bybit_client import BybitClientError, BybitInstrumentSpec, BybitOpenOrder, BybitPositionSnapshot
from new import ProductionRunner


class StopLossManagementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = ProductionRunner.__new__(ProductionRunner)
        self.runner._config = SimpleNamespace(bybit_position_idx=0)
        self.runner._bybit_client = Mock()
        self.instrument_spec = BybitInstrumentSpec(
            symbol="ENJUSDT",
            tick_size=0.000001,
            qty_step=0.1,
            min_order_qty=1.0,
            min_notional_value=1.0,
        )

    def test_upsert_stop_loss_skips_api_when_stop_already_matches(self) -> None:
        actions: list[dict[str, object]] = []

        self.runner._upsert_stop_loss(
            symbol="ENJUSDT",
            stop_price=0.090999,
            current_stop_price=0.090999,
            instrument_spec=self.instrument_spec,
            actions=actions,
        )

        self.runner._bybit_client.set_trading_stop.assert_not_called()
        self.assertEqual(
            actions,
            [
                {
                    "symbol": "ENJUSDT",
                    "action": "keep_stop_loss",
                    "stop_price": 0.090999,
                }
            ],
        )

    def test_upsert_stop_loss_treats_not_modified_as_noop(self) -> None:
        self.runner._bybit_client.set_trading_stop.side_effect = BybitClientError(
            "Bybit API call `set_trading_stop` failed: not modified (ErrCode: 34040)"
        )
        actions: list[dict[str, object]] = []

        self.runner._upsert_stop_loss(
            symbol="ENJUSDT",
            stop_price=0.090999,
            current_stop_price=None,
            instrument_spec=self.instrument_spec,
            actions=actions,
        )

        self.runner._bybit_client.set_trading_stop.assert_called_once()
        self.assertEqual(
            actions,
            [
                {
                    "symbol": "ENJUSDT",
                    "action": "keep_stop_loss",
                    "stop_price": 0.090999,
                }
            ],
        )

    def test_upsert_stop_loss_re_raises_other_bybit_errors(self) -> None:
        self.runner._bybit_client.set_trading_stop.side_effect = BybitClientError("network issue")

        with self.assertRaises(BybitClientError):
            self.runner._upsert_stop_loss(
                symbol="ENJUSDT",
                stop_price=0.090999,
                current_stop_price=None,
                instrument_spec=self.instrument_spec,
                actions=[],
            )

    def test_monitor_skips_unmanaged_live_position_and_cancels_bot_exit_orders(self) -> None:
        with TemporaryDirectory() as temp_dir:
            runner = ProductionRunner.__new__(ProductionRunner)
            runner._config = SimpleNamespace(
                bybit_position_idx=0,
                history_in_days=365,
                max_holding_days=10,
                production_monitor_interval_seconds=30,
                output_dir=Path(temp_dir),
            )
            runner._runtime_state = {
                "symbols": {
                    "ENJUSDT": {
                        "anchor_price": 0.020591,
                        "check_timestamp": "2026-04-08T11:30:00+00:00",
                        "atr_value": 0.001,
                        "tp1_consumed": False,
                        "position_opened_at": None,
                        "position_opened_at_source": "unmanaged_live_position",
                        "last_position_size": 90000.0,
                        "last_time_stop_sent_at": None,
                    }
                }
            }
            runner._managed_symbols = {"ENJUSDT"}
            runner._state_path = Path(temp_dir) / "production_runtime_state.json"
            runner._save_runtime_state = Mock()
            runner._logger = Mock()
            runner._loader = Mock()
            runner._cancel_order = Mock()
            runner._get_live_short_position = Mock(
                return_value=BybitPositionSnapshot(
                    symbol="ENJUSDT",
                    side="Sell",
                    size=90000.0,
                    position_idx=0,
                    avg_entry_price=0.03295807,
                    created_time=None,
                    updated_time=None,
                    take_profit=None,
                    stop_loss=None,
                )
            )
            runner._build_live_position_state = Mock(
                return_value=PositionState(
                    symbol="ENJUSDT",
                    side="short",
                    quantity=90000.0,
                    avg_entry_price=0.03295807,
                    opened_at=None,
                    anchor_price=0.020591,
                    frozen_atr=0.001,
                    ladder_check_timestamp=None,
                )
            )
            runner._bybit_client = Mock()
            runner._bybit_client.get_open_orders.return_value = [
                BybitOpenOrder(
                    symbol="ENJUSDT",
                    order_id="order-1",
                    order_link_id="cxtp1_260409025614_0fa538_tp1",
                    side="Buy",
                    price=0.026614,
                    qty=37500.0,
                    reduce_only=True,
                    order_status="New",
                    position_idx=0,
                    created_time=datetime(2026, 4, 9, 16, 0, tzinfo=timezone.utc),
                )
            ]

            runner._monitor_open_positions(
                as_of_time=datetime(2026, 4, 9, 16, 40, tzinfo=timezone.utc)
            )

            runner._cancel_order.assert_called_once()
            _, kwargs = runner._cancel_order.call_args
            self.assertEqual(kwargs["reason"], "unmanaged_live_position")
            runner._loader.load_candles.assert_not_called()


if __name__ == "__main__":
    unittest.main()
