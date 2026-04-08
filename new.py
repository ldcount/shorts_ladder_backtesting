from __future__ import annotations

import hashlib
import json
import signal
import time
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from typing import Any

from backtests import ensure_runtime_directories, sync_historical_data
from config import AppConfig, get_config
from core.config_verification import ConfigValidationError, validate_config
from core.logger import get_logger, setup_logging
from core.models import DailySetupState, PositionState
from data.bybit_client import (
    BybitClient,
    BybitClientError,
    BybitExecution,
    BybitInstrumentSpec,
    BybitOpenOrder,
    BybitPositionSnapshot,
)
from data.data_loader import HistoricalDataLoader
from data.sqlite_cache import SqliteCandleCache
from strategy.anchor import AnchorCalculationError, calculate_anchor_price
from strategy.entry_filter import evaluate_volume_filter
from strategy.exits import ExitRuleError, get_exit_thresholds, time_stop_deadline
from strategy.indicators import get_atr_as_of
from strategy.ladder import LadderGenerationError, build_ladder, ladder_preview
from universe.symbol_selector import SymbolSelector, UniverseSelectionError

LEGACY_ENTRY_ORDER_PREFIX = "cxlad_"
ENTRY_ORDER_PREFIX = "cxent_"
EXIT_TP1_ORDER_PREFIX = "cxtp1_"
EXIT_TP2_ORDER_PREFIX = "cxtp2_"
TIME_STOP_ORDER_PREFIX = "cxtime_"


class ProductionRunner:
    """Long-running production entrypoint suitable for a systemd service."""

    def __init__(self, *, config: AppConfig) -> None:
        self._config = config
        self._stop_requested = False
        self._logger = get_logger(__name__)
        self._bybit_client = BybitClient(
            testnet=config.bybit_testnet,
            demo=config.bybit_demo,
            api_key=config.bybit_api_key,
            api_secret=config.bybit_api_secret,
            log_requests=config.bybit_log_requests,
        )
        self._selector = SymbolSelector(self._bybit_client)
        self._loader = HistoricalDataLoader(
            bybit_client=self._bybit_client,
            candle_cache=SqliteCandleCache(config.cache_db_path),
        )
        self._instrument_specs: dict[str, BybitInstrumentSpec] = {}
        self._managed_symbols: set[str] = set()
        self._state_path = config.output_dir / "production_runtime_state.json"
        self._runtime_state = self._load_runtime_state()

        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, self._handle_stop_signal)

    def run(self) -> int:
        self._logger.info("Production service booting.")
        now_utc = datetime.now(timezone.utc)
        next_daily_update = _next_daily_update(
            reference_time=now_utc,
            check_hour_utc=self._config.check_hour_utc,
        )
        next_monitor_time = now_utc

        if self._config.production_startup_ladder_enabled:
            startup_check_timestamp = _latest_completed_5m_close(now_utc)
            self._run_cycle(
                trigger="startup",
                check_timestamp=startup_check_timestamp,
            )
            next_daily_update = _next_daily_update(
                reference_time=startup_check_timestamp + timedelta(seconds=1),
                check_hour_utc=self._config.check_hour_utc,
            )

        while not self._stop_requested:
            now_utc = datetime.now(timezone.utc)
            if now_utc >= next_monitor_time:
                self._monitor_open_positions(as_of_time=now_utc)
                next_monitor_time = now_utc + timedelta(
                    seconds=self._config.production_monitor_interval_seconds
                )

            now_utc = datetime.now(timezone.utc)
            if now_utc >= next_daily_update:
                self._run_cycle(
                    trigger="scheduled",
                    check_timestamp=next_daily_update,
                )
                next_daily_update = _next_daily_update(
                    reference_time=next_daily_update + timedelta(seconds=1),
                    check_hour_utc=self._config.check_hour_utc,
                )

            self._logger.info(
                "Next ladder refresh at %s, next position monitor at %s.",
                next_daily_update.isoformat(),
                next_monitor_time.isoformat(),
            )
            self._sleep_until(min(next_daily_update, next_monitor_time))

        self._save_runtime_state()
        self._logger.info("Production service stopped cleanly.")
        return 0

    def _run_cycle(self, *, trigger: str, check_timestamp: datetime) -> None:
        cycle_started_at = datetime.now(timezone.utc)
        resolved_universe = self._selector.select_symbols(self._config)
        self._managed_symbols = set(resolved_universe.selected_symbols)
        sync_report = sync_historical_data(
            config=self._config,
            selected_symbols=resolved_universe.selected_symbols,
            loader=self._loader,
            logger=self._logger,
        )
        ladder_plan = self._build_ladder_plan(
            selected_symbols=resolved_universe.selected_symbols,
            check_timestamp=check_timestamp,
        )
        self._update_runtime_state_from_ladder_plan(ladder_plan)
        entry_order_actions = self._synchronize_entry_orders(
            selected_symbols=resolved_universe.selected_symbols,
            ladder_plan=ladder_plan,
        )
        self._save_runtime_state()

        summary = {
            "mode": "production",
            "trigger": trigger,
            "cycle_started_at": cycle_started_at.isoformat(),
            "check_timestamp": check_timestamp.isoformat(),
            "universe_selection": resolved_universe.to_dict(),
            "data_sync": sync_report,
            "ladder_plan": ladder_plan,
            "entry_order_actions": entry_order_actions,
            "runtime_state_path": str(self._state_path),
            "next_scheduled_update": _next_daily_update(
                reference_time=check_timestamp + timedelta(seconds=1),
                check_hour_utc=self._config.check_hour_utc,
            ).isoformat(),
        }
        summary_path = self._config.output_dir / "production_cycle.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        self._logger.info(
            "Production %s cycle completed for %s symbols. Summary written to %s.",
            trigger,
            len(resolved_universe.selected_symbols),
            summary_path,
        )

    def _build_ladder_plan(
        self,
        *,
        selected_symbols: list[str],
        check_timestamp: datetime,
    ) -> dict[str, object]:
        history_start = datetime.now(timezone.utc) - timedelta(days=self._config.history_in_days)
        symbol_plans: list[dict[str, object]] = []

        for symbol in selected_symbols:
            daily_candles = self._loader.load_candles(
                symbol=symbol,
                timeframe="1d",
                start_time=history_start,
                end_time=check_timestamp,
            ).candles
            intraday_candles = self._loader.load_candles(
                symbol=symbol,
                timeframe=self._config.intraday_fill_timeframe,
                start_time=history_start,
                end_time=check_timestamp,
            ).candles

            try:
                anchor_snapshot = calculate_anchor_price(
                    intraday_candles,
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    timeframe=self._config.intraday_fill_timeframe,
                )
            except AnchorCalculationError as exc:
                symbol_plans.append(
                    {
                        "symbol": symbol,
                        "status": "skipped_anchor_unavailable",
                        "message": str(exc),
                    }
                )
                continue

            atr_snapshot = get_atr_as_of(
                daily_candles,
                symbol=symbol,
                as_of_time=check_timestamp,
                period_days=self._config.atr_period_days,
            )
            volume_filter_result = evaluate_volume_filter(
                symbol=symbol,
                daily_candles=daily_candles,
                intraday_candles=intraday_candles,
                check_timestamp=check_timestamp,
                enabled=self._config.enable_volume_filter,
                lookback_days=self._config.volume_filter_lookback_days,
                intraday_window_hours=self._config.volume_filter_intraday_window_hours,
                threshold_fraction_of_daily_avg=self._config.volume_filter_threshold_fraction_of_daily_avg,
            )

            if self._config.enable_volume_filter and not volume_filter_result.passed:
                symbol_plans.append(
                    {
                        "symbol": symbol,
                        "status": "skipped_volume_filter",
                        "message": volume_filter_result.message,
                        "volume_filter": volume_filter_result.to_dict(),
                    }
                )
                continue

            daily_setup = DailySetupState(
                symbol=symbol,
                check_timestamp=check_timestamp,
                anchor_price=anchor_snapshot.anchor_price,
                volume_filter_status=volume_filter_result.status,
                volume_filter_passed=volume_filter_result.passed,
                atr_value=atr_snapshot.atr_value if atr_snapshot is not None else None,
            )

            try:
                ladder_state = build_ladder(config=self._config, daily_setup=daily_setup)
            except LadderGenerationError as exc:
                symbol_plans.append(
                    {
                        "symbol": symbol,
                        "status": "skipped_ladder_error",
                        "message": str(exc),
                    }
                )
                continue

            symbol_plans.append(
                {
                    "symbol": symbol,
                    "status": "ready",
                    "anchor": anchor_snapshot.to_dict(),
                    "atr": atr_snapshot.to_dict() if atr_snapshot is not None else None,
                    "volume_filter": volume_filter_result.to_dict(),
                    "ladder": ladder_preview(ladder_state),
                }
            )

        return {
            "status": "completed",
            "check_timestamp": check_timestamp.isoformat(),
            "symbol_count": len(selected_symbols),
            "symbols": symbol_plans,
        }

    def _update_runtime_state_from_ladder_plan(self, ladder_plan: dict[str, object]) -> None:
        symbol_state_map = self._runtime_state.setdefault("symbols", {})
        for symbol_plan in ladder_plan["symbols"]:
            if symbol_plan.get("status") != "ready":
                continue

            symbol = str(symbol_plan["symbol"])
            state = symbol_state_map.setdefault(symbol, {})
            state["anchor_price"] = float(symbol_plan["ladder"]["anchor_price"])
            state["check_timestamp"] = str(symbol_plan["ladder"]["check_timestamp"])
            state["atr_value"] = (
                float(symbol_plan["atr"]["atr_value"])
                if symbol_plan.get("atr") is not None
                else None
            )
            if float(state.get("last_position_size", 0.0) or 0.0) <= 0:
                state["tp1_consumed"] = False
                state["position_opened_at"] = None
                state["last_time_stop_sent_at"] = None

    def _synchronize_entry_orders(
        self,
        *,
        selected_symbols: list[str],
        ladder_plan: dict[str, object],
    ) -> dict[str, object]:
        ready_by_symbol = {
            str(item["symbol"]): item
            for item in ladder_plan["symbols"]
            if item.get("status") == "ready"
        }
        selected_symbol_set = set(selected_symbols)
        managed_entry_orders = [
            order
            for order in self._bybit_client.get_open_orders(settle_coin="USDT")
            if _is_managed_entry_order(order.order_link_id)
        ]

        actions: list[dict[str, object]] = []
        cancelled_count = 0
        placed_count = 0
        orders_by_symbol: dict[str, list[BybitOpenOrder]] = {}
        for order in managed_entry_orders:
            orders_by_symbol.setdefault(order.symbol, []).append(order)

        for symbol, orders in orders_by_symbol.items():
            if symbol not in selected_symbol_set:
                for order in orders:
                    self._cancel_order(order, actions=actions, reason="symbol_not_selected")
                    cancelled_count += 1

        for symbol in selected_symbols:
            existing_orders = orders_by_symbol.get(symbol, [])
            live_position = self._get_live_short_position(symbol)
            if live_position is not None and not self._config.allow_new_orders_if_position_open:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "keep_existing_entries",
                        "message": "Position open and config keeps existing entry ladder active.",
                        "existing_entry_order_count": len(existing_orders),
                    }
                )
                continue

            for order in existing_orders:
                self._cancel_order(order, actions=actions, reason="daily_refresh")
                cancelled_count += 1

            symbol_plan = ready_by_symbol.get(symbol)
            if symbol_plan is None:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "skip_entry_place_not_ready",
                        "message": "No ready ladder plan available for this symbol.",
                    }
                )
                continue

            instrument_spec = self._get_instrument_spec(symbol)
            for level in symbol_plan["ladder"]["levels"]:
                normalized = self._normalize_order_level(
                    level=level,
                    instrument_spec=instrument_spec,
                )
                if normalized is None:
                    actions.append(
                        {
                            "symbol": symbol,
                            "action": "skip_entry_below_exchange_minimums",
                            "level_index": level["level_index"],
                        }
                    )
                    continue

                check_timestamp = datetime.fromisoformat(str(symbol_plan["ladder"]["check_timestamp"]))
                order_link_id = _build_order_link_id(
                    prefix=ENTRY_ORDER_PREFIX,
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    suffix=str(level["level_index"]),
                )
                submission = self._bybit_client.place_limit_order(
                    symbol=symbol,
                    side="Sell",
                    qty=_format_decimal(normalized["quantity"]),
                    price=_format_decimal(normalized["price"]),
                    order_link_id=order_link_id,
                    position_idx=self._config.bybit_position_idx,
                    reduce_only=False,
                    time_in_force="PostOnly",
                )
                placed_count += 1
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "place_entry",
                        "order_id": submission.order_id,
                        "order_link_id": submission.order_link_id,
                        "level_index": level["level_index"],
                        "price": normalized["price"],
                        "quantity": normalized["quantity"],
                    }
                )

        return {
            "status": "completed",
            "cancelled_order_count": cancelled_count,
            "placed_order_count": placed_count,
            "actions": actions,
        }

    def _monitor_open_positions(self, *, as_of_time: datetime) -> None:
        managed_open_orders = self._bybit_client.get_open_orders(settle_coin="USDT")
        managed_exit_orders = [
            order for order in managed_open_orders if _managed_exit_kind(order.order_link_id) is not None
        ]
        exit_orders_by_symbol: dict[str, list[BybitOpenOrder]] = {}
        for order in managed_exit_orders:
            exit_orders_by_symbol.setdefault(order.symbol, []).append(order)

        actions: list[dict[str, object]] = []
        symbol_state_map = self._runtime_state.setdefault("symbols", {})
        symbols_to_monitor = sorted(set(self._managed_symbols) | set(symbol_state_map.keys()))

        for symbol in symbols_to_monitor:
            state = symbol_state_map.setdefault(symbol, {})
            live_position = self._get_live_short_position(symbol)
            symbol_exit_orders = exit_orders_by_symbol.get(symbol, [])

            if live_position is None:
                for order in symbol_exit_orders:
                    self._cancel_order(order, actions=actions, reason="position_closed")
                state["last_position_size"] = 0.0
                state["tp1_consumed"] = False
                state["position_opened_at"] = None
                state["position_opened_at_source"] = None
                state["last_time_stop_sent_at"] = None
                continue

            position_state = self._build_live_position_state(
                symbol=symbol,
                live_position=live_position,
                state=state,
                as_of_time=as_of_time,
            )
            if position_state is None:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "skip_exit_management_no_context",
                        "message": "Live position exists but no anchor context is available.",
                    }
                )
                continue

            daily_candles = self._loader.load_candles(
                symbol=symbol,
                timeframe="1d",
                start_time=as_of_time - timedelta(days=self._config.history_in_days),
                end_time=as_of_time,
            ).candles

            deadline = time_stop_deadline(
                position_state,
                max_holding_days=self._config.max_holding_days,
            )
            if deadline is not None and as_of_time >= deadline:
                last_time_stop_sent_at = _parse_optional_iso_datetime(state.get("last_time_stop_sent_at"))
                if (
                    last_time_stop_sent_at is None
                    or (as_of_time - last_time_stop_sent_at).total_seconds()
                    >= self._config.production_monitor_interval_seconds
                ):
                    for order in symbol_exit_orders:
                        self._cancel_order(order, actions=actions, reason="time_stop_market_close")
                    market_close = self._submit_time_stop_close(
                        symbol=symbol,
                        position=live_position,
                        instrument_spec=self._get_instrument_spec(symbol),
                        as_of_time=as_of_time,
                    )
                    state["last_time_stop_sent_at"] = as_of_time.isoformat()
                    actions.append(market_close)
                continue

            try:
                thresholds = get_exit_thresholds(
                    config=self._config,
                    position=position_state,
                    daily_candles=daily_candles,
                    as_of_time=as_of_time,
                )
            except ExitRuleError as exc:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "skip_exit_thresholds",
                        "message": str(exc),
                    }
                )
                continue

            instrument_spec = self._get_instrument_spec(symbol)
            self._upsert_stop_loss(
                symbol=symbol,
                stop_price=thresholds.stop_price,
                instrument_spec=instrument_spec,
                actions=actions,
            )
            desired_exit_orders = self._build_desired_exit_orders(
                symbol=symbol,
                position=position_state,
                thresholds=thresholds,
                instrument_spec=instrument_spec,
                state=state,
                as_of_time=as_of_time,
            )
            self._sync_exit_orders(
                symbol=symbol,
                current_orders=symbol_exit_orders,
                desired_orders=desired_exit_orders,
                actions=actions,
            )

        self._runtime_state["last_monitor_run_at"] = as_of_time.isoformat()
        self._save_runtime_state()

        monitor_summary_path = self._config.output_dir / "production_monitor.json"
        monitor_summary_path.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "as_of_time": as_of_time.isoformat(),
                    "actions": actions,
                    "runtime_state_path": str(self._state_path),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def _build_live_position_state(
        self,
        *,
        symbol: str,
        live_position: BybitPositionSnapshot,
        state: dict[str, Any],
        as_of_time: datetime,
    ) -> PositionState | None:
        previous_size = float(state.get("last_position_size", 0.0) or 0.0)
        current_size = float(live_position.size)
        if current_size > previous_size + 1e-12:
            state["tp1_consumed"] = False
        elif previous_size > current_size > 0 and current_size < previous_size - 1e-12:
            state["tp1_consumed"] = True

        state["last_position_size"] = current_size
        if (
            state.get("position_opened_at") is None
            or state.get("position_opened_at_source") != "bot_execution_history"
        ):
            opened_at = self._derive_position_opened_at(
                symbol=symbol,
                current_position_size=current_size,
            )
            if opened_at is not None:
                state["position_opened_at"] = opened_at.isoformat()
                state["position_opened_at_source"] = "bot_execution_history"
            else:
                self._logger.warning(
                    "Could not derive bot-managed open time for %s at size=%s. "
                    "Time-stop management will stay disabled until a managed fill can be reconstructed.",
                    symbol,
                    current_size,
                )

        anchor_price = state.get("anchor_price")
        check_timestamp = _parse_optional_iso_datetime(state.get("check_timestamp"))
        if anchor_price is None or check_timestamp is None:
            return None

        return PositionState(
            symbol=symbol,
            side="short",
            quantity=current_size,
            avg_entry_price=live_position.avg_entry_price,
            opened_at=_parse_optional_iso_datetime(state.get("position_opened_at")),
            anchor_price=float(anchor_price),
            frozen_atr=float(state["atr_value"]) if state.get("atr_value") is not None else None,
            ladder_check_timestamp=check_timestamp,
        )

    def _upsert_stop_loss(
        self,
        *,
        symbol: str,
        stop_price: float,
        instrument_spec: BybitInstrumentSpec,
        actions: list[dict[str, object]],
    ) -> None:
        normalized_stop = _round_to_step(stop_price, instrument_spec.tick_size, rounding=ROUND_HALF_UP)
        self._bybit_client.set_trading_stop(
            symbol=symbol,
            position_idx=self._config.bybit_position_idx,
            stop_loss=_format_decimal(normalized_stop),
            take_profit="0",
        )
        actions.append(
            {
                "symbol": symbol,
                "action": "set_stop_loss",
                "stop_price": normalized_stop,
            }
        )

    def _build_desired_exit_orders(
        self,
        *,
        symbol: str,
        position: PositionState,
        thresholds,
        instrument_spec: BybitInstrumentSpec,
        state: dict[str, Any],
        as_of_time: datetime,
    ) -> list[dict[str, object]]:
        if not self._config.enable_take_profit or thresholds.tp2_price is None:
            return []

        current_qty = _round_to_step(position.quantity, instrument_spec.qty_step, rounding=ROUND_DOWN)
        if current_qty < instrument_spec.min_order_qty:
            return []

        tp1_consumed = bool(state.get("tp1_consumed", False))
        desired_orders: list[dict[str, object]] = []
        tp1_qty = 0.0

        if (
            not tp1_consumed
            and thresholds.tp1_price is not None
            and self._config.take_profit_1_close_fraction > 0
        ):
            tp1_qty = _round_to_step(
                current_qty * self._config.take_profit_1_close_fraction,
                instrument_spec.qty_step,
                rounding=ROUND_DOWN,
            )
            if tp1_qty >= instrument_spec.min_order_qty:
                desired_orders.append(
                    {
                        "kind": "tp1",
                        "price": _round_to_step(
                            thresholds.tp1_price,
                            instrument_spec.tick_size,
                            rounding=ROUND_HALF_UP,
                        ),
                        "quantity": tp1_qty,
                        "order_link_id": _build_order_link_id(
                            prefix=EXIT_TP1_ORDER_PREFIX,
                            symbol=symbol,
                            check_timestamp=as_of_time,
                            suffix="tp1",
                        ),
                    }
                )
            else:
                tp1_qty = 0.0

        tp2_qty = _round_to_step(
            current_qty - tp1_qty,
            instrument_spec.qty_step,
            rounding=ROUND_DOWN,
        )
        if tp2_qty >= instrument_spec.min_order_qty:
            desired_orders.append(
                {
                    "kind": "tp2",
                    "price": _round_to_step(
                        thresholds.tp2_price,
                        instrument_spec.tick_size,
                        rounding=ROUND_HALF_UP,
                    ),
                    "quantity": tp2_qty,
                    "order_link_id": _build_order_link_id(
                        prefix=EXIT_TP2_ORDER_PREFIX,
                        symbol=symbol,
                        check_timestamp=as_of_time,
                        suffix="tp2",
                    ),
                }
            )

        return desired_orders

    def _sync_exit_orders(
        self,
        *,
        symbol: str,
        current_orders: list[BybitOpenOrder],
        desired_orders: list[dict[str, object]],
        actions: list[dict[str, object]],
    ) -> None:
        current_by_kind: dict[str, list[BybitOpenOrder]] = {}
        for order in current_orders:
            kind = _managed_exit_kind(order.order_link_id)
            if kind is not None:
                current_by_kind.setdefault(kind, []).append(order)

        desired_by_kind = {str(order["kind"]): order for order in desired_orders}
        all_kinds = set(current_by_kind) | set(desired_by_kind)

        for kind in all_kinds:
            existing_orders = current_by_kind.get(kind, [])
            desired_order = desired_by_kind.get(kind)

            if desired_order is None:
                for order in existing_orders:
                    self._cancel_order(order, actions=actions, reason=f"remove_{kind}")
                continue

            matching_order = next(
                (
                    order
                    for order in existing_orders
                    if _floats_match(order.price, float(desired_order["price"]))
                    and _floats_match(order.qty, float(desired_order["quantity"]))
                ),
                None,
            )
            if matching_order is not None:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": f"keep_{kind}",
                        "order_id": matching_order.order_id,
                        "order_link_id": matching_order.order_link_id,
                        "price": matching_order.price,
                        "quantity": matching_order.qty,
                    }
                )
                for order in existing_orders:
                    if order.order_id != matching_order.order_id:
                        self._cancel_order(order, actions=actions, reason=f"dedupe_{kind}")
                continue

            for order in existing_orders:
                self._cancel_order(order, actions=actions, reason=f"refresh_{kind}")

            submission = self._bybit_client.place_limit_order(
                symbol=symbol,
                side="Buy",
                qty=_format_decimal(float(desired_order["quantity"])),
                price=_format_decimal(float(desired_order["price"])),
                order_link_id=str(desired_order["order_link_id"]),
                position_idx=self._config.bybit_position_idx,
                reduce_only=True,
                time_in_force="GTC",
            )
            actions.append(
                {
                    "symbol": symbol,
                    "action": f"place_{kind}",
                    "order_id": submission.order_id,
                    "order_link_id": submission.order_link_id,
                    "price": desired_order["price"],
                    "quantity": desired_order["quantity"],
                }
            )

    def _submit_time_stop_close(
        self,
        *,
        symbol: str,
        position: BybitPositionSnapshot,
        instrument_spec: BybitInstrumentSpec,
        as_of_time: datetime,
    ) -> dict[str, object]:
        normalized_quantity = _round_to_step(
            position.size,
            instrument_spec.qty_step,
            rounding=ROUND_DOWN,
        )
        submission = self._bybit_client.place_market_order(
            symbol=symbol,
            side="Buy",
            qty=_format_decimal(normalized_quantity),
            order_link_id=_build_order_link_id(
                prefix=TIME_STOP_ORDER_PREFIX,
                symbol=symbol,
                check_timestamp=as_of_time,
                suffix="close",
            ),
            position_idx=self._config.bybit_position_idx,
            reduce_only=True,
        )
        return {
            "symbol": symbol,
            "action": "place_time_stop_market_close",
            "order_id": submission.order_id,
            "order_link_id": submission.order_link_id,
            "quantity": normalized_quantity,
        }

    def _normalize_order_level(
        self,
        *,
        level: dict[str, object],
        instrument_spec: BybitInstrumentSpec,
    ) -> dict[str, float] | None:
        normalized_price = _round_to_step(
            float(level["limit_price"]),
            instrument_spec.tick_size,
            rounding=ROUND_HALF_UP,
        )
        normalized_quantity = _round_to_step(
            float(level["quantity"]),
            instrument_spec.qty_step,
            rounding=ROUND_DOWN,
        )
        if normalized_quantity < instrument_spec.min_order_qty:
            return None
        if normalized_price * normalized_quantity < instrument_spec.min_notional_value:
            return None
        return {
            "price": normalized_price,
            "quantity": normalized_quantity,
        }

    def _get_instrument_spec(self, symbol: str) -> BybitInstrumentSpec:
        instrument_spec = self._instrument_specs.get(symbol)
        if instrument_spec is None:
            instrument_spec = self._bybit_client.get_instrument_spec(symbol=symbol)
            self._instrument_specs[symbol] = instrument_spec
        return instrument_spec

    def _get_live_short_position(self, symbol: str) -> BybitPositionSnapshot | None:
        snapshots = self._bybit_client.get_position_snapshots(symbol=symbol)
        open_short_positions = [
            snapshot
            for snapshot in snapshots
            if snapshot.size > 0
            and snapshot.position_idx == self._config.bybit_position_idx
            and snapshot.side.lower() == "sell"
        ]
        if not open_short_positions:
            return None
        open_short_positions.sort(key=lambda snapshot: snapshot.size, reverse=True)
        return open_short_positions[0]

    def _derive_position_opened_at(
        self,
        *,
        symbol: str,
        current_position_size: float,
    ) -> datetime | None:
        if current_position_size <= 0:
            return None

        managed_executions: list[BybitExecution] = []
        cursor: str | None = None

        for _ in range(20):
            page, cursor = self._bybit_client.get_executions(
                symbol=symbol,
                limit=100,
                cursor=cursor,
            )
            managed_executions.extend(
                execution
                for execution in page
                if _is_managed_entry_order(execution.order_link_id)
                or _is_managed_exit_order(execution.order_link_id)
            )
            derived_opened_at = _derive_managed_short_opened_at(
                executions=managed_executions,
                current_position_size=current_position_size,
            )
            if derived_opened_at is not None:
                return derived_opened_at
            if not cursor:
                break

        return _derive_managed_short_opened_at(
            executions=managed_executions,
            current_position_size=current_position_size,
        )

    def _cancel_order(
        self,
        order: BybitOpenOrder,
        *,
        actions: list[dict[str, object]],
        reason: str,
    ) -> None:
        cancellation = self._bybit_client.cancel_order(
            symbol=order.symbol,
            order_id=order.order_id,
            order_link_id=order.order_link_id,
        )
        actions.append(
            {
                "symbol": order.symbol,
                "action": "cancel_order",
                "reason": reason,
                "order_id": cancellation.order_id,
                "order_link_id": cancellation.order_link_id,
            }
        )

    def _sleep_until(self, wake_time: datetime) -> None:
        while not self._stop_requested:
            remaining_seconds = (wake_time - datetime.now(timezone.utc)).total_seconds()
            if remaining_seconds <= 0:
                return
            time.sleep(min(remaining_seconds, 15))

    def _load_runtime_state(self) -> dict[str, object]:
        if not self._state_path.exists():
            return {"symbols": {}, "last_monitor_run_at": None}
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            self._logger.warning(
                "Could not load runtime state from %s. Starting fresh.",
                self._state_path,
            )
            return {"symbols": {}, "last_monitor_run_at": None}

    def _save_runtime_state(self) -> None:
        self._state_path.write_text(json.dumps(self._runtime_state, indent=2), encoding="utf-8")

    def _handle_stop_signal(self, signum: int, _frame) -> None:
        self._stop_requested = True
        self._logger.info("Received signal %s. Shutdown requested.", signum)


def _latest_completed_5m_close(reference_time: datetime) -> datetime:
    reference_time = _to_utc(reference_time)
    minute = reference_time.minute - (reference_time.minute % 5)
    return reference_time.replace(minute=minute, second=0, microsecond=0)


def _next_daily_update(*, reference_time: datetime, check_hour_utc: int) -> datetime:
    reference_time = _to_utc(reference_time)
    candidate = reference_time.replace(
        hour=check_hour_utc,
        minute=0,
        second=0,
        microsecond=0,
    )
    if candidate <= reference_time:
        candidate += timedelta(days=1)
    return candidate


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _build_order_link_id(
    *,
    prefix: str,
    symbol: str,
    check_timestamp: datetime,
    suffix: str,
) -> str:
    symbol_hash = hashlib.sha1(symbol.upper().encode("utf-8")).hexdigest()[:6]
    timestamp_token = check_timestamp.strftime("%y%m%d%H%M%S")
    return f"{prefix}{timestamp_token}_{symbol_hash}_{suffix}"


def _round_to_step(value: float, step: float, *, rounding: str) -> float:
    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    if step_decimal <= 0:
        raise ValueError(f"Step must be positive, got {step}.")
    rounded_units = (value_decimal / step_decimal).quantize(Decimal("1"), rounding=rounding)
    return float(rounded_units * step_decimal)


def _format_decimal(value: float) -> str:
    return format(Decimal(str(value)).normalize(), "f")


def _is_managed_entry_order(order_link_id: str) -> bool:
    return order_link_id.startswith(ENTRY_ORDER_PREFIX) or order_link_id.startswith(
        LEGACY_ENTRY_ORDER_PREFIX
    )


def _managed_exit_kind(order_link_id: str) -> str | None:
    if order_link_id.startswith(EXIT_TP1_ORDER_PREFIX):
        return "tp1"
    if order_link_id.startswith(EXIT_TP2_ORDER_PREFIX):
        return "tp2"
    return None


def _is_managed_exit_order(order_link_id: str) -> bool:
    return (
        _managed_exit_kind(order_link_id) is not None
        or order_link_id.startswith(TIME_STOP_ORDER_PREFIX)
    )


def _derive_managed_short_opened_at(
    *,
    executions: list[BybitExecution],
    current_position_size: float,
    tolerance: float = 1e-9,
) -> datetime | None:
    remaining_size = float(current_position_size)
    if remaining_size <= tolerance:
        return None

    ordered_executions = sorted(
        executions,
        key=lambda execution: execution.exec_time,
        reverse=True,
    )

    for execution in ordered_executions:
        side = execution.side.lower()
        if side == "buy" and _is_managed_exit_order(execution.order_link_id):
            remaining_size += execution.exec_qty
            continue

        if side == "sell" and _is_managed_entry_order(execution.order_link_id):
            remaining_size -= execution.exec_qty
            if remaining_size <= tolerance:
                return execution.exec_time

    return None


def _floats_match(left: float, right: float, tolerance: float = 1e-12) -> bool:
    return abs(left - right) <= tolerance


def _parse_optional_iso_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromisoformat(str(value))


def main() -> int:
    config = get_config()
    validate_config(config)
    if not config.bybit_api_key or not config.bybit_api_secret:
        raise SystemExit(
            "Production startup requires BYBIT_API_KEY and BYBIT_API_SECRET in the .env file."
        )

    ensure_runtime_directories(config)
    setup_logging(config.log_level, config.log_file_path)

    logger = get_logger(__name__)
    logger.info(
        "Production entrypoint starting with env file %s (testnet=%s, demo=%s).",
        config.env_file_path,
        config.bybit_testnet,
        config.bybit_demo,
    )

    runner = ProductionRunner(config=config)
    try:
        return runner.run()
    except (
        BybitClientError,
        UniverseSelectionError,
        ConfigValidationError,
        LadderGenerationError,
        ExitRuleError,
        ValueError,
        RuntimeError,
    ) as exc:
        logger.exception("Production startup failed.")
        raise SystemExit(f"Production startup failed: {exc}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
