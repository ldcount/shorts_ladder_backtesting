from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from backtest.execution import FillEvent
from backtest.portfolio import apply_short_exit, apply_short_fill, empty_short_position
from config import AppConfig
from core.models import DailySetupState, PendingOrder, PositionState
from data.data_loader import HistoricalDataLoader
from strategy.anchor import AnchorCalculationError, calculate_anchor_price
from strategy.entry_filter import evaluate_volume_filter
from strategy.exits import (
    ExitEvent,
    apply_buy_slippage,
    get_exit_thresholds,
    taker_fee_for_close,
    time_stop_deadline,
)
from strategy.indicators import get_atr_as_of
from strategy.ladder import LadderGenerationError, build_ladder


@dataclass(slots=True)
class DailyCheckEvent:
    symbol: str
    check_timestamp: datetime
    status: str
    canceled_pending_order_count: int
    new_order_count: int
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "check_timestamp": self.check_timestamp.isoformat(),
            "status": self.status,
            "canceled_pending_order_count": self.canceled_pending_order_count,
            "new_order_count": self.new_order_count,
            "message": self.message,
        }


@dataclass(slots=True)
class SymbolBacktestResult:
    symbol: str
    daily_check_count: int
    ladders_placed: int
    fills: list[FillEvent]
    exits: list[ExitEvent]
    daily_checks: list[DailyCheckEvent]
    final_position: PositionState
    remaining_orders: list[PendingOrder]

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "daily_check_count": self.daily_check_count,
            "ladders_placed": self.ladders_placed,
            "fill_count": len(self.fills),
            "exit_count": len(self.exits),
            "daily_checks": [event.to_dict() for event in self.daily_checks],
            "fills": [fill.to_dict() for fill in self.fills],
            "exits": [exit_event.to_dict() for exit_event in self.exits],
            "final_position": self.final_position.to_dict(),
            "remaining_orders": [order.to_dict() for order in self.remaining_orders],
        }


@dataclass(slots=True)
class BacktestEngineResult:
    start_time: datetime
    end_time: datetime
    symbol_results: list[SymbolBacktestResult]

    def to_dict(self) -> dict[str, object]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "symbol_count": len(self.symbol_results),
            "total_ladders_placed": sum(result.ladders_placed for result in self.symbol_results),
            "total_fills": sum(len(result.fills) for result in self.symbol_results),
            "total_exits": sum(len(result.exits) for result in self.symbol_results),
            "symbols": [result.to_dict() for result in self.symbol_results],
        }


class BacktestEngine:
    """Chronological day-by-day backtest loop for the current symbol universe."""

    def __init__(
        self,
        *,
        config: AppConfig,
        loader: HistoricalDataLoader,
        logger,
    ) -> None:
        self._config = config
        self._loader = loader
        self._logger = logger

    def run(self, *, selected_symbols: list[str]) -> BacktestEngineResult:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=self._config.history_in_days)
        symbol_results = [
            self._run_symbol(symbol=symbol, start_time=start_time, end_time=end_time)
            for symbol in selected_symbols
        ]
        return BacktestEngineResult(
            start_time=start_time,
            end_time=end_time,
            symbol_results=symbol_results,
        )

    def _run_symbol(
        self,
        *,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> SymbolBacktestResult:
        daily_candles = self._loader.load_candles(
            symbol=symbol,
            timeframe="1d",
            start_time=start_time,
            end_time=end_time,
        ).candles
        intraday_candles = self._loader.load_candles(
            symbol=symbol,
            timeframe=self._config.intraday_fill_timeframe,
            start_time=start_time,
            end_time=end_time,
        ).candles
        intraday_candles = intraday_candles.sort_values("open_time").reset_index(drop=True)

        position = empty_short_position(symbol)
        pending_orders: list[PendingOrder] = []
        fills: list[FillEvent] = []
        exits: list[ExitEvent] = []
        daily_checks: list[DailyCheckEvent] = []
        ladders_placed = 0
        tp1_available = True

        check_schedule = _build_check_schedule(
            start_time=start_time,
            end_time=end_time,
            check_hour_utc=self._config.check_hour_utc,
        )
        check_index = 0

        for _, candle in intraday_candles.iterrows():
            candle_open_time = candle["open_time"].to_pydatetime()
            candle_close_time = (candle["open_time"] + pd.Timedelta(minutes=5)).to_pydatetime()
            candle_high = float(candle["high"])
            candle_low = float(candle["low"])
            candle_close_price = float(candle["close"])

            while check_index < len(check_schedule) and check_schedule[check_index] <= candle_open_time:
                daily_check_event, pending_orders, ladders_added = self._process_daily_check(
                    symbol=symbol,
                    check_timestamp=check_schedule[check_index],
                    position=position,
                    pending_orders=pending_orders,
                    intraday_candles=intraday_candles,
                    daily_candles=daily_candles,
                )
                daily_checks.append(daily_check_event)
                ladders_placed += ladders_added
                check_index += 1

            fillable_orders = [
                order
                for order in pending_orders
                if order.status == "pending" and candle_high >= order.limit_price
            ]
            fillable_orders.sort(key=lambda order: order.level_index)
            if fillable_orders:
                tp1_available = True

            for order in fillable_orders:
                position = apply_short_fill(
                    position,
                    fill_timestamp=candle_open_time,
                    fill_price=order.limit_price,
                    fill_quantity=order.quantity,
                    order=order,
                )
                order.status = "filled"
                fills.append(
                    FillEvent(
                        symbol=order.symbol,
                        level_index=order.level_index,
                        fill_timestamp=candle_open_time,
                        ladder_check_timestamp=order.created_at,
                        candle_open_time=candle_open_time,
                        candle_close_time=candle_close_time,
                        anchor_price=order.anchor_price,
                        limit_price=order.limit_price,
                        fill_price=order.limit_price,
                        fill_quantity=order.quantity,
                        position_quantity_after_fill=position.quantity,
                        avg_entry_price_after_fill=float(position.avg_entry_price or 0.0),
                    )
                )

            if position.quantity <= 0 or position.avg_entry_price is None:
                continue

            thresholds = get_exit_thresholds(
                config=self._config,
                position=position,
                daily_candles=daily_candles,
                as_of_time=candle_open_time,
            )

            if candle_high >= thresholds.stop_price:
                position, event = _execute_exit(
                    config=self._config,
                    position=position,
                    reason="stop",
                    trigger_price=thresholds.stop_price,
                    candle_open_time=candle_open_time,
                    candle_close_time=candle_close_time,
                    execution_time=candle_open_time,
                    close_quantity=position.quantity,
                )
                exits.append(event)
                tp1_available = True
                continue

            if (
                self._config.enable_take_profit
                and tp1_available
                and thresholds.tp1_price is not None
                and candle_low <= thresholds.tp1_price
                and position.quantity > 0
            ):
                tp1_close_quantity = position.quantity * self._config.take_profit_1_close_fraction
                position, event = _execute_exit(
                    config=self._config,
                    position=position,
                    reason="tp1",
                    trigger_price=thresholds.tp1_price,
                    candle_open_time=candle_open_time,
                    candle_close_time=candle_close_time,
                    execution_time=candle_open_time,
                    close_quantity=tp1_close_quantity,
                )
                exits.append(event)
                tp1_available = False

            if (
                self._config.enable_take_profit
                and thresholds.tp2_price is not None
                and position.quantity > 0
                and candle_low <= thresholds.tp2_price
            ):
                position, event = _execute_exit(
                    config=self._config,
                    position=position,
                    reason="tp2",
                    trigger_price=thresholds.tp2_price,
                    candle_open_time=candle_open_time,
                    candle_close_time=candle_close_time,
                    execution_time=candle_open_time,
                    close_quantity=position.quantity,
                )
                exits.append(event)
                tp1_available = True
                continue

            deadline = time_stop_deadline(position, max_holding_days=self._config.max_holding_days)
            if deadline is not None and position.quantity > 0 and candle_close_time >= deadline:
                position, event = _execute_exit(
                    config=self._config,
                    position=position,
                    reason="time_stop",
                    trigger_price=candle_close_price,
                    candle_open_time=candle_open_time,
                    candle_close_time=candle_close_time,
                    execution_time=candle_close_time,
                    close_quantity=position.quantity,
                )
                exits.append(event)
                tp1_available = True

        while check_index < len(check_schedule):
            daily_check_event, pending_orders, ladders_added = self._process_daily_check(
                symbol=symbol,
                check_timestamp=check_schedule[check_index],
                position=position,
                pending_orders=pending_orders,
                intraday_candles=intraday_candles,
                daily_candles=daily_candles,
            )
            daily_checks.append(daily_check_event)
            ladders_placed += ladders_added
            check_index += 1

        remaining_orders = [order for order in pending_orders if order.status == "pending"]
        self._logger.info(
            "Engine completed %s: checks=%s ladders=%s fills=%s exits=%s remaining_orders=%s.",
            symbol,
            len(daily_checks),
            ladders_placed,
            len(fills),
            len(exits),
            len(remaining_orders),
        )
        return SymbolBacktestResult(
            symbol=symbol,
            daily_check_count=len(daily_checks),
            ladders_placed=ladders_placed,
            fills=fills,
            exits=exits,
            daily_checks=daily_checks,
            final_position=position,
            remaining_orders=remaining_orders,
        )

    def _process_daily_check(
        self,
        *,
        symbol: str,
        check_timestamp: datetime,
        position: PositionState,
        pending_orders: list[PendingOrder],
        intraday_candles: pd.DataFrame,
        daily_candles: pd.DataFrame,
    ) -> tuple[DailyCheckEvent, list[PendingOrder], int]:
        pending_orders = [order for order in pending_orders if order.status == "pending"]
        canceled_pending_order_count = 0

        if position.quantity <= 0:
            canceled_pending_order_count = len(pending_orders)
            pending_orders = []
        elif not self._config.allow_new_orders_if_position_open:
            return (
                DailyCheckEvent(
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    status="skipped_position_open",
                    canceled_pending_order_count=0,
                    new_order_count=0,
                    message="Position already open and config forbids adding a new ladder.",
                ),
                pending_orders,
                0,
            )

        try:
            anchor_snapshot = calculate_anchor_price(
                intraday_candles,
                symbol=symbol,
                check_timestamp=check_timestamp,
                timeframe=self._config.intraday_fill_timeframe,
            )
        except AnchorCalculationError as exc:
            return (
                DailyCheckEvent(
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    status="skipped_anchor_unavailable",
                    canceled_pending_order_count=canceled_pending_order_count,
                    new_order_count=0,
                    message=str(exc),
                ),
                pending_orders,
                0,
            )

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
            return (
                DailyCheckEvent(
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    status="skipped_volume_filter",
                    canceled_pending_order_count=canceled_pending_order_count,
                    new_order_count=0,
                    message=volume_filter_result.message,
                ),
                pending_orders,
                0,
            )

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
            return (
                DailyCheckEvent(
                    symbol=symbol,
                    check_timestamp=check_timestamp,
                    status="skipped_ladder_error",
                    canceled_pending_order_count=canceled_pending_order_count,
                    new_order_count=0,
                    message=str(exc),
                ),
                pending_orders,
                0,
            )

        if position.quantity > 0 and self._config.allow_new_orders_if_position_open:
            pending_orders = pending_orders + ladder_state.orders
            message = "Position open but config allows a fresh ladder to be added."
        else:
            pending_orders = ladder_state.orders
            message = "Placed a fresh daily ladder."

        return (
            DailyCheckEvent(
                symbol=symbol,
                check_timestamp=check_timestamp,
                status="placed_ladder",
                canceled_pending_order_count=canceled_pending_order_count,
                new_order_count=len(ladder_state.orders),
                message=message,
            ),
            pending_orders,
            1,
        )


def _build_check_schedule(
    *,
    start_time: datetime,
    end_time: datetime,
    check_hour_utc: int,
) -> list[datetime]:
    start_time = _to_utc(start_time)
    end_time = _to_utc(end_time)

    first_check = start_time.replace(hour=check_hour_utc, minute=0, second=0, microsecond=0)
    if first_check < start_time:
        first_check += timedelta(days=1)

    schedule: list[datetime] = []
    current = first_check
    while current <= end_time:
        schedule.append(current)
        current += timedelta(days=1)
    return schedule


def _execute_exit(
    *,
    config: AppConfig,
    position: PositionState,
    reason: str,
    trigger_price: float,
    candle_open_time: datetime,
    candle_close_time: datetime,
    execution_time: datetime,
    close_quantity: float,
) -> tuple[PositionState, ExitEvent]:
    avg_entry_before_exit = float(position.avg_entry_price or 0.0)
    executed_price = apply_buy_slippage(trigger_price, config.slippage_bps)
    update = apply_short_exit(
        position,
        exit_timestamp=execution_time,
        exit_price=executed_price,
        close_quantity=close_quantity,
    )
    fees = taker_fee_for_close(
        executed_price=executed_price,
        quantity=close_quantity,
        taker_fee_rate=config.taker_fee_rate,
    )
    realized_pnl = update.realized_pnl - fees
    event = ExitEvent(
        symbol=position.symbol,
        reason=reason,
        event_timestamp=execution_time,
        ladder_check_timestamp=position.ladder_check_timestamp,
        candle_open_time=candle_open_time,
        candle_close_time=candle_close_time,
        anchor_price=position.anchor_price,
        trigger_price=trigger_price,
        executed_price=executed_price,
        close_quantity=close_quantity,
        position_quantity_before_exit=position.quantity,
        remaining_quantity=update.position.quantity,
        avg_entry_before_exit=avg_entry_before_exit,
        opened_at_before_exit=position.opened_at,
        realized_pnl=realized_pnl,
        fees=fees,
        slippage_bps=config.slippage_bps,
    )
    return update.position, event


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)
