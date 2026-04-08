from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

import pandas as pd

from backtest.portfolio import apply_short_exit, apply_short_fill, empty_short_position
from config import AppConfig
from core.models import LadderState, PendingOrder, PositionState
from strategy.exits import (
    ExitEvent,
    apply_buy_slippage,
    get_exit_thresholds,
    taker_fee_for_close,
    time_stop_deadline,
)


@dataclass(slots=True)
class FillEvent:
    symbol: str
    level_index: int
    fill_timestamp: datetime
    ladder_check_timestamp: datetime
    candle_open_time: datetime
    candle_close_time: datetime
    anchor_price: float
    limit_price: float
    fill_price: float
    fill_quantity: float
    position_quantity_after_fill: float
    avg_entry_price_after_fill: float

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "level_index": self.level_index,
            "fill_timestamp": self.fill_timestamp.isoformat(),
            "ladder_check_timestamp": self.ladder_check_timestamp.isoformat(),
            "candle_open_time": self.candle_open_time.isoformat(),
            "candle_close_time": self.candle_close_time.isoformat(),
            "anchor_price": self.anchor_price,
            "limit_price": self.limit_price,
            "fill_price": self.fill_price,
            "fill_quantity": self.fill_quantity,
            "position_quantity_after_fill": self.position_quantity_after_fill,
            "avg_entry_price_after_fill": self.avg_entry_price_after_fill,
        }


@dataclass(slots=True)
class FillSimulationResult:
    symbol: str
    processed_candle_count: int
    pending_order_count: int
    filled_order_count: int
    fills: list[FillEvent]
    final_position: PositionState
    remaining_orders: list[PendingOrder]

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "processed_candle_count": self.processed_candle_count,
            "pending_order_count": self.pending_order_count,
            "filled_order_count": self.filled_order_count,
            "fill_count": len(self.fills),
            "fills": [fill.to_dict() for fill in self.fills],
            "final_position": self.final_position.to_dict(),
            "remaining_orders": [order.to_dict() for order in self.remaining_orders],
        }


@dataclass(slots=True)
class LifecycleSimulationResult:
    symbol: str
    processed_candle_count: int
    pending_order_count: int
    filled_order_count: int
    exit_count: int
    fills: list[FillEvent]
    exits: list[ExitEvent]
    final_position: PositionState
    remaining_orders: list[PendingOrder]

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "processed_candle_count": self.processed_candle_count,
            "pending_order_count": self.pending_order_count,
            "filled_order_count": self.filled_order_count,
            "exit_count": self.exit_count,
            "fills": [fill.to_dict() for fill in self.fills],
            "exits": [exit_event.to_dict() for exit_event in self.exits],
            "final_position": self.final_position.to_dict(),
            "remaining_orders": [order.to_dict() for order in self.remaining_orders],
        }


def simulate_short_limit_fills(
    *,
    ladder_state: LadderState,
    intraday_candles: pd.DataFrame,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    existing_position: PositionState | None = None,
) -> FillSimulationResult:
    """Replay intraday candles and fill short limit orders when candle highs cross their prices."""

    position = existing_position or empty_short_position(ladder_state.symbol)
    pending_orders = [replace(order) for order in ladder_state.orders]
    fills: list[FillEvent] = []

    filtered_candles = _filter_candles(
        intraday_candles=intraday_candles,
        start_time=start_time or ladder_state.check_timestamp,
        end_time=end_time,
    )

    for _, candle in filtered_candles.iterrows():
        candle_high = float(candle["high"])
        candle_open_time = candle["open_time"].to_pydatetime()
        candle_close_time = (candle["open_time"] + pd.Timedelta(minutes=5)).to_pydatetime()

        fillable_orders = [
            order
            for order in pending_orders
            if order.status == "pending" and candle_high >= order.limit_price
        ]
        fillable_orders.sort(key=lambda order: order.level_index)

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

    remaining_orders = [order for order in pending_orders if order.status == "pending"]
    return FillSimulationResult(
        symbol=ladder_state.symbol,
        processed_candle_count=len(filtered_candles),
        pending_order_count=len(ladder_state.orders),
        filled_order_count=len(fills),
        fills=fills,
        final_position=position,
        remaining_orders=remaining_orders,
    )


def simulate_short_ladder_lifecycle(
    *,
    config: AppConfig,
    ladder_state: LadderState,
    intraday_candles: pd.DataFrame,
    daily_candles: pd.DataFrame,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    existing_position: PositionState | None = None,
) -> LifecycleSimulationResult:
    """Replay fills and exit rules together on intraday candles."""

    position = existing_position or empty_short_position(ladder_state.symbol)
    pending_orders = [replace(order) for order in ladder_state.orders]
    fills: list[FillEvent] = []
    exits: list[ExitEvent] = []
    tp1_available = True

    filtered_candles = _filter_candles(
        intraday_candles=intraday_candles,
        start_time=start_time or ladder_state.check_timestamp,
        end_time=end_time,
    )

    for _, candle in filtered_candles.iterrows():
        candle_high = float(candle["high"])
        candle_low = float(candle["low"])
        candle_close_price = float(candle["close"])
        candle_open_time = candle["open_time"].to_pydatetime()
        candle_close_time = (candle["open_time"] + pd.Timedelta(minutes=5)).to_pydatetime()

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
            config=config,
            position=position,
            daily_candles=daily_candles,
            as_of_time=candle_open_time,
        )

        if candle_high >= thresholds.stop_price:
            position, event = _execute_exit(
                config=config,
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
            config.enable_take_profit
            and tp1_available
            and thresholds.tp1_price is not None
            and candle_low <= thresholds.tp1_price
            and position.quantity > 0
        ):
            tp1_close_quantity = position.quantity * config.take_profit_1_close_fraction
            position, event = _execute_exit(
                config=config,
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
            config.enable_take_profit
            and thresholds.tp2_price is not None
            and position.quantity > 0
            and candle_low <= thresholds.tp2_price
        ):
            position, event = _execute_exit(
                config=config,
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

        deadline = time_stop_deadline(position, max_holding_days=config.max_holding_days)
        if deadline is not None and position.quantity > 0 and candle_close_time >= deadline:
            position, event = _execute_exit(
                config=config,
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

    remaining_orders = [order for order in pending_orders if order.status == "pending"]
    return LifecycleSimulationResult(
        symbol=ladder_state.symbol,
        processed_candle_count=len(filtered_candles),
        pending_order_count=len(ladder_state.orders),
        filled_order_count=len(fills),
        exit_count=len(exits),
        fills=fills,
        exits=exits,
        final_position=position,
        remaining_orders=remaining_orders,
    )


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
    quantity_before_exit = position.quantity
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
        position_quantity_before_exit=quantity_before_exit,
        remaining_quantity=update.position.quantity,
        avg_entry_before_exit=avg_entry_before_exit,
        opened_at_before_exit=position.opened_at,
        realized_pnl=realized_pnl,
        fees=fees,
        slippage_bps=config.slippage_bps,
    )
    return update.position, event


def _filter_candles(
    *,
    intraday_candles: pd.DataFrame,
    start_time: datetime,
    end_time: datetime | None,
) -> pd.DataFrame:
    if intraday_candles.empty:
        return intraday_candles.copy()

    candles = intraday_candles.sort_values("open_time").reset_index(drop=True)
    mask = candles["open_time"] >= pd.Timestamp(start_time)
    if end_time is not None:
        mask &= candles["open_time"] < pd.Timestamp(end_time)
    return candles.loc[mask].copy()
