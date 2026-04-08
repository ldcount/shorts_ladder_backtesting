from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from config import AppConfig
from core.models import PositionState
from strategy.indicators import get_atr_as_of


class ExitRuleError(RuntimeError):
    """Raised when exit rules cannot be evaluated for an open position."""


@dataclass(slots=True)
class ExitThresholdSnapshot:
    symbol: str
    as_of_time: datetime
    stop_price: float
    tp1_price: float | None
    tp2_price: float | None
    stop_mode: str
    tp2_mode: str

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "as_of_time": self.as_of_time.isoformat(),
            "stop_price": self.stop_price,
            "tp1_price": self.tp1_price,
            "tp2_price": self.tp2_price,
            "stop_mode": self.stop_mode,
            "tp2_mode": self.tp2_mode,
        }


@dataclass(slots=True)
class ExitEvent:
    symbol: str
    reason: str
    event_timestamp: datetime
    ladder_check_timestamp: datetime | None
    candle_open_time: datetime
    candle_close_time: datetime
    anchor_price: float | None
    trigger_price: float
    executed_price: float
    close_quantity: float
    position_quantity_before_exit: float
    remaining_quantity: float
    avg_entry_before_exit: float
    opened_at_before_exit: datetime | None
    realized_pnl: float
    fees: float
    slippage_bps: float

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "reason": self.reason,
            "event_timestamp": self.event_timestamp.isoformat(),
            "ladder_check_timestamp": (
                self.ladder_check_timestamp.isoformat() if self.ladder_check_timestamp else None
            ),
            "candle_open_time": self.candle_open_time.isoformat(),
            "candle_close_time": self.candle_close_time.isoformat(),
            "anchor_price": self.anchor_price,
            "trigger_price": self.trigger_price,
            "executed_price": self.executed_price,
            "close_quantity": self.close_quantity,
            "position_quantity_before_exit": self.position_quantity_before_exit,
            "remaining_quantity": self.remaining_quantity,
            "avg_entry_before_exit": self.avg_entry_before_exit,
            "opened_at_before_exit": (
                self.opened_at_before_exit.isoformat() if self.opened_at_before_exit else None
            ),
            "realized_pnl": self.realized_pnl,
            "fees": self.fees,
            "slippage_bps": self.slippage_bps,
        }


def get_exit_thresholds(
    *,
    config: AppConfig,
    position: PositionState,
    daily_candles: pd.DataFrame,
    as_of_time: datetime,
) -> ExitThresholdSnapshot:
    """Calculate stop, TP1, and TP2 trigger prices using only information known at `as_of_time`."""

    as_of_time = _to_utc(as_of_time)
    if position.avg_entry_price is None or position.quantity <= 0:
        raise ExitRuleError(f"Cannot compute exit thresholds for {position.symbol} without an open position.")

    if config.stop_mode == "percent":
        stop_price = position.avg_entry_price * (1 + config.stop_percent_from_avg_entry)
    elif config.stop_mode == "atr":
        atr_snapshot = get_atr_as_of(
            daily_candles,
            symbol=position.symbol,
            as_of_time=as_of_time,
            period_days=config.atr_period_days,
        )
        if atr_snapshot is None:
            raise ExitRuleError(
                f"ATR stop requested for {position.symbol}, but no ATR is available as of {as_of_time.isoformat()}."
            )
        stop_price = position.avg_entry_price + (config.stop_atr_multiple * atr_snapshot.atr_value)
    else:
        raise ExitRuleError(f"Unsupported stop mode {config.stop_mode!r}.")

    tp1_price: float | None = None
    tp2_price: float | None = None
    if config.enable_take_profit:
        tp1_price = position.avg_entry_price * (1 - config.take_profit_1_percent)
        if config.take_profit_2_mode == "anchor_price":
            if position.anchor_price is None:
                raise ExitRuleError(
                    f"TP2 anchor-price mode requested for {position.symbol}, but no anchor is stored."
                )
            tp2_price = position.anchor_price
        elif config.take_profit_2_mode == "percent":
            tp2_price = position.avg_entry_price * (1 - config.take_profit_2_percent)
        else:
            raise ExitRuleError(f"Unsupported TP2 mode {config.take_profit_2_mode!r}.")

    return ExitThresholdSnapshot(
        symbol=position.symbol,
        as_of_time=as_of_time,
        stop_price=stop_price,
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        stop_mode=config.stop_mode,
        tp2_mode=config.take_profit_2_mode,
    )


def apply_buy_slippage(price: float, slippage_bps: float) -> float:
    """For buy-to-close executions, slippage worsens the fill upward."""

    return price * (1 + (slippage_bps / 10_000))


def taker_fee_for_close(*, executed_price: float, quantity: float, taker_fee_rate: float) -> float:
    return executed_price * quantity * taker_fee_rate


def time_stop_deadline(position: PositionState, *, max_holding_days: int) -> datetime | None:
    if position.opened_at is None:
        return None
    return position.opened_at + timedelta(days=max_holding_days)


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)
