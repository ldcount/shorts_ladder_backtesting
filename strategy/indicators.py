from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd


@dataclass(slots=True)
class ATRSnapshot:
    symbol: str
    as_of_time: datetime
    period_days: int
    source_bar_open_time: datetime
    source_bar_close_time: datetime
    completed_bar_count: int
    atr_value: float

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "as_of_time": self.as_of_time.isoformat(),
            "period_days": self.period_days,
            "source_bar_open_time": self.source_bar_open_time.isoformat(),
            "source_bar_close_time": self.source_bar_close_time.isoformat(),
            "completed_bar_count": self.completed_bar_count,
            "atr_value": self.atr_value,
        }


def compute_atr_series(daily_candles: pd.DataFrame, *, period_days: int) -> pd.DataFrame:
    """Compute a Wilder-style ATR series from daily candles."""

    if daily_candles.empty:
        return daily_candles.copy()

    candles = daily_candles.sort_values("open_time").reset_index(drop=True).copy()
    previous_close = candles["close"].shift(1)
    true_range = pd.concat(
        [
            candles["high"] - candles["low"],
            (candles["high"] - previous_close).abs(),
            (candles["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    candles["true_range"] = true_range
    candles["atr"] = true_range.ewm(
        alpha=1 / period_days,
        adjust=False,
        min_periods=period_days,
    ).mean()
    return candles


def get_atr_as_of(
    daily_candles: pd.DataFrame,
    *,
    symbol: str,
    as_of_time: datetime,
    period_days: int,
) -> ATRSnapshot | None:
    """Return the latest ATR value known strictly from completed daily bars as of a timestamp."""

    as_of_time = _to_utc(as_of_time)
    if daily_candles.empty:
        return None

    candles = daily_candles.sort_values("open_time").reset_index(drop=True).copy()
    candle_close_times = candles["open_time"] + pd.Timedelta(days=1)
    completed = candles.loc[candle_close_times <= pd.Timestamp(as_of_time)].copy()
    if completed.empty:
        return None

    completed = compute_atr_series(completed, period_days=period_days)
    completed_with_atr = completed.loc[completed["atr"].notna()].copy()
    if completed_with_atr.empty:
        return None

    latest = completed_with_atr.iloc[-1]
    return ATRSnapshot(
        symbol=symbol,
        as_of_time=as_of_time,
        period_days=period_days,
        source_bar_open_time=latest["open_time"].to_pydatetime(),
        source_bar_close_time=(latest["open_time"] + pd.Timedelta(days=1)).to_pydatetime(),
        completed_bar_count=len(completed),
        atr_value=float(latest["atr"]),
    )


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)
