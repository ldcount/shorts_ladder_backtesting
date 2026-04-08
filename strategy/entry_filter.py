from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd


@dataclass(slots=True)
class VolumeFilterResult:
    symbol: str
    check_timestamp: datetime
    enabled: bool
    passed: bool | None
    status: str
    lookback_days: int
    intraday_window_hours: int
    avg_daily_quote_turnover: float | None
    intraday_quote_turnover: float | None
    threshold_turnover: float | None
    completed_daily_candle_count: int
    intraday_candle_count: int
    message: str

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "check_timestamp": self.check_timestamp.isoformat(),
            "enabled": self.enabled,
            "passed": self.passed,
            "status": self.status,
            "lookback_days": self.lookback_days,
            "intraday_window_hours": self.intraday_window_hours,
            "avg_daily_quote_turnover": self.avg_daily_quote_turnover,
            "intraday_quote_turnover": self.intraday_quote_turnover,
            "threshold_turnover": self.threshold_turnover,
            "completed_daily_candle_count": self.completed_daily_candle_count,
            "intraday_candle_count": self.intraday_candle_count,
            "message": self.message,
        }


def evaluate_volume_filter(
    *,
    symbol: str,
    daily_candles: pd.DataFrame,
    intraday_candles: pd.DataFrame,
    check_timestamp: datetime,
    enabled: bool,
    lookback_days: int,
    intraday_window_hours: int,
    threshold_fraction_of_daily_avg: float,
) -> VolumeFilterResult:
    """Evaluate the abnormal-volume entry filter using quote turnover."""

    check_timestamp = _to_utc(check_timestamp)
    if not enabled:
        return VolumeFilterResult(
            symbol=symbol,
            check_timestamp=check_timestamp,
            enabled=False,
            passed=None,
            status="disabled",
            lookback_days=lookback_days,
            intraday_window_hours=intraday_window_hours,
            avg_daily_quote_turnover=None,
            intraday_quote_turnover=None,
            threshold_turnover=None,
            completed_daily_candle_count=0,
            intraday_candle_count=0,
            message="Volume filter disabled in config.",
        )

    completed_daily = _completed_daily_candles(daily_candles, check_timestamp=check_timestamp)
    if len(completed_daily) < lookback_days:
        return VolumeFilterResult(
            symbol=symbol,
            check_timestamp=check_timestamp,
            enabled=True,
            passed=False,
            status="insufficient_daily_history",
            lookback_days=lookback_days,
            intraday_window_hours=intraday_window_hours,
            avg_daily_quote_turnover=None,
            intraday_quote_turnover=None,
            threshold_turnover=None,
            completed_daily_candle_count=len(completed_daily),
            intraday_candle_count=0,
            message=(
                f"Need {lookback_days} completed daily candles for the volume filter, "
                f"found {len(completed_daily)}."
            ),
        )

    lookback_slice = completed_daily.tail(lookback_days)
    avg_daily_quote_turnover = float(lookback_slice["turnover"].mean())

    intraday_start = check_timestamp - timedelta(hours=intraday_window_hours)
    intraday_slice = _intraday_window(
        intraday_candles,
        window_start=intraday_start,
        check_timestamp=check_timestamp,
    )
    intraday_quote_turnover = float(intraday_slice["turnover"].sum()) if not intraday_slice.empty else 0.0
    threshold_turnover = avg_daily_quote_turnover * threshold_fraction_of_daily_avg
    passed = intraday_quote_turnover >= threshold_turnover

    return VolumeFilterResult(
        symbol=symbol,
        check_timestamp=check_timestamp,
        enabled=True,
        passed=passed,
        status="passed" if passed else "failed",
        lookback_days=lookback_days,
        intraday_window_hours=intraday_window_hours,
        avg_daily_quote_turnover=avg_daily_quote_turnover,
        intraday_quote_turnover=intraday_quote_turnover,
        threshold_turnover=threshold_turnover,
        completed_daily_candle_count=len(completed_daily),
        intraday_candle_count=len(intraday_slice),
        message=(
            "Intraday turnover meets the configured threshold."
            if passed
            else "Intraday turnover is below the configured threshold."
        ),
    )


def _completed_daily_candles(daily_candles: pd.DataFrame, *, check_timestamp: datetime) -> pd.DataFrame:
    if daily_candles.empty:
        return daily_candles.copy()

    candles = daily_candles.sort_values("open_time").reset_index(drop=True)
    close_times = candles["open_time"] + pd.Timedelta(days=1)
    return candles.loc[close_times <= pd.Timestamp(check_timestamp)].copy()


def _intraday_window(
    intraday_candles: pd.DataFrame,
    *,
    window_start: datetime,
    check_timestamp: datetime,
) -> pd.DataFrame:
    if intraday_candles.empty:
        return intraday_candles.copy()

    candles = intraday_candles.sort_values("open_time").reset_index(drop=True)
    open_times = candles["open_time"]
    return candles.loc[
        (open_times >= pd.Timestamp(window_start))
        & (open_times < pd.Timestamp(check_timestamp))
    ].copy()


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)
