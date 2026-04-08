from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd


class AnchorCalculationError(RuntimeError):
    """Raised when the anchor price cannot be derived from intraday candles."""


@dataclass(slots=True)
class AnchorSnapshot:
    symbol: str
    check_timestamp: datetime
    candle_open_time: datetime
    candle_close_time: datetime
    anchor_price: float

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "check_timestamp": self.check_timestamp.isoformat(),
            "candle_open_time": self.candle_open_time.isoformat(),
            "candle_close_time": self.candle_close_time.isoformat(),
            "anchor_price": self.anchor_price,
        }


def latest_daily_check_timestamp(*, reference_time: datetime, check_hour_utc: int) -> datetime:
    """Return the most recent daily strategy check timestamp in UTC."""

    reference_time = _to_utc(reference_time)
    candidate = reference_time.replace(
        hour=check_hour_utc,
        minute=0,
        second=0,
        microsecond=0,
    )
    if candidate > reference_time:
        candidate -= timedelta(days=1)
    return candidate


def calculate_anchor_price(
    intraday_candles: pd.DataFrame,
    *,
    symbol: str,
    check_timestamp: datetime,
    timeframe: str = "5m",
) -> AnchorSnapshot:
    """Use the last fully completed intraday candle at the check time as the anchor price."""

    if timeframe != "5m":
        raise ValueError("Anchor calculation currently supports `5m` candles only.")

    check_timestamp = _to_utc(check_timestamp)
    expected_open_time = check_timestamp - timedelta(minutes=5)

    if intraday_candles.empty:
        raise AnchorCalculationError(f"No intraday candles available to calculate anchor for {symbol}.")

    candles = intraday_candles.sort_values("open_time").reset_index(drop=True)
    matching_rows = candles.loc[candles["open_time"] == pd.Timestamp(expected_open_time)]
    if matching_rows.empty:
        raise AnchorCalculationError(
            f"Missing the expected 5m candle for {symbol} at {expected_open_time.isoformat()}."
        )

    anchor_row = matching_rows.iloc[-1]
    return AnchorSnapshot(
        symbol=symbol,
        check_timestamp=check_timestamp,
        candle_open_time=anchor_row["open_time"].to_pydatetime(),
        candle_close_time=(anchor_row["open_time"] + pd.Timedelta(minutes=5)).to_pydatetime(),
        anchor_price=float(anchor_row["close"]),
    )


def _to_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)
