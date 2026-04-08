from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd


class DataValidationError(RuntimeError):
    """Raised when candle data is structurally corrupted."""


@dataclass(slots=True)
class DataValidationReport:
    symbol: str
    timeframe: str
    requested_start_time: datetime
    requested_end_time: datetime
    input_row_count: int
    output_row_count: int
    duplicates_removed: int
    missing_candle_count: int
    missing_ranges: list[tuple[datetime, datetime]]
    invalid_ohlc_count: int
    invalid_ohlc_examples: list[dict[str, object]]
    status: str
    messages: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "requested_start_time": self.requested_start_time.isoformat(),
            "requested_end_time": self.requested_end_time.isoformat(),
            "input_row_count": self.input_row_count,
            "output_row_count": self.output_row_count,
            "duplicates_removed": self.duplicates_removed,
            "missing_candle_count": self.missing_candle_count,
            "missing_ranges": [
                {"start": start.isoformat(), "end": end.isoformat()}
                for start, end in self.missing_ranges
            ],
            "invalid_ohlc_count": self.invalid_ohlc_count,
            "invalid_ohlc_examples": self.invalid_ohlc_examples,
            "status": self.status,
            "messages": self.messages,
        }


@dataclass(slots=True)
class DataValidationResult:
    candles: pd.DataFrame
    report: DataValidationReport


def validate_candles(
    dataframe: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    requested_start_time: datetime,
    requested_end_time: datetime,
) -> DataValidationResult:
    """Sort candles, remove duplicates, detect gaps, and validate OHLC structure."""

    cleaned = dataframe.copy()
    input_row_count = len(cleaned)

    duplicates_removed = 0
    if cleaned.empty:
        cleaned = _empty_candle_frame()
    else:
        duplicates_removed = int(cleaned.duplicated(subset=["open_time"]).sum())
        cleaned = (
            cleaned.sort_values("open_time")
            .drop_duplicates(subset=["open_time"], keep="last")
            .reset_index(drop=True)
        )

    missing_ranges, missing_candle_count = _detect_missing_ranges(
        candles=cleaned,
        timeframe=timeframe,
        requested_start_time=requested_start_time,
        requested_end_time=requested_end_time,
    )
    invalid_ohlc_examples = _collect_invalid_ohlc_examples(cleaned)
    invalid_ohlc_count = len(invalid_ohlc_examples)

    messages: list[str] = []
    status = "ok"

    if duplicates_removed:
        status = "warning"
        messages.append(f"Removed {duplicates_removed} duplicate candles.")

    if missing_candle_count:
        status = "warning"
        messages.append(f"Detected {missing_candle_count} missing candles across the requested window.")

    if cleaned.empty:
        status = "warning"
        messages.append("No candles were available for the requested window.")

    if invalid_ohlc_count:
        status = "error"
        messages.append(f"Detected {invalid_ohlc_count} candles with invalid OHLC relationships.")

    report = DataValidationReport(
        symbol=symbol,
        timeframe=timeframe,
        requested_start_time=requested_start_time,
        requested_end_time=requested_end_time,
        input_row_count=input_row_count,
        output_row_count=len(cleaned),
        duplicates_removed=duplicates_removed,
        missing_candle_count=missing_candle_count,
        missing_ranges=missing_ranges,
        invalid_ohlc_count=invalid_ohlc_count,
        invalid_ohlc_examples=invalid_ohlc_examples[:5],
        status=status,
        messages=messages,
    )
    return DataValidationResult(candles=cleaned, report=report)


def _detect_missing_ranges(
    *,
    candles: pd.DataFrame,
    timeframe: str,
    requested_start_time: datetime,
    requested_end_time: datetime,
) -> tuple[list[tuple[datetime, datetime]], int]:
    step = _step_for(timeframe)
    expected_index = pd.date_range(
        start=pd.Timestamp(requested_start_time),
        end=pd.Timestamp(requested_end_time) - pd.Timedelta(step),
        freq=_pandas_freq_for(timeframe),
        tz="UTC",
    )

    if candles.empty:
        if expected_index.empty:
            return [], 0
        return [(expected_index[0].to_pydatetime(), (expected_index[-1] + step).to_pydatetime())], len(
            expected_index
        )

    actual_index = pd.DatetimeIndex(candles["open_time"])
    if actual_index.tz is None:
        actual_index = actual_index.tz_localize("UTC")
    else:
        actual_index = actual_index.tz_convert("UTC")

    missing_index = expected_index.difference(actual_index.unique().sort_values())
    if missing_index.empty:
        return [], 0

    ranges: list[tuple[datetime, datetime]] = []
    current_start = missing_index[0].to_pydatetime()
    previous = current_start

    for timestamp in missing_index[1:]:
        current = timestamp.to_pydatetime()
        if current - previous != step:
            ranges.append((current_start, previous + step))
            current_start = current
        previous = current

    ranges.append((current_start, previous + step))
    return ranges, len(missing_index)


def _collect_invalid_ohlc_examples(candles: pd.DataFrame) -> list[dict[str, object]]:
    if candles.empty:
        return []

    invalid_mask = (
        candles[["open", "high", "low", "close"]].isnull().any(axis=1)
        | (candles["open"] <= 0)
        | (candles["high"] <= 0)
        | (candles["low"] <= 0)
        | (candles["close"] <= 0)
        | (candles["low"] > candles["high"])
        | (candles["high"] < candles[["open", "close"]].max(axis=1))
        | (candles["low"] > candles[["open", "close"]].min(axis=1))
    )

    invalid_rows = candles.loc[invalid_mask, ["open_time", "open", "high", "low", "close"]]
    return [
        {
            "open_time": row["open_time"].isoformat(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
        for _, row in invalid_rows.iterrows()
    ]


def _step_for(timeframe: str) -> timedelta:
    if timeframe == "5m":
        return timedelta(minutes=5)
    if timeframe == "1d":
        return timedelta(days=1)
    raise ValueError(f"Unsupported timeframe {timeframe!r}.")


def _pandas_freq_for(timeframe: str) -> str:
    if timeframe == "5m":
        return "5min"
    if timeframe == "1d":
        return "1D"
    raise ValueError(f"Unsupported timeframe {timeframe!r}.")


def _empty_candle_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "timeframe",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
        ]
    )
