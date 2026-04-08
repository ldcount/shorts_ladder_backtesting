from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from data.bybit_client import BybitCandle, BybitClient
from data.sqlite_cache import CacheCoverage, SqliteCandleCache


@dataclass(slots=True)
class DataLoadReport:
    symbol: str
    timeframe: str
    start_time: datetime
    end_time: datetime
    requested_missing_ranges: list[tuple[datetime, datetime]]
    api_rows_fetched: int
    cache_rows_before_fetch: int
    cache_rows_after_fetch: int
    cache_coverage: CacheCoverage

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "requested_missing_ranges": [
                {"start": start.isoformat(), "end": end.isoformat()}
                for start, end in self.requested_missing_ranges
            ],
            "api_rows_fetched": self.api_rows_fetched,
            "cache_rows_before_fetch": self.cache_rows_before_fetch,
            "cache_rows_after_fetch": self.cache_rows_after_fetch,
            "cache_coverage": self.cache_coverage.to_dict(),
        }


@dataclass(slots=True)
class DataLoadResult:
    candles: pd.DataFrame
    report: DataLoadReport


class HistoricalDataLoader:
    """Fetch and cache Bybit historical klines for 5m and daily timeframes."""

    _TIMEFRAME_TO_STEP = {
        "5m": timedelta(minutes=5),
        "1d": timedelta(days=1),
    }

    def __init__(
        self,
        *,
        bybit_client: BybitClient,
        candle_cache: SqliteCandleCache,
        page_limit: int = 1000,
    ) -> None:
        self._bybit_client = bybit_client
        self._candle_cache = candle_cache
        self._page_limit = page_limit

    def load_candles(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        force_refresh: bool = False,
    ) -> DataLoadResult:
        """Load candles for [start_time, end_time) from cache plus any missing API pages."""

        normalized_symbol = symbol.upper()
        aligned_start = self._align_start(self._to_utc(start_time), timeframe)
        aligned_end = self._align_end(self._to_utc(end_time), timeframe)

        if aligned_end <= aligned_start:
            raise ValueError("`end_time` must be later than `start_time` after timeframe alignment.")

        start_ms = self._to_ms(aligned_start)
        end_ms = self._to_ms(aligned_end)

        cached_before = (
            self._empty_candle_frame()
            if force_refresh
            else self._candle_cache.load_candles(
                symbol=normalized_symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        )

        missing_ranges = self._build_missing_ranges(
            cached_open_times=cached_before["open_time"].tolist() if not cached_before.empty else [],
            timeframe=timeframe,
            start_time=aligned_start,
            end_time=aligned_end,
            force_refresh=force_refresh,
        )

        api_rows_fetched = 0
        for missing_start, missing_end in missing_ranges:
            fetched = self._fetch_range(
                symbol=normalized_symbol,
                timeframe=timeframe,
                start_time=missing_start,
                end_time=missing_end,
            )
            api_rows_fetched += len(fetched)
            self._candle_cache.upsert_candles(fetched)

        merged = self._candle_cache.load_candles(
            symbol=normalized_symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        merged = self._normalize_loaded_frame(merged)

        report = DataLoadReport(
            symbol=normalized_symbol,
            timeframe=timeframe,
            start_time=aligned_start,
            end_time=aligned_end,
            requested_missing_ranges=missing_ranges,
            api_rows_fetched=api_rows_fetched,
            cache_rows_before_fetch=len(cached_before),
            cache_rows_after_fetch=len(merged),
            cache_coverage=self._candle_cache.get_coverage(symbol=normalized_symbol, timeframe=timeframe),
        )
        return DataLoadResult(candles=merged, report=report)

    def _fetch_range(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[BybitCandle]:
        step = self._step_for(timeframe)
        step_ms = int(step.total_seconds() * 1000)
        current_start_ms = self._to_ms(start_time)
        end_ms_exclusive = self._to_ms(end_time)

        all_candles: dict[int, BybitCandle] = {}
        while current_start_ms < end_ms_exclusive:
            request_end_ms = min(
                end_ms_exclusive - step_ms,
                current_start_ms + step_ms * (self._page_limit - 1),
            )
            page = self._bybit_client.get_klines(
                symbol=symbol,
                timeframe=timeframe,
                start_ms=current_start_ms,
                end_ms=request_end_ms,
                limit=self._page_limit,
            )
            if not page:
                current_start_ms = request_end_ms + step_ms
                continue

            filtered_page = [
                candle
                for candle in page
                if current_start_ms <= candle.open_time_ms < end_ms_exclusive
            ]
            if not filtered_page:
                current_start_ms = request_end_ms + step_ms
                continue

            for candle in filtered_page:
                all_candles[candle.open_time_ms] = candle

            last_candle_open_ms = filtered_page[-1].open_time_ms
            if last_candle_open_ms < current_start_ms:
                raise RuntimeError("Kline pagination did not advance; aborting to avoid an infinite loop.")

            current_start_ms = last_candle_open_ms + step_ms

        return [all_candles[key] for key in sorted(all_candles)]

    def _build_missing_ranges(
        self,
        *,
        cached_open_times: list[pd.Timestamp],
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        force_refresh: bool,
    ) -> list[tuple[datetime, datetime]]:
        if force_refresh:
            return [(start_time, end_time)]

        expected_index = pd.date_range(
            start=pd.Timestamp(start_time),
            end=pd.Timestamp(end_time) - pd.Timedelta(self._step_for(timeframe)),
            freq=self._pandas_freq_for(timeframe),
            tz="UTC",
        )
        cached_index = pd.DatetimeIndex(cached_open_times)
        if cached_index.tz is None:
            cached_index = cached_index.tz_localize("UTC")
        else:
            cached_index = cached_index.tz_convert("UTC")

        missing_index = expected_index.difference(cached_index.unique().sort_values())
        if missing_index.empty:
            return []

        step = self._step_for(timeframe)
        ranges: list[tuple[datetime, datetime]] = []
        current_range_start = missing_index[0].to_pydatetime()
        previous_timestamp = current_range_start

        for timestamp in missing_index[1:]:
            current_timestamp = timestamp.to_pydatetime()
            if current_timestamp - previous_timestamp != step:
                ranges.append((current_range_start, previous_timestamp + step))
                current_range_start = current_timestamp
            previous_timestamp = current_timestamp

        ranges.append((current_range_start, previous_timestamp + step))
        return ranges

    def _normalize_loaded_frame(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe

        dataframe = dataframe.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last")
        dataframe = dataframe.reset_index(drop=True)
        return dataframe

    def _step_for(self, timeframe: str) -> timedelta:
        try:
            return self._TIMEFRAME_TO_STEP[timeframe]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported timeframe {timeframe!r}. Supported values: {sorted(self._TIMEFRAME_TO_STEP)}."
            ) from exc

    def _align_start(self, timestamp: datetime, timeframe: str) -> datetime:
        if timeframe == "5m":
            minute = timestamp.minute - (timestamp.minute % 5)
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        if timeframe == "1d":
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        raise ValueError(f"Unsupported timeframe {timeframe!r}.")

    def _align_end(self, timestamp: datetime, timeframe: str) -> datetime:
        aligned_start = self._align_start(timestamp, timeframe)
        if aligned_start == timestamp:
            return aligned_start
        return aligned_start + self._step_for(timeframe)

    @staticmethod
    def _to_utc(timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)

    @staticmethod
    def _to_ms(timestamp: datetime) -> int:
        return int(timestamp.timestamp() * 1000)

    @staticmethod
    def _pandas_freq_for(timeframe: str) -> str:
        if timeframe == "5m":
            return "5min"
        if timeframe == "1d":
            return "1D"
        raise ValueError(f"Unsupported timeframe {timeframe!r}.")

    @staticmethod
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
