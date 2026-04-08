from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil

from data.bybit_client import BybitCandle
from data.data_loader import HistoricalDataLoader
from data.sqlite_cache import SqliteCandleCache


class FakeBybitClient:
    def __init__(self, candles: list[BybitCandle]) -> None:
        self._candles = candles
        self.calls: list[tuple[int, int, int]] = []

    def get_klines(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[BybitCandle]:
        self.calls.append((start_ms, end_ms, limit))
        matching = [
            candle
            for candle in self._candles
            if candle.symbol == symbol
            and candle.timeframe == timeframe
            and start_ms <= candle.open_time_ms <= end_ms
        ]
        return matching[:limit]


def _build_candle(symbol: str, timeframe: str, open_time: datetime, close_price: float) -> BybitCandle:
    return BybitCandle(
        symbol=symbol,
        timeframe=timeframe,
        open_time=open_time,
        open=close_price,
        high=close_price,
        low=close_price,
        close=close_price,
        volume=1.0,
        turnover=close_price,
    )


class HistoricalDataLoaderTests(unittest.TestCase):
    def test_load_candles_scans_forward_after_empty_initial_page_for_new_listing(self) -> None:
        symbol = "WCTUSDT"
        start_time = datetime(2025, 4, 6, 0, 0, tzinfo=timezone.utc)
        end_time = start_time + timedelta(minutes=30)
        listing_start = start_time + timedelta(minutes=10)
        candles = [
            _build_candle(symbol, "5m", listing_start + timedelta(minutes=offset), 1.0 + index)
            for index, offset in enumerate((0, 5, 10, 15), start=1)
        ]

        client = FakeBybitClient(candles)
        temp_dir = Path("outputs") / "test_tmp_data_loader"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            cache = SqliteCandleCache(temp_dir / "cache.sqlite3")
            loader = HistoricalDataLoader(
                bybit_client=client,
                candle_cache=cache,
                page_limit=2,
            )

            result = loader.load_candles(
                symbol=symbol,
                timeframe="5m",
                start_time=start_time,
                end_time=end_time,
            )
        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

        self.assertEqual(result.report.api_rows_fetched, 4)
        self.assertEqual(len(result.candles), 4)
        self.assertEqual(result.candles["open_time"].tolist()[0].to_pydatetime(), listing_start)
        self.assertEqual(result.candles["open_time"].tolist()[-1].to_pydatetime(), listing_start + timedelta(minutes=15))
        self.assertGreaterEqual(len(client.calls), 3)
        self.assertEqual(client.calls[0][0], int(start_time.timestamp() * 1000))


if __name__ == "__main__":
    unittest.main()
