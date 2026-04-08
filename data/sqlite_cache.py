from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from data.bybit_client import BybitCandle


@dataclass(slots=True)
class CacheCoverage:
    row_count: int
    min_open_time: datetime | None
    max_open_time: datetime | None

    def to_dict(self) -> dict[str, object]:
        return {
            "row_count": self.row_count,
            "min_open_time": self.min_open_time.isoformat() if self.min_open_time else None,
            "max_open_time": self.max_open_time.isoformat() if self.max_open_time else None,
        }


class SqliteCandleCache:
    """SQLite-backed cache for historical OHLCV candles."""

    _TIMEFRAME_SUFFIX = {
        "5m": "5",
        "1d": "D",
    }

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def upsert_candles(self, candles: list[BybitCandle]) -> int:
        """Insert or replace candle rows and return the number written."""

        if not candles:
            return 0

        with closing(self._connect()) as connection:
            grouped_rows: dict[str, list[tuple[int, float, float, float, float, float, float]]] = {}
            for candle in candles:
                table_name = self._table_name(symbol=candle.symbol, timeframe=candle.timeframe)
                grouped_rows.setdefault(table_name, []).append(
                    (
                        candle.open_time_ms,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                        candle.turnover,
                    )
                )

            for table_name, rows in grouped_rows.items():
                self._ensure_table(connection, table_name)
                connection.executemany(
                    f"""
                    INSERT OR REPLACE INTO {self._quote_identifier(table_name)} (
                        open_time_ms,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        turnover
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            connection.commit()
        return len(candles)

    def load_candles(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
    ) -> pd.DataFrame:
        """Load cached candles in the half-open interval [start_ms, end_ms)."""

        normalized_symbol = symbol.upper()
        table_name = self._table_name(symbol=normalized_symbol, timeframe=timeframe)
        with closing(self._connect()) as connection:
            if not self._table_exists(connection, table_name):
                return self._empty_candle_frame()

            dataframe = pd.read_sql_query(
                f"""
                SELECT
                    open_time_ms,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    turnover
                FROM {self._quote_identifier(table_name)}
                WHERE open_time_ms >= ?
                  AND open_time_ms < ?
                ORDER BY open_time_ms ASC
                """,
                connection,
                params=(start_ms, end_ms),
            )

        if dataframe.empty:
            return self._empty_candle_frame()

        dataframe["open_time"] = pd.to_datetime(dataframe["open_time_ms"], unit="ms", utc=True)
        dataframe["symbol"] = normalized_symbol
        dataframe["timeframe"] = timeframe
        dataframe = dataframe.rename(
            columns={
                "open_price": "open",
                "high_price": "high",
                "low_price": "low",
                "close_price": "close",
            }
        )
        return dataframe[
            [
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
        ]

    def get_coverage(self, *, symbol: str, timeframe: str) -> CacheCoverage:
        """Return basic cached bounds for a symbol and timeframe."""

        table_name = self._table_name(symbol=symbol.upper(), timeframe=timeframe)
        with closing(self._connect()) as connection:
            if not self._table_exists(connection, table_name):
                return CacheCoverage(row_count=0, min_open_time=None, max_open_time=None)

            cursor = connection.execute(
                f"""
                SELECT COUNT(*), MIN(open_time_ms), MAX(open_time_ms)
                FROM {self._quote_identifier(table_name)}
                """,
            )
            row_count, min_open_time_ms, max_open_time_ms = cursor.fetchone()

        return CacheCoverage(
            row_count=int(row_count or 0),
            min_open_time=self._from_ms(min_open_time_ms),
            max_open_time=self._from_ms(max_open_time_ms),
        )

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    @staticmethod
    def _from_ms(value: int | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)

    @classmethod
    def _table_name(cls, *, symbol: str, timeframe: str) -> str:
        suffix = cls._TIMEFRAME_SUFFIX.get(timeframe)
        if suffix is None:
            raise ValueError(
                f"Unsupported timeframe {timeframe!r}. Supported values: {sorted(cls._TIMEFRAME_SUFFIX)}."
            )

        normalized_symbol = "".join(
            character if character.isalnum() else "_"
            for character in symbol.upper()
        ).strip("_")
        if not normalized_symbol:
            raise ValueError(f"Symbol {symbol!r} does not produce a valid sqlite table name.")

        return f"{normalized_symbol}_{suffix}"

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f'"{identifier}"'

    def _ensure_table(self, connection: sqlite3.Connection, table_name: str) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._quote_identifier(table_name)} (
                open_time_ms INTEGER PRIMARY KEY,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                turnover REAL NOT NULL
            )
            """
        )

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        cursor = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (table_name,),
        )
        return cursor.fetchone() is not None

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
