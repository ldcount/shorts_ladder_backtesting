from __future__ import annotations

from dataclasses import dataclass

from data.bybit_client import BybitTickerSnapshot


class TurnoverFilterError(ValueError):
    """Raised when turnover-based ranking cannot be resolved."""


@dataclass(slots=True)
class RankedSymbol:
    rank: int
    symbol: str
    turnover_24h: float


@dataclass(slots=True)
class TurnoverSelection:
    range_start: int
    range_finish: int
    max_symbols_per_day: int
    total_ranked_symbols: int
    selected_symbols: list[str]
    ranked_slice: list[RankedSymbol]

    def to_dict(self) -> dict[str, object]:
        return {
            "range_start": self.range_start,
            "range_finish": self.range_finish,
            "max_symbols_per_day": self.max_symbols_per_day,
            "total_ranked_symbols": self.total_ranked_symbols,
            "selected_symbols": self.selected_symbols,
            "ranked_slice": [
                {
                    "rank": ranked.rank,
                    "symbol": ranked.symbol,
                    "turnover_24h": ranked.turnover_24h,
                }
                for ranked in self.ranked_slice
            ],
        }


def select_symbols_by_turnover(
    ticker_snapshots: list[BybitTickerSnapshot],
    *,
    range_start: int,
    range_finish: int,
    max_symbols_per_day: int,
) -> TurnoverSelection:
    """Rank symbols by ascending 24h turnover and return the selected window."""

    if range_start <= 0 or range_finish <= 0:
        raise TurnoverFilterError("`range_start` and `range_finish` must be 1-based positive values.")

    if range_start > range_finish:
        raise TurnoverFilterError("`range_start` must be less than or equal to `range_finish`.")

    if max_symbols_per_day <= 0:
        raise TurnoverFilterError("`max_symbols_per_day` must be greater than 0.")

    ranked_symbols = [
        RankedSymbol(rank=index, symbol=ticker.symbol, turnover_24h=ticker.turnover_24h)
        for index, ticker in enumerate(
            sorted(ticker_snapshots, key=lambda ticker: (ticker.turnover_24h, ticker.symbol)),
            start=1,
        )
    ]

    if not ranked_symbols:
        raise TurnoverFilterError("No eligible USDT perpetual tickers were returned by Bybit.")

    if range_start > len(ranked_symbols):
        raise TurnoverFilterError(
            f"`range_start` ({range_start}) exceeds the number of ranked symbols "
            f"({len(ranked_symbols)})."
        )

    slice_end = min(range_finish, len(ranked_symbols))
    ranked_slice = ranked_symbols[range_start - 1 : slice_end]
    ranked_slice = ranked_slice[:max_symbols_per_day]

    return TurnoverSelection(
        range_start=range_start,
        range_finish=range_finish,
        max_symbols_per_day=max_symbols_per_day,
        total_ranked_symbols=len(ranked_symbols),
        selected_symbols=[ranked.symbol for ranked in ranked_slice],
        ranked_slice=ranked_slice,
    )
