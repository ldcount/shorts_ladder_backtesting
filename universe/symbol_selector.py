from __future__ import annotations

from dataclasses import dataclass

from config import AppConfig
from data.bybit_client import BybitClient
from universe.turnover_filter import TurnoverSelection, select_symbols_by_turnover


class UniverseSelectionError(ValueError):
    """Raised when a symbol universe cannot be selected from the current config."""


@dataclass(slots=True)
class UniverseSelectionResult:
    mode: str
    selected_symbols: list[str]
    eligible_symbol_count: int
    eligible_symbol_sample: list[str]
    requested_symbols: list[str] | None = None
    turnover_selection: TurnoverSelection | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "status": "resolved",
            "mode": self.mode,
            "selected_symbols": self.selected_symbols,
            "selected_symbol_count": len(self.selected_symbols),
            "eligible_symbol_count": self.eligible_symbol_count,
            "eligible_symbol_sample": self.eligible_symbol_sample,
        }
        if self.requested_symbols is not None:
            payload["requested_symbols"] = self.requested_symbols
        if self.turnover_selection is not None:
            payload["turnover_selection"] = self.turnover_selection.to_dict()
        return payload


class SymbolSelector:
    """Resolve the tradable universe for the configured backtest mode."""

    def __init__(self, bybit_client: BybitClient) -> None:
        self._bybit_client = bybit_client

    def select_symbols(self, config: AppConfig) -> UniverseSelectionResult:
        instruments = self._bybit_client.get_usdt_perpetual_instruments()
        eligible_symbols = [instrument.symbol for instrument in instruments]
        eligible_symbol_set = set(eligible_symbols)

        if config.backtest_mode == "fixed_symbols":
            return self._select_fixed_symbols(
                config=config,
                eligible_symbol_set=eligible_symbol_set,
                eligible_symbols=eligible_symbols,
            )

        if config.backtest_mode == "turnover_filter":
            ticker_snapshots = self._bybit_client.get_usdt_perpetual_tickers(instruments)
            turnover_selection = select_symbols_by_turnover(
                ticker_snapshots,
                range_start=config.range_start,
                range_finish=config.range_finish,
                max_symbols_per_day=config.max_symbols_per_day,
            )
            return UniverseSelectionResult(
                mode=config.backtest_mode,
                selected_symbols=turnover_selection.selected_symbols,
                eligible_symbol_count=len(eligible_symbols),
                eligible_symbol_sample=eligible_symbols[:10],
                turnover_selection=turnover_selection,
            )

        raise UniverseSelectionError(f"Unsupported backtest mode: {config.backtest_mode!r}.")

    def _select_fixed_symbols(
        self,
        *,
        config: AppConfig,
        eligible_symbol_set: set[str],
        eligible_symbols: list[str],
    ) -> UniverseSelectionResult:
        requested_symbols = _normalize_symbols(config.backtest_symbols)
        invalid_symbols = [symbol for symbol in requested_symbols if symbol not in eligible_symbol_set]

        if invalid_symbols:
            raise UniverseSelectionError(
                "The following configured symbols are not active Bybit USDT perpetual contracts: "
                f"{invalid_symbols}."
            )

        return UniverseSelectionResult(
            mode=config.backtest_mode,
            selected_symbols=requested_symbols,
            requested_symbols=requested_symbols,
            eligible_symbol_count=len(eligible_symbols),
            eligible_symbol_sample=eligible_symbols[:10],
        )


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        if not symbol or symbol in seen:
            continue
        normalized.append(symbol)
        seen.add(symbol)

    return normalized
