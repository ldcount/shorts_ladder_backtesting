from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from backtest.engine import BacktestEngineResult


@dataclass(slots=True)
class PerformanceMetrics:
    total_pnl: float
    total_return_pct: float
    max_drawdown_pct: float
    number_of_exits: int
    win_rate_pct: float
    avg_trade_pnl: float
    avg_holding_time_hours: float
    profit_factor: float
    avg_exposure_usdt: float
    max_exposure_usdt: float

    def to_dict(self) -> dict[str, float]:
        return {
            "total_pnl": self.total_pnl,
            "total_return_pct": self.total_return_pct,
            "max_drawdown_pct": self.max_drawdown_pct,
            "number_of_exits": self.number_of_exits,
            "win_rate_pct": self.win_rate_pct,
            "avg_trade_pnl": self.avg_trade_pnl,
            "avg_holding_time_hours": self.avg_holding_time_hours,
            "profit_factor": self.profit_factor,
            "avg_exposure_usdt": self.avg_exposure_usdt,
            "max_exposure_usdt": self.max_exposure_usdt,
        }


def build_trade_log_dataframe(engine_result: BacktestEngineResult) -> pd.DataFrame:
    """Build one trade-log row per exit event."""

    rows: list[dict[str, object]] = []

    for symbol_result in engine_result.symbol_results:
        fills = sorted(symbol_result.fills, key=lambda fill: (fill.fill_timestamp, fill.level_index))
        latest_fill = None
        fill_index = 0
        exits = sorted(symbol_result.exits, key=lambda exit_event: exit_event.event_timestamp)

        for exit_event in exits:
            while fill_index < len(fills) and fills[fill_index].fill_timestamp <= exit_event.event_timestamp:
                latest_fill = fills[fill_index]
                fill_index += 1

            rows.append(
                {
                    "symbol": exit_event.symbol,
                    "ladder_date": (
                        exit_event.ladder_check_timestamp.date().isoformat()
                        if exit_event.ladder_check_timestamp
                        else (
                            latest_fill.ladder_check_timestamp.date().isoformat()
                            if latest_fill is not None
                            else None
                        )
                    ),
                    "anchor_price": (
                        exit_event.anchor_price
                        if exit_event.anchor_price is not None
                        else (latest_fill.anchor_price if latest_fill is not None else None)
                    ),
                    "order_level": latest_fill.level_index if latest_fill is not None else None,
                    "fill_timestamp": (
                        latest_fill.fill_timestamp.isoformat() if latest_fill is not None else None
                    ),
                    "fill_price": latest_fill.fill_price if latest_fill is not None else None,
                    "fill_quantity": latest_fill.fill_quantity if latest_fill is not None else None,
                    "average_entry_after_fill": (
                        latest_fill.avg_entry_price_after_fill if latest_fill is not None else None
                    ),
                    "exit_timestamp": exit_event.event_timestamp.isoformat(),
                    "exit_price": exit_event.executed_price,
                    "exit_reason": exit_event.reason,
                    "close_quantity": exit_event.close_quantity,
                    "position_quantity_before_exit": exit_event.position_quantity_before_exit,
                    "remaining_quantity": exit_event.remaining_quantity,
                    "realized_pnl": exit_event.realized_pnl,
                    "fees": exit_event.fees,
                    "slippage_bps": exit_event.slippage_bps,
                    "holding_time_hours": (
                        (
                            exit_event.event_timestamp - exit_event.opened_at_before_exit
                        ).total_seconds()
                        / 3600
                        if exit_event.opened_at_before_exit is not None
                        else None
                    ),
                }
            )

    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "ladder_date",
                "anchor_price",
                "order_level",
                "fill_timestamp",
                "fill_price",
                "fill_quantity",
                "average_entry_after_fill",
                "exit_timestamp",
                "exit_price",
                "exit_reason",
                "close_quantity",
                "position_quantity_before_exit",
                "remaining_quantity",
                "realized_pnl",
                "fees",
                "slippage_bps",
                "holding_time_hours",
            ]
        )

    return dataframe.sort_values(["exit_timestamp", "symbol"]).reset_index(drop=True)


def build_equity_curve_dataframe(
    engine_result: BacktestEngineResult,
    *,
    initial_capital: float,
) -> pd.DataFrame:
    """Build an event-based realized equity curve plus exposure snapshots."""

    event_rows: list[dict[str, object]] = []
    for symbol_result in engine_result.symbol_results:
        for fill in symbol_result.fills:
            event_rows.append(
                {
                    "timestamp": fill.fill_timestamp,
                    "symbol": fill.symbol,
                    "event_type": "fill",
                    "position_quantity_after": fill.position_quantity_after_fill,
                    "avg_entry_after": fill.avg_entry_price_after_fill,
                    "realized_pnl_delta": 0.0,
                }
            )
        for exit_event in symbol_result.exits:
            remaining_avg_entry = (
                exit_event.avg_entry_before_exit if exit_event.remaining_quantity > 0 else None
            )
            event_rows.append(
                {
                    "timestamp": exit_event.event_timestamp,
                    "symbol": exit_event.symbol,
                    "event_type": exit_event.reason,
                    "position_quantity_after": exit_event.remaining_quantity,
                    "avg_entry_after": remaining_avg_entry,
                    "realized_pnl_delta": exit_event.realized_pnl,
                }
            )

    rows: list[dict[str, object]] = [
        {
            "timestamp": engine_result.start_time,
            "equity": initial_capital,
            "cumulative_realized_pnl": 0.0,
            "open_position_count": 0,
            "gross_exposure_usdt": 0.0,
            "event_type": "start",
            "symbol": None,
        }
    ]
    if not event_rows:
        dataframe = pd.DataFrame(rows)
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"], utc=True)
        return dataframe

    events = pd.DataFrame(event_rows).sort_values(["timestamp", "event_type", "symbol"]).reset_index(drop=True)
    symbol_state: dict[str, tuple[float, float | None]] = {}
    cumulative_realized_pnl = 0.0

    for _, event in events.iterrows():
        symbol = str(event["symbol"])
        symbol_state[symbol] = (
            float(event["position_quantity_after"]),
            float(event["avg_entry_after"]) if pd.notna(event["avg_entry_after"]) else None,
        )
        cumulative_realized_pnl += float(event["realized_pnl_delta"])
        gross_exposure_usdt = sum(
            quantity * avg_entry
            for quantity, avg_entry in symbol_state.values()
            if quantity > 0 and avg_entry is not None
        )
        open_position_count = sum(1 for quantity, _ in symbol_state.values() if quantity > 0)
        rows.append(
            {
                "timestamp": event["timestamp"],
                "equity": initial_capital + cumulative_realized_pnl,
                "cumulative_realized_pnl": cumulative_realized_pnl,
                "open_position_count": open_position_count,
                "gross_exposure_usdt": gross_exposure_usdt,
                "event_type": event["event_type"],
                "symbol": symbol,
            }
        )

    dataframe = pd.DataFrame(rows)
    dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"], utc=True)
    return dataframe


def compute_performance_metrics(
    *,
    trade_log: pd.DataFrame,
    equity_curve: pd.DataFrame,
    initial_capital: float,
) -> PerformanceMetrics:
    """Compute overall backtest metrics from trade log and realized equity curve."""

    total_pnl = float(trade_log["realized_pnl"].sum()) if not trade_log.empty else 0.0
    total_return_pct = (total_pnl / initial_capital * 100) if initial_capital else 0.0

    if equity_curve.empty:
        max_drawdown_pct = 0.0
        avg_exposure_usdt = 0.0
        max_exposure_usdt = 0.0
    else:
        running_peak = equity_curve["equity"].cummax()
        drawdown = (equity_curve["equity"] - running_peak) / running_peak.replace(0, pd.NA)
        max_drawdown_pct = abs(float(drawdown.min() * 100)) if not drawdown.empty else 0.0
        avg_exposure_usdt = float(equity_curve["gross_exposure_usdt"].mean())
        max_exposure_usdt = float(equity_curve["gross_exposure_usdt"].max())

    number_of_exits = int(len(trade_log))
    winning_exits = int((trade_log["realized_pnl"] > 0).sum()) if not trade_log.empty else 0
    win_rate_pct = (winning_exits / number_of_exits * 100) if number_of_exits else 0.0
    avg_trade_pnl = float(trade_log["realized_pnl"].mean()) if not trade_log.empty else 0.0
    avg_holding_time_hours = (
        float(trade_log["holding_time_hours"].dropna().mean())
        if not trade_log.empty and trade_log["holding_time_hours"].notna().any()
        else 0.0
    )

    gross_profit = (
        float(trade_log.loc[trade_log["realized_pnl"] > 0, "realized_pnl"].sum())
        if not trade_log.empty
        else 0.0
    )
    gross_loss = (
        abs(float(trade_log.loc[trade_log["realized_pnl"] < 0, "realized_pnl"].sum()))
        if not trade_log.empty
        else 0.0
    )
    if gross_loss == 0:
        profit_factor = float("inf") if gross_profit > 0 else 0.0
    else:
        profit_factor = gross_profit / gross_loss

    return PerformanceMetrics(
        total_pnl=total_pnl,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        number_of_exits=number_of_exits,
        win_rate_pct=win_rate_pct,
        avg_trade_pnl=avg_trade_pnl,
        avg_holding_time_hours=avg_holding_time_hours,
        profit_factor=profit_factor,
        avg_exposure_usdt=avg_exposure_usdt,
        max_exposure_usdt=max_exposure_usdt,
    )


def build_symbol_stats_dataframe(trade_log: pd.DataFrame) -> pd.DataFrame:
    """Aggregate exit-level performance by symbol."""

    if trade_log.empty:
        return pd.DataFrame(columns=["symbol", "exits", "net_pnl", "win_rate_pct"])

    grouped = trade_log.groupby("symbol", dropna=False)
    dataframe = grouped["realized_pnl"].agg(["count", "sum"]).rename(columns={"count": "exits", "sum": "net_pnl"})
    dataframe["win_rate_pct"] = grouped["realized_pnl"].apply(
        lambda values: (values.gt(0).sum() / len(values) * 100) if len(values) else 0.0
    )
    dataframe = dataframe.reset_index().sort_values("symbol").reset_index(drop=True)
    return dataframe


def format_holding_time(hours: float) -> str:
    total_minutes = int(round(hours * 60))
    days, remainder_minutes = divmod(total_minutes, 60 * 24)
    whole_hours, _ = divmod(remainder_minutes, 60)
    return f"{days} days {whole_hours} hours"
