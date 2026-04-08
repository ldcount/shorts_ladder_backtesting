from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from backtest.engine import BacktestEngineResult
from backtest.metrics import (
    PerformanceMetrics,
    build_equity_curve_dataframe,
    build_symbol_stats_dataframe,
    build_trade_log_dataframe,
    compute_performance_metrics,
    format_holding_time,
)


@dataclass(slots=True)
class ReportBundle:
    trade_log: pd.DataFrame
    equity_curve: pd.DataFrame
    summary_csv: pd.DataFrame
    symbol_stats: pd.DataFrame
    metrics: PerformanceMetrics
    cli_summary: str
    trade_log_path: Path
    equity_curve_path: Path
    summary_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "trade_log_path": str(self.trade_log_path),
            "equity_curve_path": str(self.equity_curve_path),
            "summary_path": str(self.summary_path),
            "metrics": self.metrics.to_dict(),
        }


def generate_reports(
    *,
    engine_result: BacktestEngineResult,
    initial_capital: float,
    output_dir: Path,
) -> ReportBundle:
    """Build and write the required report files."""

    trade_log = build_trade_log_dataframe(engine_result)
    equity_curve = build_equity_curve_dataframe(engine_result, initial_capital=initial_capital)
    metrics = compute_performance_metrics(
        trade_log=trade_log,
        equity_curve=equity_curve,
        initial_capital=initial_capital,
    )
    symbol_stats = build_symbol_stats_dataframe(trade_log)
    summary_csv = build_summary_csv(metrics=metrics, symbol_stats=symbol_stats)

    output_dir.mkdir(parents=True, exist_ok=True)
    trade_log_path = output_dir / "trade_log.csv"
    equity_curve_path = output_dir / "equity_curve.csv"
    summary_path = output_dir / "summary.csv"

    trade_log.to_csv(trade_log_path, index=False)
    equity_curve.to_csv(equity_curve_path, index=False)
    summary_csv.to_csv(summary_path, index=False)

    return ReportBundle(
        trade_log=trade_log,
        equity_curve=equity_curve,
        summary_csv=summary_csv,
        symbol_stats=symbol_stats,
        metrics=metrics,
        cli_summary=format_cli_summary(metrics=metrics, symbol_stats=symbol_stats),
        trade_log_path=trade_log_path,
        equity_curve_path=equity_curve_path,
        summary_path=summary_path,
    )


def build_summary_csv(
    *,
    metrics: PerformanceMetrics,
    symbol_stats: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for metric_name, metric_value in metrics.to_dict().items():
        rows.append(
            {
                "section": "overall",
                "name": metric_name,
                "symbol": None,
                "value": metric_value,
                "exits": None,
                "net_pnl": None,
                "win_rate_pct": None,
            }
        )

    for _, row in symbol_stats.iterrows():
        rows.append(
            {
                "section": "symbol",
                "name": "symbol_stats",
                "symbol": row["symbol"],
                "value": None,
                "exits": int(row["exits"]),
                "net_pnl": float(row["net_pnl"]),
                "win_rate_pct": float(row["win_rate_pct"]),
            }
        )

    return pd.DataFrame(rows)


def format_cli_summary(*, metrics: PerformanceMetrics, symbol_stats: pd.DataFrame) -> str:
    symbol_lines = []
    for _, row in symbol_stats.sort_values("symbol").iterrows():
        symbol_lines.append(
            f"  {row['symbol']:<14} : {int(row['exits']):>4} exits | "
            f"Net PnL: {float(row['net_pnl']):>9.2f} USDT | WR: {float(row['win_rate_pct']):>6.2f}%"
        )

    summary_lines = [
        "=================================================================",
        f"BACKTEST PERFORMANCE SUMMARY",
        f"Total Net PnL    : {metrics.total_pnl:.2f} USDT",
        "=================================================================",
        f"Total Return     : {metrics.total_return_pct:.2f} %",
        f"Max Drawdown     : {metrics.max_drawdown_pct:.2f} %",
        f"Number of Exits  : {metrics.number_of_exits}",
        f"Win Rate         : {metrics.win_rate_pct:.2f} %",
        f"Avg Trade PnL    : {metrics.avg_trade_pnl:.2f} USDT",
        f"Profit Factor    : {metrics.profit_factor:.2f}" if metrics.profit_factor != float('inf') else "Profit Factor    : inf",
        f"Avg Holding Time : {format_holding_time(metrics.avg_holding_time_hours)}",
        "",
        "Symbol Level Stats:",
    ]
    summary_lines.extend(symbol_lines)
    summary_lines.append("=================================================================")
    summary_lines.append(
        f"OVERALL          : {metrics.number_of_exits:>4} exits | "
        f"Net PnL: {metrics.total_pnl:>9.2f} USDT | WR: {metrics.win_rate_pct:>6.2f}%"
    )
    return "\n".join(summary_lines)
