from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone

from backtest.engine import BacktestEngine, BacktestEngineResult
from backtest.execution import simulate_short_ladder_lifecycle, simulate_short_limit_fills
from backtest.reporting import ReportBundle, generate_reports
from config import AppConfig, get_config
from core.config_verification import validate_config
from core.logger import get_logger, setup_logging
from core.models import DailySetupState
from data.bybit_client import BybitClient, BybitClientError
from data.data_loader import HistoricalDataLoader
from data.data_validator import DataValidationError, validate_candles
from data.sqlite_cache import SqliteCandleCache
from strategy.anchor import AnchorCalculationError, calculate_anchor_price, latest_daily_check_timestamp
from strategy.entry_filter import evaluate_volume_filter
from strategy.exits import ExitRuleError
from strategy.indicators import get_atr_as_of
from strategy.ladder import LadderGenerationError, build_ladder, ladder_preview
from universe.symbol_selector import SymbolSelector, UniverseSelectionError


def ensure_runtime_directories(config: AppConfig) -> None:
    """Create the directories the bootstrap flow depends on."""

    for path in (config.data_dir, config.output_dir, config.logs_dir):
        path.mkdir(parents=True, exist_ok=True)


def build_runtime_summary(
    config: AppConfig,
    *,
    universe_selection: dict[str, object] | None = None,
    data_sync: dict[str, object] | None = None,
    daily_setup_preview: dict[str, object] | None = None,
    ladder_preview_summary: dict[str, object] | None = None,
    fill_replay_preview: dict[str, object] | None = None,
    exit_preview_summary: dict[str, object] | None = None,
    engine_preview_summary: dict[str, object] | None = None,
    reporting_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build a compact startup summary that is easy to inspect manually."""

    now_utc = datetime.now(timezone.utc)
    history_start = now_utc - timedelta(days=config.history_in_days)

    symbol_scope = {
        "mode": config.backtest_mode,
        "configured_symbols": config.backtest_symbols,
        "configured_symbol_count": len(config.backtest_symbols),
        "range_start": config.range_start,
        "range_finish": config.range_finish,
        "max_symbols_per_day": config.max_symbols_per_day,
    }

    return {
        "application": "Bybit USDT Perpetual Backtester",
        "bootstrap_status": "ready",
        "utc_now": now_utc.isoformat(),
        "history_window": {
            "start": history_start.isoformat(),
            "end": now_utc.isoformat(),
            "days": config.history_in_days,
        },
        "check_hour_utc": config.check_hour_utc,
        "initial_capital": config.initial_capital,
        "symbol_scope": symbol_scope,
        "universe_selection": universe_selection
        or {
            "status": "not_requested",
            "mode": config.backtest_mode,
        },
        "data_sync": data_sync
        or {
            "status": "not_requested",
        },
        "daily_setup_preview": daily_setup_preview
        or {
            "status": "not_requested",
        },
        "ladder_preview": ladder_preview_summary
        or {
            "status": "not_requested",
        },
        "fill_replay_preview": fill_replay_preview
        or {
            "status": "not_requested",
        },
        "exit_preview": exit_preview_summary
        or {
            "status": "not_requested",
        },
        "engine_preview": engine_preview_summary
        or {
            "status": "not_requested",
        },
        "reporting": reporting_summary
        or {
            "status": "not_requested",
        },
        "paths": {
            "project_root": str(config.project_root),
            "data_dir": str(config.data_dir),
            "output_dir": str(config.output_dir),
            "cache_db_path": str(config.cache_db_path),
            "log_file_path": str(config.log_file_path),
        },
    }


def print_startup_banner(summary: dict[str, object]) -> None:
    print("=" * 72)
    print("BYBIT USDT PERPETUAL BACKTESTER")
    print("=" * 72)
    print(json.dumps(summary, indent=2))


def print_cli_summary(report_bundle: ReportBundle) -> None:
    print()
    print(report_bundle.cli_summary)


def sync_historical_data(
    *,
    config: AppConfig,
    selected_symbols: list[str],
    loader: HistoricalDataLoader,
    logger,
) -> dict[str, object]:
    """Download and cache the historical windows required by the backtest."""

    now_utc = datetime.now(timezone.utc)
    history_start = now_utc - timedelta(days=config.history_in_days)
    timeframes = ["1d", config.intraday_fill_timeframe]
    sync_reports: list[dict[str, object]] = []
    total_api_rows_fetched = 0
    total_cache_rows_after_fetch = 0
    validation_warning_count = 0

    for symbol in selected_symbols:
        for timeframe in timeframes:
            logger.info(
                "Syncing %s candles for %s from %s to %s.",
                timeframe,
                symbol,
                history_start.isoformat(),
                now_utc.isoformat(),
            )
            result = loader.load_candles(
                symbol=symbol,
                timeframe=timeframe,
                start_time=history_start,
                end_time=now_utc,
            )
            validation_result = validate_candles(
                result.candles,
                symbol=symbol,
                timeframe=timeframe,
                requested_start_time=result.report.start_time,
                requested_end_time=result.report.end_time,
            )
            if validation_result.report.status == "error":
                raise DataValidationError(
                    f"Invalid OHLC data detected for {symbol} {timeframe}: "
                    f"{validation_result.report.messages}"
                )

            report = result.report.to_dict()
            report["loaded_row_count"] = len(validation_result.candles)
            report["validation"] = validation_result.report.to_dict()
            sync_reports.append(report)
            total_api_rows_fetched += result.report.api_rows_fetched
            total_cache_rows_after_fetch += result.report.cache_rows_after_fetch
            if validation_result.report.status == "warning":
                validation_warning_count += 1
                logger.warning(
                    "Validation warning for %s %s: %s",
                    symbol,
                    timeframe,
                    "; ".join(validation_result.report.messages),
                )
            logger.info(
                "Synced %s %s candles: loaded=%s, fetched=%s, missing_ranges=%s, validation=%s.",
                symbol,
                timeframe,
                len(validation_result.candles),
                result.report.api_rows_fetched,
                len(result.report.requested_missing_ranges),
                validation_result.report.status,
            )

    return {
        "status": "completed",
        "history_in_days": config.history_in_days,
        "timeframes": timeframes,
        "symbol_count": len(selected_symbols),
        "total_api_rows_fetched": total_api_rows_fetched,
        "total_cache_rows_after_fetch": total_cache_rows_after_fetch,
        "validation_warning_count": validation_warning_count,
        "reports": sync_reports,
    }


def build_daily_setup_preview(
    *,
    config: AppConfig,
    selected_symbols: list[str],
    loader: HistoricalDataLoader,
    logger,
) -> dict[str, object]:
    """Compute anchor, ATR, and volume-filter snapshots for the latest check time."""

    now_utc = datetime.now(timezone.utc)
    history_start = now_utc - timedelta(days=config.history_in_days)
    check_timestamp = latest_daily_check_timestamp(
        reference_time=now_utc,
        check_hour_utc=config.check_hour_utc,
    )

    previews: list[dict[str, object]] = []
    for symbol in selected_symbols:
        daily_result = loader.load_candles(
            symbol=symbol,
            timeframe="1d",
            start_time=history_start,
            end_time=now_utc,
        )
        intraday_result = loader.load_candles(
            symbol=symbol,
            timeframe=config.intraday_fill_timeframe,
            start_time=history_start,
            end_time=now_utc,
        )

        daily_candles = daily_result.candles
        intraday_candles = intraday_result.candles

        symbol_preview: dict[str, object] = {
            "symbol": symbol,
            "check_timestamp": check_timestamp.isoformat(),
        }

        try:
            anchor_snapshot = calculate_anchor_price(
                intraday_candles,
                symbol=symbol,
                check_timestamp=check_timestamp,
                timeframe=config.intraday_fill_timeframe,
            )
            symbol_preview["anchor"] = {
                "status": "ok",
                **anchor_snapshot.to_dict(),
            }
        except AnchorCalculationError as exc:
            symbol_preview["anchor"] = {
                "status": "unavailable",
                "error": str(exc),
            }
            logger.warning("Anchor preview unavailable for %s: %s", symbol, exc)

        atr_snapshot = get_atr_as_of(
            daily_candles,
            symbol=symbol,
            as_of_time=check_timestamp,
            period_days=config.atr_period_days,
        )
        symbol_preview["atr"] = (
            {"status": "ok", **atr_snapshot.to_dict()}
            if atr_snapshot is not None
            else {
                "status": "unavailable",
                "error": (
                    f"Need at least {config.atr_period_days} completed daily candles before "
                    f"{check_timestamp.isoformat()} to compute ATR."
                ),
            }
        )

        volume_filter_result = evaluate_volume_filter(
            symbol=symbol,
            daily_candles=daily_candles,
            intraday_candles=intraday_candles,
            check_timestamp=check_timestamp,
            enabled=config.enable_volume_filter,
            lookback_days=config.volume_filter_lookback_days,
            intraday_window_hours=config.volume_filter_intraday_window_hours,
            threshold_fraction_of_daily_avg=config.volume_filter_threshold_fraction_of_daily_avg,
        )
        symbol_preview["volume_filter"] = volume_filter_result.to_dict()
        previews.append(symbol_preview)

    return {
        "status": "completed",
        "check_timestamp": check_timestamp.isoformat(),
        "symbol_count": len(selected_symbols),
        "symbols": previews,
    }


def build_ladder_preview_summary(
    *,
    config: AppConfig,
    daily_setup_preview: dict[str, object],
    logger,
) -> dict[str, object]:
    """Build ladders from the daily setup preview for visual inspection."""

    previews: list[dict[str, object]] = []

    for symbol_preview in daily_setup_preview["symbols"]:
        symbol = str(symbol_preview["symbol"])
        anchor_data = symbol_preview["anchor"]
        atr_data = symbol_preview["atr"]
        volume_filter_data = symbol_preview["volume_filter"]

        if anchor_data.get("status") != "ok":
            previews.append(
                {
                    "symbol": symbol,
                    "status": "unavailable",
                    "error": "Anchor price unavailable, so ladder cannot be generated.",
                }
            )
            continue

        daily_setup = DailySetupState(
            symbol=symbol,
            check_timestamp=datetime.fromisoformat(str(symbol_preview["check_timestamp"])),
            anchor_price=float(anchor_data["anchor_price"]),
            volume_filter_status=str(volume_filter_data["status"]),
            volume_filter_passed=volume_filter_data["passed"],
            atr_value=float(atr_data["atr_value"]) if atr_data.get("status") == "ok" else None,
        )

        try:
            ladder_state = build_ladder(config=config, daily_setup=daily_setup)
            preview = {
                "status": "ok",
                **ladder_preview(ladder_state),
            }
        except LadderGenerationError as exc:
            logger.warning("Ladder preview unavailable for %s: %s", symbol, exc)
            preview = {
                "symbol": symbol,
                "status": "unavailable",
                "error": str(exc),
            }

        previews.append(preview)

    return {
        "status": "completed",
        "ladder_mode": config.ladder_mode,
        "symbol_count": len(previews),
        "symbols": previews,
    }


def build_fill_replay_preview(
    *,
    config: AppConfig,
    daily_setup_preview: dict[str, object],
    loader: HistoricalDataLoader,
    logger,
) -> dict[str, object]:
    """Replay the latest intraday candles against the current day's ladder for visual inspection."""

    now_utc = datetime.now(timezone.utc)
    previews: list[dict[str, object]] = []

    for symbol_preview in daily_setup_preview["symbols"]:
        symbol = str(symbol_preview["symbol"])
        anchor_data = symbol_preview["anchor"]
        atr_data = symbol_preview["atr"]
        volume_filter_data = symbol_preview["volume_filter"]
        check_timestamp = datetime.fromisoformat(str(symbol_preview["check_timestamp"]))

        if anchor_data.get("status") != "ok":
            previews.append(
                {
                    "symbol": symbol,
                    "status": "unavailable",
                    "error": "Anchor price unavailable, so fill replay cannot run.",
                }
            )
            continue

        daily_setup = DailySetupState(
            symbol=symbol,
            check_timestamp=check_timestamp,
            anchor_price=float(anchor_data["anchor_price"]),
            volume_filter_status=str(volume_filter_data["status"]),
            volume_filter_passed=volume_filter_data["passed"],
            atr_value=float(atr_data["atr_value"]) if atr_data.get("status") == "ok" else None,
        )
        ladder_state = build_ladder(config=config, daily_setup=daily_setup)
        intraday_result = loader.load_candles(
            symbol=symbol,
            timeframe=config.intraday_fill_timeframe,
            start_time=check_timestamp,
            end_time=now_utc,
        )
        simulation = simulate_short_limit_fills(
            ladder_state=ladder_state,
            intraday_candles=intraday_result.candles,
            start_time=check_timestamp,
            end_time=now_utc,
        )
        previews.append(
            {
                "status": "ok",
                "symbol": symbol,
                "check_timestamp": check_timestamp.isoformat(),
                "intraday_row_count": len(intraday_result.candles),
                "filled_order_count": simulation.filled_order_count,
                "remaining_order_count": len(simulation.remaining_orders),
                "fills": [fill.to_dict() for fill in simulation.fills],
                "final_position": simulation.final_position.to_dict(),
            }
        )
        logger.info(
            "Fill replay for %s processed %s candles and filled %s ladder levels.",
            symbol,
            simulation.processed_candle_count,
            simulation.filled_order_count,
        )

    return {
        "status": "completed",
        "symbol_count": len(previews),
        "symbols": previews,
    }


def build_exit_preview_summary(
    *,
    config: AppConfig,
    daily_setup_preview: dict[str, object],
    loader: HistoricalDataLoader,
    logger,
) -> dict[str, object]:
    """Run a combined fill-and-exit lifecycle replay for the latest daily setup."""

    now_utc = datetime.now(timezone.utc)
    history_start = now_utc - timedelta(days=config.history_in_days)
    previews: list[dict[str, object]] = []

    for symbol_preview in daily_setup_preview["symbols"]:
        symbol = str(symbol_preview["symbol"])
        anchor_data = symbol_preview["anchor"]
        atr_data = symbol_preview["atr"]
        volume_filter_data = symbol_preview["volume_filter"]
        check_timestamp = datetime.fromisoformat(str(symbol_preview["check_timestamp"]))

        if anchor_data.get("status") != "ok":
            previews.append(
                {
                    "symbol": symbol,
                    "status": "unavailable",
                    "error": "Anchor price unavailable, so exit replay cannot run.",
                }
            )
            continue

        daily_setup = DailySetupState(
            symbol=symbol,
            check_timestamp=check_timestamp,
            anchor_price=float(anchor_data["anchor_price"]),
            volume_filter_status=str(volume_filter_data["status"]),
            volume_filter_passed=volume_filter_data["passed"],
            atr_value=float(atr_data["atr_value"]) if atr_data.get("status") == "ok" else None,
        )
        ladder_state = build_ladder(config=config, daily_setup=daily_setup)
        daily_result = loader.load_candles(
            symbol=symbol,
            timeframe="1d",
            start_time=history_start,
            end_time=now_utc,
        )
        intraday_result = loader.load_candles(
            symbol=symbol,
            timeframe=config.intraday_fill_timeframe,
            start_time=check_timestamp,
            end_time=now_utc,
        )
        simulation = simulate_short_ladder_lifecycle(
            config=config,
            ladder_state=ladder_state,
            intraday_candles=intraday_result.candles,
            daily_candles=daily_result.candles,
            start_time=check_timestamp,
            end_time=now_utc,
        )
        previews.append(
            {
                "status": "ok",
                "symbol": symbol,
                "check_timestamp": check_timestamp.isoformat(),
                "fill_count": len(simulation.fills),
                "exit_count": len(simulation.exits),
                "fills": [fill.to_dict() for fill in simulation.fills],
                "exits": [exit_event.to_dict() for exit_event in simulation.exits],
                "final_position": simulation.final_position.to_dict(),
            }
        )
        logger.info(
            "Exit replay for %s produced %s fills and %s exits.",
            symbol,
            len(simulation.fills),
            len(simulation.exits),
        )

    return {
        "status": "completed",
        "symbol_count": len(previews),
        "symbols": previews,
    }


def build_engine_preview_summary(engine_result: BacktestEngineResult) -> dict[str, object]:
    """Compress the full engine result into a readable startup summary."""

    symbol_summaries: list[dict[str, object]] = []
    for symbol_result in engine_result.symbol_results:
        check_status_counts = Counter(event.status for event in symbol_result.daily_checks)
        exit_reason_counts = Counter(event.reason for event in symbol_result.exits)
        symbol_summaries.append(
            {
                "symbol": symbol_result.symbol,
                "daily_check_count": symbol_result.daily_check_count,
                "ladders_placed": symbol_result.ladders_placed,
                "fill_count": len(symbol_result.fills),
                "exit_count": len(symbol_result.exits),
                "remaining_order_count": len(symbol_result.remaining_orders),
                "final_position": symbol_result.final_position.to_dict(),
                "daily_check_status_counts": dict(check_status_counts),
                "exit_reason_counts": dict(exit_reason_counts),
                "daily_check_samples": [
                    event.to_dict()
                    for event in (symbol_result.daily_checks[:2] + symbol_result.daily_checks[-2:])
                ]
                if symbol_result.daily_checks
                else [],
            }
        )

    return {
        "status": "completed",
        "start_time": engine_result.start_time.isoformat(),
        "end_time": engine_result.end_time.isoformat(),
        "symbol_count": len(engine_result.symbol_results),
        "total_ladders_placed": sum(result.ladders_placed for result in engine_result.symbol_results),
        "total_fills": sum(len(result.fills) for result in engine_result.symbol_results),
        "total_exits": sum(len(result.exits) for result in engine_result.symbol_results),
        "symbols": symbol_summaries,
    }


def main() -> int:
    config = get_config()
    validate_config(config)
    ensure_runtime_directories(config)
    setup_logging(config.log_level, config.log_file_path)

    logger = get_logger(__name__)
    bybit_client = BybitClient()
    selector = SymbolSelector(bybit_client)
    loader = HistoricalDataLoader(
        bybit_client=bybit_client,
        candle_cache=SqliteCandleCache(config.cache_db_path),
    )
    engine = BacktestEngine(
        config=config,
        loader=loader,
        logger=logger,
    )

    try:
        resolved_universe = selector.select_symbols(config)
        logger.info(
            "Universe selection resolved %s symbols for mode `%s`.",
            len(resolved_universe.selected_symbols),
            config.backtest_mode,
        )
        data_sync = sync_historical_data(
            config=config,
            selected_symbols=resolved_universe.selected_symbols,
            loader=loader,
            logger=logger,
        )
        daily_setup_preview = build_daily_setup_preview(
            config=config,
            selected_symbols=resolved_universe.selected_symbols,
            loader=loader,
            logger=logger,
        )
        ladder_preview_summary = build_ladder_preview_summary(
            config=config,
            daily_setup_preview=daily_setup_preview,
            logger=logger,
        )
        fill_replay_preview = build_fill_replay_preview(
            config=config,
            daily_setup_preview=daily_setup_preview,
            loader=loader,
            logger=logger,
        )
        exit_preview_summary = build_exit_preview_summary(
            config=config,
            daily_setup_preview=daily_setup_preview,
            loader=loader,
            logger=logger,
        )
        engine_result = engine.run(selected_symbols=resolved_universe.selected_symbols)
        engine_preview_summary = build_engine_preview_summary(engine_result)
        report_bundle = generate_reports(
            engine_result=engine_result,
            initial_capital=config.initial_capital,
            output_dir=config.output_dir,
        )
        logger.info(
            "Reports written: trade_log=%s equity_curve=%s summary=%s.",
            report_bundle.trade_log_path,
            report_bundle.equity_curve_path,
            report_bundle.summary_path,
        )
    except (
        BybitClientError,
        UniverseSelectionError,
        DataValidationError,
        LadderGenerationError,
        ExitRuleError,
        ValueError,
        RuntimeError,
    ) as exc:
        logger.exception("Bootstrap failed during universe resolution or historical sync.")
        raise SystemExit(f"Backtester startup failed: {exc}") from exc

    universe_selection = resolved_universe.to_dict()
    startup_summary = build_runtime_summary(
        config,
        universe_selection=universe_selection,
        data_sync=data_sync,
        daily_setup_preview=daily_setup_preview,
        ladder_preview_summary=ladder_preview_summary,
        fill_replay_preview=fill_replay_preview,
        exit_preview_summary=exit_preview_summary,
        engine_preview_summary=engine_preview_summary,
        reporting_summary={
            "status": "completed",
            **report_bundle.to_dict(),
        },
    )

    logger.info("Bootstrap completed successfully.")
    logger.info("Backtest mode: %s", config.backtest_mode)
    logger.info("Output directory: %s", config.output_dir)
    logger.info("Cache database path: %s", config.cache_db_path)

    print_startup_banner(startup_summary)
    print_cli_summary(report_bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
