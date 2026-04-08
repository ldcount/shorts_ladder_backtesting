from __future__ import annotations

from config import AppConfig


class ConfigValidationError(ValueError):
    """Raised when application configuration is invalid."""


def _require_positive(errors: list[str], name: str, value: float) -> None:
    if value <= 0:
        errors.append(f"`{name}` must be greater than 0, got {value}.")


def _require_fraction(errors: list[str], name: str, value: float) -> None:
    if not 0 < value <= 1:
        errors.append(f"`{name}` must be in the interval (0, 1], got {value}.")


def _require_non_empty_list(errors: list[str], name: str, value: list[float] | list[str]) -> None:
    if not value:
        errors.append(f"`{name}` must not be empty.")


def validate_config(config: AppConfig) -> None:
    """Validate the configuration and raise a single aggregated error if needed."""

    errors: list[str] = []

    _require_positive(errors, "history_in_days", config.history_in_days)
    _require_positive(errors, "initial_capital", config.initial_capital)
    _require_positive(errors, "ladder_order_usdt_size", config.ladder_order_usdt_size)
    _require_positive(errors, "volume_filter_lookback_days", config.volume_filter_lookback_days)
    _require_positive(
        errors,
        "volume_filter_intraday_window_hours",
        config.volume_filter_intraday_window_hours,
    )
    _require_positive(errors, "max_holding_days", config.max_holding_days)
    _require_positive(errors, "maker_fee_rate", config.maker_fee_rate)
    _require_positive(errors, "taker_fee_rate", config.taker_fee_rate)

    if not 0 <= config.check_hour_utc <= 23:
        errors.append(f"`check_hour_utc` must be between 0 and 23, got {config.check_hour_utc}.")

    if config.backtest_mode not in {"fixed_symbols", "turnover_filter"}:
        errors.append(
            "`backtest_mode` must be either `fixed_symbols` or `turnover_filter`, "
            f"got {config.backtest_mode!r}."
        )

    if config.backtest_mode == "fixed_symbols":
        _require_non_empty_list(errors, "backtest_symbols", config.backtest_symbols)
        invalid_symbols = [symbol for symbol in config.backtest_symbols if not symbol.endswith("USDT")]
        if invalid_symbols:
            errors.append(
                "`backtest_symbols` must contain Bybit USDT perpetual symbols only. "
                f"Invalid entries: {invalid_symbols}."
            )

    _require_positive(errors, "range_start", config.range_start)
    _require_positive(errors, "range_finish", config.range_finish)
    _require_positive(errors, "max_symbols_per_day", config.max_symbols_per_day)
    if config.range_start > config.range_finish:
        errors.append(
            f"`range_start` ({config.range_start}) must be less than or equal to "
            f"`range_finish` ({config.range_finish})."
        )

    if config.ladder_mode not in {"percent", "atr"}:
        errors.append(f"`ladder_mode` must be `percent` or `atr`, got {config.ladder_mode!r}.")
    if config.ladder_mode == "percent":
        _require_non_empty_list(errors, "ladder_percents", config.ladder_percents)
        invalid_ladder_percents = [value for value in config.ladder_percents if value <= 0]
        if invalid_ladder_percents:
            errors.append(
                "`ladder_percents` must contain positive values only. "
                f"Invalid entries: {invalid_ladder_percents}."
            )
    if config.ladder_mode == "atr":
        _require_positive(errors, "atr_period_days", config.atr_period_days)
        _require_non_empty_list(errors, "ladder_atr_multiples", config.ladder_atr_multiples)
        invalid_atr_multiples = [value for value in config.ladder_atr_multiples if value <= 0]
        if invalid_atr_multiples:
            errors.append(
                "`ladder_atr_multiples` must contain positive values only. "
                f"Invalid entries: {invalid_atr_multiples}."
            )

    _require_positive(
        errors,
        "volume_filter_threshold_fraction_of_daily_avg",
        config.volume_filter_threshold_fraction_of_daily_avg,
    )

    if config.stop_mode not in {"percent", "atr"}:
        errors.append(f"`stop_mode` must be `percent` or `atr`, got {config.stop_mode!r}.")
    if config.stop_mode == "percent":
        _require_positive(errors, "stop_percent_from_avg_entry", config.stop_percent_from_avg_entry)
    if config.stop_mode == "atr":
        _require_positive(errors, "stop_atr_multiple", config.stop_atr_multiple)

    if config.enable_take_profit:
        _require_positive(errors, "take_profit_1_percent", config.take_profit_1_percent)
        _require_fraction(
            errors,
            "take_profit_1_close_fraction",
            config.take_profit_1_close_fraction,
        )
        if config.take_profit_2_mode not in {"anchor_price", "percent"}:
            errors.append(
                "`take_profit_2_mode` must be `anchor_price` or `percent`, "
                f"got {config.take_profit_2_mode!r}."
            )
        if config.take_profit_2_mode == "percent":
            _require_positive(errors, "take_profit_2_percent", config.take_profit_2_percent)

    if config.intraday_fill_timeframe != "5m":
        errors.append(
            "`intraday_fill_timeframe` must be `5m` in v1 to match the PRD, "
            f"got {config.intraday_fill_timeframe!r}."
        )

    if config.slippage_bps < 0:
        errors.append(f"`slippage_bps` must be 0 or greater, got {config.slippage_bps}.")

    if config.bybit_position_idx not in {0, 1, 2}:
        errors.append(
            "`bybit_position_idx` must be one of 0 (one-way), 1 (hedge buy), or 2 (hedge sell), "
            f"got {config.bybit_position_idx}."
        )
    _require_positive(
        errors,
        "production_monitor_interval_seconds",
        config.production_monitor_interval_seconds,
    )

    if errors:
        joined_errors = "\n".join(f"- {message}" for message in errors)
        raise ConfigValidationError(f"Configuration validation failed:\n{joined_errors}")
