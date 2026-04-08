from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class AppConfig:
    """Application configuration for the Bybit backtester."""

    # General
    history_in_days: int = 45
    check_hour_utc: int = 10
    initial_capital: float = 30_000.0
    # fixed_symbols or turnover_filter
    backtest_mode: str = "turnover_filter"

    # Universe selection
    backtest_symbols: list[str] = field(
        default_factory=lambda: [
            "AEROUSDT",
            "THEUSDT",
            "ARCUSDT",
            "ATHUSDT",
            "JASMYUSDT",
            "FIDAUSDT",
            "ANKRUSDT",
            "APEUSDT",
            "MYXUSDT",
            "NAORISUSDT",
            "PLUMEUSDT",
            #"PYTHUSDT",
            #"SKYUSDT",
            #"SQDUSDT",
            #"BIGTIMEUSDT",
            
        ]
    )
    range_start: int = 310
    range_finish: int = 3370
    max_symbols_per_day: int = 60

    # Ladder mode percent or atr
    ladder_mode: str = "percent"
    ladder_order_usdt_size: float = 20.0
    ladder_percents: list[float] = field(
        default_factory=lambda: [0.25, 0.35, 0.50, 0.70, 1]
    )
    atr_period_days: int = 28
    ladder_atr_multiples: list[float] = field(
        default_factory=lambda: [1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    )

    # Entry filter
    enable_volume_filter: bool = False
    volume_filter_lookback_days: int = 20
    volume_filter_intraday_window_hours: int = 4
    volume_filter_threshold_fraction_of_daily_avg: float = 0.5

    # Risk management
    stop_mode: str = "percent"
    stop_percent_from_avg_entry: float = 2.0
    stop_atr_multiple: float = 8.0

    # Profit taking
    enable_take_profit: bool = True
    take_profit_1_percent: float = 0.20
    take_profit_1_close_fraction: float = 0.50
    take_profit_2_mode: str = "anchor_price"
    take_profit_2_percent: float = 0.30

    # Position lifecycle
    max_holding_days: int = 10
    allow_new_orders_if_position_open: bool = False

    # Execution assumptions
    intraday_fill_timeframe: str = "5m"
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.00055
    slippage_bps: float = 5.0
    apply_funding: bool = False

    # Production runtime
    production_startup_ladder_enabled: bool = True
    bybit_api_key: str | None = field(default=None, repr=False)
    bybit_api_secret: str | None = field(default=None, repr=False)
    bybit_testnet: bool = False
    bybit_demo: bool = False
    bybit_log_requests: bool = False
    bybit_position_idx: int = 0
    production_monitor_interval_seconds: int = 30

    # Runtime paths and logging
    project_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent, repr=False
    )
    log_level: str = "INFO"

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def output_dir(self) -> Path:
        return self.project_root / "outputs"

    @property
    def logs_dir(self) -> Path:
        return self.output_dir / "logs"

    @property
    def cache_db_path(self) -> Path:
        return self.data_dir / "backtest_cache.sqlite3"

    @property
    def log_file_path(self) -> Path:
        return self.logs_dir / "backtester.log"

    @property
    def env_file_path(self) -> Path:
        return self.project_root / ".env"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the configuration."""

        config_dict = asdict(self)
        config_dict["project_root"] = str(self.project_root)
        config_dict["data_dir"] = str(self.data_dir)
        config_dict["output_dir"] = str(self.output_dir)
        config_dict["logs_dir"] = str(self.logs_dir)
        config_dict["cache_db_path"] = str(self.cache_db_path)
        config_dict["log_file_path"] = str(self.log_file_path)
        config_dict["env_file_path"] = str(self.env_file_path)
        if self.bybit_api_key:
            config_dict["bybit_api_key"] = "***configured***"
        if self.bybit_api_secret:
            config_dict["bybit_api_secret"] = "***configured***"
        return config_dict


def _parse_bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _read_dotenv(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        values[key.strip()] = raw_value.strip().strip("\"'")
    return values


def get_config(*, env_path: Path | None = None) -> AppConfig:
    """Return config defaults with optional `.env` overrides for secrets/runtime flags."""

    config = AppConfig()
    env_values = _read_dotenv(env_path or config.env_file_path)

    config.bybit_api_key = env_values.get("BYBIT_API_KEY") or os.getenv("BYBIT_API_KEY")
    config.bybit_api_secret = env_values.get("BYBIT_API_SECRET") or os.getenv("BYBIT_API_SECRET")
    config.bybit_testnet = _parse_bool_env(
        env_values.get("BYBIT_TESTNET", os.getenv("BYBIT_TESTNET")),
        config.bybit_testnet,
    )
    config.bybit_demo = _parse_bool_env(
        env_values.get("BYBIT_DEMO", os.getenv("BYBIT_DEMO")),
        config.bybit_demo,
    )
    config.bybit_log_requests = _parse_bool_env(
        env_values.get("BYBIT_LOG_REQUESTS", os.getenv("BYBIT_LOG_REQUESTS")),
        config.bybit_log_requests,
    )
    bybit_position_idx = env_values.get("BYBIT_POSITION_IDX") or os.getenv("BYBIT_POSITION_IDX")
    if bybit_position_idx is not None:
        try:
            config.bybit_position_idx = int(bybit_position_idx)
        except ValueError:
            pass
    production_monitor_interval_seconds = env_values.get(
        "PRODUCTION_MONITOR_INTERVAL_SECONDS"
    ) or os.getenv("PRODUCTION_MONITOR_INTERVAL_SECONDS")
    if production_monitor_interval_seconds is not None:
        try:
            config.production_monitor_interval_seconds = int(production_monitor_interval_seconds)
        except ValueError:
            pass
    config.production_startup_ladder_enabled = _parse_bool_env(
        env_values.get(
            "PRODUCTION_STARTUP_LADDER_ENABLED",
            os.getenv("PRODUCTION_STARTUP_LADDER_ENABLED"),
        ),
        config.production_startup_ladder_enabled,
    )
    return config
