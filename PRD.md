# PRD: Python Backtester for Bybit USDT Perpetual Pump-and-Dump Short Strategy

## Objective

Build a Python backtesting framework for a short-selling strategy on low-liquidity Bybit USDT perpetual contracts that aims to exploit pump-and-dump behavior in scam coins. The strategy places a ladder of short limit orders above a daily anchor price and profits from mean reversion after abnormal upward spikes.

Use `pandas`, `numpy` and `pybit` where appropriate. Structure the project clearly so the codebase is readable and easy to extend.

---

## Scope

The backtester must:

* download historical OHLCV data from Bybit with proper pagination
* validate downloaded data for integrity
* support two pre-defined set-up modes:

1. MODE A: fixed list of symbols pre-defined by user in `config.py`
2. MODE B: turnover filter: there has to be a module `turnover_filter.py` that scans all perps on ByBit and sorts them based on a 24h turnover stats in ascending order. Than the system choses the symbols from `range_start` to `range_finish`. This turnover must be fetched from Bybit’s live 24h ticker stats for the last day, no reconsturction required. ByBit has its own 24h turnover stats in the ticker info. The turnover must be in USDT.

* simulate daily ladder order placement
* simulate fills using intraday candles
* support configurable stop-loss and take-profit logic
* generate trade logs and performance summary

All instruments are  **Bybit USDT perpetuals only** . Exclude USDC/USDS contracts.

---

## Configuration (`config.py`)

### General

* `history_in_days: int` — default 365
* `check_hour_utc: int` — hour of the daily strategy check, default is 10 am in CET
* `initial_capital: float` — default 30_000
* `backtest_mode: str` — `"fixed_symbols"` or `"turnover_filter"`, default `"fixed_symbols"`

### Universe selection

* `backtest_symbols: list[str]` — used when `backtest_mode == "fixed_symbols"`
* `range_start: int` — default 60 (meaning 60th symbol with least turnover)
* `range_finish: int` — default 90 (meaning 90th symbol with least turnover)
* `max_symbols_per_day: int` — default 30

### Ladder mode

* `ladder_mode: str` — `"percent"` or `"atr"`
* `ladder_order_usdt_size: float` — default 100

If `ladder_mode == "percent"`:

* `ladder_percents: list[float]` — default `[0.20, 0.30, 0.40, 0.50, 0.60, 0.90]`

If `ladder_mode == "atr"`:

* `atr_period_days: int` — default 28
* `ladder_atr_multiples: list[float]` — default `[1.5, 2.0, 2.5, 3.0, 3.5, 4.0]`

### Entry filter

* `enable_volume_filter: bool` — default `False`
* `volume_filter_lookback_days: int` — default 20
* `volume_filter_intraday_window_hours: int` — default 4
* `volume_filter_threshold_fraction_of_daily_avg: float` — default 0.5

### Risk management

* `stop_mode: str` — `"percent"` or `"atr"`

If `stop_mode == "percent"`:

* `stop_percent_from_avg_entry: float` — default 2.0

If `stop_mode == "atr"`:

* `stop_atr_multiple: float` — default 8.0

### Profit taking

* `enable_take_profit: bool` — default `True`
* `take_profit_1_percent: float` — default 0.20
* `take_profit_1_close_fraction: float` — default 0.50
* `take_profit_2_mode: str` — `"anchor_price"` or `"percent"`, default `"anchor_price"`
* `take_profit_2_percent: float` — default 0.30

### Position lifecycle

* `max_holding_days: int` — default 10
* `allow_new_orders_if_position_open: bool` — default `False`

### Execution assumptions

* `intraday_fill_timeframe: str` — default `"5m"`
* `maker_fee_rate: float` — default 0.0002
* `taker_fee_rate: float` — default 0.00055
* `slippage_bps: float` — default 5.0
* `apply_funding: bool` — default `False` in v1

---

## Strategy logic

### 1. Universe selection

#### Mode A: fixed symbols

Trade only the symbols listed in `backtest_symbols`.

#### Mode B: turnover filter

At each daily check time:

* get all Bybit USDT perpetual symbols
* get 24h turnover stats for each symbol
* sort symbols by turnover in ascending order
* keep symbols in between `range_start` and `range_finish`

---

### 2. Daily anchor price

At the daily check timestamp, define for each symbol:

`anchor_price = last traded price`, namely close of the last fully completed 5-minute candle at the daily check timestamp

This anchor price is stored separately for each symbol and each day’s ladder.
The anchor price is not the previous ByBit daily candle, but actually the price of the symbol at the moment of the daily check. The daily check happens at “check_hour_utc” variable set in the config. The anchor doesn’t change or get set up again if on a particular symbol there is a running position.

---

### 3. Entry filter

Before placing a ladder for a symbol, require abnormal volume if `enable_volume_filter = True`:

* compute average daily quote volume over the previous `volume_filter_lookback_days` completed daily candles
* compute quote volume over the last `volume_filter_intraday_window_hours` ending at check time
* allow ladder placement only if:

`intraday_quote_volume >= volume_filter_threshold_fraction_of_daily_avg * avg_daily_quote_volume`

If the filter fails, skip that symbol for that day.

---

### 4. Ladder placement

At each daily check:

* cancel all unfilled pending entry orders from the previous day, except for the symbols that have an open position
* if a symbol already has an open position and `allow_new_orders_if_position_open == False`, do not place a new ladder
* otherwise place a new ladder of short limit orders above `anchor_price`

General logic on how we manage multiple ladder entries.
Every time a limit order of a particular ladder level gets filled, it changes the average price of the overall order, which would mean that the TP1 and TP2 shall be repositioned. Thus, when TP1 gets hit, the algorithm shall close the exact outstanding USDT value of that unified position. This matches Bybits’ one-way mode when the orders on the same symbol get accumulated in the same opened position.

Thus, the ladders shall reset at the new calendar day if the position on a symbol wasn’t open. If there is a running opened position, obviously, new ladders are just not set as we have them set already.

#### Percent ladder

Each order price is:

`anchor_price * (1 + ladder_percent)`

Each order notional is `ladder_order_usdt_size`.

#### ATR ladder

Compute ATR on daily candles using `atr_period_days`.

Each order price is:

`anchor_price + ATR * ladder_atr_multiple`

Each order notional is `ladder_order_usdt_size`.
When a position is already open intraday, you freeze the ATR from the ladder day. It's not get recalculated every day for open positions.
ATR computed from the previous fully closed daily bar only.

---

### 5. Fill model

Use 5-minute candles for execution simulation.

For short limit orders:

* if candle high >= limit price, mark the order as filled at the limit price
* if multiple order levels are crossed in the same candle, fill all of them

Average entry price must be updated as a weighted average of all filled tranches.

---

### 6. Risk management

Each open position must have a stop-loss.

#### Percent stop

Stop price:

`avg_entry_price * (1 + stop_percent_from_avg_entry)`

#### ATR stop

Stop price:

`avg_entry_price + stop_atr_multiple * daily_ATR`

ATR should be the most recently known ATR at the time of position monitoring, based only on data available up to that timestamp.

If stop-loss is triggered:

* close the full remaining position
* assume taker fee and configured slippage

---

### 7. Take-profit logic

TP1 and TP2 are calculated based on the average entry price of the unified position.

TP1 and TP2 are calculated with a taker fee.

If enabled:

#### TP1

When price falls by `take_profit_1_percent` below average entry price:

* close `take_profit_1_close_fraction` of the current open position
  TP1 triggers based on the last traded price.
  50% in coin quantity.
  If a new tranche fills after TP1 has already partially closed, TP1 is allowed to fire again later on the re-expanded position.

#### TP2

For the remaining position, either:

* close when price returns to the stored `anchor_price` from the day the ladder was placed, if `take_profit_2_mode == "anchor_price"`
* or close when price falls by `take_profit_2_percent` below average entry if `take_profit_2_mode == "percent"`

---

### 8. Time stop

If a position remains open longer than `max_holding_days`, close the remainder at market on the final allowed bar.

Time stop mechanics. The time stop logic is the following: if the TP1 or TP2 wasn’t hit the position gets closed anyway after “max_holding_days” variable. It shall be measured exactly as “max_holding_days * 24 hours”.

---

## Backtest integrity rules

The backtester must:

* download historical data with pagination
* sort all candles by timestamp
* remove duplicates
* detect and log missing candles
* validate OHLC relationships
* fail loudly or warn clearly when data is incomplete or corrupted

Do not use future information in:

* universe selection
* ATR calculation
* volume filter calculation
* fill simulation

Avoid look-ahead bias.

## Project functional blocks and project structure

project/
│
├── config.py
├── main.py
├── requirements.txt
│
├── data/
|   ├── data_loader.py
│   ├── data_validator.py
|   └── data_base.py
│
├── strategy/
│   ├── config_verification.py
│   ├── logger_logic.py
│   ├── turnover_filter.py
│   ├── daily_checks.py
│   └── regular_checks.py
│
├── backtest/
│   ├── engine.py
│   ├── reporting.py
│   └── metrics.py
│
├── outputs/
│   ├── trade_log.csv
│   ├── equity_curve.csv
│   └── summary.csv

data_loader.py - downloads historical data from Bybit API
data_validator.py - validates historical data for integrety and consistency. It checks for gaps in data, duplicates, and OHLC relationships. When data is incomplete for a symbol during the backtest,skip the symbol for that day.
data_base.py - stores historical data in sqlite3 database (used as cache for future runs)

config_verification.py - verifies that config.py is valid
logger_logic.py - handles logging
turnover_filter.py - implements turnover filter
daily_checks.py - implements daily checks and setups (orders set ups, TP1, TP2, stop, time stop)
regular_checks.py - implements regular checks to update the orders on the currently open positions (risk management, TP1, TP2 updates, time stop check)

engine.py - implements backtest engine
metrics.py - implements metrics calculation
reporting.py - implements reporting

## Required outputs

The script must produce:

### 1. Trade log is returned in trade_log.csv

For every trade and partial exit:

* symbol
* ladder date
* anchor price
* order level
* fill timestamp
* fill price
* fill quantity
* average entry after fill
* exit timestamp
* exit price
* exit reason (`tp1`, `tp2`, `stop`, `time_stop`)
* realized PnL
* fees
* slippage

### 2. Performance summary

* total PnL
* total return %
* max drawdown
* number of trades
* win rate
* average trade PnL
* average holding time
* profit factor
* symbol-level stats
* exposure over time

#### 2.1 The looks of the report to be printed in CLI

```
=================================================================
BACKTEST PERFORMANCE SUMMARYTotal Net PnL    : XXX USDT
=================================================================
Total Return     : xx %
Max Drawdown     : xx %
Number of Exits  : xx
Win Rate         : xx %
Avg Trade PnL    : xx USDT
Profit Factor    : xx
Avg Holding Time : xx days xx hours

Symbol Level Stats:
  1000FLOKIUSDT :     4 exits | Net PnL:    22.81 USDT | WR: 75.00%
  1000PEPEUSDT  :    4 exits  | Net PnL:   -32.73 USDT | WR: 50.00%
  API3USDT      :   13 exits  | Net PnL:    91.73 USDT | WR: 92.31%
  ARKUSDT       :   11 exits  | Net PnL:    38.68 USDT | WR: 81.82%
  AUCTIONUSDT   :    9 exits  | Net PnL:   181.01 USDT | WR: 88.89%
  BIGTIMEUSDT   :    3 exits  | Net PnL:     0.25 USDT | WR: 66.67%
  BLURUSDT      :    5 exits  | Net PnL:    61.59 USDT | WR: 100.00%
  =================================================================
  OVERALL   :   49 exits | Net PnL:   363.33 USDT | WR: 83.67%
```

The exit is either TP1, TP2, stop, or time_stop. The number of exits is the number of times a position was closed. The code shall precisely count the individual partial and full closure events reported as exit line items.

### 3. Equity curve

Portfolio equity by timestamp.
