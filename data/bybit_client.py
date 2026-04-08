from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Any

try:
    from pybit.unified_trading import HTTP as BybitHTTP
except ImportError:  # pragma: no cover - exercised only when dependency is absent
    BybitHTTP = None


class BybitClientError(RuntimeError):
    """Raised when the Bybit client cannot return the requested data."""


@dataclass(slots=True)
class BybitInstrument:
    symbol: str
    base_coin: str
    quote_coin: str
    settle_coin: str
    contract_type: str
    status: str


@dataclass(slots=True)
class BybitTickerSnapshot:
    symbol: str
    turnover_24h: float
    last_price: float | None
    mark_price: float | None


@dataclass(slots=True)
class BybitInstrumentSpec:
    symbol: str
    tick_size: float
    qty_step: float
    min_order_qty: float
    min_notional_value: float


@dataclass(slots=True)
class BybitOpenOrder:
    symbol: str
    order_id: str
    order_link_id: str
    side: str
    price: float
    qty: float
    reduce_only: bool
    order_status: str
    position_idx: int
    created_time: datetime


@dataclass(slots=True)
class BybitPositionSnapshot:
    symbol: str
    side: str
    size: float
    position_idx: int
    avg_entry_price: float | None
    created_time: datetime | None
    updated_time: datetime | None
    take_profit: float | None
    stop_loss: float | None


@dataclass(slots=True)
class BybitOrderSubmission:
    symbol: str
    order_id: str
    order_link_id: str


@dataclass(slots=True)
class BybitCandle:
    symbol: str
    timeframe: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float

    @property
    def open_time_ms(self) -> int:
        return int(self.open_time.timestamp() * 1000)


class BybitClient:
    """Small wrapper around the pybit unified trading client."""

    _KLINE_INTERVALS = {
        "5m": "5",
        "1d": "D",
    }
    _REQUEST_SPACING_SECONDS = {
        "get_instruments_info": 0.05,
        "get_tickers": 0.05,
        "get_kline": 0.20,
        "get_positions": 0.10,
        "get_open_orders": 0.10,
        "place_order": 0.15,
        "cancel_order": 0.10,
        "set_trading_stop": 0.10,
    }
    _MAX_RETRIES = 6
    _BASE_RETRY_DELAY_SECONDS = 1.5

    def __init__(
        self,
        *,
        testnet: bool = False,
        demo: bool = False,
        api_key: str | None = None,
        api_secret: str | None = None,
        log_requests: bool = False,
        http_client: Any | None = None,
        sleep_func: Any | None = None,
        monotonic_func: Any | None = None,
    ) -> None:
        self._http = http_client or self._build_default_http_client(
            testnet=testnet,
            demo=demo,
            api_key=api_key,
            api_secret=api_secret,
            log_requests=log_requests,
        )
        self._sleep = sleep_func or time.sleep
        self._monotonic = monotonic_func or time.monotonic
        self._last_request_started_at = 0.0

    def get_usdt_perpetual_instruments(self) -> list[BybitInstrument]:
        """Return all active Bybit USDT perpetual contracts."""

        raw_instruments = self._paginate_instruments()
        instruments: list[BybitInstrument] = []

        for item in raw_instruments:
            instrument = self._parse_instrument(item)
            if self._is_usdt_perpetual(instrument):
                instruments.append(instrument)

        instruments.sort(key=lambda instrument: instrument.symbol)
        return instruments

    def get_usdt_perpetual_tickers(
        self,
        instruments: list[BybitInstrument] | None = None,
    ) -> list[BybitTickerSnapshot]:
        """Return ticker snapshots for active USDT perpetual contracts only."""

        if instruments is None:
            instruments = self.get_usdt_perpetual_instruments()

        eligible_symbols = {instrument.symbol for instrument in instruments}
        response = self._call("get_tickers", category="linear")
        rows = response.get("list", [])

        tickers: list[BybitTickerSnapshot] = []
        for item in rows:
            symbol = str(item.get("symbol", "")).upper()
            if symbol not in eligible_symbols:
                continue

            turnover_24h = self._parse_float(item.get("turnover24h"))
            if turnover_24h is None:
                continue

            tickers.append(
                BybitTickerSnapshot(
                    symbol=symbol,
                    turnover_24h=turnover_24h,
                    last_price=self._parse_float(item.get("lastPrice")),
                    mark_price=self._parse_float(item.get("markPrice")),
                )
            )

        tickers.sort(key=lambda ticker: (ticker.turnover_24h, ticker.symbol))
        return tickers

    def get_klines(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[BybitCandle]:
        """Fetch one page of historical kline data for a Bybit linear contract."""

        interval = self._KLINE_INTERVALS.get(timeframe)
        if interval is None:
            raise BybitClientError(
                f"Unsupported timeframe {timeframe!r}. Supported values: {sorted(self._KLINE_INTERVALS)}."
            )

        response = self._call(
            "get_kline",
            category="linear",
            symbol=symbol.upper(),
            interval=interval,
            start=start_ms,
            end=end_ms,
            limit=limit,
        )
        rows = response.get("list", [])

        candles = [self._parse_candle(symbol=symbol.upper(), timeframe=timeframe, row=row) for row in rows]
        candles.sort(key=lambda candle: candle.open_time)
        return candles

    def get_instrument_spec(self, *, symbol: str) -> BybitInstrumentSpec:
        response = self._call(
            "get_instruments_info",
            category="linear",
            symbol=symbol.upper(),
            limit=1,
        )
        rows = response.get("list", [])
        if not rows:
            raise BybitClientError(f"No instrument spec returned for {symbol.upper()}.")
        return self._parse_instrument_spec(rows[0])

    def get_open_orders(
        self,
        *,
        symbol: str | None = None,
        settle_coin: str = "USDT",
    ) -> list[BybitOpenOrder]:
        orders: list[BybitOpenOrder] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {
                "category": "linear",
                "openOnly": 0,
                "limit": 50,
            }
            if symbol is not None:
                params["symbol"] = symbol.upper()
            else:
                params["settleCoin"] = settle_coin.upper()
            if cursor:
                params["cursor"] = cursor

            response = self._call("get_open_orders", **params)
            rows = response.get("list", [])
            orders.extend(self._parse_open_order(item) for item in rows)
            cursor = response.get("nextPageCursor") or None
            if not cursor:
                break

        return orders

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        order_link_id: str | None = None,
    ) -> BybitOrderSubmission:
        if not order_id and not order_link_id:
            raise BybitClientError("cancel_order requires either `order_id` or `order_link_id`.")

        params: dict[str, Any] = {
            "category": "linear",
            "symbol": symbol.upper(),
        }
        if order_id:
            params["orderId"] = order_id
        if order_link_id:
            params["orderLinkId"] = order_link_id

        response = self._call("cancel_order", **params)
        return BybitOrderSubmission(
            symbol=symbol.upper(),
            order_id=str(response.get("orderId", order_id or "")),
            order_link_id=str(response.get("orderLinkId", order_link_id or "")),
        )

    def place_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        price: str,
        order_link_id: str,
        position_idx: int = 0,
        reduce_only: bool = False,
        time_in_force: str = "PostOnly",
    ) -> BybitOrderSubmission:
        response = self._call(
            "place_order",
            category="linear",
            symbol=symbol.upper(),
            side=side,
            orderType="Limit",
            qty=qty,
            price=price,
            orderLinkId=order_link_id,
            positionIdx=position_idx,
            reduceOnly=reduce_only,
            timeInForce=time_in_force,
        )
        return BybitOrderSubmission(
            symbol=symbol.upper(),
            order_id=str(response.get("orderId", "")),
            order_link_id=str(response.get("orderLinkId", order_link_id)),
        )

    def get_position_snapshots(self, *, symbol: str) -> list[BybitPositionSnapshot]:
        response = self._call(
            "get_positions",
            category="linear",
            symbol=symbol.upper(),
        )
        rows = response.get("list", [])
        return [self._parse_position_snapshot(item) for item in rows]

    def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        qty: str,
        order_link_id: str,
        position_idx: int = 0,
        reduce_only: bool = True,
    ) -> BybitOrderSubmission:
        response = self._call(
            "place_order",
            category="linear",
            symbol=symbol.upper(),
            side=side,
            orderType="Market",
            qty=qty,
            orderLinkId=order_link_id,
            positionIdx=position_idx,
            reduceOnly=reduce_only,
        )
        return BybitOrderSubmission(
            symbol=symbol.upper(),
            order_id=str(response.get("orderId", "")),
            order_link_id=str(response.get("orderLinkId", order_link_id)),
        )

    def set_trading_stop(
        self,
        *,
        symbol: str,
        position_idx: int,
        stop_loss: str | None = None,
        take_profit: str | None = None,
    ) -> None:
        params: dict[str, Any] = {
            "category": "linear",
            "symbol": symbol.upper(),
            "tpslMode": "Full",
            "positionIdx": position_idx,
        }
        if stop_loss is not None:
            params["stopLoss"] = stop_loss
            params["slTriggerBy"] = "LastPrice"
        if take_profit is not None:
            params["takeProfit"] = take_profit
            params["tpTriggerBy"] = "LastPrice"
        self._call("set_trading_stop", **params)

    def _paginate_instruments(self) -> list[dict[str, Any]]:
        instruments: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor

            response = self._call("get_instruments_info", **params)
            page_rows = response.get("list", [])
            instruments.extend(page_rows)

            cursor = response.get("nextPageCursor") or None
            if not cursor:
                break

        return instruments

    def _call(self, method_name: str, **kwargs: Any) -> dict[str, Any]:
        method = getattr(self._http, method_name, None)
        if method is None:
            raise BybitClientError(f"Bybit HTTP client does not expose `{method_name}`.")

        for attempt in range(1, self._MAX_RETRIES + 1):
            self._throttle(method_name)
            try:
                payload = method(**kwargs)
            except Exception as exc:  # pragma: no cover - depends on external client behavior
                if self._is_retryable_rate_limit_exception(exc) and attempt < self._MAX_RETRIES:
                    self._sleep(self._retry_delay_seconds(attempt))
                    continue
                raise BybitClientError(f"Bybit API call `{method_name}` failed: {exc}") from exc

            if payload.get("retCode") in (0, "0", None):
                break

            if self._is_retryable_rate_limit_payload(payload) and attempt < self._MAX_RETRIES:
                self._sleep(self._retry_delay_seconds(attempt))
                continue

            raise BybitClientError(
                "Bybit API returned an error for "
                f"`{method_name}`: retCode={payload.get('retCode')} "
                f"retMsg={payload.get('retMsg')!r}."
            )
        else:
            raise BybitClientError(
                f"Bybit API call `{method_name}` exhausted retry attempts without a valid response."
            )

        result = payload.get("result")
        if not isinstance(result, dict):
            raise BybitClientError(
                f"Bybit API returned an unexpected payload for `{method_name}`: {payload!r}"
            )
        return result

    def _throttle(self, method_name: str) -> None:
        minimum_spacing = self._REQUEST_SPACING_SECONDS.get(method_name)
        if minimum_spacing is None:
            return

        now = self._monotonic()
        elapsed = now - self._last_request_started_at
        if elapsed < minimum_spacing:
            self._sleep(minimum_spacing - elapsed)
        self._last_request_started_at = self._monotonic()

    @classmethod
    def _is_retryable_rate_limit_exception(cls, exc: Exception) -> bool:
        if isinstance(exc, KeyError):
            return str(exc).strip("'").lower() == "x-bapi-limit-reset-timestamp"

        message = str(exc).lower()
        return "10006" in message or "rate limit" in message or "too many visits" in message

    @staticmethod
    def _is_retryable_rate_limit_payload(payload: dict[str, Any]) -> bool:
        ret_code = str(payload.get("retCode", "")).strip()
        ret_msg = str(payload.get("retMsg", "")).lower()
        return ret_code == "10006" or "rate limit" in ret_msg or "too many visits" in ret_msg

    def _retry_delay_seconds(self, attempt: int) -> float:
        return self._BASE_RETRY_DELAY_SECONDS * attempt

    def _build_default_http_client(
        self,
        *,
        testnet: bool,
        demo: bool,
        api_key: str | None,
        api_secret: str | None,
        log_requests: bool,
    ) -> Any:
        if BybitHTTP is None:
            raise BybitClientError(
                "pybit is not installed. Install dependencies from requirements.txt before "
                "requesting live Bybit data."
            )
        return BybitHTTP(
            testnet=testnet,
            demo=demo,
            api_key=api_key,
            api_secret=api_secret,
            log_requests=log_requests,
        )

    @staticmethod
    def _parse_instrument(item: dict[str, Any]) -> BybitInstrument:
        return BybitInstrument(
            symbol=str(item.get("symbol", "")).upper(),
            base_coin=str(item.get("baseCoin", "")).upper(),
            quote_coin=str(item.get("quoteCoin", "")).upper(),
            settle_coin=str(item.get("settleCoin", "")).upper(),
            contract_type=str(item.get("contractType", "")),
            status=str(item.get("status", "")),
        )

    @classmethod
    def _parse_instrument_spec(cls, item: dict[str, Any]) -> BybitInstrumentSpec:
        symbol = str(item.get("symbol", "")).upper()
        price_filter = item.get("priceFilter") or {}
        lot_size_filter = item.get("lotSizeFilter") or {}
        return BybitInstrumentSpec(
            symbol=symbol,
            tick_size=cls._require_float(
                price_filter.get("tickSize"),
                field_name="priceFilter.tickSize",
                symbol=symbol,
            ),
            qty_step=cls._require_float(
                lot_size_filter.get("qtyStep"),
                field_name="lotSizeFilter.qtyStep",
                symbol=symbol,
            ),
            min_order_qty=cls._require_float(
                lot_size_filter.get("minOrderQty"),
                field_name="lotSizeFilter.minOrderQty",
                symbol=symbol,
            ),
            min_notional_value=cls._require_float(
                lot_size_filter.get("minNotionalValue"),
                field_name="lotSizeFilter.minNotionalValue",
                symbol=symbol,
            ),
        )

    @classmethod
    def _parse_open_order(cls, item: dict[str, Any]) -> BybitOpenOrder:
        symbol = str(item.get("symbol", "")).upper()
        return BybitOpenOrder(
            symbol=symbol,
            order_id=str(item.get("orderId", "")),
            order_link_id=str(item.get("orderLinkId", "")),
            side=str(item.get("side", "")),
            price=cls._require_float(item.get("price"), field_name="price", symbol=symbol),
            qty=cls._require_float(item.get("qty"), field_name="qty", symbol=symbol),
            reduce_only=bool(item.get("reduceOnly", False)),
            order_status=str(item.get("orderStatus", "")),
            position_idx=int(item.get("positionIdx", 0) or 0),
            created_time=datetime.fromtimestamp(int(item.get("createdTime", "0")) / 1000, tz=timezone.utc),
        )

    @classmethod
    def _parse_position_snapshot(cls, item: dict[str, Any]) -> BybitPositionSnapshot:
        symbol = str(item.get("symbol", "")).upper()
        size = cls._parse_float(item.get("size")) or 0.0
        return BybitPositionSnapshot(
            symbol=symbol,
            side=str(item.get("side", "")),
            size=size,
            position_idx=int(item.get("positionIdx", 0) or 0),
            avg_entry_price=cls._parse_float(item.get("avgPrice")),
            created_time=cls._parse_timestamp_ms(item.get("createdTime")),
            updated_time=cls._parse_timestamp_ms(item.get("updatedTime")),
            take_profit=cls._parse_float(item.get("takeProfit")),
            stop_loss=cls._parse_float(item.get("stopLoss")),
        )

    @staticmethod
    def _is_usdt_perpetual(instrument: BybitInstrument) -> bool:
        contract_type = instrument.contract_type.upper()
        status = instrument.status.upper()

        return (
            instrument.symbol.endswith("USDT")
            and instrument.quote_coin == "USDT"
            and instrument.settle_coin == "USDT"
            and "PERPETUAL" in contract_type
            and status in {"TRADING", ""}
            and not instrument.symbol.endswith("USDC")
            and not instrument.symbol.endswith("USDS")
        )

    @classmethod
    def _parse_candle(cls, *, symbol: str, timeframe: str, row: list[Any]) -> BybitCandle:
        if len(row) < 7:
            raise BybitClientError(f"Unexpected kline row format for {symbol}: {row!r}")

        open_time_ms = int(row[0])
        return BybitCandle(
            symbol=symbol,
            timeframe=timeframe,
            open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
            open=cls._require_float(row[1], field_name="open", symbol=symbol),
            high=cls._require_float(row[2], field_name="high", symbol=symbol),
            low=cls._require_float(row[3], field_name="low", symbol=symbol),
            close=cls._require_float(row[4], field_name="close", symbol=symbol),
            volume=cls._require_float(row[5], field_name="volume", symbol=symbol),
            turnover=cls._require_float(row[6], field_name="turnover", symbol=symbol),
        )

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        if value in (None, ""):
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _require_float(cls, value: Any, *, field_name: str, symbol: str) -> float:
        parsed = cls._parse_float(value)
        if parsed is None:
            raise BybitClientError(
                f"Bybit returned a non-numeric `{field_name}` value for {symbol}: {value!r}."
            )
        return parsed

    @staticmethod
    def _parse_timestamp_ms(value: Any) -> datetime | None:
        if value in (None, "", "0", 0):
            return None
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        except (TypeError, ValueError):
            return None
