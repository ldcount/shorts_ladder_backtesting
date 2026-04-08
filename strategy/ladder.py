from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from config import AppConfig
from core.models import DailySetupState, LadderState, PendingOrder


class LadderGenerationError(RuntimeError):
    """Raised when a ladder cannot be generated from the provided setup state."""


@dataclass(slots=True)
class LadderLevelPreview:
    level_index: int
    trigger_value: float
    limit_price: float
    quantity: float
    notional_usdt: float

    def to_dict(self) -> dict[str, object]:
        return {
            "level_index": self.level_index,
            "trigger_value": self.trigger_value,
            "limit_price": self.limit_price,
            "quantity": self.quantity,
            "notional_usdt": self.notional_usdt,
        }


def build_ladder(
    *,
    config: AppConfig,
    daily_setup: DailySetupState,
) -> LadderState:
    """Build pending short limit orders from the configured ladder mode."""

    if daily_setup.anchor_price <= 0:
        raise LadderGenerationError(
            f"Anchor price must be positive to build a ladder for {daily_setup.symbol}."
        )

    if config.ladder_mode == "percent":
        trigger_values = config.ladder_percents
        frozen_atr = None
        price_builder = lambda trigger_value: daily_setup.anchor_price * (1 + trigger_value)
    elif config.ladder_mode == "atr":
        if daily_setup.atr_value is None:
            raise LadderGenerationError(
                f"ATR ladder requested for {daily_setup.symbol}, but no ATR value is available."
            )
        trigger_values = config.ladder_atr_multiples
        frozen_atr = daily_setup.atr_value
        price_builder = lambda trigger_value: daily_setup.anchor_price + (daily_setup.atr_value * trigger_value)
    else:
        raise LadderGenerationError(f"Unsupported ladder mode {config.ladder_mode!r}.")

    orders: list[PendingOrder] = []
    for level_index, trigger_value in enumerate(trigger_values, start=1):
        limit_price = float(price_builder(trigger_value))
        if limit_price <= 0:
            raise LadderGenerationError(
                f"Ladder level {level_index} for {daily_setup.symbol} produced a non-positive price."
            )

        quantity = config.ladder_order_usdt_size / limit_price
        orders.append(
            PendingOrder(
                symbol=daily_setup.symbol,
                created_at=daily_setup.check_timestamp,
                ladder_mode=config.ladder_mode,
                level_index=level_index,
                side="sell",
                order_type="limit",
                status="pending",
                trigger_value=trigger_value,
                limit_price=limit_price,
                notional_usdt=config.ladder_order_usdt_size,
                quantity=quantity,
                anchor_price=daily_setup.anchor_price,
                frozen_atr=frozen_atr,
            )
        )

    return LadderState(
        symbol=daily_setup.symbol,
        ladder_mode=config.ladder_mode,
        check_timestamp=daily_setup.check_timestamp,
        anchor_price=daily_setup.anchor_price,
        order_notional_usdt=config.ladder_order_usdt_size,
        frozen_atr=frozen_atr,
        orders=orders,
    )


def ladder_preview(ladder_state: LadderState) -> dict[str, object]:
    """Return a compact preview focused on order levels and prices."""

    levels = [
        LadderLevelPreview(
            level_index=order.level_index,
            trigger_value=order.trigger_value,
            limit_price=order.limit_price,
            quantity=order.quantity,
            notional_usdt=order.notional_usdt,
        ).to_dict()
        for order in ladder_state.orders
    ]
    return {
        "symbol": ladder_state.symbol,
        "ladder_mode": ladder_state.ladder_mode,
        "check_timestamp": ladder_state.check_timestamp.isoformat(),
        "anchor_price": ladder_state.anchor_price,
        "frozen_atr": ladder_state.frozen_atr,
        "order_notional_usdt": ladder_state.order_notional_usdt,
        "order_count": len(ladder_state.orders),
        "levels": levels,
    }
