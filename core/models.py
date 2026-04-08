from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class DailySetupState:
    """Daily setup inputs frozen at the strategy check timestamp."""

    symbol: str
    check_timestamp: datetime
    anchor_price: float
    volume_filter_status: str
    volume_filter_passed: bool | None
    atr_value: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "check_timestamp": self.check_timestamp.isoformat(),
            "anchor_price": self.anchor_price,
            "volume_filter_status": self.volume_filter_status,
            "volume_filter_passed": self.volume_filter_passed,
            "atr_value": self.atr_value,
        }


@dataclass(slots=True)
class PendingOrder:
    """One pending short limit order generated from a ladder level."""

    symbol: str
    created_at: datetime
    ladder_mode: str
    level_index: int
    side: str
    order_type: str
    status: str
    trigger_value: float
    limit_price: float
    notional_usdt: float
    quantity: float
    anchor_price: float
    frozen_atr: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "created_at": self.created_at.isoformat(),
            "ladder_mode": self.ladder_mode,
            "level_index": self.level_index,
            "side": self.side,
            "order_type": self.order_type,
            "status": self.status,
            "trigger_value": self.trigger_value,
            "limit_price": self.limit_price,
            "notional_usdt": self.notional_usdt,
            "quantity": self.quantity,
            "anchor_price": self.anchor_price,
            "frozen_atr": self.frozen_atr,
        }


@dataclass(slots=True)
class LadderState:
    """The full ladder created for one symbol on one daily check."""

    symbol: str
    ladder_mode: str
    check_timestamp: datetime
    anchor_price: float
    order_notional_usdt: float
    frozen_atr: float | None
    orders: list[PendingOrder] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "ladder_mode": self.ladder_mode,
            "check_timestamp": self.check_timestamp.isoformat(),
            "anchor_price": self.anchor_price,
            "order_notional_usdt": self.order_notional_usdt,
            "frozen_atr": self.frozen_atr,
            "order_count": len(self.orders),
            "orders": [order.to_dict() for order in self.orders],
        }


@dataclass(slots=True)
class PositionState:
    """Current unified short position state for one symbol."""

    symbol: str
    side: str
    quantity: float
    avg_entry_price: float | None
    opened_at: datetime | None
    anchor_price: float | None
    frozen_atr: float | None
    ladder_check_timestamp: datetime | None

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "avg_entry_price": self.avg_entry_price,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "anchor_price": self.anchor_price,
            "frozen_atr": self.frozen_atr,
            "ladder_check_timestamp": (
                self.ladder_check_timestamp.isoformat() if self.ladder_check_timestamp else None
            ),
        }
