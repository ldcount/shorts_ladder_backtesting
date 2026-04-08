from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from core.models import PendingOrder, PositionState


class PositionUpdateError(RuntimeError):
    """Raised when a position cannot be updated from a fill event."""


@dataclass(slots=True)
class PositionExitUpdate:
    position: PositionState
    realized_pnl: float


def empty_short_position(symbol: str) -> PositionState:
    """Create an empty one-way short position state for a symbol."""

    return PositionState(
        symbol=symbol,
        side="short",
        quantity=0.0,
        avg_entry_price=None,
        opened_at=None,
        anchor_price=None,
        frozen_atr=None,
        ladder_check_timestamp=None,
    )


def apply_short_fill(
    position: PositionState,
    *,
    fill_timestamp: datetime,
    fill_price: float,
    fill_quantity: float,
    order: PendingOrder,
) -> PositionState:
    """Apply one short-entry fill to the unified position and update weighted average entry."""

    if fill_price <= 0:
        raise PositionUpdateError(f"Fill price must be positive, got {fill_price}.")
    if fill_quantity <= 0:
        raise PositionUpdateError(f"Fill quantity must be positive, got {fill_quantity}.")
    if position.symbol != order.symbol:
        raise PositionUpdateError(
            f"Order symbol {order.symbol} does not match position symbol {position.symbol}."
        )

    existing_notional = (
        position.quantity * position.avg_entry_price
        if position.avg_entry_price is not None and position.quantity > 0
        else 0.0
    )
    new_quantity = position.quantity + fill_quantity
    if new_quantity <= 0:
        raise PositionUpdateError(
            f"Updated position quantity must stay positive after a short-entry fill, got {new_quantity}."
        )

    new_avg_entry_price = (existing_notional + (fill_price * fill_quantity)) / new_quantity
    return PositionState(
        symbol=position.symbol,
        side=position.side,
        quantity=new_quantity,
        avg_entry_price=new_avg_entry_price,
        opened_at=position.opened_at or fill_timestamp,
        anchor_price=position.anchor_price if position.anchor_price is not None else order.anchor_price,
        frozen_atr=position.frozen_atr if position.frozen_atr is not None else order.frozen_atr,
        ladder_check_timestamp=(
            position.ladder_check_timestamp if position.ladder_check_timestamp is not None else order.created_at
        ),
    )


def apply_short_exit(
    position: PositionState,
    *,
    exit_timestamp: datetime,
    exit_price: float,
    close_quantity: float,
) -> PositionExitUpdate:
    """Apply a short-position reduction or full close and return the updated state plus realized PnL."""

    if position.avg_entry_price is None or position.quantity <= 0:
        raise PositionUpdateError(f"Cannot exit position for {position.symbol} because no quantity is open.")
    if exit_price <= 0:
        raise PositionUpdateError(f"Exit price must be positive, got {exit_price}.")
    if close_quantity <= 0:
        raise PositionUpdateError(f"Close quantity must be positive, got {close_quantity}.")
    if close_quantity - position.quantity > 1e-12:
        raise PositionUpdateError(
            f"Cannot close {close_quantity} units from {position.symbol}; only {position.quantity} are open."
        )

    remaining_quantity = max(position.quantity - close_quantity, 0.0)
    realized_pnl = (position.avg_entry_price - exit_price) * close_quantity

    if remaining_quantity == 0:
        updated_position = PositionState(
            symbol=position.symbol,
            side=position.side,
            quantity=0.0,
            avg_entry_price=None,
            opened_at=None,
            anchor_price=None,
            frozen_atr=None,
            ladder_check_timestamp=None,
        )
    else:
        updated_position = PositionState(
            symbol=position.symbol,
            side=position.side,
            quantity=remaining_quantity,
            avg_entry_price=position.avg_entry_price,
            opened_at=position.opened_at or exit_timestamp,
            anchor_price=position.anchor_price,
            frozen_atr=position.frozen_atr,
            ladder_check_timestamp=position.ladder_check_timestamp,
        )

    return PositionExitUpdate(position=updated_position, realized_pnl=realized_pnl)
