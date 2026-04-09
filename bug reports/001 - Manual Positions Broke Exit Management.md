# 001 - Manual Positions Broke Exit Management

## Summary

Two related production bugs were identified while investigating missing exit management on `BLURUSDT`, `RAVEUSDT`, and a manually opened `ENJUSDT` position.

The first bug was that the bot always called Bybit `set_trading_stop` on every monitor pass, even when the desired stop-loss was already set on the exchange. Bybit responded with `ErrCode 34040` (`not modified`), and the bot treated that response as a fatal error. This crashed the monitor loop and restarted the service repeatedly, preventing later symbols from being processed.

The second bug was that the bot attempted to manage TP and SL orders for live positions that were not opened by the bot itself. In practice, a manual `ENJUSDT` position was picked up by the monitor, and the bot tried to place bot-style TP orders for it. That caused a second fatal Bybit error (`ErrCode 110017`, order quantity would be truncated to zero), which also crashed the service.

## Symptoms

- `BLURUSDT` stop-loss updates repeatedly failed with Bybit `ErrCode 34040`.
- `RAVEUSDT` had filled entry orders but no TP1 or TP2 orders for a period of time because the monitor loop could be interrupted before reaching it.
- A manual `ENJUSDT` position interfered with the bot and caused repeated service restarts.
- `backtesting-codex.service` entered restart loops instead of completing monitor passes.

## Root Cause

1. Stop-loss synchronization was not idempotent.
   The monitor loop always tried to re-apply the same stop-loss through `set_trading_stop`, and the unchanged response from Bybit was treated as an exception instead of a no-op.

2. Exit management did not distinguish bot-managed positions from manual positions.
   If a live short existed, the bot tried to manage it as if it had opened it, even when no bot-tagged execution history could be reconstructed for that position.

## Solution

### 1. Make stop-loss sync idempotent

Changed the stop-loss update path in `new.py` so the bot:

- reads the current live stop-loss from the Bybit position snapshot
- skips the API call when the current stop already matches the desired stop
- treats Bybit `ErrCode 34040` (`not modified`) as a no-op instead of a fatal error

This prevents unchanged stop-loss updates from crashing the monitor loop.

### 2. Ignore manual positions for exit management

Changed the live position handling in `new.py` so the bot:

- reconstructs `position_opened_at` only from bot-tagged execution history
- marks positions with no reconstructable bot-managed open time as `unmanaged_live_position`
- skips TP/SL/time-stop management for those unmanaged positions
- cancels any leftover bot-managed exit orders attached to such positions

This ensures manual positions do not receive bot TP or SL management and cannot break the service.

## Outcome

After the fixes:

- `BLURUSDT` remained managed correctly with live SL and TP1/TP2 orders.
- `RAVEUSDT` regained normal management and now has SL and TP1/TP2 orders.
- `ENJUSDT` is explicitly skipped as an unmanaged live position.
- `backtesting-codex.service` stays up and completes monitor passes normally.

## Tests Added

Added regression coverage in `tests/test_stop_loss_management.py` for:

- unchanged stop-loss skipping the Bybit API call
- Bybit `ErrCode 34040` being treated as a no-op
- unmanaged live positions being skipped for exit management
