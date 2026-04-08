from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import get_config
from data.bybit_client import BybitClient, BybitClientError

ENTRY_ORDER_PREFIXES = ("cxent_", "cxlad_")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect startup entry orders from production_cycle.json and optional live Bybit state."
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        default=Path("outputs/production_cycle.json"),
        help="Path to the production cycle summary JSON.",
    )
    parser.add_argument(
        "--live-open-orders",
        action="store_true",
        help="Also fetch currently live managed entry orders from Bybit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data = json.loads(args.summary_path.read_text(encoding="utf-8"))

    placed_orders = [
        action
        for action in data["entry_order_actions"]["actions"]
        if action["action"] == "place_entry"
    ]
    skipped_symbols = [
        symbol_plan
        for symbol_plan in data["ladder_plan"]["symbols"]
        if symbol_plan.get("status") != "ready"
    ]

    selected_symbol_count = int(data["universe_selection"]["selected_symbol_count"])
    configured_level_count = _infer_level_count(data)
    ideal_order_count = selected_symbol_count * configured_level_count

    print(f"Summary file: {args.summary_path}")
    print(f"Selected symbols: {selected_symbol_count}")
    print(f"Configured ladder levels: {configured_level_count}")
    print(f"Ideal startup entry orders: {ideal_order_count}")
    print(f"Placed startup entry orders: {len(placed_orders)}")
    print(f"Missing startup entry orders: {ideal_order_count - len(placed_orders)}")
    print()

    print("Placement counts by action:")
    action_counts = Counter(action["action"] for action in data["entry_order_actions"]["actions"])
    for action_name, count in sorted(action_counts.items()):
        print(f"  {action_name}: {count}")
    print()

    if skipped_symbols:
        print("Skipped symbols:")
        for symbol_plan in skipped_symbols:
            print(
                f"  {symbol_plan['symbol']}: {symbol_plan['status']} | "
                f"{symbol_plan.get('message', 'no message')}"
            )
        print()

    print("Placed startup entry orders:")
    for order in sorted(placed_orders, key=lambda item: (item["symbol"], item["level_index"])):
        print(
            f"  {order['symbol']} L{order['level_index']} "
            f"price={order['price']} qty={order['quantity']} "
            f"link={order['order_link_id']}"
        )

    if args.live_open_orders:
        print()
        print("Live managed entry orders:")
        _print_live_open_orders()

    return 0


def _infer_level_count(data: dict[str, object]) -> int:
    ready_symbols = [
        symbol_plan
        for symbol_plan in data["ladder_plan"]["symbols"]
        if symbol_plan.get("status") == "ready"
    ]
    if ready_symbols:
        return max(len(symbol_plan["ladder"]["levels"]) for symbol_plan in ready_symbols)

    placed_orders = [
        action
        for action in data["entry_order_actions"]["actions"]
        if action["action"] == "place_entry"
    ]
    if placed_orders:
        return max(int(action["level_index"]) for action in placed_orders)

    return len(get_config().ladder_percents)


def _print_live_open_orders() -> None:
    config = get_config()
    if not config.bybit_api_key or not config.bybit_api_secret:
        print("  BYBIT_API_KEY / BYBIT_API_SECRET are not configured.")
        return

    client = BybitClient(
        testnet=config.bybit_testnet,
        demo=config.bybit_demo,
        api_key=config.bybit_api_key,
        api_secret=config.bybit_api_secret,
        log_requests=config.bybit_log_requests,
    )
    try:
        orders = client.get_open_orders(settle_coin="USDT")
    except BybitClientError as exc:
        print(f"  Failed to fetch live open orders: {exc}")
        return

    managed_orders = [
        order
        for order in orders
        if order.order_link_id.startswith(ENTRY_ORDER_PREFIXES)
    ]
    if not managed_orders:
        print("  No managed entry orders are currently open.")
        return

    for order in sorted(managed_orders, key=lambda item: (item.symbol, item.order_link_id)):
        print(
            f"  {order.symbol} price={order.price} qty={order.qty} "
            f"status={order.order_status} link={order.order_link_id}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
