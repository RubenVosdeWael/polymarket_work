"""
Confirms whether create_and_post_market_order() has its OWN order_type
parameter separate from MarketOrderArgs.order_type — if so, that's why
setting OrderType.FAK on the args object had no effect and orders kept
being submitted as FOK.

Usage:
    python check_market_order_signature.py
"""
import inspect
import sys

sys.path.insert(0, ".")
from . import setup

client = setup.setup_and_return_client()

for name in ("create_and_post_market_order", "create_market_order", "post_order"):
    method = getattr(client, name, None)
    if method is None:
        print(f"{name}: not found on client")
        continue
    print(f"\n=== {name} ===")
    try:
        print("  signature:", inspect.signature(method))
    except (TypeError, ValueError) as e:
        print(f"  signature unavailable: {e}")
    try:
        print("  source:")
        print(inspect.getsource(method))
    except OSError:
        print("  (source not available)")