"""
Verifies two pricing/sizing assumptions baked into main.py and place_orders.py:

  1. That prices are valid at 1-cent (0.01) increments — main.py rounds all
     buy/sell prices to round(x, 2). If Polymarket enforces a finer tick
     size for these markets (e.g. 0.001), some computed prices could be
     silently invalid or get rejected.

  2. That 5 shares (place_orders.SHARES) really is at/above the minimum
     order size for BOTH outcome tokens of these specific markets, and
     that it's a valid size increment (not everything requires whole-share
     multiples on every platform).

This hits Polymarket's public Gamma API — no auth, no order placement, pure
read-only.

Usage:
    python check_market_metadata.py
"""
import sys, json, time
import requests

sys.path.insert(0, ".")
from helper_functions import setup


def main():
    unix_time = str(setup.get_slug_time())
    for market in setup.markets:
        slug = market + unix_time
        url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
        try:
            resp = requests.get(url, timeout=10).json()
        except Exception as e:
            print(f"{market}: ERROR fetching {url}: {e}")
            continue

        try:
            m = resp["markets"][0]
        except (KeyError, IndexError):
            print(f"{market}: unexpected response shape: {json.dumps(resp)[:300]}")
            continue

        print(f"\n=== {market} ({m.get('conditionId')}) ===")
        # Print every key that plausibly relates to tick size / min order
        # size / min tick, since the exact field name varies by API version.
        interesting_substrings = ("tick", "min", "increment", "precision", "decimal")
        found_any = False
        for key, val in m.items():
            if any(s in key.lower() for s in interesting_substrings):
                print(f"  {key}: {val}")
                found_any = True
        if not found_any:
            print("  (no tick-size/min-order-size-looking fields found in this "
                  "response — check the full dump below, or check Polymarket's "
                  "CLOB API docs / a GET to the CLOB market endpoint directly)")

        print("\n  Full market object for manual inspection:")
        print(json.dumps(m, indent=2, default=str))


if __name__ == "__main__":
    main()