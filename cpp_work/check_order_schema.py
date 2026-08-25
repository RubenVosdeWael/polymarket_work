"""
Run this BEFORE trusting the bot with real size again.

It doesn't place any orders — it just fetches and pretty-prints whatever
order IDs you give it, so you can see the REAL shape of client.get_order()
and confirm (or correct) the field-name guesses in main.py's
_extract_filled_shares().

Usage:
    python check_order_schema.py <order_id_1> [<order_id_2> ...]

Grab a couple of real order IDs straight out of your own logs — ideally one
that ended up "matched" (fully filled) and, if you can find one, one that
was resized/partially filled before being cancelled (search your logs for
"resizing_order" or "cancel_confirmed" entries — the order_id is right
there). That combination is exactly what determines whether
_extract_filled_shares in main.py is reading the right field.

What to look for in the output:
  - Is there a field representing "how much of this order has matched so
    far"? What is it actually called? (main.py currently tries:
    size_matched, sizeMatched, matchedAmount, filledSize, matched_size)
  - What are ALL the distinct values you see in "status" across a few
    different orders? main.py currently only recognizes "LIVE" (implicitly,
    by not matching anything else), "MATCHED", "FILLED", "CANCELLED". If
    there's something like "PARTIALLY_FILLED" that's a status main.py
    doesn't currently special-case anywhere.
"""
import sys
import json

sys.path.insert(0, ".")  # adjust if this script isn't run from your project root
from . import setup


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    client = setup.setup_and_return_client()

    for order_id in sys.argv[1:]:
        print(f"\n=== {order_id} ===")
        try:
            order = client.get_order(order_id)
        except Exception as e:
            print(f"  ERROR fetching order: {e}")
            continue

        print(json.dumps(order, indent=2, default=str))

        if isinstance(order, dict):
            print("\n  Keys present:", sorted(order.keys()))
        else:
            print(f"\n  NOTE: get_order() returned a {type(order)}, not a dict — "
                  f"main.py's order.get(...) calls will break on this. "
                  f"You'll need to adapt _extract_filled_shares/get_order_status_sync "
                  f"to however this object actually exposes its fields.")


if __name__ == "__main__":
    main()