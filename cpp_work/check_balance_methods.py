"""
Every fix so far for the "not enough balance" cascades has been reactive:
parse whatever error message the server happens to send back after a
failed order. That's now failed twice for two different message shapes.

The more robust fix is to stop guessing from error text entirely and query
actual balance directly before attempting to sell. This script looks for
a method on your ClobClient that does that.

Usage:
    python check_balance_methods.py
"""
import sys, inspect

sys.path.insert(0, ".")
from . import setup


def main():
    client = setup.setup_and_return_client()

    candidates = [m for m in dir(client) if ('balance' in m.lower() or 'allowance' in m.lower())
                  and not m.startswith('_')]

    if not candidates:
        print("No obviously-named balance/allowance methods found on this client.")
        print("Full method list for manual inspection:")
        print([m for m in dir(client) if not m.startswith('_')])
        return

    print("Candidate balance-related methods:", candidates)
    for name in candidates:
        method = getattr(client, name)
        print(f"\n=== {name} ===")
        try:
            print("  signature:", inspect.signature(method))
        except (TypeError, ValueError):
            print("  (signature unavailable)")
        try:
            print("  docstring:", (method.__doc__ or "").strip()[:300])
        except Exception:
            pass

    print("\nOnce you find the right one, try calling it for a token you "
          "currently hold and compare the result against what the exchange "
          "actually shows in your positions tab — that confirms both the "
          "method AND its unit convention (raw shares vs micro-units) "
          "before we wire it into liquidate()/place_orders.py.")


if __name__ == "__main__":
    main()