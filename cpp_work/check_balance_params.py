"""
Confirms the shape of BalanceAllowanceParams and what get_balance_allowance
actually returns, so we can wire in a real ground-truth balance check before
liquidate() retries instead of continuing to infer "what do I actually
hold" from order-response fields — which has now been wrong twice this
session (wrong field name, then wrong field for the side).

Usage:
    python check_balance_params.py <token_id>

Pass a token_id you currently hold (or held recently) so the actual
returned numbers can be sanity-checked against what you see in your
Polymarket positions tab.
"""
import sys, inspect

sys.path.insert(0, ".")
from helper_functions import setup
from py_clob_client_v2.clob_types import BalanceAllowanceParams


def main():
    print("=== BalanceAllowanceParams ===")
    try:
        print("  signature:", inspect.signature(BalanceAllowanceParams))
    except (TypeError, ValueError) as e:
        print(f"  signature unavailable: {e}")
    try:
        print("  fields:", list(inspect.signature(BalanceAllowanceParams).parameters.keys()))
    except (TypeError, ValueError):
        pass
    try:
        print("  source:")
        print(inspect.getsource(BalanceAllowanceParams))
    except OSError:
        print("  (source not available)")

    if len(sys.argv) < 2:
        print("\nNo token_id passed — skipping the live call. Re-run with a "
              "token_id you hold to see a real response:")
        print("  python check_balance_params.py <token_id>")
        return

    token_id = sys.argv[1]
    client = setup.setup_and_return_client()

    print(f"\n=== Trying get_balance_allowance for token {token_id} ===")
    # Try the most likely construction; if BalanceAllowanceParams needs
    # different fields, the signature above will tell us what to change here.
    for kwargs in (
        {"token_id": token_id},
        {"asset_type": "CONDITIONAL", "token_id": token_id},
    ):
        try:
            params = BalanceAllowanceParams(**kwargs)
            result = client.get_balance_allowance(params)
            print(f"  kwargs={kwargs} -> {result!r}")
        except Exception as e:
            print(f"  kwargs={kwargs} -> FAILED: {e}")


if __name__ == "__main__":
    main()