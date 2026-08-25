"""
A real incident showed a market order submitted with order_type=OrderType.FAK
being rejected by the server with:
    "order couldn't be fully filled. FOK orders are fully filled or killed."

That message can only be returned for an order the SERVER is treating as
FOK — meaning either OrderType.FAK doesn't genuinely differ from
OrderType.FOK in this py_clob_client_v2 build (aliased to the same
underlying value), or something in create_and_post_market_order maps it
back to FOK before submission.

This prints every OrderType member and its underlying value with no
network calls and no risk — pure introspection.

Usage:
    python check_order_types.py
"""
from py_clob_client_v2.clob_types import OrderType

print("All OrderType members:")
for name in dir(OrderType):
    if name.startswith('_'):
        continue
    val = getattr(OrderType, name)
    print(f"  {name} = {val!r}")

fak = getattr(OrderType, "FAK", None)
fok = getattr(OrderType, "FOK", None)

print(f"\nOrderType.FAK exists: {fak is not None}")
print(f"OrderType.FOK exists: {fok is not None}")
if fak is not None and fok is not None:
    print(f"OrderType.FAK == OrderType.FOK ? {fak == fok}")
    print(f"OrderType.FAK is OrderType.FOK ? {fak is fok}")
    print(f"repr(FAK) = {fak!r}, repr(FOK) = {fok!r}")
    if fak == fok or fak is fok:
        print("\n*** FAK and FOK are the same value in this build. ***")
        print("This confirms market orders are always effectively FOK here —")
        print("place_orders.py's comments claiming FAK behavior are wrong for")
        print("this build and need to be revisited (e.g. sizing requests")
        print("conservatively enough to reliably fill in one shot, or finding")
        print("whatever this library's real IOC/partial-fill option is called).")
    else:
        print("\nFAK and FOK are genuinely distinct here — the earlier incident's")
        print("rejection needs a different explanation (check create_market_order/")
        print("calculate_market_price in the installed py_clob_client_v2 source)")