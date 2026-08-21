import json, requests, asyncio
from datetime import datetime, timezone
from helper_functions import place_orders

POLL_INTERVAL = 2      # seconds between order-status checks
BUY_PRICE = 0.55
SELL_PRICE = BUY_PRICE + 0.10


async def get_order_status(client, order_id, which_order):
    """
    Replace this with however you're currently retrieving
    order status from the Polymarket client.

    Should return "FILLED" or "ON MARKET" (or None if we can't tell / no order exists).
    """
    if order_id is None:
        return None

    try:
        order = await asyncio.to_thread(client.get_order, order_id)
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "poll", "order_id": order_id, "errorMsg": str(e)}))
        return None

    if order:
        status = order.get("status") if isinstance(order, dict) else None
        print(json.dumps({"status": "tracking", "order_id": order_id, "Which Order": which_order, "order_status": status}))
        if status in ("FILLED", "MATCHED", "CANCELLED"):
            return "FILLED"

    return "ON MARKET"


async def cancel_order(client, order_id):
    """Cancel an order without blocking the event loop.

    ClobClient has no `.cancel()` method — the real API is
    `client.cancel_order(OrderPayload(orderID=...))`, wrapped here as
    place_orders.cancel_order_sync.
    """
    if order_id is None:
        return
    try:
        await asyncio.to_thread(place_orders.cancel_order_sync, client, order_id)
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "cancel", "order_id": order_id, "errorMsg": str(e)}))


async def place_order(client, token_id, price, deadline, side):
    """Place one order in a thread. Returns (order_id, filled) — (None, None) on failure."""
    try:
        return await asyncio.to_thread(
            place_orders.place_limit_order_sync,
            client,
            token_id,
            price,
            deadline,
            side=side
        )
    except Exception as e:
        print(json.dumps({
            "status": "error", "stage": "place",
            "token_id": token_id, "side": side, "price": price,
            "errorMsg": str(e)
        }))
        return None, None


async def place_buy_orders(client, up_token, down_token, deadline):
    """
    Only the BUY legs go out up front. We don't own any shares yet, so
    SELL orders are placed later, one at a time, as each BUY actually fills.
    """
    up_result, down_result = await asyncio.gather(
        place_order(client, up_token, BUY_PRICE, deadline, "BUY"),
        place_order(client, down_token, BUY_PRICE, deadline, "BUY"),
    )

    up_buy_id, _ = up_result
    down_buy_id, _ = down_result

    return {"up": up_buy_id, "down": down_buy_id}


def _opposite(side):
    return "down" if side == "up" else "up"


async def _try_place_sell(client, state, side, deadline):
    """Called the moment we've confirmed we hold shares on `side`. Places its SELL leg."""
    s = state[side]
    try:
        sell_id, _ = await place_order(client, s["token"], SELL_PRICE, deadline, "SELL")
        s["sell_id"] = sell_id
        if sell_id is None:
            print(f"Failed to place SELL order for {side.upper()} — no order id returned.")
        else:
            print(f"{side.upper()} BUY filled -> placed {side.upper()} SELL ({sell_id})")
    except Exception as e:
        print(f"Unexpected error placing SELL for {side.upper()}: {e}")


async def _handle_sell_filled(client, state, side):
    """When one side's SELL fills, cancel the other side's resting orders unless its BUY already filled."""
    other = _opposite(side)
    other_state = state[other]

    if other_state["buy_filled"]:
        print(f"{other.upper()} BUY already filled -> keeping {other.upper()} SELL resting")
        return

    if other_state["buy_cancelled"]:
        return

    print(f"{other.upper()} BUY not filled -> cancelling {other.upper()} BUY (and SELL if resting)")
    other_state["buy_cancelled"] = True

    cancels = [cancel_order(client, other_state["buy_id"])]
    if other_state["sell_id"]:
        cancels.append(cancel_order(client, other_state["sell_id"]))

    await asyncio.gather(*cancels, return_exceptions=True)


async def monitor_and_manage(client, state, deadline):
    """
    Single loop that:
      1. Watches resting BUY orders. The instant one fills, immediately places
         the matching SELL — we never try to sell shares we don't own yet.
      2. Watches resting SELL orders and applies the cross-side cancellation rule
         (if one side's SELL fills but the other side's BUY never did, cancel
         the other side's resting orders since the hedge didn't form).
    Runs until both sells are filled or the market deadline hits.
    """
    while datetime.now(timezone.utc).timestamp() < deadline:

        for side in ("up", "down"):
            s = state[side]

            if s["buy_cancelled"] or s["sell_filled"]:
                continue

            if not s["buy_filled"]:
                try:
                    status = await get_order_status(client, s["buy_id"], f"{side.upper()} Buy")
                except Exception as e:
                    print(f"Error checking {side.upper()} BUY status: {e}")
                    status = None

                if status == "FILLED":
                    s["buy_filled"] = True
                    print(f"{side.upper()} BUY FILLED")
                    await _try_place_sell(client, state, side, deadline)
                continue

            if s["sell_id"] and not s["sell_filled"]:
                try:
                    status = await get_order_status(client, s["sell_id"], f"{side.upper()} Sell")
                except Exception as e:
                    print(f"Error checking {side.upper()} SELL status: {e}")
                    status = None

                if status == "FILLED":
                    s["sell_filled"] = True
                    print(f"{side.upper()} SELL FILLED")
                    await _handle_sell_filled(client, state, side)

        if state["up"]["sell_filled"] and state["down"]["sell_filled"]:
            print("Both sells filled. Strategy complete.")
            break

        await asyncio.sleep(POLL_INTERVAL)
    else:
        print("Deadline reached. Stopping order monitoring.")


async def cleanup_open_orders(client, state):
    """Cancel anything still resting once we're done (deadline hit, error, or early completion)."""
    to_cancel = []
    for side in ("up", "down"):
        s = state[side]
        if not s["buy_filled"] and not s["buy_cancelled"] and s["buy_id"]:
            to_cancel.append(s["buy_id"])
        if s["sell_id"] and not s["sell_filled"]:
            to_cancel.append(s["sell_id"])

    if to_cancel:
        await asyncio.gather(*(cancel_order(client, oid) for oid in to_cancel), return_exceptions=True)


async def run_logic(client, up_token, down_token, deadline):

    state = {
        "up":   {"token": up_token,   "buy_id": None, "buy_filled": False, "buy_cancelled": False, "sell_id": None, "sell_filled": False},
        "down": {"token": down_token, "buy_id": None, "buy_filled": False, "buy_cancelled": False, "sell_id": None, "sell_filled": False},
    }

    try:
        # ---------------------------------------------------------
        # 1. Place ONLY the buy legs
        # ---------------------------------------------------------
        buy_ids = await place_buy_orders(client, up_token, down_token, deadline)
        state["up"]["buy_id"] = buy_ids["up"]
        state["down"]["buy_id"] = buy_ids["down"]

        # If a buy order flat-out failed to place, don't waste cycles polling it
        if buy_ids["up"] is None:
            print("UP BUY failed to place — that side is dead for this market.")
            state["up"]["buy_cancelled"] = True
        if buy_ids["down"] is None:
            print("DOWN BUY failed to place — that side is dead for this market.")
            state["down"]["buy_cancelled"] = True

        if buy_ids["up"] is None and buy_ids["down"] is None:
            print("Both BUY orders failed to place — aborting this market.")
            return

        print("Buy orders placed:")
        print(buy_ids)

        # ---------------------------------------------------------
        # 2. Monitor buys -> place sells as buys fill -> monitor sells
        # ---------------------------------------------------------
        await monitor_and_manage(client, state, deadline)

    except Exception as e:
        print(f"Unexpected error in run_logic: {e}")

    finally:
        # ---------------------------------------------------------
        # 3. Always try to clean up anything left resting
        # ---------------------------------------------------------
        try:
            await cleanup_open_orders(client, state)
        except Exception as e:
            print(f"Error during cleanup: {e}")