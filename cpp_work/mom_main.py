import time, json, asyncio

from . import setup, place_orders, fast_feed_client
from py_clob_client_v2.clob_types import OrderPayload

MARKET_DURATION      = 300
ENTRY_DELAY_SEC      = 20
TRADE_WINDOW_SEC     = 180
FINAL_STOP_SEC       = 45
MAXIMAL_LOSS         = 0.10   # ← FIXED: was 1 (100%), now 0.10 (10 cents)
SPREAD_HALF          = 0.01
MAXIMAL_BID_PLUS_ONE = 0.89
POLL_INTERVAL        = 1
BUY_FILL_TIMEOUT_SEC = 15
CANCEL_CONFIRM_TIMEOUT_SEC = 10
MAX_LIQUIDATE_ATTEMPTS = 6

# NEW: Re-quoting parameters
REQUOTE_INTERVAL_SEC   = 3    # check if sell needs re-pricing every 3s
REQUOTE_THRESHOLD      = 0.02 # re-quote if bid moved more than 2 cents from original

_background_tasks = set()

def fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


FEED_CMD_TIMEOUT_SEC = 5

async def _safe_watch(feed, token_id, buy_price, max_loss):
    try:
        return await asyncio.wait_for(feed.watch(token_id, buy_price, max_loss), timeout=FEED_CMD_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        print(json.dumps({"status": "error", "stage": "watch_timeout", "token_id": token_id}))
        return False

async def _safe_unwatch(feed, token_id):
    try:
        await asyncio.wait_for(feed.unwatch(token_id), timeout=FEED_CMD_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        print(json.dumps({"status": "error", "stage": "unwatch_timeout", "token_id": token_id}))


async def liquidate(client, token_id, shares, deadline_ts=None):
    """Sell shares via market order(s). Now has a deadline to prevent infinite retry."""
    remaining = shares
    for attempt in range(1, MAX_LIQUIDATE_ATTEMPTS + 1):
        if remaining <= 0:
            return
        if deadline_ts is not None and time.time() >= deadline_ts:
            print(json.dumps({"status": "error", "stage": "liquidate_deadline",
                               "token_id": token_id, "remaining": remaining}))
            return
        _order_id, filled = await asyncio.to_thread(
            place_orders.place_market_order_sync, client, token_id, "SELL", remaining
        )
        remaining = max(remaining - (filled or 0.0), 0.0)
        if remaining <= 0:
            return
        print(json.dumps({"status": "liquidate_retry", "token_id": token_id,
                           "attempt": attempt, "remaining": remaining}))
        await asyncio.sleep(POLL_INTERVAL)

    if remaining > 0:
        print(json.dumps({"status": "error", "stage": "liquidate_incomplete", "token_id": token_id,
                           "remaining": remaining,
                           "errorMsg": "COULD NOT FULLY LIQUIDATE - CHECK THIS TOKEN MANUALLY"}))


def get_order_status_sync(client, order_id):
    try:
        return client.get_order(order_id)
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "poll", "order_id": order_id, "errorMsg": str(e)}))
        return None


def cancel_order_sync(client, order_id):
    try:
        client.cancel_order(OrderPayload(orderID=order_id))
        return True
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "cancel", "order_id": order_id, "errorMsg": str(e)}))
        return False


def cancel_and_confirm(client, order_id):
    cancel_order_sync(client, order_id)
    deadline = time.time() + CANCEL_CONFIRM_TIMEOUT_SEC
    while time.time() < deadline:
        order = get_order_status_sync(client, order_id)
        if order and not _order_is_live(order):
            filled = _filled_shares(order)
            print(json.dumps({"status": "cancel_resolved", "order_id": order_id,
                               "order_status": _order_status(order), "filled": filled}))
            return "resolved", filled
        time.sleep(POLL_INTERVAL)
    print(json.dumps({"status": "error", "stage": "cancel_confirm", "order_id": order_id,
                       "errorMsg": "timed out waiting for cancel confirmation"}))
    return "unknown", 0.0


def _order_status(order):
    return str(order.get("status", "")).upper()

def _filled_shares(order):
    for key in ("size_matched", "sizeMatched", "matchedAmount", "filledSize", "matched_size"):
        val = order.get(key)
        if val not in (None, "", 0, "0"):
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0

def _order_is_live(order):
    return _order_status(order) == "LIVE"


def _current_bid(feed, token_id):
    """Get the latest known bid for a token, or None."""
    return feed.prices.get(token_id, {}).get("bid")


async def run_position(client, feed, positions, market_id, token_id, buy_price, sell_price,
                        order_deadline_ts, final_stop_ts, stop_event):
    """Buy → watch → sell (with re-quoting) → close."""

    # ============ PHASE 1: BUY ============
    shares = 0.0
    max_buy_retries = 4  # ← NEW: cap retries to avoid infinite loop

    for buy_attempt in range(1, max_buy_retries + 1):
        now = time.time()
        if now >= final_stop_ts or now >= order_deadline_ts or stop_event.is_set():
            positions[market_id] = None
            return

        order_id, filled = await asyncio.to_thread(
            place_orders.place_limit_order_sync, client, token_id, buy_price, order_deadline_ts, "BUY"
        )
        if order_id is None:
            positions[market_id] = None
            return

        shares = filled or 0.0
        placed_at = time.time()

        # Inner loop: wait for this specific order to fill or timeout
        while shares <= 0:
            now = time.time()
            if now >= final_stop_ts or now >= order_deadline_ts or stop_event.is_set():
                result, filled_amt = await asyncio.to_thread(cancel_and_confirm, client, order_id)
                if filled_amt > 0:
                    await liquidate(client, token_id, filled_amt, final_stop_ts)
                positions[market_id] = None
                return

            if now - placed_at >= BUY_FILL_TIMEOUT_SEC:
                result, filled_amt = await asyncio.to_thread(cancel_and_confirm, client, order_id)
                if result == "unknown":
                    positions[market_id] = None
                    return
                if filled_amt > 0:
                    shares = filled_amt
                    break
                # Re-price based on current bid and loop back
                latest_bid = _current_bid(feed, token_id)
                if latest_bid:
                    buy_price  = round(latest_bid - SPREAD_HALF, 2)
                    sell_price = round(latest_bid + SPREAD_HALF, 2)
                break

            order = await asyncio.to_thread(get_order_status_sync, client, order_id)
            if order:
                filled_now = _filled_shares(order)
                if not _order_is_live(order):
                    if filled_now <= 0:
                        # Order cancelled/expired with no fill — try re-pricing
                        break
                    shares = filled_now
                    break
                elif filled_now > 0:
                    result, confirmed_filled = await asyncio.to_thread(cancel_and_confirm, client, order_id)
                    if result == "unknown":
                        positions[market_id] = None
                        return
                    shares = confirmed_filled if confirmed_filled > 0 else filled_now
                    break
            await asyncio.sleep(POLL_INTERVAL)

        if shares > 0:
            break  # got a fill, exit retry loop

    if shares <= 0:
        positions[market_id] = None
        return

    # ============ PHASE 2: PROTECT ============
    positions[market_id]["shares"] = shares
    positions[market_id]["token_id"] = token_id
    positions[market_id]["buy_price"] = buy_price

    watched_ok = await _safe_watch(feed, token_id, buy_price, MAXIMAL_LOSS)
    if not watched_ok:
        print(json.dumps({"status": "error", "stage": "watch_unconfirmed", "token_id": token_id}))
        await liquidate(client, token_id, shares, final_stop_ts)
        positions[market_id] = None
        return

    if stop_event.is_set():
        await liquidate(client, token_id, shares, final_stop_ts)
        await _safe_unwatch(feed, token_id)
        positions[market_id] = None
        return

    # ============ PHASE 3: SELL (WITH RE-QUOTING) ============
    # ← THIS IS THE KEY FIX. Instead of placing one sell and waiting forever,
    #   we continuously re-quote the sell at the current bid + spread.
    #   If the bid moves against us, we follow it. If it moves in our favor,
    #   we tighten to bank the profit.

    current_sell_price = sell_price
    sell_order_id = None
    last_requote_ts = 0.0

    while True:
        now = time.time()

        # Hard exit conditions
        if now >= final_stop_ts or stop_event.is_set():
            if sell_order_id:
                result, filled_amt = await asyncio.to_thread(cancel_and_confirm, client, sell_order_id)
                remaining = max(shares - filled_amt, 0.0)
            else:
                remaining = shares
            if remaining > 0:
                await liquidate(client, token_id, remaining, final_stop_ts)
            await _safe_unwatch(feed, token_id)
            positions[market_id] = None
            return

        # Check if sell order filled
        if sell_order_id:
            order = await asyncio.to_thread(get_order_status_sync, client, sell_order_id)
            if order and not _order_is_live(order):
                filled_now = _filled_shares(order)
                remaining = max(shares - filled_now, 0.0)
                if remaining > 0:
                    await liquidate(client, token_id, remaining, final_stop_ts)
                await _safe_unwatch(feed, token_id)
                positions[market_id] = None
                return
            # If the sell order was cancelled externally or expired with no fill,
            # fall through to the re-quote logic below.
            elif order and _order_status(order) in ("CANCELED", "EXPIRED", "CANCELLED"):
                sell_order_id = None  # need to re-place

        # ← RE-QUOTING LOGIC
        # Every REQUOTE_INTERVAL_SEC, check if the bid has moved enough to warrant
        # a new sell price. Cancel the old sell, place a new one.
        if now - last_requote_ts >= REQUOTE_INTERVAL_SEC:
            latest_bid = _current_bid(feed, token_id)
            if latest_bid:
                new_sell_price = round(latest_bid + SPREAD_HALF, 2)

                # Only re-quote if the price actually moved meaningfully
                if abs(new_sell_price - current_sell_price) >= REQUOTE_THRESHOLD:
                    # Cancel the old sell (best-effort)
                    if sell_order_id:
                        cancel_order_sync(client, sell_order_id)
                        # Brief wait to confirm cancel
                        await asyncio.sleep(0.5)

                    # Guard against invalid prices
                    if new_sell_price < 0.01 or new_sell_price > 0.99:
                        new_sell_price = max(0.01, min(0.99, new_sell_price))

                    # Place new sell
                    sell_order_id, _ = await asyncio.to_thread(
                        place_orders.place_limit_order_sync,
                        client, token_id, new_sell_price, order_deadline_ts, "SELL", shares
                    )
                    current_sell_price = new_sell_price
                    last_requote_ts = now
                    positions[market_id]["sell_order_id"] = sell_order_id

                    print(json.dumps({
                        "status": "sell_requoted", "token_id": token_id,
                        "old_price": sell_price if new_sell_price != sell_price else None,
                        "new_price": new_sell_price, "bid": latest_bid,
                        "shares": shares
                    }))
                else:
                    last_requote_ts = now  # still update timestamp to avoid hammering

        # Initial sell placement (first iteration)
        if sell_order_id is None and last_requote_ts == 0.0:
            latest_bid = _current_bid(feed, token_id)
            if latest_bid:
                sell_price = round(latest_bid + SPREAD_HALF, 2)
            if 0.01 < sell_price < 0.99:
                sell_order_id, _ = await asyncio.to_thread(
                    place_orders.place_limit_order_sync,
                    client, token_id, sell_price, order_deadline_ts, "SELL", shares
                )
                current_sell_price = sell_price
                last_requote_ts = time.time()
                positions[market_id]["sell_order_id"] = sell_order_id

        await asyncio.sleep(POLL_INTERVAL)


async def main():
    slug = setup.get_slug_time()
    client = setup.setup_and_return_client()

    feed = fast_feed_client.FastFeed()
    await feed.connect()

    while True:
        try:
            all_ids = await asyncio.to_thread(setup.get_clob_ids, str(slug))
        except Exception as e:
            print(json.dumps({"status": "error", "stage": "get_clob_ids_fatal", "slug": slug, "errorMsg": str(e)}))
            await asyncio.sleep(5)
            continue

        positions = {market_id: None for market_id in all_ids}

        token_to_market = {}
        for market_id, (up_token, down_token) in all_ids.items():
            token_to_market[up_token]   = market_id
            token_to_market[down_token] = market_id

        all_tokens = [t for pair in all_ids.values() for t in pair]
        await feed.subscribe(all_tokens)

        window_start_ts = slug
        entry_open_from  = window_start_ts + ENTRY_DELAY_SEC
        entry_open_until = window_start_ts + TRADE_WINDOW_SEC
        market_end_ts    = window_start_ts + MARKET_DURATION
        final_stop_ts    = market_end_ts - FINAL_STOP_SEC
        market_time      = market_end_ts * 1000

        # ← NEW: Clear stale prices on (re)subscription to avoid decisions on old data
        feed.prices.clear()

        async for evt in feed.stream():
            try:
                etype = evt.get("type")

                if etype == "hello":
                    feed.prices.clear()  # ← FIX: clear stale prices on reconnect
                    for m_id, pos in positions.items():
                        if pos and pos.get("shares") and pos.get("token_id"):
                            ok = await _safe_watch(feed, pos["token_id"], pos["buy_price"], MAXIMAL_LOSS)
                            print(json.dumps({"status": "watch_rearmed" if ok else "watch_rearm_failed",
                                               "token_id": pos["token_id"]}))
                    continue

                if etype == "stop_loss":
                    token_id = evt["token_id"]
                    market_id = token_to_market.get(token_id)
                    pos = positions.get(market_id) if market_id else None
                    if not pos:
                        continue
                    print(f"stop Loss hit for {token_id} at {evt['ask']}")
                    pos["stop_event"].set()
                    continue

                if etype != "tick":
                    continue

                token_id = evt["token_id"]
                market_id = token_to_market.get(token_id)
                if market_id is None:
                    continue

                curr_time = time.time()
                curr_time_ms = int(curr_time * 1000)

                # Allow re-entry after a stopped-out position (NEW)
                if (positions[market_id] is None
                        and entry_open_from <= curr_time <= entry_open_until):
                    up_token, down_token = all_ids[market_id]
                    up_bid   = feed.prices.get(up_token, {}).get("bid")
                    down_bid = feed.prices.get(down_token, {}).get("bid")

                    chosen = None
                    if up_bid and down_bid:
                        if up_bid > down_bid and up_bid < MAXIMAL_BID_PLUS_ONE:
                            chosen = (up_token, up_bid, "UP")
                        elif down_bid > up_bid and down_bid < MAXIMAL_BID_PLUS_ONE:
                            chosen = (down_token, down_bid, "DOWN")

                    if chosen:
                        tok, bid, label = chosen
                        buy_price  = round(bid - SPREAD_HALF, 2)
                        sell_price = round(bid + SPREAD_HALF, 2)

                        if buy_price <= 0 or sell_price >= 1:
                            continue

                        stop_event = asyncio.Event()
                        positions[market_id] = {"stop_event": stop_event, "sell_order_id": None,
                                                 "shares": None, "token_id": None, "buy_price": None}
                        fire_and_forget(run_position(
                            client, feed, positions, market_id, tok, buy_price, sell_price,
                            market_end_ts, final_stop_ts, stop_event
                        ))
                        print(f"market making {label} at bid {bid}: buy {buy_price:.2f} / sell {sell_price:.2f}")

                if curr_time_ms >= market_time:
                    print("the moment for next market has begun")
                    for m_id, pos in positions.items():
                        if pos:
                            pos["stop_event"].set()
                    # ← NEW: Wait briefly for background tasks to start their exit
                    await asyncio.sleep(2)
                    break

            except Exception as e:
                print(json.dumps({"status": "error", "stage": "event_handling",
                                   "errorMsg": str(e), "evt": str(evt)[:200]}))
                continue

        slug = slug + MARKET_DURATION


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down...")
