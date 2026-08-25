import time, asyncio

from helper_functions import place_orders
from . import fast_feed_client, setup

MARKET_DURATION  = 300      # 5-minute markets; unfilled legs get cancelled when the window closes
BEFORE_ENTER = 20           # How many seconds before we want to enter
MAXIMAL_LOSS = 0.10         # how much we are willing to lose per share max
MAXIMAL_BID_PLUS_ONE = 0.89 # how much I am willing to bet to stay positive 50/50 at 89 i make 1 cent a share if i win 50/50


# Keep strong references to background order tasks so they aren't garbage
# collected mid-flight, and fire them without blocking the event loop that's
# consuming the feed — a blocked loop can't see the next tick (or the next
# stop_loss push) until the order call returns.
_background_tasks = set()

def fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def enter_position(client, state, feed, token_id, ask_price, deadline_ts):
    """Place the entry order in the background, correct state['shares'] once
    the real fill amount is known, then hand the position to the C++ side to
    watch for stop-loss — it checks the threshold inline against every tick
    it parses, no round trip back to Python needed to detect the breach."""
    _order_id, filled = await asyncio.to_thread(
        place_orders.place_limit_order_sync, client, token_id, ask_price + 0.01, deadline_ts
    )
    if filled:
        state['shares'] = filled
    await feed.watch(token_id, ask_price, MAXIMAL_LOSS)


async def main():
    slug = setup.get_slug_time()

    # authenticate once up front, outside the loop, so we never re-handshake
    client = setup.setup_and_return_client()

    # One long-lived connection to the C++ feed process for the whole run.
    # Make sure `polymarket_feed` (built from the cpp/ Makefile) is already
    # running before starting this script.
    feed = fast_feed_client.FastFeed()
    await feed.connect()

    while True:
        all_ids = setup.get_clob_ids(str(slug))
        entered = {
            market: {"up": False, "down": False, "buy_price": None, "shares": None, "exited": False}
            for market in all_ids
        }
        token_to_market = {}
        for market_id, (up_token, down_token) in all_ids.items():
            token_to_market[up_token]   = (market_id, True)
            token_to_market[down_token] = (market_id, False)

        # One call replaces the entire subscribed token set — the C++ side
        # reconnects to Polymarket once with all of them, instead of
        # reconnecting once per market.
        all_tokens = [t for pair in all_ids.values() for t in pair]
        await feed.subscribe(all_tokens)

        market_time = (slug + MARKET_DURATION) * 1000  # ms, matches Polymarket timestamps

        async for evt in feed.stream():
            etype = evt.get("type")

            if etype == "stop_loss":
                token_id = evt["token_id"]
                if token_id not in token_to_market:
                    continue  # stale event for a token from a market we've already rolled past
                market_id, is_up = token_to_market[token_id]
                state = entered[market_id]
                side = "up" if is_up else "down"
                print(f"stop Loss hit for {side} trade at {evt['ask']}")
                state['exited'] = True
                state[side] = False
                sell_size = state['shares'] or place_orders.SHARES
                fire_and_forget(asyncio.to_thread(
                    place_orders.place_market_order_sync, client, token_id, "SELL", sell_size
                ))
                continue

            if etype != "tick":
                continue  # ignore "hello" and anything else

            token_id = evt["token_id"]
            if token_id not in token_to_market:
                continue  # tick for a token from a market we've already rolled past
            market_id, _is_up = token_to_market[token_id]
            state = entered[market_id]

            up_token, down_token = all_ids[market_id]
            up_ask   = feed.prices.get(up_token, {}).get("ask")
            down_ask = feed.prices.get(down_token, {}).get("ask")
            curr_time = int(time.time() * 1000)

            # if we are 20 seconds out
            if up_ask and down_ask and not (state['up'] or state['down'] or state['exited']) and (market_time - curr_time) <= (BEFORE_ENTER * 1000):
                # place a buy order if less than 89, on whichever side is higher
                if up_ask > down_ask and up_ask < MAXIMAL_BID_PLUS_ONE:
                    state['up'] = True
                    state['buy_price'] = up_ask
                    state['shares'] = place_orders.SHARES
                    fire_and_forget(enter_position(client, state, feed, up_token, up_ask, market_time / 1000))
                    print(f"entered an up trade at price: {up_ask}\nWith up_ask : {up_ask} and down_ask : {down_ask}")
                elif down_ask > up_ask and down_ask < MAXIMAL_BID_PLUS_ONE:
                    state['down'] = True
                    state['buy_price'] = down_ask
                    state['shares'] = place_orders.SHARES
                    fire_and_forget(enter_position(client, state, feed, down_token, down_ask, market_time / 1000))
                    print(f"entered a down trade at price: {down_ask}\nWith up_ask : {up_ask} and down_ask : {down_ask}")

            if curr_time >= market_time:
                print("the moment for next market has begun")
                # Clean up any still-open watch before its tokens go stale —
                # harmless if left behind, but tidy.
                for m_id, (up_t, down_t) in all_ids.items():
                    st = entered[m_id]
                    if st['up']:
                        await feed.unwatch(up_t)
                    if st['down']:
                        await feed.unwatch(down_t)
                break

        slug = slug + MARKET_DURATION


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down...")