import requests, json, os, time, math, asyncio, traceback

from helper_functions import setup, websocket_work, place_orders
from datetime import datetime, timezone

POLL_INTERVAL    = 2      # seconds between order-status checks
MARKET_DURATION  = 300    # 5-minute markets; unfilled legs get cancelled when the window closes
BEFORE_ENTER = 20           # How many seconds before we want to enter
MAXIMAL_LOSS = 0.10         # how much we are willing to lose per share max
MAXIMAL_BID_PLUS_ONE = 0.89 # how much I am willing to bet to stay positive 50/50 at 89 i make 1 cent a share if i win 50/50




def get_order_status_sync(client, order_id):
    try:
        return client.get_order(order_id)
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "poll", "order_id": order_id, "errorMsg": str(e)}))
        return None


def cancel_order_sync(client, order_id):
    try:
        client.cancel(order_id)
        print(json.dumps({"status": "cancelled", "order_id": order_id}))
    except Exception as e:
        print(json.dumps({"status": "error", "stage": "cancel", "order_id": order_id, "errorMsg": str(e)}))


async def get_top_of_book_bid(token):
    url = f'https://clob.polymarket.com/book?token_id={token}'

    response = requests.get(url).json()

    return float(response['bids'][-1]['price'])




async def track_order(client, order_id, token_id, deadline_ts):
    """Poll a resting order until it fills/cancels, or cancel it once the market window closes."""
    if order_id is None:
        return

    while True:
        now   = datetime.now(timezone.utc).timestamp()
        order = await asyncio.to_thread(get_order_status_sync, client, order_id)

        if order:
            status = order.get("status") if isinstance(order, dict) else None
            print(json.dumps({"status": "tracking", "order_id": order_id, "token_id": token_id, "order_status": status}))
            if status in ("FILLED", "MATCHED", "CANCELLED"):
                break

        if now >= deadline_ts:
            # market window is closing — don't let a stale limit order carry into the next one
            await asyncio.to_thread(cancel_order_sync, client, order_id)
            break

        await asyncio.sleep(POLL_INTERVAL)


async def purchase_side(client, token_id, price, deadline_ts):
    order_id, _filled = await asyncio.to_thread(place_orders.place_limit_order_sync, client, token_id, price, deadline_ts)
    await track_order(client, order_id, token_id, deadline_ts)


# Keep strong references to background order tasks so they aren't garbage
# collected mid-flight, and fire them without blocking the tick loop that's
# watching prices — a blocked loop is a loop that can't see the next price
# update (or check other markets' stop-losses) until the order call returns.
_background_tasks = set()

def fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def enter_position(client, state, token_id, price, deadline_ts):
    """Place the entry order in the background and correct state['shares']
    once we know the real fill amount, without blocking the tick loop."""
    _order_id, filled = await asyncio.to_thread(
        place_orders.place_limit_order_sync, client, token_id, price, deadline_ts
    )
    if filled:
        state['shares'] = filled


async def main():
    slug = setup.get_slug_time()

    # authenticate once up front, outside the loop, so we never re-handshake
    client = setup.setup_and_return_client()
    while True:
        all_ids = setup.get_clob_ids(str(slug))
        entered = {
            market: {"up": False, "down": False, "buy_price": None, "shares": None, "exited": False}
            for market in all_ids
        }

        # the start time for the next market
        market_time = (slug + MARKET_DURATION) * 1000
        # check to see if the connection was dropped or if we finished the market
        window_complete = False
        while not window_complete:
            ws_stream = websocket_work.monitor_top_bid_ask(all_ids)
            try:
                async for market in ws_stream:
                    # print(json.dumps(market, indent=2))
                    curr_time = market['timestamp']
                    for market_id, (up_token, down_token) in all_ids.items():
                        up_bid, up_ask = market[market_id][up_token]
                        down_bid, down_ask = market[market_id][down_token]
                        state = entered[market_id]

                        # Stop-loss is a ONE-SHOT trigger per position. `exited` is
                        # set the instant we decide to sell (synchronously, before
                        # any await/task), so no later tick can re-fire it and no
                        # later tick can treat this market as flat and re-enter it
                        # for the rest of this window.
                        if state['up'] and state['buy_price'] and not state['exited']:
                            if up_ask is not None and up_ask <= state['buy_price'] - MAXIMAL_LOSS:
                                print(f"stop Loss hit for up trade at {up_ask}")
                                state['exited'] = True
                                state['up'] = False
                                sell_size = state['shares'] or place_orders.SHARES
                                fire_and_forget(asyncio.to_thread(
                                    place_orders.place_market_order_sync, client, up_token, "SELL", sell_size
                                ))
                        if state['down'] and state['buy_price'] and not state['exited']:
                            if down_ask is not None and down_ask <= state['buy_price'] - MAXIMAL_LOSS:
                                print(f"stop Loss hit for down trade at {down_ask}")
                                state['exited'] = True
                                state['down'] = False
                                sell_size = state['shares'] or place_orders.SHARES
                                fire_and_forget(asyncio.to_thread(
                                    place_orders.place_market_order_sync, client, down_token, "SELL", sell_size
                                ))

                        # if we are 20 seconds out
                        if up_ask and down_ask and not (state['up'] or state['down'] or state['exited']) and (market_time - curr_time) <= (BEFORE_ENTER * 1000):
                            #place a buy order if less than 89
                            # check which is larger
                            # here the up is higher
                            if up_ask > down_ask and up_ask < MAXIMAL_BID_PLUS_ONE:
                                state['up'] = True
                                state['buy_price'] = up_ask
                                state['shares'] = place_orders.SHARES
                                fire_and_forget(enter_position(client, state, up_token, (up_ask + 0.01), (market_time / 1000)))
                                print(f"entered an up trade at price: {up_ask}\nWith up_ask : {up_ask} and down_ask : {down_ask}")
                            elif down_ask > up_ask and down_ask < MAXIMAL_BID_PLUS_ONE:
                                state['down'] = True
                                state['buy_price'] = down_ask
                                state['shares'] = place_orders.SHARES
                                fire_and_forget(enter_position(client, state, down_token, (down_ask + 0.01), (market_time / 1000)))
                                print(f"entered a down trade at price: {down_ask}\nWith up_ask : {up_ask} and down_ask : {down_ask}")


                            # set the limit stop loss (which i'll have to build)
                            # take the other side? idk
                        
                        

                        # here we can go to the next market
                    if curr_time >= market_time:
                        print("the moment for next market has begun")
                        window_complete = True
                        break
            finally:
                await ws_stream.aclose()

            if not window_complete:
                print(json.dumps({"status": "reconnecting_same_market", "market_time": slug}))
                await asyncio.sleep(1)
                

        slug = slug + MARKET_DURATION


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down...")