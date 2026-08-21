import json, asyncio
from helper_functions import setup, websocket_work, place_orders, momentum_following_logic
from datetime import datetime, timezone


MARKET_DURATION  = 300    # 5-minute markets; unfilled legs get cancelled when the window closes
BEFORE_ENTER = 20           # How many seconds before we want to enter
MAXIMAL_LOSS = 0.10         # how much we are willing to lose per share max
MAXIMAL_BID_PLUS_ONE = 0.89 # how much I am willing to bet to stay positive 50/50 at 89 i make 1 cent a share if i win 50/50
SECONDS_BEFORE = 5
ENTRY_PRICE = .80


async def main():
    slug = setup.get_slug_time()

    # authenticate once up front, outside the loop, so we never re-handshake
    client = setup.setup_and_return_client()
    while True:
        all_ids = setup.get_clob_ids(str(slug))
        entered = {
            market: {"up": False, "down": False, "buy_price": None, "shares": None, "exited": False, "sell_price": None}
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

  
                        # if we hit our target entry price
                        if up_ask and down_ask and not state['up'] and up_ask >= (ENTRY_PRICE - 0.02):
                            #place a buy order if we hit 80 cents
                            # check which is larger
                            # here the up is higher
                            _order_id, filled = await asyncio.to_thread(place_orders.place_limit_order_sync, client=client, token_id=up_token, price=ENTRY_PRICE, deadline_ts=(market_time / 1000))
                            state['up'] = True
                            await asyncio.sleep(0.1)
                            await asyncio.to_thread(place_orders.place_limit_order_sync, client=client, token_id=up_token, price=(ENTRY_PRICE + 0.10), deadline_ts=(market_time / 1000), side="SELL")
                        elif up_ask and down_ask and not state['down'] and down_ask >= (ENTRY_PRICE - 0.02):
                            _order_id, filled = await asyncio.to_thread(place_orders.place_limit_order_sync, client=client, token_id=down_token, price=ENTRY_PRICE, deadline_ts=(market_time / 1000))
                            state['down'] = True
                            await asyncio.sleep(0.1)
                            await asyncio.to_thread(place_orders.place_limit_order_sync, client=client, token_id=down_token, price=(ENTRY_PRICE + 0.10), deadline_ts=(market_time / 1000), side="SELL")

                        

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