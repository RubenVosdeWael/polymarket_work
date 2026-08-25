import requests, json, os, time, math, asyncio, traceback

from helper_functions import setup, websocket_work, place_orders
from datetime import datetime, timezone

MARKET_DURATION  = 300    # 5-minute markets; unfilled legs get cancelled when the window closes
BEFORE_ENTER = 20           # How many seconds before we want to enter
ENTER_PRICE = 0.48




async def main():
    slug = setup.get_slug_time() + MARKET_DURATION

    # authenticate once up front, outside the loop, so we never re-handshake
    client = setup.setup_and_return_client()
    while True:
        all_ids = setup.get_clob_ids(str(slug))

        curr_time = int(time.time())
        while (slug - curr_time) > BEFORE_ENTER:
            curr_time = int(time.time())
            time.sleep(2)

        for market, (up_token, down_token) in all_ids:
            place_orders.place_limit_order_sync(client=client, token_id=up_token, price=ENTER_PRICE, deadline_ts=(slug + MARKET_DURATION), side="BUY", size=5)
            place_orders.place_limit_order_sync(client=client, token_id=down_token, price=ENTER_PRICE, deadline_ts=(slug + MARKET_DURATION), side="BUY", size=5)
                
        slug = slug + MARKET_DURATION


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down...")