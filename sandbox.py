import requests, json, asyncio, websockets
from helper_functions import setup

client = setup.setup_and_return_client()

markets = ['btc-updown-5m-']
start_time = "1787179200"
slug = markets[0] + start_time

url = f"https://gamma-api.polymarket.com/events/slug/{slug}"

response = requests.get(url).json()
clob_token_ids_str = response['markets'][0]['clobTokenIds']

clob_token_ids = json.loads(clob_token_ids_str)
print(clob_token_ids)
print(type(clob_token_ids))

ids = clob_token_ids
up_token = ids[0]
down_token = ids[1]

print(f'id 1: {ids[0]} \n id 2: {ids[1]}')

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def main(up_token, down_token):
    up_best_bid = 0
    up_best_ask = 0
    down_best_bid = 0
    down_best_ask = 0
    try:
        async with websockets.connect(WS_URL) as ws:
            print("Connected to Polymarket WebSocket")

            # message to subscribe to which markets, add both the up and down market, so we can watch both sides
            message = {
                "assets_ids": [ids[0], ids[1]],
                "type": "market"
            }

            # send the message above essentially tell the websocket what we want to see from them
            await ws.send(json.dumps(message))
            print("Subscription message sent")

            
            while True:
                # wait to recieve a message from the server
                message = await ws.recv()
                # turn it into a working json so we can parse it
                data = json.loads(message)
                # Normalize WebSocket response so it is always a list
                if isinstance(data, dict):
                    blocks = [data]
                elif isinstance(data, list):
                    blocks = data
                else:
                    print(f"Unexpected data type: {type(data)}")
                    continue

                print(json.dumps(blocks, indent=2))
                break
                
                # then for each entry in the list (because books are a list of the markets you're subscribed to)
                for block in blocks:
                    # get the event type, there are 4 possible event types but we just care about the order books and the price changes
                    event_type = block['event_type']

                    match event_type:
                        case "book":
                            # the asset id is the clobID so specifically it is which outcome it is, so either up or down in a binary market
                            asset_id = block['asset_id']
                            if asset_id == up_token:
                                bids = block.get('bids')
                                up_best_bid = None if not bids else float(bids[-1]['price'])

                                asks = block.get('asks')
                                up_best_ask = None if not asks else float(asks[-1]['price'])
                            if asset_id == down_token:
                                bids = block.get('bids')
                                down_best_bid = None if not bids else float(bids[-1]['price'])

                                asks = block.get('asks')
                                down_best_ask = None if not asks else float(asks[-1]['price'])
                        case "price_change":
                            # price changes itself is a list containing all of the price changes so we need to grab that list
                            price_changes = block['price_changes']
                            # we then go through each side of the list
                            for change in price_changes:
                                # update based on the price changes
                                asset_id = change['asset_id']
                                if asset_id == up_token:
                                    up_best_bid = float(change['best_bid'])

                                    up_best_ask = float(change['best_ask'])
                                if asset_id == down_token:
                                    down_best_bid = float(change['best_bid'])

                                    down_best_ask = float(change['best_ask'])
                        case _:
                            pass

                print(f'UP:\n Bid: {up_best_bid} Ask: {up_best_ask}\nDOWN:\n Bid: {down_best_bid} Ask: {down_best_ask}')   

    except asyncio.CancelledError:
        print("\nShutting down...")

    except websockets.exceptions.ConnectionClosed:
        print("\nWebSocket connection closed.")

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Closing connection...")


if __name__ == "__main__":
    try:
        asyncio.run(main(up_token, down_token))
    except KeyboardInterrupt:
        print("\nExited gracefully.")




# url = f'https://clob.polymarket.com/book?token_id={ids[0]}'

# response = requests.get(url).json()

# top_of_book_bid = response['bids'][-1]

# print(json.dumps(response, indent=2))
# print(top_of_book_bid)

