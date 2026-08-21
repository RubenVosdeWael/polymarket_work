import websockets, asyncio, json

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

async def monitor_top_bid_ask(all_tokens):
    watch_market = {}
    list_of_tokens = []
    for market, tokens in all_tokens.items():
        # 0: best bid,
        # 1: best ask,
        watch_market[market] = {
            tokens[0] : [],
            tokens[1] : []
        }
        for token in tokens: list_of_tokens.append(token)
    watch_market['timestamp'] = None
    try:
        async with websockets.connect(WS_URL) as ws:
            print("Connected to Polymarket WebSocket")

            # message to subscribe to which markets, add both the up and down market, so we can watch both sides
            message = {
                "assets_ids": list_of_tokens,
                "type": "market"
            }

            # send the message above essentially tell the websocket what we want to see from them
            await ws.send(json.dumps(message))
            print("Subscription message sent")

            
            while True:
                # wait to recieve a message from the server
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    print(json.dumps({"status": "ws_stall", "detail": "no message in 15s"}))
                    continue
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
                
                # then for each entry in the list (because books are a list of the markets you're subscribed to)
                for block in blocks:
                    # get the event type, there are 4 possible event types but we just care about the order books and the price changes
                    event_type = block['event_type']

                    match event_type:
                        case "book":
                            # the asset id is the clobID so specifically it is which outcome it is, so either up or down in a binary market
                            asset_id = block['asset_id']
                            curr_market = block['market']

                            bids = block.get('bids')
                            best_bid = 0.0 if not bids else float(bids[-1]['price'])

                            asks = block.get('asks')
                            best_ask = 1.0 if not asks else float(asks[-1]['price'])

                            watch_market[curr_market][asset_id] = [best_bid, best_ask]
                        
                        case "price_change":
                            # price changes itself is a list containing all of the price changes so we need to grab that list
                            price_changes = block['price_changes']
                            curr_market = block['market']
                            # we then go through each side of the list
                            for change in price_changes:
                                # update based on the price changes
                                # print(change)
                                asset_id = change['asset_id']

                                best_bid = float(change['best_bid'])
                                best_ask = float(change['best_ask'])

                                watch_market[curr_market][asset_id] = [best_bid, best_ask]

                        case _:
                            pass
                    watch_market['timestamp'] = int(block['timestamp'])

                # Send current state back to caller
                yield watch_market

    except asyncio.CancelledError:
        print("\nShutting down...")

    except websockets.exceptions.ConnectionClosed as e:
        print(json.dumps({"status": "ws_closed", "code": e.code, "reason": e.reason}))

    except KeyboardInterrupt:
        print("\nCtrl+C detected. Closing connection...")