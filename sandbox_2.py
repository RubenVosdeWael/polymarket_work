import requests, json, time

# url = "https://gamma-api.polymarket.com/events/slug/sol-updown-5m-1787181300"

# response = requests.get(url).json()

# print(json.dumps(response['markets'][0]['conditionId'], indent=2))


# url = "https://gamma-api.polymarket.com/markets/slug/sol-updown-5m-1787181300"

# response = requests.get(url).json()

# print(response['conditionId'])

now = int(time.time())

print(now - (now % 300))

from helper_functions import setup

client = setup.setup_and_return_client()

print([m for m in dir(client) if 'cancel' in m.lower()])

import inspect
print(inspect.signature(client.cancel_order))
try:
    print(inspect.getsource(client.cancel_order))
except OSError:
    print("source not available (probably compiled/installed package)")