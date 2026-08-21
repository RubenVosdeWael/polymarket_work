import requests, json, time

# url = "https://gamma-api.polymarket.com/events/slug/sol-updown-5m-1787181300"

# response = requests.get(url).json()

# print(json.dumps(response['markets'][0]['conditionId'], indent=2))


# url = "https://gamma-api.polymarket.com/markets/slug/sol-updown-5m-1787181300"

# response = requests.get(url).json()

# print(response['conditionId'])

now = int(time.time())

print(now - (now % 300))

