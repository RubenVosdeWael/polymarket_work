import json, os, time, requests
from py_clob_client_v2 import ClobClient, ApiCreds


CREDS_FILE = "helper_files/creds.json"
HOST       = "https://clob.polymarket.com"
CHAIN_ID   = 137

markets = [
    'eth-updown-5m-',
    # 'btc-updown-5m-',
    # 'sol-updown-5m-',
    # 'xrp-updown-5m-',
    # 'doge-updown-5m-'
]

# for clob token id index 0 is up token and index 1 is down token
def get_clob_ids(unix_time, max_retries=5, retry_delay=1):
    to_return = {}
    for market in markets:
        slug = market + unix_time
        url = f"https://gamma-api.polymarket.com/events/slug/{slug}"

        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, timeout=10).json()
                clob_token_ids_str = response['markets'][0]['clobTokenIds']
                ids = json.loads(clob_token_ids_str)
                to_return[response['markets'][0]['conditionId']] = ids
                last_err = None
                break
            except (requests.exceptions.RequestException, KeyError, IndexError,
                    ValueError, json.JSONDecodeError) as e:
                # Broadened beyond just network errors: a malformed or
                # unexpected-shape response (missing key, bad JSON, empty
                # markets list) is just as retryable as a network blip, and
                # letting it propagate uncaught would crash the whole bot —
                # including abandoning any open positions main.py is still
                # actively managing in the background.
                last_err = e
                print(json.dumps({"status": "error", "stage": "get_clob_ids", "market": market,
                                   "attempt": attempt, "errorMsg": str(e)}))
                if attempt < max_retries:
                    time.sleep(retry_delay)

        if last_err is not None:
            raise last_err

    return to_return

def get_slug_time():
    now = int(time.time())

    return now - (now % 300)


def set_up_environ_vars():
    with open("helper_files/config.json") as f:
        config = json.load(f)

    for key, value in config.items():
        os.environ[key] = str(value)

def load_saved_creds():
    try:
        with open(CREDS_FILE, "r") as f:
            data = json.load(f)

            return ApiCreds(
                api_key        = data["api_key"],
                api_secret     = data["api_secret"],
                api_passphrase = data["api_passphrase"],
            )
    except Exception:
        return None

def save_creds(creds):
    try:
        with open(CREDS_FILE, "w") as f:
            json.dump({
                "api_key":        creds.api_key,
                "api_secret":     creds.api_secret,
                "api_passphrase": creds.api_passphrase,
            }, f, indent=2)
    except Exception:
        pass

def get_authenticated_client(key, funder):

    saved = load_saved_creds()
    if saved:
        return ClobClient(
            host           = HOST,
            chain_id       = CHAIN_ID,
            key            = key,
            creds          = saved,
            signature_type = 3,
            funder         = funder,
        )

    l1_client = ClobClient(host=HOST, chain_id=CHAIN_ID, key=key)
    nonce     = int(time.time() * 1000)
    creds     = l1_client.create_or_derive_api_key(nonce=nonce)
    save_creds(creds)

    return ClobClient(
        host           = HOST,
        chain_id       = CHAIN_ID,
        key            = key,
        creds          = creds,
        signature_type = 3,
        funder         = funder,
    )


def setup_and_return_client():
    set_up_environ_vars()
    key    = os.environ.get("api_key")
    funder = os.environ.get("account_funder")

    return get_authenticated_client(key, funder)