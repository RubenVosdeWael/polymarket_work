import json, re, traceback, time
from py_clob_client_v2.clob_types import OrderArgs, OrderType
from py_clob_client_v2 import MarketOrderArgs
from datetime import datetime, timezone

RETRY_DELAY      = 1
SHARES = 5

ZERO_BALANCE_GRACE_RETRIES = 5
ZERO_BALANCE_GRACE_DELAY   = 1

MAX_ATTEMPTS = 10  # ← NEW: absolute cap on retries for any order function

_BALANCE_RE_SIMPLE   = re.compile(r'balance:\s*(\d+),\s*order amount:\s*(\d+)')
_BALANCE_RE_RESERVED = re.compile(r'balance:\s*(\d+),\s*sum of matched orders:\s*(\d+)')


def _parse_available_balance(msg):
    m = _BALANCE_RE_RESERVED.search(msg)
    if m:
        balance  = float(m.group(1))
        reserved = float(m.group(2))
        return max(balance - reserved, 0.0) / 1_000_000
    m = _BALANCE_RE_SIMPLE.search(msg)
    if m:
        return float(m.group(1)) / 1_000_000
    return None


def _extract_filled_shares(resp, side):
    if not isinstance(resp, dict):
        return 0.0
    key = "takingAmount" if side == "BUY" else "makingAmount"
    try:
        return float(resp.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def place_limit_order_sync(client, token_id, price, deadline_ts=None, side="BUY", size=None):
    size = size if size is not None else SHARES
    attempt = 0
    zero_balance_attempts = 0

    while attempt < MAX_ATTEMPTS:  # ← FIX: capped retries
        attempt += 1
        try:
            order_args   = OrderArgs(token_id=token_id, price=price, size=size, side=side)
            signed_order = client.create_order(order_args)
            resp         = client.post_order(signed_order, OrderType.GTC)

            order_id = None
            filled_shares = 0.0
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("id")
                filled_shares = _extract_filled_shares(resp, side)

            print(json.dumps({
                "status": "placed", "token_id": token_id, "price": price, "size": size,
                "attempt": attempt,
                "resp": resp if isinstance(resp, dict) else str(resp),
            }))
            return order_id, filled_shares

        except Exception as e:
            msg = str(e)

            if 'not enough balance' in msg:
                avail = _parse_available_balance(msg)

                if avail is None:
                    zero_balance_attempts += 1
                    if zero_balance_attempts <= ZERO_BALANCE_GRACE_RETRIES:
                        print(json.dumps({
                            "status": "unparsed_balance_error_grace_retry",
                            "token_id": token_id, "attempt": zero_balance_attempts,
                        }))
                        time.sleep(ZERO_BALANCE_GRACE_DELAY)
                        continue
                    return None, 0.0

                if avail <= 0:
                    zero_balance_attempts += 1
                    if zero_balance_attempts <= ZERO_BALANCE_GRACE_RETRIES:
                        time.sleep(ZERO_BALANCE_GRACE_DELAY)
                        continue
                    return None, 0.0

                if avail != size:
                    size = avail
                    continue

            # ← FIX: Check deadline BEFORE sleeping
            if deadline_ts is not None and datetime.now(timezone.utc).timestamp() >= deadline_ts:
                return None, 0.0

            time.sleep(RETRY_DELAY)

    # ← FIX: If we exhaust MAX_ATTEMPTS, return failure
    print(json.dumps({"status": "max_attempts_reached", "token_id": token_id,
                       "side": side, "attempt": attempt}))
    return None, 0.0


def place_market_order_sync(client, token_id, side="SELL", amount=None):
    """Now has a timeout. Will not retry forever."""
    amount = amount if amount is not None else SHARES
    attempt = 0
    zero_balance_attempts = 0
    start_time = time.time()
    MARKET_ORDER_TIMEOUT = 30  # ← NEW: absolute 30-second timeout for market orders

    while attempt < MAX_ATTEMPTS and (time.time() - start_time) < MARKET_ORDER_TIMEOUT:  # ← FIX
        attempt += 1
        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                side=side,
                amount=amount,
                order_type=OrderType.FAK
            )
            resp = client.create_and_post_market_order(
                    order_args=order_args,
                    order_type=OrderType.FAK,
                )

            order_id = None
            filled_shares = 0.0
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("id")
                filled_shares = _extract_filled_shares(resp, side)

            print(json.dumps({
                "status": "placed", "token_id": token_id, "amount": amount,
                "attempt": attempt,
                "resp": resp if isinstance(resp, dict) else str(resp),
            }))
            return order_id, filled_shares

        except Exception as e:
            msg = str(e)

            if 'not enough balance' in msg:
                avail = _parse_available_balance(msg)

                if avail is None:
                    zero_balance_attempts += 1
                    if zero_balance_attempts <= ZERO_BALANCE_GRACE_RETRIES:
                        time.sleep(ZERO_BALANCE_GRACE_DELAY)
                        continue
                    return None, 0.0

                if avail <= 0:
                    zero_balance_attempts += 1
                    if zero_balance_attempts <= ZERO_BALANCE_GRACE_RETRIES:
                        time.sleep(ZERO_BALANCE_GRACE_DELAY)
                        continue
                    return None, 0.0

                if avail != amount:
                    amount = avail
                    continue

            time.sleep(RETRY_DELAY)

    # ← FIX: Timed out or max attempts — return what we have
    print(json.dumps({"status": "market_order_timeout", "token_id": token_id,
                       "attempt": attempt, "remaining_amount": amount}))
    return None, 0.0
