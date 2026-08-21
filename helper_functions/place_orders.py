import json, re, traceback, time
from py_clob_client_v2.clob_types import OrderArgs, OrderType
from py_clob_client_v2 import MarketOrderArgs
from datetime import datetime, timezone

RETRY_DELAY      = 1      # seconds to wait between failed placement attempts
SHARES = 5      # Polymarket's minimum order size, in shares — independent of price/dollars

# Polymarket's "not enough balance" error embeds the real numbers, e.g.:
# "the balance is not enough -> balance: 1880000, order amount: 5000000"
# (micro-units, 6 decimals -> 1.88 shares available vs 5.0 requested).
_BALANCE_RE = re.compile(r'balance:\s*(\d+),\s*order amount:\s*(\d+)')


def place_limit_order_sync(client, token_id, price, deadline_ts=None, side="BUY", size=None):
    """Place a resting (GTC) limit order. Runs in a worker thread.

    Returns (order_id, filled_shares). filled_shares is a best-effort read of
    how much matched immediately (0 if nothing matched / it's just resting).

    Keeps retrying on transient exceptions until it succeeds, or until
    deadline_ts has passed (if provided). If Polymarket reports the order
    size exceeds what we actually have available (e.g. selling more than we
    hold because an earlier buy only partially filled), the real available
    balance is parsed out of the error and the order is retried once at that
    corrected size instead of being abandoned or endlessly retried at the
    wrong size.
    """
    size = size if size is not None else SHARES
    attempt = 0
    while True:
        attempt += 1
        try:
            order_args   = OrderArgs(token_id=token_id, price=price, size=size, side=side)
            signed_order = client.create_order(order_args)
            resp         = client.post_order(signed_order, OrderType.GTC)

            order_id = None
            filled_shares = 0.0
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("id")
                try:
                    filled_shares = float(resp.get("takingAmount", 0) or 0)
                except (TypeError, ValueError):
                    filled_shares = 0.0

            print(json.dumps({
                "status": "placed", "token_id": token_id, "price": price, "size": size,
                "attempt": attempt,
                "resp": resp if isinstance(resp, dict) else str(resp),
            }))
            return order_id, filled_shares

        except Exception as e:
            msg = str(e)

            if 'not enough balance' in msg:
                m = _BALANCE_RE.search(msg)
                if m:
                    avail = float(m.group(1)) / 1_000_000
                    if avail <= 0:
                        print(json.dumps({"status": "no_balance", "token_id": token_id}))
                        return None, 0.0
                    if avail != size:
                        print(json.dumps({
                            "status": "resizing_order", "token_id": token_id,
                            "requested_size": size, "available_size": avail,
                        }))
                        size = avail
                        continue  # retry immediately at the corrected size

                print('Not Enough Balance error')
                return None, 0.0

            traceback.print_exc()
            print(json.dumps({
                "status": "error", "stage": "place", "token_id": token_id,
                "attempt": attempt, "errorMsg": msg,
            }))

            if deadline_ts is not None and datetime.now(timezone.utc).timestamp() >= deadline_ts:
                print(json.dumps({
                    "status": "give_up", "stage": "place", "token_id": token_id,
                    "attempt": attempt, "reason": "deadline passed",
                }))
                return None, 0.0

            time.sleep(RETRY_DELAY)


def place_market_order_sync(client, token_id, side="SELL", amount=None):
    """Place a market order. Same balance-correction behavior as above —
    used for the stop-loss so it (a) actually clears at the real size you
    hold, and (b) executes immediately instead of resting as an unfilled
    limit order while price keeps falling away from it.
    """
    amount = amount if amount is not None else SHARES
    attempt = 0
    while True:
        attempt += 1
        try:
            order_args = MarketOrderArgs(
                token_id=token_id,
                side=side,
                amount=amount,
                # FAK (fill-and-kill / IOC) instead of FOK: on a fast-crashing
                # book there often isn't enough resting size to fill the full
                # amount in one shot. FOK rejects the whole order in that case
                # (see the repeated "couldn't be fully filled" retries in the
                # logs), burning time on retries while price keeps falling.
                # FAK takes whatever's available immediately and kills the
                # rest instead of erroring out. Verify OrderType.FAK exists in
                # your py_clob_client_v2 build before relying on this.
                order_type=OrderType.FAK
            )
            resp = client.create_and_post_market_order(
                    order_args=order_args
                )

            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("id")

            print(json.dumps({
                "status": "placed", "token_id": token_id, "amount": amount,
                "attempt": attempt,
                "resp": resp if isinstance(resp, dict) else str(resp),
            }))
            return order_id

        except Exception as e:
            msg = str(e)

            if 'not enough balance' in msg:
                m = _BALANCE_RE.search(msg)
                if m:
                    avail = float(m.group(1)) / 1_000_000
                    if avail <= 0:
                        print(json.dumps({"status": "no_balance", "token_id": token_id}))
                        return None
                    if avail != amount:
                        print(json.dumps({
                            "status": "resizing_order", "token_id": token_id,
                            "requested_size": amount, "available_size": avail,
                        }))
                        amount = avail
                        continue
                print('Not Enough Balance error')
                return None

            traceback.print_exc()
            print(json.dumps({
                "status": "error", "stage": "place", "token_id": token_id,
                "attempt": attempt, "errorMsg": msg,
            }))

            time.sleep(RETRY_DELAY)