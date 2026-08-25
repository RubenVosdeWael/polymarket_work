import asyncio, json

IPC_HOST = "127.0.0.1"
IPC_PORT = 47654


class FastFeed:
    """Client for the standalone C++ ingestion process (polymarket_feed).

    Replaces websocket_work.py's direct connection to Polymarket. The C++
    process owns the actual exchange websocket — a dedicated, non-async,
    non-GIL-bound read loop that won't fall behind and trigger a "slow
    consumer" disconnect — and checks any registered stop-loss threshold
    inline the instant a price update is parsed, before it even talks to
    Python. This class just:
      - maintains the latest ask/bid per token_id from "tick" messages
        (a dict, "latest value wins" — nothing to overflow, ever)
      - surfaces "stop_loss" events immediately so main.py can act on them
      - lets main.py replace the full subscribed token set on rollover,
        and register/clear stop-loss watches on entry/exit

    Reconnects automatically if the C++ process restarts.
    """

    def __init__(self, host=IPC_HOST, port=IPC_PORT):
        self.host = host
        self.port = port
        self.prices = {}          # token_id -> {"ask": float, "bid": float}
        self._reader = None
        self._writer = None
        self._connected = asyncio.Event()
        # Multiple run_position tasks can each call watch()/unwatch()
        # concurrently. Without serializing reconnect attempts, two of them
        # detecting a dead connection at once could each call
        # asyncio.open_connection() independently and overwrite each
        # other's _reader/_writer — a real race, not hypothetical, given
        # up to 5 markets can have open positions simultaneously.
        self._connect_lock = asyncio.Lock()

    async def connect(self):
        """Establish (or re-establish) the connection. Retries until it
        succeeds. Safe to call concurrently from multiple coroutines — only
        one will actually reconnect; the rest wait for it and return once
        it's done."""
        async with self._connect_lock:
            if self._connected.is_set():
                return  # someone else already reconnected while we waited
            while True:
                try:
                    self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
                    self._connected.set()
                    print(json.dumps({"status": "ipc_connected"}))
                    return
                except OSError as e:
                    print(json.dumps({"status": "ipc_connect_retry", "errorMsg": str(e)}))
                    await asyncio.sleep(1)

    async def _send(self, obj):
        """Returns True only if the write genuinely went out over a live
        connection. Callers that need protection guarantees (watch/unwatch)
        must check this — a silently dropped watch() means a position trades
        with no stop-loss and nothing ever says so."""
        await self._connected.wait()
        try:
            self._writer.write((json.dumps(obj) + "\n").encode())
            await self._writer.drain()
            return True
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._connected.clear()

        # One reconnect-and-retry attempt before giving up, since a
        # momentary hiccup shouldn't permanently lose a stop-loss watch.
        await self.connect()
        try:
            self._writer.write((json.dumps(obj) + "\n").encode())
            await self._writer.drain()
            return True
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(json.dumps({"status": "ipc_send_failed", "cmd": obj, "errorMsg": str(e)}))
            return False

    async def subscribe(self, token_ids):
        """Replace the entire subscribed token set. Call this once per
        market rollover with every up/down token id across all markets
        you're tracking — the C++ side reconnects to Polymarket with the
        new set."""
        return await self._send({"cmd": "subscribe", "tokens": list(token_ids)})

    async def watch(self, token_id, buy_price, max_loss):
        """Returns True only if the watch is confirmed sent. Check this —
        proceeding as if a position is protected when it isn't is worse
        than knowing immediately that it isn't."""
        return await self._send({"cmd": "watch", "token_id": token_id, "buy_price": buy_price, "max_loss": max_loss})

    async def unwatch(self, token_id):
        return await self._send({"cmd": "unwatch", "token_id": token_id})

    async def stream(self):
        """Yields events as they arrive: {"type": "tick", ...} or
        {"type": "stop_loss", ...}. Also updates self.prices for ticks."""
        if not self._connected.is_set():
            await self.connect()

        while True:
            try:
                while True:
                    line = await self._reader.readline()
                    if not line:
                        raise ConnectionResetError("C++ feed closed the connection")
                    evt = json.loads(line)

                    if evt.get("type") == "tick":
                        self.prices[evt["token_id"]] = {"ask": evt["ask"], "bid": evt["bid"]}

                    yield evt
            except (ConnectionResetError, OSError, json.JSONDecodeError) as e:
                print(json.dumps({"status": "ipc_disconnected", "errorMsg": str(e)}))
                self._connected.clear()
                await self.connect()