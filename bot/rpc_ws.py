"""
Polygon chain access over WebSocket — no HTTP RPC on the critical path, and no timers.

Two things this bot needs from the chain are plain `eth_call` reads: the funded wallet's
pUSD balance, and the Chainlink aggregator's `latestRoundData`. Both were previously done
over HTTP (`AsyncWeb3.AsyncHTTPProvider`), the balance on a 10-second timer.

`eth_call` works over the WebSocket JSON-RPC transport exactly as it does over HTTP
(verified live against polygon-bor-rpc.publicnode.com and polygon.drpc.org), so both move
here. More importantly, an ERC-20 balance CHANGE emits a `Transfer` log, which means the
balance does not have to be re-read on a timer at all: it can be subscribed to.

  PolygonRpcStream  — one persistent socket carrying request/response `eth_call` AND
                      `eth_subscribe` log streams, with reconnect + re-subscription.
  PusdBalanceWatcher— the live pUSD balance, updated when a transfer touches our wallet.
  ChainlinkReader   — BTC/USD from the on-chain aggregator via `eth_call` over the socket.
"""

import asyncio
import json
import time
from typing import Optional, Callable, Dict, List, Any

import aiohttp

from .config import settings
from .net_utils import get_proxy_url_for


class PolygonRpcStream:
    """One persistent Polygon JSON-RPC WebSocket serving both reads and subscriptions.

    A JSON-RPC id correlates each `eth_call` with its reply; `eth_subscribe` returns a
    subscription id whose later notifications are dispatched to a registered callback.

    Reconnects are the delicate part:
      - Pending calls are failed immediately rather than left awaiting a dead socket.
      - A subscription id is only meaningful for the connection that issued it, so the
        whole id->label map is rebuilt on the new socket.
      - `on_connect` hooks re-read anything derived from chain state, because a `logs`
        subscription reports only what changes AFTER it is established — a change that
        happened while we were disconnected would otherwise be invisible forever.
    """

    DEFAULTS = [
        "wss://polygon-bor-rpc.publicnode.com",
        "wss://polygon.drpc.org",
    ]
    CALL_TIMEOUT_S = 8.0

    def __init__(self, urls: Optional[List[str]] = None):
        cfg = list(settings.POLYGON_WSS_URLS or [])
        if settings.POLYGON_WSS_URL:
            cfg.append(settings.POLYGON_WSS_URL)
        self.urls = list(dict.fromkeys((urls or []) + cfg + self.DEFAULTS))
        self._ws = None
        self._next_id = 1
        self._pending: Dict[int, asyncio.Future] = {}
        # label -> {"params": [...], "cb": callable, "sub_id": str|None}
        self._watches: Dict[str, Dict] = {}
        self._sub_to_label: Dict[str, str] = {}
        self._on_connect: List[Callable] = []
        self.connected = False
        self.closed = False
        self.last_error: Optional[str] = None
        self.url: Optional[str] = None

    def on_connect(self, fn: Callable):
        self._on_connect.append(fn)

    # -- request / response ---------------------------------------------------
    async def _rpc(self, method: str, params: list) -> Any:
        ws = self._ws
        if ws is None or ws.closed:
            raise ConnectionError("polygon rpc ws not connected")
        rid = self._next_id
        self._next_id += 1
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            await ws.send_json({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
            return await asyncio.wait_for(fut, timeout=self.CALL_TIMEOUT_S)
        finally:
            self._pending.pop(rid, None)

    async def call(self, to: str, data: str, block: str = "latest") -> Optional[str]:
        """`eth_call` -> raw hex result, or None. Never raises into the caller: a chain
        read that fails must degrade to 'unknown', not abort a trading tick."""
        try:
            return await self._rpc("eth_call", [{"to": to, "data": data}, block])
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            return None

    # -- subscriptions --------------------------------------------------------
    def watch_logs(self, label: str, address: str, topics: list, cb: Callable):
        """Register a log subscription, applied now if connected and re-applied on every
        reconnect. Re-registering a label replaces it (the funded wallet can change when
        credentials change)."""
        self._watches[label] = {
            "params": ["logs", {"address": address, "topics": topics}],
            "cb": cb,
            "sub_id": None,
        }
        if self.connected:
            asyncio.create_task(self._apply_watch(label))

    def unwatch(self, label: str):
        w = self._watches.pop(label, None)
        if w and w.get("sub_id"):
            self._sub_to_label.pop(w["sub_id"], None)

    async def _apply_watch(self, label: str):
        w = self._watches.get(label)
        if not w:
            return
        try:
            sub_id = await self._rpc("eth_subscribe", w["params"])
            if isinstance(sub_id, str):
                w["sub_id"] = sub_id
                self._sub_to_label[sub_id] = label
        except Exception as e:
            self.last_error = f"subscribe {label}: {type(e).__name__}: {e}"

    def _dispatch(self, fn: Callable, *args):
        try:
            res = fn(*args)
            if asyncio.iscoroutine(res):
                asyncio.create_task(res)
        except Exception:
            pass

    async def start(self):
        url_idx = 0
        while not self.closed:
            if not self.urls:
                await asyncio.sleep(5)
                continue
            url = self.urls[url_idx % len(self.urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None,
                                                  heartbeat=15) as ws:
                        self._ws = ws
                        self.url = url
                        self.connected = True
                        print(f"Connected to Polygon RPC WS: {url}")

                        self._sub_to_label.clear()
                        for lbl in list(self._watches):
                            self._watches[lbl]["sub_id"] = None
                            await self._apply_watch(lbl)
                        for fn in self._on_connect:
                            self._dispatch(fn)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            try:
                                d = json.loads(msg.data)
                            except Exception:
                                continue
                            rid = d.get("id")
                            if rid is not None:
                                fut = self._pending.get(rid)
                                if fut is not None and not fut.done():
                                    if "error" in d:
                                        fut.set_exception(RuntimeError(str(d["error"])))
                                    else:
                                        fut.set_result(d.get("result"))
                                continue
                            if d.get("method") == "eth_subscription":
                                p = d.get("params") or {}
                                label = self._sub_to_label.get(str(p.get("subscription") or ""))
                                w = self._watches.get(label) if label else None
                                if w:
                                    self._dispatch(w["cb"], p.get("result"))
            except Exception as e:
                print(f"Polygon RPC WS failed ({url}): {e}")
                self.last_error = f"{type(e).__name__}: {e}"
            finally:
                self.connected = False
                self._ws = None
                for fut in list(self._pending.values()):
                    if not fut.done():
                        fut.set_exception(ConnectionError("polygon rpc ws disconnected"))
                self._pending.clear()
            if not self.closed:
                await asyncio.sleep(2)

    def close(self):
        self.closed = True


class PusdBalanceWatcher:
    """The live pUSD balance, EVENT-DRIVEN instead of re-read on a 10s timer.

    pUSD is an ERC-20, so every change to our balance emits a `Transfer` log carrying our
    wallet in `from` or `to`. We subscribe to exactly those and, on a hit, re-read
    `balanceOf` over the same socket.

    The log is a TRIGGER, not arithmetic. Applying the log's own `+amount`/`-amount` would
    drift permanently the moment one event is missed across a reconnect, and this number
    sizes every trade. Re-reading is authoritative and costs one `eth_call`.

    Topic-position filtering is mandatory, not an optimisation: the unfiltered pUSD
    Transfer stream measured ~280 logs/second.
    """

    PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"   # Polymarket V2 collateral (Polygon)
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    DECIMALS = 6

    # A trade can settle as several Transfer legs landing in the same block. Each is a
    # trigger, but they all describe ONE final balance, so refreshes are coalesced: an
    # in-flight read is not duplicated, and reads are spaced by at least this much.
    MIN_REFRESH_INTERVAL_S = 0.25

    def __init__(self, rpc: PolygonRpcStream, on_update: Optional[Callable] = None):
        self.rpc = rpc
        self.on_update = on_update
        self.address: Optional[str] = None
        self.balance: Optional[float] = None
        self.updated_at: Optional[float] = None
        self._inflight = False
        self._last_read = 0.0
        self._pending_again = False
        rpc.on_connect(self._on_reconnect)

    @staticmethod
    def _pad(addr: str) -> str:
        return "0x" + addr[2:].lower().rjust(64, "0")

    def _on_reconnect(self):
        if self.address:
            return self.refresh()
        return None

    def watch(self, address: Optional[str]):
        """Point the watcher at the funded wallet. No-op if unchanged (called per tick)."""
        addr = (address or "").strip()
        if not addr:
            return
        if self.address and addr.lower() == self.address.lower():
            return
        self.address = addr
        padded = self._pad(addr)
        # `topics` filters by POSITION, so incoming (to == us) and outgoing (from == us)
        # cannot be expressed as a single filter.
        self.rpc.watch_logs("pusd_in", self.PUSD_ADDRESS,
                            [self.TRANSFER_TOPIC, None, [padded]], self._on_transfer)
        self.rpc.watch_logs("pusd_out", self.PUSD_ADDRESS,
                            [self.TRANSFER_TOPIC, [padded], None], self._on_transfer)
        asyncio.create_task(self.refresh())

    def _on_transfer(self, _log):
        return self.refresh(coalesce=True)

    async def refresh(self, coalesce: bool = False) -> Optional[float]:
        """Re-read the balance. `coalesce=True` (the log-triggered path) collapses a burst
        of transfers into one read; an explicit refresh (startup, reconnect, post-trade)
        always goes through."""
        if not self.address:
            return None
        if coalesce:
            if self._inflight:
                # Something arrived mid-read: the value we are about to store may already
                # be behind, so schedule exactly one follow-up rather than dropping it.
                self._pending_again = True
                return self.balance
            wait = self.MIN_REFRESH_INTERVAL_S - (time.time() - self._last_read)
            if wait > 0:
                await asyncio.sleep(wait)
        self._inflight = True
        try:
            return await self._read()
        finally:
            self._inflight = False
            self._last_read = time.time()
            if self._pending_again:
                self._pending_again = False
                asyncio.create_task(self.refresh(coalesce=True))

    async def _read(self) -> Optional[float]:
        data = "0x70a08231" + self.address[2:].lower().rjust(64, "0")   # balanceOf(address)
        raw = await self.rpc.call(self.PUSD_ADDRESS, data)
        if not raw or raw == "0x":
            return None
        try:
            val = int(raw, 16) / (10 ** self.DECIMALS)
        except (TypeError, ValueError):
            return None
        self.balance = val
        self.updated_at = time.time()
        if self.on_update:
            try:
                res = self.on_update(val)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
        return val

    def get(self) -> Dict:
        age = (time.time() - self.updated_at) if self.updated_at else None
        return {"balance": self.balance, "address": self.address, "age": age}


class ChainlinkReader:
    """Chainlink BTC/USD from the on-chain aggregator, read over the SAME WebSocket.

    This replaces `bot/chainlink.py`'s HTTP provider chain. It stays a read rather than a
    subscription because `AnswerUpdated` fires only on a deviation or heartbeat (minutes
    apart), so a fresh process would have no price at all until the next update — the
    aggregator's stored answer is what we actually want. `ChainlinkPriceStream` already
    subscribes to the event for push updates; this is the cold-start / fallback read.
    """

    LATEST_ROUND_DATA = "0xfeaf968c"    # latestRoundData()
    DECIMALS_SELECTOR = "0x313ce567"    # decimals()

    def __init__(self, rpc: PolygonRpcStream, aggregator: Optional[str] = None):
        self.rpc = rpc
        self.aggregator = aggregator or settings.CHAINLINK_BTC_USD_AGGREGATOR
        self._decimals: Optional[int] = None
        self.cached = {"price": None, "updatedAt": None, "source": "chainlink_ws_call"}
        self._fetched_at = 0.0
        self.min_interval_s = 2.0

    def set_aggregator(self, aggregator: str):
        if aggregator and aggregator.lower() != (self.aggregator or "").lower():
            self.aggregator = aggregator
            self._decimals = None

    async def fetch(self) -> Dict:
        now = time.time()
        if self._fetched_at and now - self._fetched_at < self.min_interval_s:
            return self.cached
        if not self.rpc.connected or not self.aggregator:
            return self.cached
        if self._decimals is None:
            raw = await self.rpc.call(self.aggregator, self.DECIMALS_SELECTOR)
            if raw and raw != "0x":
                try:
                    self._decimals = int(raw, 16)
                except (TypeError, ValueError):
                    self._decimals = None
        if self._decimals is None:
            return self.cached

        raw = await self.rpc.call(self.aggregator, self.LATEST_ROUND_DATA)
        if not raw or len(raw) < 2 + 64 * 5:
            return self.cached
        try:
            body = raw[2:]
            answer = int(body[64:128], 16)
            if answer >= 2 ** 255:
                answer -= 2 ** 256
            updated_at = int(body[192:256], 16)
        except (TypeError, ValueError):
            return self.cached

        price = answer / (10 ** self._decimals)
        # Reject a degenerate round rather than mis-mark a settlement with it.
        if price <= 0:
            return self.cached
        self.cached = {"price": price, "updatedAt": updated_at * 1000,
                       "source": "chainlink_ws_call"}
        self._fetched_at = now
        return self.cached
