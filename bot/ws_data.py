import asyncio
import json
import aiohttp
import time
from typing import Optional, Callable, Dict, List
from .config import settings
from .net_utils import get_proxy_url_for

class BinanceTradeStream:
    """Fast spot-price feed from Binance @trade — the leading signal for fair_prob."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_ts = None
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    # heartbeat detects a half-open socket (no CLOSE frame) so we reconnect
                    # instead of hanging forever on a silently-dead feed.
                    async with session.ws_connect(url, proxy=proxy if proxy else None, heartbeat=10) as ws:
                        print(f"Connected to Binance WS: {self.symbol}")
                        while not self.closed:
                            # Same watchdog as the Polymarket price stream: BTCUSDT trades
                            # arrive many times a second, so 30s of silence means the socket
                            # died without telling us. Without this the spot price freezes
                            # and entries halt indefinitely on `MAX_SPOT_AGE_S`.
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=30.0)
                            except asyncio.TimeoutError:
                                print("Binance trade WS went quiet — reconnecting")
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                # One malformed frame must not tear down the whole socket.
                                try:
                                    p = float(json.loads(msg.data).get("p"))
                                except (ValueError, TypeError, KeyError):
                                    continue
                                self._process_trade(p)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance trade WS failed: {e}")
            # Backoff on ANY exit (clean close or error) so an insta-closing server can't
            # spin a tight reconnect loop.
            if not self.closed:
                await asyncio.sleep(2)

    def _process_trade(self, p: float):
        self.last_price = p
        self.last_ts = time.time()

        if self.on_update:
            asyncio.create_task(self.on_update({"price": self.last_price, "ts": self.last_ts}))

    def get_last(self):
        return {"price": self.last_price, "ts": self.last_ts}

    def close(self):
        self.closed = True

class BinanceKlineStream:
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None, heartbeat=10) as ws:
                        print(f"Connected to Binance Kline WS: {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    k = json.loads(msg.data).get("k", {})
                                    candle = {
                                        "openTime": int(k["t"]),
                                        "open": float(k["o"]),
                                        "high": float(k["h"]),
                                        "low": float(k["l"]),
                                        "close": float(k["c"]),
                                        "volume": float(k["v"]),
                                        "closeTime": int(k["T"]),
                                        "isClosed": k.get("x")
                                    }
                                except (ValueError, TypeError, KeyError):
                                    continue     # skip a malformed frame, keep the socket
                                self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance Kline WS failed: {e}")
            if not self.closed:
                await asyncio.sleep(5)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
        else:
            if candle["openTime"] == self.candles[-1]["openTime"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)
        if len(self.candles) > self.limit:
            self.candles.pop(0)

    def set_candles(self, candles: List[Dict]):
        self.candles = candles[-self.limit:]

    def get_candles(self):
        return self.candles

    def close(self):
        self.closed = True

class ClobBookStream:
    """Live Polymarket CLOB order books over WebSocket, replacing REST /book polling.

    Polling /book was the tick-rate floor (~2.8s round trip). This is a latency strategy,
    so the rate at which we SEE the book is the strategy — a 2.8s-old view of a book we
    claim is stale is not an edge. Pushed updates take that to milliseconds.

    Protocol (verified live against the market channel):
      - `book`         : full snapshot for one asset_id — replaces that book.
      - `price_change` : {price_changes: [{asset_id, price, size, side, ...}]} where each
                         entry REPLACES the size at that price level. side BUY = bid,
                         SELL = ask, size 0 = remove the level.
    Levels arrive unsorted; `data.normalize_levels` imposes the order when reading.

    Subscription is fixed at connect time, so changing assets (every 15m window, when the
    market rolls) reconnects. `is_live()` reports whether a usable book exists, and the
    caller falls back to REST whenever it doesn't.

    ── Staleness ──
    A frozen book is the single most dangerous input this bot can have: the whole thesis is
    "the book hasn't repriced yet", so a book that has STOPPED updating is indistinguishable
    from an enormous latency edge, and the bot would size into it at maximum conviction.
    That is the same failure the Binance spot guard exists to prevent.

    We cannot age out the book DATA — a quiet market legitimately produces no updates, and
    treating "unchanged" as "stale" would throw away good books. So we age out the SOCKET:
    an app-level `PING` goes out every 10s (the venue's own keepalive interval) and ANY
    inbound frame — PONG included — refreshes the clock. If nothing arrives for
    MAX_BOOK_AGE_S the stream is not trusted, `is_live()` goes false, the caller falls back
    to REST, and we force a reconnect.
    """

    URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    PING_INTERVAL_S = 10.0     # venue keepalive cadence

    def __init__(self, ws_url: Optional[str] = None):
        self.ws_url = ws_url or self.URL
        self.asset_ids: List[str] = []
        # asset_id -> {"bids": {price: size}, "asks": {price: size}, "ts": epoch_seconds}
        self._books: Dict[str, Dict] = {}
        # Assets for which a full `book` snapshot has arrived. Deltas are meaningless
        # without one (see _apply_price_change).
        self._snapshot_seen: set = set()
        # condition_id / winning asset_id -> resolution, pushed by the venue on `market_resolved`.
        self.resolutions: Dict[str, Dict] = {}
        self._last_msg_ts: float = 0.0
        self._want_reconnect = False
        self.connected = False
        self.closed = False

    def set_assets(self, asset_ids: List[str]):
        """Point the stream at a new market. No-op if unchanged (called every tick)."""
        ids = [str(a) for a in asset_ids if a]
        if set(ids) == set(self.asset_ids):
            return
        self.asset_ids = ids
        self._books = {}          # old market's books are dead to us
        self._snapshot_seen = set()
        self._want_reconnect = True

    def _is_crossed(self, b: Dict) -> bool:
        """A delta-built book can transiently cross (best_bid >= best_ask) if an update was
        dropped or bid/ask frames arrived split. Such a book is not trustworthy to price on."""
        if not b["bids"] or not b["asks"]:
            return False
        return max(b["bids"]) >= min(b["asks"])

    def socket_age(self) -> Optional[float]:
        """Seconds since ANY frame arrived on the socket, or None if never connected."""
        return (time.time() - self._last_msg_ts) if self._last_msg_ts else None

    def socket_fresh(self) -> bool:
        # Requires the connection too: after a drop, `_last_msg_ts` still holds the time of
        # the last frame, so age alone would read "fresh" on a dead socket.
        age = self.socket_age()
        return bool(self.connected) and age is not None and age <= settings.MAX_BOOK_AGE_S

    def is_live(self, asset_id: str) -> bool:
        b = self._books.get(str(asset_id))
        # Not live if: socket down or gone quiet, no full snapshot yet, no book, it's
        # empty, or it's crossed. Any of these => caller falls back to REST /book.
        if not (self.connected and self.socket_fresh()):
            return False
        if str(asset_id) not in self._snapshot_seen:
            return False
        return bool(b and (b["bids"] or b["asks"]) and not self._is_crossed(b))

    def get_book(self, asset_id: str) -> Optional[Dict]:
        """Same shape as the REST /book response, so summarize_order_book is unchanged.
        Returns None for a crossed book so the caller re-fetches over REST instead of pricing
        against a corrupt book."""
        b = self._books.get(str(asset_id))
        if not b or self._is_crossed(b):
            return None
        return {
            "bids": [{"price": p, "size": s} for p, s in b["bids"].items()],
            "asks": [{"price": p, "size": s} for p, s in b["asks"].items()],
        }

    def age(self, asset_id: str) -> Optional[float]:
        b = self._books.get(str(asset_id))
        return (time.time() - b["ts"]) if b else None

    def _blank(self, asset_id: str) -> Dict:
        return self._books.setdefault(str(asset_id), {"bids": {}, "asks": {}, "ts": time.time()})

    def _apply_snapshot(self, msg: Dict):
        aid = str(msg.get("asset_id") or "")
        # Ignore frames for assets we're no longer subscribed to (an in-flight old-market
        # frame after set_assets() would otherwise resurrect a stale book in the cleared dict).
        if not aid or aid not in self.asset_ids:
            return
        book = {"bids": {}, "asks": {}, "ts": time.time()}
        for key, side in (("bids", "bids"), ("asks", "asks")):
            for lvl in msg.get(key) or []:
                try:
                    p = float(lvl["price"]); s = float(lvl["size"])
                except (KeyError, TypeError, ValueError):
                    continue
                if s > 0:
                    book[side][p] = s
        self._books[aid] = book
        self._snapshot_seen.add(aid)

    def _apply_market_resolved(self, msg: Dict):
        """The venue PUSHES the settlement result. Recording it here replaces polling Gamma
        for the market's `closed` flag and gives the authoritative winner within a frame of
        it being decided, instead of up to 15s later."""
        winner = str(msg.get("winning_asset_id") or "")
        entry = {
            "winning_asset_id": winner,
            "winning_outcome": msg.get("winning_outcome"),
            "condition_id": msg.get("market"),
            "ts": time.time(),
        }
        for key in (msg.get("market"), msg.get("slug"), winner):
            if key:
                self.resolutions[str(key)] = entry
        # Also key by every token in the market so a holder of the LOSING side can look up
        # its own asset id and learn that it lost.
        for tid in (msg.get("assets_ids") or msg.get("asset_ids") or []):
            if tid:
                self.resolutions[str(tid)] = entry

    def resolution_for(self, *keys) -> Optional[Dict]:
        for k in keys:
            if k and str(k) in self.resolutions:
                return self.resolutions[str(k)]
        return None

    def _apply_price_change(self, msg: Dict):
        for ch in msg.get("price_changes") or []:
            aid = str(ch.get("asset_id") or "")
            if not aid or aid not in self.asset_ids:   # only apply to the current subscription
                continue
            # A delta REPLACES one level. Applied to a book we never got a snapshot for, it
            # builds a phantom book out of the handful of levels that happened to change —
            # whose "best ask" can be nowhere near the real one. Wait for the snapshot.
            if aid not in self._snapshot_seen:
                continue
            try:
                p = float(ch["price"]); s = float(ch["size"])
            except (KeyError, TypeError, ValueError):
                continue
            book = self._blank(aid)
            side = "bids" if str(ch.get("side", "")).upper() == "BUY" else "asks"
            if s > 0:
                book[side][p] = s      # level replacement (absolute size, not a delta)
            else:
                book[side].pop(p, None)  # size 0 removes the level
            book["ts"] = time.time()

    async def start(self):
        while not self.closed:
            if not self.asset_ids:
                await asyncio.sleep(0.5)
                continue

            self._want_reconnect = False
            subscribed = list(self.asset_ids)
            ping_task = None
            try:
                proxy = get_proxy_url_for(self.ws_url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None,
                                                  heartbeat=10) as ws:
                        await ws.send_json({"assets_ids": subscribed, "type": "market"})
                        self.connected = True
                        self._last_msg_ts = time.time()   # connected counts as activity
                        print(f"Connected to CLOB book WS ({len(subscribed)} assets)")

                        # App-level keepalive. aiohttp's `heartbeat` handles protocol pings
                        # internally and never surfaces the pong, so on a quiet book we would
                        # see no frames at all and could not distinguish "nothing changed"
                        # from "the feed died". The venue answers "PING" with a "PONG" TEXT
                        # frame, which DOES reach us and proves the stream is alive.
                        async def _ping():
                            try:
                                while not self.closed and not ws.closed:
                                    await asyncio.sleep(self.PING_INTERVAL_S)
                                    await ws.send_str("PING")
                            except Exception:
                                pass
                        ping_task = asyncio.create_task(_ping())

                        while not self.closed and not self._want_reconnect:
                            try:
                                msg = await asyncio.wait_for(
                                    ws.receive(), timeout=settings.MAX_BOOK_AGE_S)
                            except asyncio.TimeoutError:
                                # Socket silently stalled: no data, no PONG. Drop it and
                                # reconnect rather than serve a frozen book as live.
                                print("CLOB book WS went quiet — reconnecting")
                                break
                            # ANY frame proves the stream is alive, including the PONG and
                            # non-text control frames.
                            self._last_msg_ts = time.time()
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            if msg.data.strip() in ("PONG", "PING", "OK"):
                                continue
                            try:
                                payload = json.loads(msg.data)
                            except Exception:
                                continue
                            for item in (payload if isinstance(payload, list) else [payload]):
                                if not isinstance(item, dict):
                                    continue
                                et = item.get("event_type")
                                if et == "book":
                                    self._apply_snapshot(item)
                                elif et == "price_change":
                                    self._apply_price_change(item)
                                elif et == "market_resolved":
                                    self._apply_market_resolved(item)
            except Exception as e:
                print(f"CLOB book WS failed: {e}")
            finally:
                self.connected = False
                if ping_task:
                    ping_task.cancel()

            if not self.closed and not self._want_reconnect:
                await asyncio.sleep(1)

    def close(self):
        self.closed = True


class PolymarketChainlinkStream:
    """Polymarket's own crypto price feeds — the per-second streams the site uses to show
    the live price and each market's "Price to Beat".

    ── WHICH FEED IS THE STRIKE? ──
    The venue publishes TWO crypto topics, and they are NOT the same price:

        crypto_prices             symbol "btcusdt"  value 63673.99   <- USDT-quoted spot
        crypto_prices_chainlink   symbol "btc/usd"  value 63588.80   <- Chainlink BTC/USD

    Measured live against the on-chain Chainlink BTC/USD aggregator as an independent
    reference: the chainlink stream sat 0.012% away, `crypto_prices` 0.146% away (~$93 on
    BTC). The gap is the USDT peg — `crypto_prices` is quoted in USDT, not dollars.

    ── WHICH FEED DOES THE 15m MARKET SETTLE ON? ──
    The 5m market resolved on the plain Chainlink BTC/USD stream, which IS this topic. The
    15m market does NOT: its description and `resolutionSource` name the 60-SECOND TWAP
    stream, data.chain.link/streams/btc-usd-twap-60s-streams — "the time-weighted average
    price (TWAP) of Bitcoin, generated by Chainlink".

    The venue's live-data socket does not publish that TWAP anywhere: probing every
    plausible topic name returns data for `crypto_prices_chainlink` and `crypto_prices`
    only. So the strike we mark is the Chainlink SPOT value at `eventStartTime`, which is
    the closest available proxy for the TWAP, not the TWAP itself. They differ by roughly
    the average move over the trailing minute.

    That matters mainly for the price-based settlement FALLBACK; the authoritative paths
    (the venue's pushed `market_resolved` and Gamma's `closed` flag) are unaffected, and
    they are what actually settles a trade. Treat a near-the-money close-vs-open call as
    approximate on this market.

    Open and close for a given window MUST come from the same feed (a cross-feed offset can
    by itself flip a near-the-money result), so every reading is tagged with its source and
    the caller pins settlement to whichever feed marked the open.

    Subscription (verified live):
        {"action":"subscribe","subscriptions":[{"topic":<topic>,"type":"update"}]}
    payloads:
        crypto_prices            {"symbol":"btcusdt","value":63763.59,
                                  "full_accuracy_value":"63763.59000000","timestamp":...}
        crypto_prices_chainlink  {"symbol":"sol/usd","value":73.50276497,
                                  "full_accuracy_value":"73502764970000000000",...}
    Note the differing `full_accuracy_value` scale: plain decimal on one topic, 1e18 fixed
    point on the other. `value` is already human-scaled on both.

    A short TIMESTAMPED HISTORY per feed lets the strike be read at a window's exact
    `eventStartTime` second (`price_at`) rather than "first price seen near the boundary".
    """
    HISTORY_MS = 20 * 60 * 1000  # keep ~20 min of samples (covers a 15m window + slack)
    CHAINLINK_TOPIC = "crypto_prices_chainlink"
    USDT_TOPIC = "crypto_prices"
    # Preference order: the feed the market actually resolves on comes first.
    TOPICS = (CHAINLINK_TOPIC, USDT_TOPIC)

    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url or "wss://ws-live-data.polymarket.com"
        # Feed symbols are "btcusdt" (usdt topic) and "btc/usd" (chainlink topic);
        # symbol_includes is the asset only ("btc"/"eth"/...), so it anchors both.
        self.symbol_includes = (symbol_includes or "btc").lower()
        self.on_update = on_update
        self.closed = False
        # Per-topic state. Kept separate so the two prices are never blended.
        self.feeds = {t: {"history": [], "last": None, "updated_at": None, "recv": None}
                      for t in self.TOPICS}

    # ── preferred-feed accessors (back-compat with single-feed callers) ─────────
    def preferred_source(self) -> Optional[str]:
        """The best feed that currently has data: Chainlink if live, else USDT spot."""
        for t in self.TOPICS:
            if self.feeds[t]["last"] is not None:
                return t
        return None

    @property
    def last_price(self):
        s = self.preferred_source()
        return self.feeds[s]["last"] if s else None

    @property
    def last_updated_at(self):
        s = self.preferred_source()
        return self.feeds[s]["updated_at"] if s else None

    @property
    def last_recv_ts(self):
        s = self.preferred_source()
        return self.feeds[s]["recv"] if s else None

    def _record(self, topic: str, value: float, ts_ms: float):
        f = self.feeds[topic]
        h = f["history"]
        # Any inbound sample proves the feed is alive, even one we then discard as
        # out-of-order, so stamp the liveness clock before the ordering check.
        f["recv"] = time.time()
        if h and ts_ms < h[-1][0]:
            return  # out-of-order/duplicate — ignore entirely (don't clobber last either)
        f["last"] = value
        f["updated_at"] = ts_ms
        h.append((ts_ms, value))
        cutoff = ts_ms - self.HISTORY_MS
        if h[0][0] < cutoff:
            i = 0
            while i < len(h) and h[i][0] < cutoff:
                i += 1
            del h[:i]

    def price_at(self, ts_ms: float, tol_ms: float = 2000.0, source: Optional[str] = None):
        """The feed value at (or nearest within tol) `ts_ms` — the authoritative strike at a
        window's eventStartTime.

        Returns {"price", "source"} or None if no sample is close enough. `source` pins the
        lookup to one feed, which is how settlement guarantees it closes a window against
        the SAME feed that marked its open."""
        topics = (source,) if source else self.TOPICS
        for t in topics:
            f = self.feeds.get(t)
            if not f:
                continue
            best = None; bestd = None
            for ts, v in f["history"]:
                d = abs(ts - ts_ms)
                if bestd is None or d < bestd:
                    bestd = d; best = v
                if ts > ts_ms + tol_ms:
                    break
            if best is not None and bestd is not None and bestd <= tol_ms:
                return {"price": best, "source": t}
        return None

    def strike_miss_reason(self, ts_ms: float, tol_ms: float) -> str:
        """Why `price_at` found nothing at `ts_ms`.

        A window with no strike is simply not traded, so this is the difference between
        "the bot is working and skipped one window" and "the bot has been dead for hours" —
        and without it the dashboard says only "not marked", which looks identical in both
        cases. Only the first reason below is something the user has to act on.
        """
        n = sum(len(f["history"]) for f in self.feeds.values())
        if n == 0:
            return f"price feed has delivered NO samples — check connectivity to {self.ws_url}"
        firsts = [f["history"][0][0] for f in self.feeds.values() if f["history"]]
        lasts = [f["history"][-1][0] for f in self.feeds.values() if f["history"]]
        earliest, latest = min(firsts), max(lasts)
        if earliest > ts_ms + tol_ms:
            return "feed history starts after this window's open (bot started mid-window)"
        if latest < ts_ms - tol_ms:
            age = (time.time() * 1000 - latest) / 1000.0
            return f"price feed stopped {age:.0f}s ago, before this window's open"
        return f"price feed had a gap around the open (no sample within {tol_ms/1000:.1f}s)"

    @staticmethod
    def _parse_value(topic: str, update: dict) -> Optional[float]:
        """`value` is already human-scaled on both topics. `full_accuracy_value` carries more
        precision but is 1e18 fixed point on the Chainlink topic and a plain decimal on the
        other — so it is only used when it agrees with `value`, which makes a future change
        to the scaling fail safe instead of silently producing a nonsense price."""
        try:
            coarse = float(update.get("value"))
        except (TypeError, ValueError):
            coarse = None
        raw = update.get("full_accuracy_value")
        fine = None
        if raw not in (None, ""):
            try:
                fine = float(raw)
                if topic == PolymarketChainlinkStream.CHAINLINK_TOPIC:
                    fine = fine / 1e18
            except (TypeError, ValueError):
                fine = None
        if fine is not None and coarse is not None:
            return fine if abs(fine - coarse) <= max(1e-6, abs(coarse) * 1e-6) else coarse
        return fine if fine is not None else coarse

    async def start(self):
        if not self.ws_url:
            return

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None, heartbeat=5) as ws:
                        # Subscribe to BOTH price topics on the one connection. The
                        # Chainlink stream is the settlement feed; the USDT one is a
                        # fallback for assets/outages where Chainlink isn't publishing.
                        await ws.send_json({"action": "subscribe",
                                            "subscriptions": [{"topic": t, "type": "update"}
                                                              for t in self.TOPICS]})
                        print(f"Connected to Polymarket price WS ({', '.join(self.TOPICS)})")

                        while not self.closed:
                            # WATCHDOG. A bare `await ws.receive()` waits FOREVER on a
                            # socket that has silently died (laptop sleep, VPN drop, NAT
                            # timeout) — the connection object stays "open" and no
                            # exception is ever raised, so the reconnect loop below is
                            # never reached. Observed: the machine slept, this stream hung,
                            # and the bot ran for 9.6 HOURS with no price samples at all —
                            # meaning no strike could be marked and every window showed
                            # "not marked (window not traded)" while looking healthy.
                            # This feed publishes ~1/second, so silence for as long as we'd
                            # tolerate for settlement means it is dead: drop it and reconnect.
                            try:
                                msg = await asyncio.wait_for(
                                    ws.receive(), timeout=settings.MAX_SETTLE_PRICE_AGE_S)
                            except asyncio.TimeoutError:
                                print("Polymarket price WS went quiet — reconnecting")
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            if msg.data in ("PONG", "OK"):
                                continue
                            try:
                                d = json.loads(msg.data)
                            except Exception:
                                continue
                            topic = d.get("topic")
                            if topic not in self.feeds:
                                continue
                            payload = d.get("payload", {})
                            if isinstance(payload, str):
                                try: payload = json.loads(payload)
                                except Exception: continue
                            for update in (payload if isinstance(payload, list) else [payload]):
                                if not isinstance(update, dict):
                                    continue
                                sym = str(update.get("symbol") or "").lower()
                                # Both topics broadcast EVERY asset (xrpusdt, sol/usd, ...),
                                # so filtering is mandatory. Anchor to the START of the
                                # symbol: "btcusdt"/"btc/usd" startswith "btc" ✓, but
                                # "ethbtc" (a BTC-quoted pair) ✗ — an unanchored `in` would
                                # wrongly record ETH/BTC as the BTC price.
                                if not sym.startswith(self.symbol_includes):
                                    continue
                                val = self._parse_value(topic, update)
                                if val is None:
                                    continue
                                ts = update.get("timestamp") or time.time() * 1000
                                ts = float(ts)
                                if ts < 10_000_000_000:
                                    ts *= 1000
                                self._record(topic, val, ts)
                                # Only the preferred (settlement) feed drives the live price.
                                if self.on_update and topic == self.preferred_source():
                                    await self.on_update({"price": val, "updatedAt": ts, "source": topic})
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
            if not self.closed:      # backoff on ANY exit (clean close or error)
                await asyncio.sleep(2)

    def get_last(self, source: Optional[str] = None):
        """Latest value from `source`, or from the preferred feed when unpinned.
        `age` is how long since we RECEIVED a sample — settlement must not be scored
        against a frozen feed, so the caller checks it (see MAX_SETTLE_PRICE_AGE_S)."""
        s = source or self.preferred_source()
        if not s or s not in self.feeds:
            return {"price": None, "updatedAt": None, "age": None, "source": None}
        f = self.feeds[s]
        age = (time.time() - f["recv"]) if f["recv"] else None
        return {"price": f["last"], "updatedAt": f["updated_at"], "age": age, "source": s}

    def close(self):
        self.closed = True


class ClobUserStream:
    """Authenticated CLOB user channel — pushes OUR order and trade events.

    This exists to answer one question truthfully: how many shares did we actually get?
    A Fill-Or-Kill guarantees the USDC spent, not the share count, so the paper-side
    estimate (stake walked over the book) is only ever an estimate of the real fill. Every
    downstream number depends on the true count: the size of the exit SELL, and the
    `shares x $1` payout on a win. An estimate that is too high makes the exit unfillable.

    Protocol (from polymarket-apis, verified field names):
      connect  wss://ws-subscriptions-clob.polymarket.com/ws/user
      send     {"auth": {"apiKey":..., "secret":..., "passphrase":...}, "type": "user"}
      recv     event_type "order" -> {id, size_matched, original_size, status, ...}
               event_type "trade" -> {taker_order_id, size, price, status, ...}

    Fills are accumulated per order id and read back by `filled(order_id)`. The caller
    reconciles asynchronously — we never block a trading tick waiting for a fill report.
    """

    URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    PING_INTERVAL_S = 10.0

    def __init__(self, creds_provider: Callable, ws_url: Optional[str] = None):
        # creds_provider() -> {"apiKey","secret","passphrase"} or None when not in live mode
        self.creds_provider = creds_provider
        self.ws_url = ws_url or self.URL
        self._fills: Dict[str, Dict] = {}   # order_id -> {"shares","usdc","price","status"}
        self.connected = False
        self.closed = False

    def filled(self, order_id: str) -> Optional[Dict]:
        return self._fills.get(str(order_id)) if order_id else None

    def _record_trade(self, msg: Dict):
        oid = str(msg.get("taker_order_id") or "")
        if not oid:
            return
        try:
            size = float(msg.get("size"))
            price = float(msg.get("price"))
        except (TypeError, ValueError):
            return
        if size <= 0:
            return
        f = self._fills.setdefault(oid, {"shares": 0.0, "usdc": 0.0, "price": None, "status": ""})
        # A taker order can match several makers: sum the legs rather than overwrite.
        seen = f.setdefault("_trade_ids", set())
        tid = str(msg.get("id") or msg.get("trade_id") or "")
        if tid and tid in seen:
            return
        if tid:
            seen.add(tid)
        f["shares"] += size
        f["usdc"] += size * price
        f["price"] = f["usdc"] / f["shares"] if f["shares"] else None
        f["status"] = str(msg.get("status") or "")

    def _record_order(self, msg: Dict):
        oid = str(msg.get("id") or msg.get("order_id") or "")
        if not oid:
            return
        try:
            matched = float(msg.get("size_matched"))
        except (TypeError, ValueError):
            return
        f = self._fills.setdefault(oid, {"shares": 0.0, "usdc": 0.0, "price": None, "status": ""})
        # size_matched is cumulative and authoritative — prefer it over summed trade legs.
        if matched > f["shares"]:
            f["shares"] = matched
            try:
                px = float(msg.get("price"))
                f["price"] = px
                f["usdc"] = matched * px
            except (TypeError, ValueError):
                pass
        f["status"] = str(msg.get("status") or "")

    async def start(self):
        while not self.closed:
            creds = None
            try:
                creds = self.creds_provider()
            except Exception:
                creds = None
            if not creds:
                await asyncio.sleep(5)      # not live / no key yet — idle cheaply
                continue

            ping_task = None
            try:
                proxy = get_proxy_url_for(self.ws_url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None,
                                                  heartbeat=10) as ws:
                        await ws.send_json({"auth": creds, "type": "user"})
                        self.connected = True
                        print("Connected to CLOB user WS (fill reporting)")

                        async def _ping():
                            try:
                                while not self.closed and not ws.closed:
                                    await asyncio.sleep(self.PING_INTERVAL_S)
                                    await ws.send_str("PING")
                            except Exception:
                                pass
                        ping_task = asyncio.create_task(_ping())

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            if msg.data.strip() in ("PONG", "PING", "OK"):
                                continue
                            try:
                                payload = json.loads(msg.data)
                            except Exception:
                                continue
                            for item in (payload if isinstance(payload, list) else [payload]):
                                if not isinstance(item, dict):
                                    continue
                                et = item.get("event_type")
                                if et == "trade":
                                    self._record_trade(item)
                                elif et == "order":
                                    self._record_order(item)
            except Exception as e:
                print(f"CLOB user WS failed: {e}")
            finally:
                self.connected = False
                if ping_task:
                    ping_task.cancel()
            if not self.closed:
                await asyncio.sleep(2)

    def close(self):
        self.closed = True


class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None, heartbeat=15) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data_res = json.loads(msg.data)
                                except Exception:
                                    continue
                                if data_res.get("method") == "eth_subscription":
                                    log = data_res.get("params", {}).get("result", {})
                                    topics = log.get("topics", [])
                                    if len(topics) >= 2:
                                        answer = int(topics[1], 16)
                                        if answer >= 2**255:
                                            answer -= 2**256

                                        self.last_price = answer / (10 ** self.decimals)
                                        data_hex = log.get("data", "0x")
                                        if len(data_hex) >= 66:
                                            self.last_updated_at = int(data_hex[2:66], 16) * 1000

                                        if self.on_update:
                                            await self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
            if not self.closed:      # backoff on ANY exit (clean close or error)
                await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"}

    def close(self):
        self.closed = True
