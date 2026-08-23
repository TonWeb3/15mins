import asyncio
import time
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.engines as engines
import bot.utils as utils
import bot.rpc_ws as rpc_ws
from bot.clob_trader import clob_trader
from bot.risk import window_risk

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load previous state
    load_state()

    # Initial seeding
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_15m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(clob_book_stream.start()),
        asyncio.create_task(clob_user_stream.start()),
        asyncio.create_task(polygon_rpc.start()),
        asyncio.create_task(chainlink_refresh_loop()),
        asyncio.create_task(redeem_loop()),
        asyncio.create_task(update_loop())
    ]

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_15m.close()
    polymarket_ws_stream.close()
    chainlink_ws_stream.close()
    clob_book_stream.close()
    clob_user_stream.close()
    polygon_rpc.close()

app = FastAPI(title="Polymarket BTC 15m Assistant", lifespan=lifespan)
# Prefixes every downloaded file (console log / closed-trades CSV) so a user running
# several bots can tell which one a download came from and store it correctly.
STRATEGY_LABEL = "15m-cross-risk"
templates = Jinja2Templates(directory="templates")

# Global state to store the latest data
state = {
    "latest_data": {},
    "last_update_ts": 0,
    "trading_mode": settings.MODE,
    "paper_balance": settings.PAPER_BALANCE_USD,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "last_trade_side": None,
    # Per-window risk snapshot for the dashboard (owned by bot/risk.py).
    "window_risk": {},
    "running": False,   # trading is OFF until the user presses Start on the dashboard
    # Why this window has no open price, when it doesn't (shown on the dashboard).
    "strike_status": None,
    # Auto-withdrawal (capital extractor) state machine:
    #   ARMED -> (balance>=trigger) WAITING_FLAT -> (no open trades) WITHDRAWING
    #         -> WITHDRAW_SUBMITTED -> ARMED (+ resume)
    "withdraw_state": "ARMED",
    "last_withdrawal": None,   # {"amount","tx","time"} of the most recent withdrawal
    # Per-window OPEN prices ("Price to Beat"), keyed by window start ms:
    #   {start_ms: {"chainlink": float|None, "close": float|None, "source": str|None}}
    # `chainlink` holds the Polymarket crypto_prices value read AT eventStartTime, `close`
    # the same feed's value at the window end. Used to settle trades the way Polymarket
    # does (close vs open, same feed on both ends).
    "market_opens": {},
    "last_window_start": None,
    # Live wins awaiting redemption into pUSD (drained by redeem_loop).
    "pending_redemptions": []
}

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"]
        }
        with open("state_data.json", "w") as f:
            json.dump(data_to_save, f, indent=2)
            
        # Only persist the PAPER bankroll. In live mode `paper_balance` mirrors the real
        # on-chain pUSD balance, and writing that into config would overwrite the user's
        # configured paper starting balance with a live account number.
        if state["trading_mode"] == "paper" and os.path.exists("config.json"):
            with open("config.json", "r") as f:
                cfg = json.load(f)
            cfg["paper_balance_usd"] = state["paper_balance"]
            with open("config.json", "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        if os.path.exists("state_data.json"):
            with open("state_data.json", "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                log_message("State loaded from state_data.json")
    except Exception as e:
        print(f"Error loading state: {e}")

def log_message(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    # A Windows console (or a redirected stdout) is usually cp1252, and printing a
    # character it cannot encode raises UnicodeEncodeError. log_message is called from
    # inside the 500ms trading tick, so that exception propagates into update_loop's
    # handler and ABORTS THE WHOLE TICK — observed live: one '─' in the window-roll
    # message silently killed the tick that marks the strike. The console is the least
    # important consumer of a log line; it must never be able to stop the bot trading.
    # flush=True: redirected stdout is block-buffered, so `python main.py > log` otherwise
    # shows nothing for minutes while the buffer fills — useless for watching it live.
    try:
        print(formatted, flush=True)
    except Exception:
        try:
            print(formatted.encode("ascii", "replace").decode("ascii"), flush=True)
        except Exception:
            pass
    state["logs"].append(formatted)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)
    # Persist the FULL console log to a file so the whole history can be downloaded
    # (the in-memory list above is capped at 100 for the live view).
    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/console.log", "a", encoding="utf-8") as _lf:
            _lf.write(f"{datetime.now():%Y-%m-%d} {formatted}\n")
    except Exception:
        pass

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
# 15m is the WINDOW base: each candle's open IS the 15m market's open (same boundaries,
# :00/:15/:30/:45), so it anchors the spot MOVE applied to the venue's real strike.
binance_kline_15m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="15m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
# Live CLOB order books, pushed. Replaces REST /book polling on the critical path.
clob_book_stream = ws_data.ClobBookStream()
# Authenticated user channel — pushes OUR fills so `shares` is the venue's number and not
# our estimate. Idles until live mode has a key (the provider returns None in paper).
clob_user_stream = ws_data.ClobUserStream(
    creds_provider=lambda: clob_trader.get_api_creds() if settings.MODE == "live" else None
)

# ── Chain access, over WebSocket ────────────────────────────────────────────────
# One persistent Polygon JSON-RPC socket carries BOTH `eth_call` reads and log
# subscriptions, so nothing on the chain side is fetched over HTTP or on a timer:
#   - the live pUSD balance updates when a Transfer touches our wallet (was a 10s poll)
#   - the Chainlink aggregator is read over the same socket (was an HTTP RPC failover
#     chain measured at ~4.5s, which is why it had to be moved off the tick at all)
polygon_rpc = rpc_ws.PolygonRpcStream()
chainlink_reader = rpc_ws.ChainlinkReader(polygon_rpc, settings.get_aggregator(settings.SYMBOL))
pusd_watcher = rpc_ws.PusdBalanceWatcher(polygon_rpc)

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

# The ACTIVE market only changes once per 15-minute window, but the order books change
# constantly. Resolving the market via Gamma on every tick would be a wasted request at a
# 500ms poll, so cache it and re-resolve only when it expires (or every MARKET_CACHE_TTL_S
# as a safety net). The books/prices below are still fetched fresh every tick — that is
# where the latency edge lives.
# How close to `eventStartTime` a feed sample has to be to count as the strike. The venue
# publishes ~1/second, but the Chainlink topic was measured with gaps up to 3s — at the old
# 2.5s the strike fell through to the USDT-quoted feed (0.13% off, ~$85 on BTC) or, if that
# also gapped, the window was skipped entirely. 5s comfortably covers the observed gap; a
# few seconds of BTC drift is far smaller than the cross-feed offset it avoids.
STRIKE_TOL_MS = 5000.0

# The market IDENTITY is the one input with no push feed: the venue's live-data socket
# publishes no market/event lifecycle topic (probed live — every plausible name returns
# `topic ... not found`), so Gamma REST is the only way to learn which window is trading.
#
# It is therefore driven by an EVENT rather than a timer: a 15m market is only ever
# replaced when the current one expires, so we re-resolve on that boundary and prefetch
# the NEXT window shortly before it, instead of re-asking every 10s for an answer that
# changes four times an hour. `_market_cache["next"]` is promoted at the roll, so the
# boundary tick — the one that marks the strike — never waits on a request.
PREFETCH_BEFORE_END_S = 20.0
# Floor between Gamma resolutions, engaged only when a market has no usable `endDate`
# (see resolve_active_market). Never reached in normal operation.
MIN_RESOLVE_INTERVAL_S = 5.0
_market_cache: Dict[str, Any] = {"market": None, "fetched_at": 0.0, "end_ms": 0.0,
                                 "attempted_at": 0.0, "next": None, "next_start_ms": 0.0,
                                 "prefetching": False}


def _parse_ms(iso: Optional[str]) -> float:
    if not iso:
        return 0.0
    try:
        return datetime.fromisoformat(iso.replace('Z', '+00:00')).timestamp() * 1000
    except Exception:
        return 0.0


async def _pick_live_market(now_ms: float) -> Optional[Dict[str, Any]]:
    """Ask Gamma which market is trading now, and remember the one after it."""
    if settings.POLYMARKET_SLUG:
        return await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    if not settings.POLYMARKET_AUTO_SELECT_LATEST:
        return None
    events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
    markets = data.flatten_event_markets(events)
    # Every window that hasn't ended yet, soonest first: [0] is trading now, [1] is next.
    live_markets = [m for m in markets if _parse_ms(m.get("endDate")) > now_ms]
    if not live_markets:
        return None
    live_markets.sort(key=lambda x: x["endDate"])
    if len(live_markets) > 1:
        nxt = live_markets[1]
        _market_cache["next"] = nxt
        _market_cache["next_start_ms"] = _parse_ms(nxt.get("eventStartTime")) or 0.0
    return live_markets[0]


async def _prefetch_next_market():
    """Warm the NEXT window's market a little before the roll, off the trading tick."""
    if _market_cache["prefetching"]:
        return
    _market_cache["prefetching"] = True
    try:
        await _pick_live_market(time.time() * 1000)
    except Exception:
        pass          # a failed prefetch just means the roll resolves inline, as before
    finally:
        _market_cache["prefetching"] = False


async def resolve_active_market() -> Optional[Dict[str, Any]]:
    now_ms = time.time() * 1000
    cached = _market_cache["market"]

    if cached is not None and now_ms < _market_cache["end_ms"]:
        # Still inside the cached market's window — its identity cannot have changed.
        # Warm the next one shortly before the boundary so the roll costs no latency.
        if (_market_cache["end_ms"] - now_ms) <= PREFETCH_BEFORE_END_S * 1000 and not _market_cache["next"]:
            asyncio.create_task(_prefetch_next_market())
        return cached

    # The cached market has expired. Promote the prefetched successor if it has started.
    nxt = _market_cache.get("next")
    if nxt is not None and _parse_ms(nxt.get("endDate")) > now_ms:
        _market_cache.update({"market": nxt, "fetched_at": time.time(),
                              "end_ms": _parse_ms(nxt.get("endDate")),
                              "next": None, "next_start_ms": 0.0})
        return nxt

    # A network resolve is needed. FLOOR IT — this is a safety valve, not a poll. Whenever
    # Gamma returns nothing usable (an outage, or markets with no parseable `endDate`) the
    # cache can never be satisfied, and without a floor this would fire a request on every
    # 500ms tick for as long as the condition lasts. Stamping the ATTEMPT (not just a
    # success) is what closes that: retries are spaced, and the caller simply sees
    # `market_not_found` in between, exactly as it would anyway.
    # In normal operation this never engages — a valid `end_ms` short-circuits above, and
    # at a genuine window roll the last attempt was ~15 minutes ago.
    if (time.time() - _market_cache["attempted_at"]) < MIN_RESOLVE_INTERVAL_S:
        return cached
    _market_cache["attempted_at"] = time.time()

    market = None
    try:
        market = await _pick_live_market(now_ms)
    except Exception as e:
        # A transient Gamma failure must not cost the whole tick. The market IDENTITY only
        # changes once per 5 minutes, so a slightly stale-but-unexpired cached market is far
        # better than no market at all (no book, no EV, no exit marking). Only the identity
        # is reused — the books and prices are still fetched fresh every tick.
        if cached is not None and now_ms < _market_cache["end_ms"]:
            log_message(f"Gamma lookup failed ({type(e).__name__}) — reusing cached market for this window")
            return cached
        raise

    if market:
        _market_cache.update({"market": market, "fetched_at": time.time(),
                              "end_ms": _parse_ms(market.get("endDate"))})
    return market


async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = await resolve_active_market()

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # ── The book: pushed over WebSocket, REST only as a fallback ────────────────
    # Polling /book was the tick-rate floor (~2.8s). The whole thesis is that we see a
    # move before the book reprices, so a seconds-old view of the book defeats the point.
    # The WS carries a live snapshot + level deltas; we read it with zero network cost.
    clob_book_stream.set_assets([up_token_id, down_token_id])
    up_book = clob_book_stream.get_book(up_token_id) if clob_book_stream.is_live(up_token_id) else None
    down_book = clob_book_stream.get_book(down_token_id) if clob_book_stream.is_live(down_token_id) else None
    book_source = "ws"

    if up_book is None or down_book is None:
        # Not connected yet, or the market just rolled and we're resubscribing.
        book_source = "rest"
        try:
            up_book, down_book = await asyncio.gather(
                data.fetch_order_book(up_token_id),
                data.fetch_order_book(down_token_id)
            )
        except Exception:
            up_book = down_book = None

    empty = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None,
             "askLiquidity": None, "askLevels": [], "bidLevels": []}
    up_book_summary = data.summarize_order_book(up_book) if up_book else dict(empty)
    down_book_summary = data.summarize_order_book(down_book) if down_book else dict(empty)
    if up_book is None and down_book is None:
        book_source = "none"

    # ENTRY prices = the BEST ASK, i.e. what we actually PAY to buy the share.
    #
    # These feed the EV gate (EV = fair - price), so they must be the ask. They used to
    # come from CLOB /price?side=buy, but that endpoint returns the best BID (verified
    # live: side=buy -> 0.84 while the book's best ask was 0.85) — so every edge was
    # overstated by the full spread, and we'd enter trades that were not actually
    # EV-positive once the real fill price was paid. On a 15m market, where a 1c spread is
    # a quarter of the 4c EV threshold, that is the difference between an edge and a leak.
    #
    # Reading the ask straight off the book we already fetch is both correct and two
    # fewer round trips per tick. Exits/marks still use bestBid from `orderbook` below
    # (what we'd RECEIVE selling), which was already right.
    # The window's OPEN time ("Price to Beat" is fixed at this second). Polymarket gives
    # it as eventStartTime; fall back to the aligned endDate - window if absent.
    event_start_ms = None
    est = market.get("eventStartTime")
    if est:
        try:
            event_start_ms = datetime.fromisoformat(est.replace('Z', '+00:00')).timestamp() * 1000
        except Exception:
            event_start_ms = None
    if event_start_ms is None and market.get("endDate"):
        try:
            end_ms = datetime.fromisoformat(market["endDate"].replace('Z', '+00:00')).timestamp() * 1000
            event_start_ms = end_ms - settings.CANDLE_WINDOW_MINUTES * 60_000
        except Exception:
            event_start_ms = None

    return {
        "ok": True,
        "market": market,
        "book_source": book_source,
        "event_start_ms": event_start_ms,
        "prices": {
            "up": up_book_summary["bestAsk"] if up_book_summary["bestAsk"] is not None else gamma_yes,
            "down": down_book_summary["bestAsk"] if down_book_summary["bestAsk"] is not None else gamma_no
        },
        # Midpoints = the real-time mark Polymarket's site shows (matches the % on the page).
        "mids": {
            "up": up_book_summary.get("mid"),
            "down": down_book_summary.get("mid")
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        }
    }


def compute_stake() -> float:
    """Dollars to put at risk on one trade — RISK_VALUE% of the balance RECORDED AT THE
    START OF THIS WINDOW (or a flat RISK_VALUE), owned by bot/risk.py.

    Sizing off the recorded balance rather than the live one is the point: every trade in
    a window is the same size, so a stop-loss doesn't shrink the next stake and a win
    doesn't inflate it. It is also what makes the window's loss/win caps mean a fixed
    number of trades ("three stop-losses"), which they would not if the stake moved."""
    return window_risk.stake()


def effective_entry(orderbook: Dict[str, Any], side: str, stake_usd: float,
                    fallback_price: Optional[float]) -> Dict[str, Any]:
    """The price we'd ACTUALLY pay to put `stake_usd` into `side`, by walking the asks.

    The EV gate must run on this, not on the best ask: an "edge" that exists only for the
    first few shares at the top of the book is not an edge for the size we actually trade.
    Returns the blended VWAP plus how much of the stake the book can absorb.
    """
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    levels = ob.get("askLevels") or []
    if not levels or not stake_usd or stake_usd <= 0:
        return {"vwap": fallback_price, "shares": None, "fillable_usd": None,
                "worst": fallback_price, "walked": False}
    w = data.walk_asks(levels, stake_usd)
    if not w["vwap"]:
        return {"vwap": fallback_price, "shares": None, "fillable_usd": 0.0,
                "worst": fallback_price, "walked": False}
    # `worst` = the deepest ask the fill has to reach. This, not the VWAP or the best ask,
    # is what a live marketable limit must be priced against.
    return {"vwap": w["vwap"], "shares": w["shares"], "fillable_usd": w["filled_usd"],
            "worst": w["worst_price"], "walked": True}

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], target_open: float, token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None, strike_source: Optional[str] = None):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    # No Polymarket/Chainlink open for this window → do NOT open. We can't score the
    # trade against a real open, so we wait for the next window where we mark it.
    if target_open is None:
        return "no_open_price"

    side = decision["side"]

    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    # ── Risk per trade, priced by WALKING THE BOOK ──────────────────────────────
    # The stake is this WINDOW's risk-per-trade (RISK_TYPE/RISK_VALUE applied to the
    # balance recorded when the window opened), so it is identical for every trade in
    # the window.
    #
    # The fill is then priced by walking the ask levels. Taking `stake / best_ask` (the
    # old behaviour) assumed the whole stake cleared at the top of the book, which on a
    # thin 15m book overstates the share count — and does so worst on exactly the trades
    # that look most attractive. `walk_asks` returns the true blended price and how much
    # of the stake the book can actually absorb.
    balance = state["paper_balance"]
    stake = compute_stake()
    if stake <= 0:
        return "stake_zero"

    eff = effective_entry(orderbook, side, stake, price)
    if not eff.get("walked"):
        return "no_book"   # no levels to price against — never guess a fill

    # The stake is EXACTLY the configured risk per trade — never trimmed to fit the book
    # and never scaled up. If the ask side cannot absorb the whole stake, we don't take a
    # smaller position: we don't take the trade at all.
    fillable = eff["fillable_usd"] or 0.0
    if fillable + 1e-6 < stake:
        log_message(f"Skip {side}: book absorbs only ${fillable:.2f} of the ${stake:.2f} stake")
        return "thin_book"
    if fillable < settings.MIN_BOOK_LIQUIDITY_USD:
        log_message(f"Skip {side}: thin book (${fillable:.2f} on the ask)")
        return "thin_book"

    amount_to_risk = stake                 # always the full risk per trade
    shares = eff["shares"] or 0.0
    fill_price = eff["vwap"]

    if not fill_price or shares <= 0:
        return "no_price"

    # What the position would fetch if sold RIGHT NOW (walking the bids). Recorded only so
    # every close can report how much of its P/L was the unavoidable round trip we paid to
    # open — spread plus book depth — versus the market actually moving. Diagnostic; it
    # does not gate anything.
    ob_side = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    liq = data.walk_bids(ob_side.get("bidLevels") or [], shares)
    entry_value = liq["proceeds"] if liq["sold_shares"] + 1e-6 >= shares else None
    entry_cost = (entry_value - amount_to_risk) if entry_value is not None else None
    if balance < amount_to_risk:
        print(f"Insufficient paper balance ({balance}) for risk amount ({amount_to_risk})")
        return "insufficient_balance"

    slippage = (fill_price - price) if price else 0.0

    end_date_str = market.get("endDate")
    end_ts = 0
    if end_date_str:
        try:
            end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
        except: pass
    # Fallback so a trade always has a definite expiry even if endDate is missing/unparseable
    if not end_ts:
        end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

    trade = {
        "market_id": market["id"],
        "market_slug": market.get("slug"),
        # Kept at entry so a win can be REDEEMED into pUSD later without re-resolving the
        # market (which may have rolled). Redemption is what turns a live win into
        # spendable balance; see maybe_redeem_settled().
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "neg_risk": bool(market.get("negRisk") or market.get("neg_risk") or False),
        "side": side,
        # The held side's CLOB token id, stored at entry so a manual/live close can always
        # sell it without re-deriving from the current snapshot (robust if the window rolled).
        "token_id": (token_ids or {}).get("up" if side == "UP" else "down"),
        "entry_price": fill_price,      # blended VWAP actually paid, not the best ask
        "best_ask_at_entry": price,     # top of book, for slippage attribution
        "slippage": slippage,
        # Liquidation value at entry. Every close is reported as
        # (entry cost) + (market move) against this, so a big P/L on a book that barely
        # moved is visibly the round trip rather than a mystery.
        "entry_value": entry_value,
        "entry_cost": entry_cost,
        "amount": amount_to_risk,
        "shares": shares,
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": target_open,
        # Which venue feed marked the Price to Beat at eventStartTime. Settlement reads the
        # close from THIS SAME feed so the two ends of the window can't straddle the ~0.13%
        # gap between the venue's USDT-quoted and Chainlink BTC/USD prices.
        "strike_source": strike_source,
        # "above_open"/"below_open" (fresh signal) or "reverse_entry" (the buy half of a
        # close-and-reverse on a cross of the open).
        "open_reason": decision.get("reason", "entry"),
        "end_ts": end_ts,
        # The window this trade BELONGS TO. Taken from the risk manager's current window id
        # (the same key `state["market_opens"]` uses), not re-derived from endDate: it is
        # what charges the trade's P/L to the right window's loss/win budget, and deriving
        # it separately would silently mis-attribute — i.e. fail OPEN on the loss cap —
        # whenever endDate is missing or off the aligned boundary. It also lets an
        # early-exited trade be scored against the market's true open→close later.
        "window_start_ms": (window_risk.window_start_ms
                            if window_risk.window_start_ms is not None
                            else int(end_ts * 1000) - settings.CANDLE_WINDOW_MINUTES * 60_000),
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        window_risk.record_open()
        save_state()

        open_why = "REVERSE" if decision.get("reason") == "reverse_entry" else "cross signal"
        log_message(f"Executed PAPER trade [{open_why}]: {side} @ {fill_price:.3f} VWAP "
                    f"(ask {price:.2f}, slip {slippage*100:+.1f}¢) {market.get('slug')} "
                    f"(${amount_to_risk:.2f}, {shares:.1f} sh)")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        # Quote the order against the WORST ask the stake has to reach, not the best ask.
        # The limit becomes worst + CLOB_MAX_SLIPPAGE, so an order sized by walking the
        # book can actually clear the levels it was sized against. Quoting off the top of
        # book got the FOK killed on exactly the thin books this sizing exists to handle.
        marketable_quote = eff.get("worst") or price
        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, marketable_quote)
        if result.get("ok"):
            resp = result.get("response") or {}
            order_id = result.get("order_id")
            if not order_id and isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            trade["order_id"] = order_id
            trade["order_response"] = resp
            trade["quote_at_entry"] = marketable_quote

            # A FOK fixes the USDC spent, not the share count. Where the venue reports the
            # real fill, use it — `shares` drives both the exit SELL size and the
            # `shares x $1` payout, and an estimate that is too high makes the exit
            # unfillable. Otherwise keep the walked estimate and let the user-channel
            # stream reconcile it (see reconcile_live_fills()).
            if result.get("filled_shares"):
                trade["shares"] = result["filled_shares"]
                trade["entry_price"] = result.get("fill_price") or trade["entry_price"]
                trade["shares_source"] = "order_response"
            else:
                trade["shares_source"] = "estimate"

            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            # Debit the mirrored balance immediately. It is refreshed from chain only every
            # on a chain EVENT, so until our own fill's Transfer log lands the stake would
            # be counted twice (still in cash AND in the position's mark). Deliberately do
            # NOT force a chain re-read here: it would race the settlement of our own fill
            # and could read the pre-trade balance, undoing this debit. The Transfer log
            # from that fill corrects it authoritatively a moment later.
            state["paper_balance"] = max(0.0, state["paper_balance"] - amount_to_risk)
            window_risk.record_open()
            save_state()
            open_why = "REVERSE" if decision.get("reason") == "reverse_entry" else "cross signal"
            log_message(f"Executed LIVE trade [{open_why}]: {side} ${amount_to_risk:.2f} on {market.get('slug')} "
                        f"(order {order_id}, {trade['shares']:.2f} sh via {trade['shares_source']})")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def close_position_at_bid(trade: Dict[str, Any], ob: Dict[str, Any],
                                token_id: Optional[str], reason: str) -> Optional[Dict[str, Any]]:
    """Sell the whole held position into the bids and book the close.

    Shared by the take-profit/stop-loss exit and the close-and-reverse, so both price the
    exit the same way: WALK THE BIDS for the full size (valuing every share at the best
    bid overstates the proceeds and would fire a take-profit on a sale we can't actually
    get), and quote a live order against the LOWEST bid the sale must reach.

    Returns None without touching anything if the bids can't absorb the whole position or
    the live sell is killed — the caller must treat that as "still holding".
    """
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])
    exit_price = ex["vwap"]
    if not exit_price or ex["sold_shares"] + 1e-6 < trade["shares"]:
        return None

    if state["trading_mode"] == "live":
        if not token_id:
            return None
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id,
                                         trade["shares"], ex.get("worst_price") or exit_price)
        if not result.get("ok"):
            log_message(f"{reason.upper()} sell FAILED ({trade['side']}): {result.get('error')}")
            return None
        # The live balance is re-read from chain on its own cadence, not credited here.
    else:
        state["paper_balance"] += ex["proceeds"]   # walked bids, not best-bid × shares

    pl = ex["proceeds"] - trade["amount"]
    # Split the result into the part that was never recoverable (the round trip we paid to
    # open: spread + book depth on both sides) and the part the market actually gave or
    # took. Without this, a −20% close on a book whose quoted bid barely moved looks wrong.
    entry_cost = trade.get("entry_cost")
    market_move = (ex["proceeds"] - trade["entry_value"]) if trade.get("entry_value") else None
    trade["exit_entry_cost"] = entry_cost
    trade["exit_market_move"] = market_move

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = reason
    trade["settlement_price_at_expiry"] = exit_price
    trade["exit_mark"] = exit_price          # price we sold out at, not the market close
    trade["profit_loss"] = pl
    # Record the market OPEN now; the CLOSE and whether this side would have won is
    # backfilled once the window ends (see the backfill pass in update_loop). That is why
    # an early exit can book a loss on a side the market later resolves in favour of.
    trade["open_price"] = trade.get("strike_price")
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None

    # Charge the realized P/L to the window that OWNS this trade and re-check the budget.
    blocked = window_risk.record_close(trade.get("window_start_ms"), pl, reason, trade["side"])
    save_state()
    return {"exit_price": exit_price, "proceeds": ex["proceeds"], "pl": pl, "blocked": blocked,
            "entry_cost": entry_cost, "market_move": market_move,
            "breakdown": _pl_breakdown(trade["amount"], entry_cost, market_move)}


def _pl_breakdown(stake: float, entry_cost: Optional[float], market_move: Optional[float]) -> str:
    """'entry cost -8.2%, market -11.9%' — appended to every close log so the size of a
    P/L can be read against what actually caused it."""
    if entry_cost is None or market_move is None or not stake:
        return ""
    return (f" [entry cost {entry_cost / stake * 100:+.1f}%, "
            f"market {market_move / stake * 100:+.1f}%]")


async def maybe_tp_sl(poly_snapshot: Dict[str, Any]):
    """Take-profit / stop-loss — the only discretionary exit on an open position.

    Closes when unrealized P/L (marked on the walked bids) reaches +TAKE_PROFIT_PCT or
    -STOP_LOSS_PCT of the stake. The consequence is then the window risk budget's, not a
    market lock: a take-profit ends the window (STOP_AFTER_WIN), and a stop-loss disarms
    entries until spot crosses the open again. Returns "take_profit"/"stop_loss" if it
    closed, else None."""
    if not settings.TP_SL_ENABLED or not state["active_trades"]:
        return None
    if not poly_snapshot.get("ok"):
        return None

    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    if str(trade.get("market_id")) != str(market.get("id")):
        return None  # position is in a prior market — let it settle on its own

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = (poly_snapshot.get("orderbook", {}) or {}).get(held_key) or {}
    # Mark on what a full liquidation would really return.
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])
    if not ex["vwap"] or ex["sold_shares"] + 1e-6 < trade["shares"]:
        return None

    amount = trade["amount"]
    unreal = ex["proceeds"] - amount            # the true, spread-inclusive P/L
    pl_pct = (unreal / amount) * 100.0 if amount else 0.0

    # ── TP/SL MEASURE THE MARKET, NOT THE SPREAD WE PAID TO GET IN ─────────────
    # We buy at the ask (walking up) and are marked at the bid (walking down), so a
    # position is already 6-12% down the instant it fills — at ~50¢ with a 5¢ spread that
    # is MORE than a 10% stop. Measured against the stake, the stop was therefore tripped
    # before the market did anything: live history showed an UP position entered and
    # stopped out IN THE SAME SECOND for -$16, and 2-4 second stop-outs for -$11 to -$21,
    # while every trade that survived ~9s won. It also made the two exits wildly
    # asymmetric — the stop needed 0% of market movement, the take-profit needed ~40%.
    #
    # So both are measured from the position's LIQUIDATION VALUE AT ENTRY: "-10%" now
    # means the market took 10% of the stake away, not that we crossed the spread.
    # The P/L that is booked is still the real one, spread included.
    basis = trade.get("entry_value") or amount
    move = ex["proceeds"] - basis
    move_pct = (move / amount) * 100.0 if amount else 0.0

    hit = None
    if settings.TAKE_PROFIT_PCT > 0 and move_pct >= settings.TAKE_PROFIT_PCT:
        hit = "take_profit"
    elif settings.STOP_LOSS_PCT > 0 and move_pct <= -settings.STOP_LOSS_PCT:
        hit = "stop_loss"
    if not hit:
        return None

    token_id = trade.get("token_id") or (poly_snapshot.get("token_ids", {}) or {}).get(held_key)
    closed = await close_position_at_bid(trade, ob, token_id, hit)
    if not closed:
        return None

    label = "TAKE PROFIT" if hit == "take_profit" else "STOP LOSS"
    tail = ""
    if closed["blocked"]:
        tail = f"; window CLOSED ({closed['blocked']})"
    elif hit == "stop_loss":
        tail = f"; {trade['side']} blocked until price is on the other side of the open"
    log_message(f"{label}: closed {trade['side']} @ {closed['exit_price']:.2f} "
                f"(market {move_pct:+.1f}% -> P/L ${closed['pl']:+.2f}, {pl_pct:+.1f}% of stake)"
                f"{closed['breakdown']}{tail}")
    return hit


async def maybe_reverse_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any],
                                 seconds_left: Optional[float], strike_open: Optional[float],
                                 strike_source: Optional[str] = None):
    """CLOSE AND REVERSE when spot crosses to the other side of the open.

    This is the second half of the entry rule, not an optional extra: the side of the open
    IS the position, so the moment spot crosses, the held side is sold and the other side
    bought in the same tick.

    The close always runs (an exit is never blocked by the risk budget — being on the
    wrong side of the open is exactly what we're trying to stop paying for). The re-open
    only runs if the window still allows entries, so a reversal whose CLOSE was a winner
    (STOP_AFTER_WIN) or which exhausted the loss budget leaves us flat by design.
    """
    def _skip(reason: str):
        if state.get("last_reverse_skip") != reason:
            state["last_reverse_skip"] = reason
            log_message(f"REVERSE skipped: {reason}")

    if decision.get("action") != "REVERSE" or not state["active_trades"]:
        return None
    if not poly_snapshot.get("ok"):
        return None

    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})
    new_side = decision["side"]

    if str(trade.get("market_id")) != str(market.get("id")):
        _skip("position is in a prior market (window rolled) — letting it settle")
        return None

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    token_id = trade.get("token_id") or token_ids.get(held_key)
    closed = await close_position_at_bid(trade, ob, token_id, "reverse")
    if not closed:
        _skip(f"bids can't absorb the {trade['side']} position")
        return None
    state["last_reverse_skip"] = None

    log_message(f"REVERSE: price is on the {new_side} side of the open — closed "
                f"{trade['side']} @ {closed['exit_price']:.2f} (P/L ${closed['pl']:+.2f})"
                f"{closed['breakdown']}; taking {new_side}")

    # Re-open the other side, unless the window's risk budget just closed, a withdrawal is
    # waiting for the account to go flat, or there is no longer enough time for a fresh
    # position (the same gate a flat entry uses).
    if state["withdraw_state"] != "ARMED":
        log_message(f"REVERSE close-only: withdrawal pending ({state['withdraw_state']}) — now flat")
        return {"side": new_side, "opened": False, "reason": "withdraw_pending"}
    allowed, why = window_risk.can_enter()
    if not allowed:
        log_message(f"REVERSE close-only: not re-entering {new_side} ({why}) — now flat")
        return {"side": new_side, "opened": False, "reason": why}
    if seconds_left is not None and seconds_left < settings.MIN_SECONDS_LEFT:
        log_message(f"REVERSE close-only: only {seconds_left:.0f}s left — now flat")
        return {"side": new_side, "opened": False, "reason": "too_late_to_reenter"}

    reverse_decision = {"action": "ENTER", "side": new_side, "phase": "CROSS",
                        "strength": "REVERSE", "reason": "reverse_entry"}
    res = await execute_trade(reverse_decision, poly_snapshot["prices"], market, strike_open,
                              token_ids, orderbook, strike_source)
    # Only report a completed reversal if the new side actually opened — execute_trade can
    # refuse (thin book, no strike, stake > balance), which leaves us flat.
    if res != "entered":
        log_message(f"REVERSE incomplete: closed {trade['side']} but could not open {new_side} ({res}) — now flat")
        return {"side": new_side, "opened": False, "reason": res}
    return {"side": new_side, "opened": True, "reason": res}


async def close_active_trade_now() -> Dict[str, Any]:
    """Manually close the open position RIGHT NOW at the current bid, then close the
    window's risk budget so nothing re-enters until the next 15m window.

    Unlike TP/SL this is a forced exit: it closes even when the bid side can't absorb the
    whole position. In LIVE that means a real FOK sell — if the book is too thin the venue
    kills it and we report the failure (nothing was sold, so nothing is locked). In PAPER
    we book the walked-bid proceeds for whatever the bids absorb and mark any unsellable
    remainder at the last bid we saw (conservative), so the position always ends flat.
    """
    if not state["active_trades"]:
        return {"ok": False, "error": "no_active_trade"}

    trade = state["active_trades"][0]
    held_key = "up" if trade["side"] == "UP" else "down"

    # Fresh book for the trade's own market (may differ from the active one if the window
    # just rolled).
    snapshot = await fetch_polymarket_snapshot()

    same_market = snapshot.get("ok") and str(trade.get("market_id")) == str(snapshot["market"].get("id"))
    ob = (snapshot.get("orderbook", {}) or {}).get(held_key) or {} if same_market else {}
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])

    # Exit mark: the walked-bid VWAP if we have one, else the last marked bid, else entry.
    exit_price = ex["vwap"] or trade.get("cur_bid") or trade.get("entry_price")
    if not exit_price or exit_price <= 0:
        return {"ok": False, "error": "no_exit_price"}

    if state["trading_mode"] == "live":
        # Prefer the token id stored at entry (always correct); fall back to the snapshot.
        token_id = trade.get("token_id") or (snapshot.get("token_ids", {}) if same_market else {}).get(held_key)
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        # Quote against the LOWEST bid we'd have to reach; on a forced close the walk may
        # not cover the whole size, so fall back to the marked exit price.
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id,
                                         trade["shares"], ex.get("worst_price") or exit_price)
        if not result.get("ok"):
            log_message(f"MANUAL CLOSE sell FAILED ({trade['side']}): {result.get('error')}")
            return {"ok": False, "error": result.get("error", "sell_failed")}
        proceeds = ex["proceeds"] if ex["vwap"] else trade["shares"] * exit_price
    else:
        # Paper: real proceeds for the absorbed portion + remainder marked at the exit price.
        remainder = max(0.0, trade["shares"] - ex["sold_shares"])
        proceeds = ex["proceeds"] + remainder * exit_price
        state["paper_balance"] += proceeds

    unreal = proceeds - trade["amount"]
    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "manual"
    trade["settlement_price_at_expiry"] = exit_price
    trade["exit_mark"] = exit_price
    trade["profit_loss"] = unreal
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = trade.get("close_price")
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    # A manual close ends the window: the P/L is charged to the window's budget and
    # entries stop until the next 15m window rolls.
    window_risk.record_close(trade.get("window_start_ms"), unreal, "manual", trade["side"])
    save_state()
    log_message(f"MANUAL CLOSE: {trade['side']} @ {exit_price:.3f} (P/L ${unreal:+.2f}); "
                f"no re-entry until the next 15m window")
    return {"ok": True, "side": trade["side"], "exit_price": exit_price, "profit_loss": unreal}


async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here so open and close come from the SAME feed (no cross-feed offset
    # can flip a near-the-money result). Binance spot is only a last-resort fallback.
    #
    # `settle_price_fresh` says whether that feed is actually alive. A dead feed keeps
    # returning its LAST value forever, and settling close-vs-strike against a frozen
    # number silently mis-resolves every near-the-money window in the same direction. When
    # it is stale we simply don't use the price-based fallback; the trade waits for the
    # venue's own resolution (or the grace-period void), which is always the safer answer.
    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    settle_price_fresh = bool(current_prices.get("chainlink_fresh"))
    # If still unresolvable this long past expiry, void it. Kept under one 15m window so a
    # stuck trade can never block the slot across two consecutive markets.
    SETTLEMENT_GRACE_SECONDS = 120

    for trade in state["active_trades"]:
        # Keep a rolling price snapshot so settlement always has a recent value,
        # even if the feed drops out exactly at expiry.
        if cur_price:
            trade["last_price"] = cur_price

        # Effective window end. If endDate was missing at entry (end_ts == 0), derive
        # it from entry_time + window so a trade can never wait forever.
        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts

        # Freeze the CLOSE the instant the window ends, from the SAME feed as the open:
        # the Polymarket crypto_prices value AT end_ts (the authoritative settlement second).
        # This mirrors how Polymarket resolves — close-vs-open on one feed — so a near-the-
        # money result can't be flipped by cross-feed offset or post-expiry drift. Falls
        # back to the last price we saw only if the feed has no sample at that second.
        # The lookup is PINNED to the feed that marked this window's open (`strike_source`).
        # The venue publishes a USDT-quoted price and a Chainlink BTC/USD price ~0.13% apart;
        # closing against a different feed than the one that set the strike would inject that
        # whole offset into the result and flip near-the-money windows on its own.
        if expired and trade.get("close_price") is None:
            at_close = polymarket_ws_stream.price_at(end_ts * 1000, tol_ms=3000,
                                                     source=trade.get("strike_source"))
            frozen_close = (at_close or {}).get("price") or cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        # ── Authoritative resolution: PUSHED, not polled ─────────────────────────
        # The CLOB market channel we are already subscribed to emits `market_resolved`
        # with the winning asset id. That arrives within a frame of the venue deciding,
        # instead of up to 15s later, and costs no request. The REST poll below is kept
        # only as a fallback for when the socket missed the event (e.g. it fired while we
        # were reconnecting across the window roll), and is throttled to 15s as before.
        ws_res = clob_book_stream.resolution_for(trade.get("condition_id"),
                                                 trade.get("market_slug"),
                                                 trade.get("token_id"))
        market = None
        if ws_res is None and trade.get("last_api_check", 0) < now_ts - 15:
            try:
                market = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                market = None
            trade["last_api_check"] = now_ts
            if market is not None:
                trade["_market_closed"] = bool(market.get("closed"))
        market_closed = bool(ws_res) or trade.get("_market_closed", False)

        # Still live: window running and market still open → keep waiting.
        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        # ---- Determine the winning outcome ----
        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None

        # 0) Best source: the venue PUSHED the winner over the market WebSocket. This is the
        # settlement itself, not a price we infer it from, and it needs no request.
        if ws_res:
            win_asset = str(ws_res.get("winning_asset_id") or "")
            win_outcome = str(ws_res.get("winning_outcome") or "").lower()
            if win_asset and str(trade.get("token_id") or "") == win_asset:
                winning_index = up_index if trade["side"] == "UP" else down_index
                resolution = "ws_market_resolved"
            elif win_outcome in (settings.POLYMARKET_UP_LABEL.lower(), settings.POLYMARKET_DOWN_LABEL.lower()):
                winning_index = up_index if win_outcome == settings.POLYMARKET_UP_LABEL.lower() else down_index
                resolution = "ws_market_resolved"
            elif win_asset and trade.get("token_id"):
                # We hold a KNOWN token id and it is not the winner -> we lost. Guarded on
                # token_id being set: without it we cannot tell which side won, and
                # defaulting to "lost" would book a false loss on a winning trade.
                winning_index = down_index if trade["side"] == "UP" else up_index
                resolution = "ws_market_resolved"

        # 1) Authoritative — ONLY when Polymarket says the market is CLOSED. outcomePrices is
        # a (laggy) market price, not a settlement flag: a side trading >0.9 while the market
        # is still open is just the favourite, not the resolved winner. Requiring `closed`
        # avoids mis-settling on a transient pre-settlement price.
        if winning_index == -1 and market_closed:
            for i, p in enumerate(outcome_prices):
                try:
                    if float(p) > 0.9:
                        winning_index = i
                        resolution = "polymarket_settled"
                        break
                except Exception:
                    pass

        # 2) Fallback once the window/market is over: frozen CLOSE vs STRIKE (open). Both are
        # the Polymarket crypto_prices value (open at eventStartTime, close at end), so this
        # mirrors Polymarket's rule exactly: close >= open → Up (a tie resolves UP, per the
        # market's "greater than OR EQUAL TO" wording).
        strike = trade.get("strike_price")  # the marked OPEN
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)  # the frozen CLOSE
        # `close_price` was frozen from a live feed AT expiry, so it is trustworthy even if
        # the feed died afterwards. Anything else is only a running last-seen value, and
        # using it while the feed is stale means resolving against a frozen number — so in
        # that case we decline to settle and let the grace period handle it.
        close_is_frozen = trade.get("close_price") is not None
        if winning_index == -1 and (expired or market_closed):
            if strike and settlement_price and (close_is_frozen or settle_price_fresh):
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price >= strike else down_index
                resolution = "close_vs_open"
            elif strike and settlement_price and not trade.get("_logged_stale_settle"):
                trade["_logged_stale_settle"] = True
                log_message(f"Settlement feed stale for {trade['market_slug']} — holding for "
                            f"the venue's resolution instead of scoring on a frozen price")

        # ---- Could not resolve yet ----
        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            # Grace exhausted — void so a single bad trade can't block forever.
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            trade["exit_time"] = datetime.now().isoformat()
            trade["profit_loss"] = 0.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += trade["amount"]  # refund the stake
            state["trade_history"].append(trade)
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        # Open/close context — record it and show it in the log so the direction
        # (and which side that made win) is always visible.
        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        if won:
            payout = trade["shares"] * 1.0
            # Paper credits the simulated balance; live balance comes from the
            # on-chain USDC refresh in the main loop, not credited here.
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += payout
            else:
                # A live win pays out in ERC-1155 OUTCOME TOKENS, not collateral. Until they
                # are redeemed they are invisible to get_pusd_balance(), so the mirrored
                # balance would only ever fall — every stake debits and no win ever credits,
                # and percent-of-balance sizing would shrink after a winning trade. Queue
                # the redemption; it runs off the trading tick.
                state["pending_redemptions"].append({
                    "condition_id": trade.get("condition_id"),
                    "neg_risk": trade.get("neg_risk", False),
                    "token_id": trade.get("token_id"),
                    "side": trade["side"],
                    "shares": trade["shares"],
                    "slug": trade.get("market_slug"),
                    "attempts": 0,
                })
            trade["profit_loss"] = payout - trade["amount"]
            result = "WIN"
        else:
            trade["profit_loss"] = -trade["amount"]
            result = "LOSS"

        # Charge it to the window the TRADE belongs to. A position held to expiry settles
        # after that window has already rolled, and `record_close` ignores it in that case
        # — the new window's budget must start clean, not inherit the last one's result.
        window_risk.record_close(trade.get("window_start_ms"), trade["profit_loss"],
                                 "settled", trade["side"])

        log_message(f"{result} [{resolution or 'unknown'}] {trade['side']}: {dir_txt} -> P/L ${trade['profit_loss']:.2f} ({trade['market_slug']})")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(trade)
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def redeem_loop():
    """Convert resolved winning outcome tokens into pUSD — OFF the trading tick.

    Redemption is an on-chain transaction (seconds), so it must never sit in the 500ms
    loop. Without it a live win is stranded as ERC-1155 tokens that `get_pusd_balance()`
    cannot see: the dashboard balance would drop on every entry and never rise on a win.

    Each queued win is retried a few times (the payout is only redeemable once the venue
    has actually reported the market, which can trail our own settlement by a few seconds).
    """
    while True:
        try:
            if (state["trading_mode"] == "live" and settings.AUTO_REDEEM_ENABLED
                    and state["pending_redemptions"]):
                job = state["pending_redemptions"][0]
                job["attempts"] += 1

                # Redeem what the wallet ACTUALLY holds, not what we think we hold.
                held = await asyncio.to_thread(clob_trader.get_token_balance, job.get("token_id"))
                if held is not None and held <= 0:
                    # Nothing there: either it was already swept (auto-redeem) or the venue
                    # has not credited yet. Retry a few times, then drop it.
                    state["pending_redemptions"].pop(0)
                    if job["attempts"] <= 3:
                        state["pending_redemptions"].append(job)
                    else:
                        log_message(f"Redeem: no tokens held for {job.get('slug')} — assuming already redeemed")
                else:
                    shares = held if held is not None else job.get("shares") or 0.0
                    up_sh = shares if job.get("side") == "UP" else 0.0
                    down_sh = shares if job.get("side") == "DOWN" else 0.0
                    res = await asyncio.to_thread(
                        clob_trader.redeem_position, job.get("condition_id"),
                        up_sh, down_sh, job.get("neg_risk", False))
                    state["pending_redemptions"].pop(0)
                    if res.get("ok"):
                        await pusd_watcher.refresh()   # payout landed — re-read now
                        log_message(f"Redeemed {shares:.2f} winning {job.get('side')} shares "
                                    f"({job.get('slug')}) -> pUSD (tx {res.get('tx')})")
                    elif job["attempts"] < 5:
                        state["pending_redemptions"].append(job)   # transient — try again
                        log_message(f"Redeem retry {job['attempts']}/5 for {job.get('slug')}: {res.get('error')}")
                    else:
                        log_message(f"Redeem FAILED for {job.get('slug')} after 5 attempts: {res.get('error')}. "
                                    f"Redeem manually on polymarket.com to release the funds.")
        except Exception as e:
            print(f"Redeem loop error: {e}")
        await asyncio.sleep(5)


async def reconcile_live_fills():
    """Correct an open live trade's share count from the venue's own fill report.

    The order response usually carries the fill, but when it doesn't we fall back to the
    walked-book estimate — which is only an estimate. The authenticated user channel pushes
    the real `size_matched`, so we reconcile as soon as it arrives. Getting this right
    matters twice: the exit SELL must not ask for more shares than we hold (the FOK would
    be killed), and the win payout is `shares x $1`.
    """
    if state["trading_mode"] != "live":
        return
    for t in state["active_trades"]:
        if t.get("shares_source") == "user_ws" or not t.get("order_id"):
            continue
        fill = clob_user_stream.filled(t["order_id"])
        if not fill or not fill.get("shares"):
            continue
        old = t.get("shares") or 0.0
        new = float(fill["shares"])
        t["shares"] = new
        if fill.get("price"):
            t["entry_price"] = fill["price"]
        t["shares_source"] = "user_ws"
        if abs(new - old) > 1e-6:
            log_message(f"Fill reconciled ({t['side']}): {old:.2f} -> {new:.2f} shares "
                        f"@ {t['entry_price']:.3f} (venue report)")
        save_state()


async def ensure_balance_watch():
    """Point the pUSD balance watcher at the funded wallet, once.

    Deriving the wallet needs the CLOB client, which does blocking network work, so it is
    resolved ONCE in a worker thread and never on the trading tick. Until it resolves the
    balance simply stays at its last value — the same as any other feed that hasn't spoken
    yet. `clob_trader.reset()` (a credentials change) clears the guard so the next tick
    re-derives against the new key.
    """
    if pusd_watcher.address or state.get("_funder_init_inflight"):
        return
    state["_funder_init_inflight"] = True

    async def _init():
        try:
            if not clob_trader.funder:
                await asyncio.to_thread(clob_trader.ensure_ready)
            if clob_trader.funder:
                pusd_watcher.watch(clob_trader.funder)
                log_message(f"Balance watcher armed on {clob_trader.funder} "
                            f"(pUSD Transfer logs — no polling)")
            else:
                log_message(f"Balance watcher not armed: {clob_trader.last_error or 'no funded wallet'}")
        except Exception as e:
            log_message(f"Balance watcher init failed: {type(e).__name__}: {e}")
        finally:
            state["_funder_init_inflight"] = False

    asyncio.create_task(_init())


async def maybe_auto_withdraw():
    """Auto-withdrawal (capital extractor) state machine — LIVE mode only.

        ARMED --(balance >= trigger)--> WAITING_FLAT --(no open trades)--> WITHDRAWING
              --(withdrawal submitted)--> WITHDRAW_SUBMITTED --> ARMED (+ auto-resume)

    While the state is not ARMED, new entries are paused (see update_loop) so
    the account can settle flat before funds are extracted. Withdraws WITHDRAW_AMOUNT
    of pUSD to your own wallet — the EOA derived from the key/seed (gasless)."""
    if state["trading_mode"] != "live" or not settings.AUTO_WITHDRAW_ENABLED:
        if state["withdraw_state"] != "ARMED":   # disabled → never keep entries paused
            state["withdraw_state"] = "ARMED"
        return

    st = state["withdraw_state"]
    bal = state["paper_balance"]  # live pUSD balance is mirrored here

    if st == "ARMED":
        if bal is not None and bal >= settings.WITHDRAW_TRIGGER_BALANCE:
            state["withdraw_state"] = "WAITING_FLAT"
            log_message(f"Auto-withdraw: balance ${bal:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f} → pausing entries, waiting to go flat")

    elif st == "WAITING_FLAT":
        # FOK market orders never rest, so "flat" == no open positions.
        if not state["active_trades"]:
            state["withdraw_state"] = "WITHDRAWING"
            log_message("Auto-withdraw: account is flat → withdrawing")

    elif st == "WITHDRAWING":
        # Destination: the user-set address, or fall back to your own wallet (the EOA
        # derived from the key/seed) when left blank.
        recipient = settings.WITHDRAW_ADDRESS or clob_trader.get_eoa_address()
        if not recipient:
            log_message("Auto-withdraw aborted: no wallet/key available. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        amount = min(float(settings.WITHDRAW_AMOUNT), float(bal or 0))
        if amount <= 0:
            state["withdraw_state"] = "ARMED"
            return
        result = await asyncio.to_thread(clob_trader.withdraw_pusd, recipient, amount)
        if result.get("ok"):
            state["last_withdrawal"] = {"amount": result.get("amount"), "tx": result.get("tx"),
                                        "to": result.get("recipient"), "time": datetime.now().isoformat()}
            state["withdraw_state"] = "WITHDRAW_SUBMITTED"
            # Remember what we expect the balance to fall to, so "confirmed" mode has
            # something concrete to verify against.
            state["withdraw_expect_below"] = max(0.0, float(bal or 0) - amount * 0.5)
            await pusd_watcher.refresh()
            log_message(f"Auto-withdraw: submitted ${amount:.2f} → {recipient} (tx {result.get('tx')})")
        else:
            log_message(f"Auto-withdraw FAILED: {result.get('error')}. Disarming.")
            state["withdraw_state"] = "ARMED"

    elif st == "WITHDRAW_SUBMITTED":
        # `resume_after` is honoured here (it used to be stored, shown in the UI, and then
        # ignored — the bot always resumed at 'submitted').
        #   submitted: transfer_pusd already waited for a receipt, so resume immediately.
        #   confirmed: additionally wait until the on-chain balance actually reflects the
        #              withdrawal, so we can never resume — or re-trigger — on a stale read.
        # No read here on purpose. The withdrawal is itself a pUSD Transfer OUT of our
        # wallet, so the balance watcher's subscription fires on it and re-reads the
        # balance the moment it lands — polling `balanceOf` on every tick while waiting
        # would be the very thing the watcher exists to remove.
        bal = pusd_watcher.balance if pusd_watcher.balance is not None else bal
        if str(settings.WITHDRAW_RESUME_AFTER or "submitted").lower() == "confirmed":
            expect_below = state.get("withdraw_expect_below")
            if expect_below is not None and bal is not None and bal > expect_below:
                if not state.get("_await_confirm_logged"):
                    state["_await_confirm_logged"] = True
                    log_message(f"Auto-withdraw: awaiting confirmation (balance ${bal:.2f} "
                                f"has not dropped below ${expect_below:.2f} yet)")
                return   # stay in WITHDRAW_SUBMITTED; entries remain paused
        state["_await_confirm_logged"] = False
        state["withdraw_expect_below"] = None
        if not settings.WITHDRAW_AUTO_RESUME:
            state["running"] = False
            log_message("Auto-withdraw complete; auto-resume OFF → bot stopped.")
        else:
            log_message("Auto-withdraw complete; trading resumed.")
        state["withdraw_state"] = "ARMED"


async def chainlink_refresh_loop():
    """Cold-start Chainlink price — read over the Polygon WebSocket, off the trading tick.

    This is the LAST-RESORT price source, and it exists only for the gap at startup before
    either push feed has spoken. `ChainlinkPriceStream` subscribes to `AnswerUpdated`, but
    that event fires only on a deviation or heartbeat (minutes apart), so a fresh process
    has no price from it at all until the next update — the aggregator's STORED answer is
    what we actually want, and that is a read, not a subscription.

    The read is now an `eth_call` over the same persistent Polygon socket as the balance
    watcher (bot/rpc_ws.py), not the old HTTP provider chain whose multi-provider failover
    measured ~4.5s. It stops entirely as soon as either push feed produces a price, and
    `chainlink.chainlink_fetcher` (HTTP) remains only as a fallback for a socket that will
    not connect at all.
    """
    while True:
        try:
            # Only pay for a read while neither push feed has produced a price yet.
            if not (polymarket_ws_stream.get_last().get("price") or chainlink_ws_stream.get_last().get("price")):
                res = None
                if polygon_rpc.connected:
                    res = await chainlink_reader.fetch()
                if not (res or {}).get("price"):
                    # The WS RPC is unreachable — fall back to HTTP rather than run blind.
                    res = await chainlink.chainlink_fetcher.fetch_chainlink_btc_usd()
                if (res or {}).get("price"):
                    state["chainlink_rest"] = res
        except Exception as e:
            print(f"Chainlink refresh failed: {e}")
        await asyncio.sleep(2)


async def seed_kline_buffers():
    try:
        k15m = await data.fetch_klines(settings.SYMBOL, "15m", 200)
        binance_kline_15m.set_candles(k15m)
        log_message(f"Seeded Binance 15m kline buffer for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        # The signal itself: where spot sits relative to the window's open.
        "strike", "effective_spot", "distance", "signal_side",
        "mkt_up", "mkt_down", "fill_up", "fill_down",
        # Window risk budget on EVERY tick, so a no-trade stretch is explainable.
        "window_balance", "risk_per_trade", "window_pl", "window_wins", "window_losses",
        "blocked_side", "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            # ── SPOT FRESHNESS (safety) ──────────────────────────────────────────
            # A silently-stalled Binance socket (alive connection, no messages) freezes
            # last_price. BTC keeps moving, so the frozen price keeps reporting whichever
            # side of the open it died on — the bot would hold (or keep buying) a side the
            # market has already left, and would miss every cross. A stale spot is not a
            # spot: entries halt, and no cross is registered, until a fresh price arrives.
            #
            # We deliberately do NOT fall back to the Chainlink/Polymarket price here: it
            # updates on its own slow cadence and would make the cross detection lag.
            binance_ws = binance_stream.get_last()
            ws_age = (time.time() - binance_ws["ts"]) if binance_ws.get("ts") else None
            ws_fresh = (binance_ws.get("price") is not None and ws_age is not None
                        and ws_age <= settings.MAX_SPOT_AGE_S)

            # ── Tick budget ──────────────────────────────────────────────────────
            # On a 300s window the tick period is how fast we see spot cross the open, so
            # nothing may block it that isn't strictly needed. The Binance REST price and
            # the Chainlink RPC are FALLBACKS ONLY — the WS feeds above are preferred for
            # both (see spot_price / current_price below). Once a WS feed has a price we
            # stop calling its REST twin, which takes the slow RPC (~4.5s) out of the
            # steady-state tick entirely. The CLOB books are always read fresh: they price
            # the entry, the exit and the take-profit/stop-loss mark.
            need_binance_rest = not ws_fresh   # WS silent or stale -> try REST for a fresh spot

            coros = [fetch_polymarket_snapshot()]
            if need_binance_rest:
                coros.append(data.fetch_last_price(settings.SYMBOL))

            results = await asyncio.gather(*coros, return_exceptions=True)

            def _ok(idx):
                return results[idx] if idx < len(results) and not isinstance(results[idx], Exception) else None

            poly_snapshot = _ok(0) or {"ok": False}
            last_price = _ok(1) if need_binance_rest else None
            # Refreshed off-tick by chainlink_refresh_loop — never awaited here.
            chainlink_data = state.get("chainlink_rest") or {}

            klines_15m = binance_kline_15m.get_candles()

            # Resolve spot + whether it is trustworthy enough to trade on.
            if ws_fresh:
                spot_price, spot_source, spot_age = binance_ws["price"], "binance_ws", ws_age
            elif last_price is not None:
                spot_price, spot_source, spot_age = last_price, "binance_rest", 0.0
            else:
                # Keep the last known value for the DISPLAY only — it is not tradeable.
                spot_price, spot_source, spot_age = binance_ws.get("price"), "stale", ws_age
            spot_fresh = spot_source in ("binance_ws", "binance_rest")

            if spot_fresh != state.get("spot_fresh_last"):
                state["spot_fresh_last"] = spot_fresh
                if not spot_fresh:
                    age_txt = f"{spot_age:.0f}s old" if spot_age else "no data"
                    log_message(f"SPOT FEED STALE ({age_txt}) — entries halted until it recovers")
                else:
                    log_message(f"Spot feed OK ({spot_source})")

            # ── Time to expiry ───────────────────────────────────────────────────
            # The market's own endDate is authoritative; the local aligned-window clock
            # is the fallback. Kept in FRACTIONAL minutes: the fair-prob horizon is
            # continuous, and on a 15m market every second of decay moves the number.
            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]
            time_left_min = max(0.0, time_left_min)
            seconds_left = time_left_min * 60.0

            # The Binance 15m candle that opens on THIS window's boundary. It anchors the
            # fast move applied to the venue's strike, so it must be exactly that candle.
            #
            # This used to accept `openTime <= start` and take the newest match. At the
            # boundary the new candle has not been pushed yet, so that picked the PREVIOUS
            # window's candle — and "distance from the open" became the whole previous
            # window's move (tens of dollars) while the real distance from the new open was
            # cents. The bot fired instantly at HH:MM:00 on that phantom distance and
            # stopped out in the same second, repeatedly, for multiples of the stop.
            # Requiring an exact match makes a missing candle absent rather than wrong.
            binance_open = None
            if klines_15m:
                start_ms_b = int(timing["startMs"])
                for c in reversed(klines_15m):
                    if int(c["openTime"]) == start_ms_b:
                        binance_open = c["open"]
                        break
                    if int(c["openTime"]) < start_ms_b:
                        break        # candles are ordered; older ones cannot match

            current_price = None
            price_source = None
            settle_fresh = False

            # Prefer Polymarket's OWN Chainlink WS feed — it's the exact price stream
            # Polymarket settles on, so marking open/close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            #
            # Each source carries a freshness verdict. A dead WS keeps handing back its last
            # value forever, and settling a window against a frozen price mis-resolves every
            # near-the-money result the same way — so `settle_fresh` travels with the price
            # into update_trades, which refuses to score close-vs-open on a stale feed.
            poly_age = poly_ws.get("age")
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
                settle_fresh = poly_age is not None and poly_age <= settings.MAX_SETTLE_PRICE_AGE_S
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
                # AnswerUpdated only fires on a deviation/heartbeat (minutes apart), so this
                # feed cannot be judged by age; it is authoritative whenever it speaks.
                settle_fresh = True
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"
                settle_fresh = True

            if settle_fresh != state.get("settle_fresh_last"):
                state["settle_fresh_last"] = settle_fresh
                if not settle_fresh and current_price:
                    age_txt = f"{poly_age:.0f}s" if poly_age is not None else "unknown age"
                    log_message(f"SETTLEMENT FEED STALE ({age_txt}) — trades will wait for "
                                f"the venue's own resolution rather than settle on a frozen price")

            # ── Mark each window's OPEN ("Price to Beat") from eventStartTime ─────
            # Polymarket fixes the strike as the Chainlink data-stream price at the market's
            # eventStartTime. We read the SAME value from Polymarket's per-second
            # `crypto_prices` feed AT THAT EXACT SECOND (price_at) — the authoritative
            # strike — rather than "the first price we happened to see near the local clock".
            #
            # If the bot started mid-window and its feed history doesn't reach back to
            # eventStartTime, the strike stays None and this window is simply NOT TRADED —
            # we wait for the next window whose boundary we actually observe. There is
            # deliberately no Binance approximation: Binance is a different feed, and
            # approximating the strike from it reintroduces exactly the cross-feed
            # settlement error the eventStartTime read exists to remove.
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = poly_snapshot.get("event_start_ms") if poly_snapshot.get("ok") else None
            start_ms = int(event_start_ms) if event_start_ms else int(timing["startMs"])
            opens = state["market_opens"]
            prev_ws = state.get("last_window_start")

            # When the window rolls, freeze the PRIOR window's CLOSE = the feed value at its
            # end second (same feed as the open), falling back to the last price we saw.
            if prev_ws is not None and prev_ws != start_ms and prev_ws in opens and opens[prev_ws].get("close") is None:
                # Pinned to the feed that marked THAT window's open — see update_trades.
                at_close = polymarket_ws_stream.price_at(prev_ws + window_ms, tol_ms=3000,
                                                         source=opens[prev_ws].get("source"))
                close_val = (at_close or {}).get("price") or state.get("last_seen_price")
                if close_val:
                    opens[prev_ws]["close"] = close_val
            if current_price:
                state["last_seen_price"] = current_price

            if start_ms not in opens:
                opens[start_ms] = {"chainlink": None, "close": None, "source": None}
                for k in list(opens.keys()):           # prune old windows
                    if k < start_ms - 4 * window_ms:
                        del opens[k]
            win = opens[start_ms]

            # Capture the strike EXACTLY at eventStartTime from Polymarket's OWN feed — the
            # authoritative Price to Beat, the same value the market settles against. No other
            # source: Binance is a different feed and would reintroduce the very settlement
            # errors this fix removes. If we weren't running at the boundary (the feed has no
            # sample at that second), the strike stays None and this window simply isn't
            # traded — we wait for the next window whose open we actually observe.
            if win["chainlink"] is None:
                strike = polymarket_ws_stream.price_at(start_ms, tol_ms=STRIKE_TOL_MS)
                if strike is not None:
                    win["chainlink"] = strike["price"]
                    # Record WHICH feed marked it. The close is later read from this same
                    # feed, so the two ends of the window can never straddle the ~0.13%
                    # gap between the venue's USDT-quoted and Chainlink BTC/USD prices.
                    win["source"] = strike["source"]
                    feed_label = ("Chainlink BTC/USD" if strike["source"] == polymarket_ws_stream.CHAINLINK_TOPIC
                                  else "USDT spot (Chainlink unavailable)")
                    log_message(f"Strike @ eventStartTime {datetime.utcfromtimestamp(start_ms/1000).strftime('%H:%M:%S')}Z "
                                f"= {strike['price']:.2f} (Price to Beat, {feed_label})")
                    state["strike_status"] = None
                else:
                    # No strike = this window is not traded. Say WHY, once per window: a
                    # skipped window and a bot that has been dead for hours look identical
                    # on the dashboard otherwise ("not marked" in both cases).
                    reason = polymarket_ws_stream.strike_miss_reason(start_ms, STRIKE_TOL_MS)
                    state["strike_status"] = reason
                    if state.get("_strike_miss_logged") != start_ms:
                        state["_strike_miss_logged"] = start_ms
                        log_message(f"No strike for the "
                                    f"{datetime.utcfromtimestamp(start_ms/1000).strftime('%H:%M')}Z window "
                                    f"— not trading it: {reason}")
            state["last_window_start"] = start_ms

            # Strike (open) for a trade entered now: ONLY the Polymarket-feed value at
            # eventStartTime. None until/unless we observed the boundary, in which case no
            # trade opens this window.
            strike_open = win["chainlink"]
            strike_source = win.get("source")

            # ── WINDOW ROLL: record the balance this window is sized against ─────
            # Every 15m window is its own session. The balance is snapshotted HERE, once,
            # and the whole window's risk-per-trade and win/loss caps are derived from it —
            # so the stake can't shrink after a stop-loss or swell after a take-profit
            # mid-window, and "three stop-losses" stays three stop-losses.
            #
            # Equity (cash + the marked value of anything still open), not cash: a position
            # from the previous window that hasn't settled yet has its stake sitting outside
            # `paper_balance`, and sizing off cash alone would understate the bankroll.
            open_marked = sum((t.get("shares") or 0.0) * (t.get("cur_bid") or t.get("entry_price") or 0.0)
                              for t in state["active_trades"])
            if window_risk.roll(start_ms, state["paper_balance"] + open_marked):
                log_message(
                    f"NEW 15m WINDOW {datetime.utcfromtimestamp(start_ms/1000).strftime('%H:%M')}Z: "
                    f"balance ${window_risk.balance_at_start:.2f} -> risk/trade ${window_risk.risk_per_trade:.2f} "
                    f"(max loss ${window_risk.loss_budget():.2f} / max win ${window_risk.win_budget():.2f})")

            # ── THE SIGNAL: which side of the open is price on? ──────────────────
            # The strike is the venue's real Price to Beat. Preferred anchor: the FAST
            # Binance feed supplies only the MOVE since its own 15m open, re-anchored onto
            # that strike —
            #   effective = strike + (binance_spot - binance_open)
            # — as fast as Binance but measured against the number the market settles on,
            # and offset-free because both ends of the delta come from the same feed.
            #
            # For the first second or two of a window that candle does not exist yet. Rather
            # than reach for the previous one (which reported the LAST window's move as this
            # window's distance and fired instant losing entries at HH:MM:00), fall back to
            # the venue's own live price against the strike: same feed on both ends, so also
            # offset-free, just slower. If neither is available there is no signal at all.
            price_anchor = None
            if strike_open and binance_open and spot_price:
                effective_spot = strike_open + (spot_price - binance_open)
                price_anchor = "binance"
            elif strike_open and current_price and settle_fresh:
                effective_spot = current_price          # venue price vs venue strike
                price_anchor = "venue"
            else:
                effective_spot = None if strike_open else (current_price or spot_price)

            # The signal needs a price we trust. Binance-anchored requires a fresh spot (a
            # frozen one keeps asserting whichever side it died on); the venue-anchored
            # fallback is already gated on its own freshness above.
            usable = effective_spot is not None and (price_anchor == "venue" or spot_fresh)
            sig_side = engines.signal_side(effective_spot, strike_open,
                                           settings.MIN_MOVE_USD) if usable else None
            distance = (effective_spot - strike_open) if (effective_spot and strike_open) else None
            # Price being on the other side of the open is what clears a stop-loss block.
            window_risk.note_signal(sig_side)

            # ── The price we'd ACTUALLY pay, for the size we'd actually trade ────
            # Shown on the dashboard and used for the entry: the blended cost of filling the
            # FULL stake by walking the ask levels, not the best ask (which only exists for
            # the first few shares on a thin 15m book).
            ob_snap = poly_snapshot.get("orderbook", {}) if poly_snapshot["ok"] else {}
            best_ask_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            best_ask_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None
            # Midpoints = the real-time mark Polymarket's site shows — the DISPLAY odds.
            mids = poly_snapshot.get("mids", {}) if poly_snapshot["ok"] else {}
            market_up = mids.get("up")      # what the site shows for UP (mid)
            market_down = mids.get("down")  # ... and DOWN

            # Actual fill cost for the FULL stake (walk the asks) — this window's stake,
            # so the displayed fill price is the one an entry would really get.
            stake = compute_stake()
            eff_up = effective_entry(ob_snap, "UP", stake, best_ask_up)
            eff_down = effective_entry(ob_snap, "DOWN", stake, best_ask_down)
            fill_up = eff_up["vwap"]
            fill_down = eff_down["vwap"]

            # What the market implies, for DISPLAY only — the decision below does not
            # consult it. The odds are the cost of the trade, not the trigger for it.
            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)

            held_side = state["active_trades"][0]["side"] if state["active_trades"] else None

            decision = engines.decide_side({
                "spot": effective_spot,
                "strike": strike_open,
                "minMove": settings.MIN_MOVE_USD,
                "heldSide": held_side,
                "blockedSide": window_risk.blocked_side,
                "secondsLeft": seconds_left,
                "minSecondsLeft": settings.MIN_SECONDS_LEFT,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price,
                                   "chainlink_fresh": settle_fresh}

            exec_result = None
            reverse_side = None
            reverse_result = None
            tp_sl_hit = None

            if poly_snapshot["ok"] and state["running"]:
                # ── BEING ON THE WRONG SIDE OUTRANKS THE STOP-LOSS ───────────────
                # Both usually fire on the SAME tick, because price moving to the other
                # side of the open is precisely what makes the held side lose value.
                # Checking the stop first booked those closes as `stop_loss`, which
                # mislabels the history, spends the loss budget under the wrong heading,
                # and blocks a direction we are about to deliberately take. The reason we
                # are closing is that we're on the wrong side, so the reversal goes first.
                #
                # A REVERSE is an exit first, so it runs whenever we're started — even if
                # the risk budget has closed entries. It closes, and only re-opens if the
                # budget still allows it (see maybe_reverse_position).
                if decision["action"] == "REVERSE":
                    reverse_result = await maybe_reverse_position(decision, poly_snapshot, seconds_left,
                                                                  strike_open, strike_source)
                    reverse_side = reverse_result.get("side") if reverse_result else None

                # Take-profit / stop-loss — but NOT on a tick that just reversed. The
                # position opened by a reversal is underwater by its own entry cost (spread
                # + book depth) the instant it exists, so stop-checking it in the same tick
                # would stop it out before the market has had a chance to move at all.
                if reverse_result is None:
                    tp_sl_hit = await maybe_tp_sl(poly_snapshot)

            # ── Entry permission ─────────────────────────────────────────────────
            # Started, no withdrawal pending, spot provably fresh (a frozen price can't be
            # compared to the open), and the WINDOW'S RISK BUDGET still open — that last one
            # is where "3 losses" and "stop after a win" are enforced. Re-checked HERE, after
            # the exits above, because one of them may have just closed the window.
            risk_ok, risk_reason = window_risk.can_enter()
            entries_allowed = (state["running"] and state["withdraw_state"] == "ARMED"
                               and not tp_sl_hit and spot_fresh and risk_ok)

            # A fresh entry only when flat and everything above is satisfied. Never in the
            # same tick as a reversal — the reversal already took the new side.
            if poly_snapshot["ok"] and reverse_result is None and entries_allowed:
                exec_result = await execute_trade(decision, poly_snapshot["prices"],
                                                  poly_snapshot["market"], strike_open,
                                                  poly_snapshot.get("token_ids", {}),
                                                  poly_snapshot.get("orderbook", {}), strike_source)

            if exec_result is None and reverse_result is None:
                if not state["running"]:
                    exec_result = "stopped"
                elif tp_sl_hit:
                    exec_result = tp_sl_hit
                elif not spot_fresh:
                    exec_result = "stale_spot"
                elif not risk_ok:
                    exec_result = risk_reason          # window_win_stop / window_loss_cap / ...
                elif state["withdraw_state"] != "ARMED":
                    exec_result = "withdraw_pending"

            # Correct any live position's share count from the venue's own fill report
            # before it is marked, exited or paid out.
            await reconcile_live_fills()

            await update_trades(current_prices_dict)

            # ── Mark-to-market: value each OPEN position at the current bid of its
            # held side (what you'd get selling right now) and roll it into equity so
            # the headline number doesn't just drop by the stake on entry.
            open_value = 0.0
            snap_ob = poly_snapshot.get("orderbook", {}) if poly_snapshot.get("ok") else {}
            snap_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            for t in state["active_trades"]:
                held_key = "up" if t["side"] == "UP" else "down"
                # Only trust the live book if this trade is in the market we just snapshotted.
                # Mark at the price we'd actually get LIQUIDATING the whole position (walk
                # the bids), not at the best bid — same reason exits do.
                if snap_market_id is not None and str(t.get("market_id")) == snap_market_id:
                    levels = (snap_ob.get(held_key) or {}).get("bidLevels") or []
                    ex = data.walk_bids(levels, t["shares"])
                    if ex["vwap"] and ex["sold_shares"] + 1e-6 >= t["shares"]:
                        t["cur_bid"] = ex["vwap"]
                mark = t.get("cur_bid") or t.get("entry_price")  # fall back to entry (neutral) until we see a bid
                t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                open_value += t["shares"] * mark
            equity = state["paper_balance"] + open_value

            # ── Backfill the market OUTCOME onto EARLY-EXITED trades ─────────────
            # Every early exit (reverse, take-profit, stop-loss, manual) books its P/L on
            # the sell, so a side can show a loss even though the market later resolves in
            # its favour. Once that window's CLOSE is known, score the side against the true
            # open→close so the history shows whether the market actually went its way.
            EARLY_EXITS = ("reverse", "take_profit", "stop_loss", "manual")
            backfilled = False
            for h in state["trade_history"]:
                if h.get("exit_reason") not in EARLY_EXITS or h.get("market_won") is not None:
                    continue
                ws = h.get("window_start_ms")
                winfo = opens.get(ws) if ws is not None else None
                close_px = winfo.get("close") if winfo else None
                open_px = h.get("open_price") or h.get("strike_price")
                if close_px is not None and open_px:
                    h["close_price"] = close_px
                    h["market_won"] = (("UP" if close_px >= open_px else "DOWN") == h["side"])
                    backfilled = True
            if backfilled:
                save_state()

            # In live mode, reflect the real on-chain pUSD balance in the dashboard
            # (refreshed ~10s so the header balance tracks near-realtime).
            if state["trading_mode"] == "live":
                # The balance is not polled. `pusd_watcher` subscribes to the pUSD
                # Transfer logs naming our wallet and re-reads `balanceOf` when one
                # lands, so the number moves the moment a fill, a payout or a
                # withdrawal actually settles on chain — not up to 10s later.
                await ensure_balance_watch()
                if pusd_watcher.balance is not None:
                    state["paper_balance"] = pusd_watcher.balance
                # Auto-withdrawal (capital extractor) — pause/flat/withdraw/resume.
                await maybe_auto_withdraw()

            # A reversal both closes and opens, so surface it rather than letting the tick
            # read as a bare "NO TRADE". A reversal whose re-entry was refused (or blocked
            # by the risk budget) is reported as close-only, not as a completed reversal.
            if reverse_side:
                signal_label = f"REVERSE {reverse_side}"
                exec_result = (f"reversed_to_{reverse_side}" if reverse_result.get("opened")
                               else f"reverse_close_only_{reverse_result.get('reason')}")
            else:
                signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            wr = window_risk
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, strike_open, effective_spot, distance, sig_side,
                market_up, market_down, fill_up, fill_down,
                wr.balance_at_start, wr.risk_per_trade, wr.realized_pl, wr.wins, wr.losses,
                wr.blocked_side or "",
                f"{decision['side']}:{decision['action']}" if decision["action"] != "NO_TRADE" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            # ── TRACE ────────────────────────────────────────────────────────────
            # What the bot is looking at and why it is (not) acting. Logged when the
            # decision changes, and on a slow heartbeat otherwise, so the console shows the
            # reasoning without one line per 500ms tick. With a position open it also
            # reports the live MARKET move — the number TP/SL actually trigger on — next to
            # the booked P/L, so you can watch a trade approach its exit.
            trace_key = (decision.get("reason"), decision.get("action"), sig_side,
                         held_side, window_risk.blocked_side, exec_result)
            now_tr = time.time()
            if trace_key != state.get("_trace_key") or now_tr - state.get("_trace_ts", 0) >= 15:
                state["_trace_key"] = trace_key
                state["_trace_ts"] = now_tr
                bits = [f"open={strike_open:.2f}" if strike_open else "open=—",
                        f"px={effective_spot:.2f}" if effective_spot else "px=—",
                        f"d={distance:+.2f}" if distance is not None else "d=—",
                        f"anchor={price_anchor or '—'}",
                        f"sig={sig_side or '—'}",
                        f"held={held_side or 'flat'}",
                        f"{decision['action']}:{decision.get('reason')}"]
                if window_risk.blocked_side:
                    bits.append(f"BLOCKED={window_risk.blocked_side}")
                if exec_result:
                    bits.append(f"exec={exec_result}")
                for t in state["active_trades"]:
                    basis = t.get("entry_value") or t.get("amount")
                    mark = t.get("cur_bid")
                    if mark and basis and t.get("amount"):
                        val = t["shares"] * mark
                        bits.append(f"pos {t['side']}@{t['entry_price']:.2f} "
                                    f"market={(val - basis)/t['amount']*100:+.1f}% "
                                    f"(pl={(val - t['amount'])/t['amount']*100:+.1f}%)")
                bits.append(f"wPL=${window_risk.realized_pl:+.2f}/"
                            f"-${window_risk.loss_budget():.2f}")
                log_message("  " + "  ".join(bits))

            state["window_risk"] = window_risk.snapshot()
            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "balance": state["paper_balance"],     # cash only
                    "open_value": open_value,              # mark-to-market value of open positions
                    "equity": equity,                      # cash + open position value
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "tp_sl": {"enabled": settings.TP_SL_ENABLED,
                              "take_profit_pct": settings.TAKE_PROFIT_PCT,
                              "stop_loss_pct": settings.STOP_LOSS_PCT},
                    "symbol": settings.SYMBOL,
                    "window_minutes": settings.CANDLE_WINDOW_MINUTES,
                    "running": state["running"],
                    "withdraw": {
                        "enabled": settings.AUTO_WITHDRAW_ENABLED,
                        "state": state["withdraw_state"],
                        "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
                        "amount": settings.WITHDRAW_AMOUNT,
                        "last": state["last_withdrawal"],
                    }
                },
                "prices": {
                    "spot": spot_price,
                    "spot_source": spot_source,          # binance_ws | binance_rest | stale
                    "spot_age_s": spot_age,
                    "spot_fresh": spot_fresh,            # false => entries are halted
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "settle_fresh": settle_fresh,        # false => won't settle on this price
                    "settle_age_s": poly_age,
                    "poly_up": market_up,                # MID = the real-time mark the site shows
                    "poly_down": market_down,
                    "fill_up": fill_up,                  # VWAP you'd actually pay for the full stake
                    "fill_down": fill_down,
                    "best_ask_up": best_ask_up,          # top of book
                    "best_ask_down": best_ask_down,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else "none",
                    "book_age_s": clob_book_stream.socket_age(),   # since ANY frame on the book socket
                    "book_fresh": clob_book_stream.socket_fresh(),
                    "window_open": strike_open,          # this window's marked OPEN (Price to Beat)
                    "window_open_source": strike_source, # which venue feed marked it (Chainlink preferred)
                    "effective_spot": effective_spot,    # strike + the Binance move since its 15m open
                    "distance": distance,                # + = above the open, − = below
                    "market_implied_up": market_implied_up   # display only — not a trade input
                },
                # The signal: which side of the open we are on, and whether a cross just
                # happened (a cross reverses an open position and re-arms after a stop-loss).
                "signal": {
                    "side": sig_side,            # which side of the open price is on now
                    "min_move_usd": settings.MIN_MOVE_USD,   # dead band around the open
                    "held_side": held_side,
                    "blocked_side": window_risk.blocked_side,   # can't re-open after its SL
                    "strike_status": state.get("strike_status"),  # why there's no open price
                },
                "window_risk": state["window_risk"],
                "analysis": {
                    "decision": decision
                }
            }
            state["last_update_ts"] = time.time()

            # Push this tick to any connected dashboard (replaces the browser's 1s poll).
            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/api/latest")
async def get_latest():
    """Kept as the dashboard's FALLBACK transport (and for scripting). The page itself
    uses /ws and only polls this if the socket won't connect."""
    return state["latest_data"]


# ── Dashboard push ──────────────────────────────────────────────────────────────
# The browser used to poll /api/latest every second, which both lagged the 500ms trading
# tick and re-sent the whole payload regardless of whether anything changed. The tick now
# PUSHES to connected dashboards instead, so the UI moves at the speed the bot actually
# thinks, and an idle bot sends nothing.
_dashboard_clients: set = set()


async def broadcast_state():
    """Push the current snapshot to every connected dashboard. Called from the trading
    tick, so it must never raise and never block: a slow or dead client is dropped rather
    than allowed to hold up trading."""
    if not _dashboard_clients:
        return
    payload = {"latest": state["latest_data"], "logs": state["logs"][-100:],
               "history_len": len(state["trade_history"])}
    try:
        msg = json.dumps(payload, default=str)
    except Exception:
        return
    dead = []
    for ws in list(_dashboard_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _dashboard_clients.discard(ws)


@app.websocket("/ws")
async def dashboard_ws(websocket: WebSocket):
    await websocket.accept()
    _dashboard_clients.add(websocket)
    try:
        # Send immediately so a freshly-opened page isn't blank until the next tick.
        await websocket.send_text(json.dumps(
            {"latest": state["latest_data"], "logs": state["logs"][-100:],
             "history_len": len(state["trade_history"])}, default=str))
        while True:
            # The dashboard is push-only; this receive exists to notice the client
            # disconnecting (and to swallow any keepalive it sends).
            await websocket.receive_text()
    except Exception:
        pass
    finally:
        _dashboard_clients.discard(websocket)

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

@app.get("/logs.txt")
async def download_logs():
    """Download the FULL console log as a text file (the live view is capped at 100 lines)."""
    content = ""
    try:
        if os.path.exists("logs/console.log"):
            with open("logs/console.log", "r", encoding="utf-8") as f:
                content = f.read()
    except Exception:
        content = ""
    if not content:
        content = "\n".join(state["logs"])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(content=content, media_type="text/plain",
                    headers={"Content-Disposition": f'attachment; filename="console_log_{STRATEGY_LABEL}_{ts}.txt"'})

@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@app.get("/api/settings")
async def get_settings():
    pk = settings.PRIVATE_KEY
    masked_pk = pk[:6] + "..." + pk[-4:] if pk and len(pk) > 10 else pk

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "relayer": {
            "api_key": "set" if settings.RELAYER_API_KEY else ""
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "entry": {
            "min_move_usd": settings.MIN_MOVE_USD,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD,
            "min_seconds_left": settings.MIN_SECONDS_LEFT
        },
        "window_risk": {
            "max_loss_pct": settings.MAX_WINDOW_LOSS_PCT,
            "max_win_pct": settings.MAX_WINDOW_WIN_PCT,
            "stop_after_win": settings.STOP_AFTER_WIN
        },
        "tp_sl": {
            "enabled": settings.TP_SL_ENABLED,
            "take_profit_pct": settings.TAKE_PROFIT_PCT,
            "stop_loss_pct": settings.STOP_LOSS_PCT
        },
        "chainlink": {
            "alchemy_api_key": "set" if settings.ALCHEMY_API_KEY else ""
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_15m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        # masked value returned by GET — keep the stored key unchanged
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk is not None:
        from bot.config import normalize_private_key
        settings.PRIVATE_KEY = normalize_private_key(new_pk)
        new_settings["private_key"] = settings.PRIVATE_KEY  # persist the derived hex key, never the seed

    # "set" is the masked sentinel returned by GET for stored secrets — if the form
    # sends it back unchanged, don't overwrite the real key with the sentinel.
    if isinstance(new_settings.get("relayer"), dict) and new_settings["relayer"].get("api_key") == "set":
        new_settings["relayer"].pop("api_key", None)
    if isinstance(new_settings.get("chainlink"), dict) and new_settings["chainlink"].get("alchemy_api_key") == "set":
        new_settings["chainlink"].pop("alchemy_api_key", None)

    # Deep-merge into the existing config so keys not present in the settings form
    # (chainlink, binance_base_url, poll_interval_ms, etc.) are preserved.
    existing_cfg = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open("config.json", "w") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "entry" in new_settings:
        e = new_settings["entry"]
        settings.MIN_MOVE_USD = float(e.get("min_move_usd", settings.MIN_MOVE_USD))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))
        settings.MIN_SECONDS_LEFT = float(e.get("min_seconds_left", settings.MIN_SECONDS_LEFT))

    if "window_risk" in new_settings:
        w = new_settings["window_risk"]
        settings.MAX_WINDOW_LOSS_PCT = float(w.get("max_loss_pct", settings.MAX_WINDOW_LOSS_PCT))
        settings.MAX_WINDOW_WIN_PCT = float(w.get("max_win_pct", settings.MAX_WINDOW_WIN_PCT))
        if "stop_after_win" in w:
            settings.STOP_AFTER_WIN = bool(w["stop_after_win"])

    if "tp_sl" in new_settings:
        ts = new_settings["tp_sl"]
        if "enabled" in ts: settings.TP_SL_ENABLED = bool(ts["enabled"])
        settings.TAKE_PROFIT_PCT = float(ts.get("take_profit_pct", settings.TAKE_PROFIT_PCT))
        settings.STOP_LOSS_PCT = float(ts.get("stop_loss_pct", settings.STOP_LOSS_PCT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "relayer" in new_settings and isinstance(new_settings["relayer"], dict):
        if "api_key" in new_settings["relayer"]:
            settings.RELAYER_API_KEY = new_settings["relayer"]["api_key"]

    if "chainlink" in new_settings and isinstance(new_settings["chainlink"], dict):
        if "alchemy_api_key" in new_settings["chainlink"]:
            settings.ALCHEMY_API_KEY = new_settings["chainlink"]["alchemy_api_key"]

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "withdraw_address" in ce: settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
        if "auto_resume_after_withdrawal" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

    # Credentials may have changed — drop the cached CLOB client so the next live
    # order re-initialises with the new key / relayer / alchemy settings.
    clob_trader.reset()
    # A new key means a different funded wallet, so the balance subscription is now
    # watching the wrong address. Drop it; the next live tick re-derives and re-subscribes.
    polygon_rpc.unwatch("pusd_in")
    polygon_rpc.unwatch("pusd_out")
    pusd_watcher.address = None
    pusd_watcher.balance = None
    state["_funder_init_inflight"] = False

    state["trading_mode"] = settings.MODE
    # Only reset the displayed balance in paper mode; live mode reads the on-chain
    # pUSD balance and we don't want to clobber it with the paper default on save.
    if settings.MODE == "paper":
        state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
        asyncio.create_task(binance_stream.start())

        binance_kline_15m.close()
        binance_kline_15m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="15m", limit=200)
        asyncio.create_task(binance_kline_15m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
        )
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
        asyncio.create_task(chainlink_ws_stream.start())

        # The cold-start reader points at a per-asset aggregator too.
        chainlink_reader.set_aggregator(settings.get_aggregator(settings.SYMBOL))

    return {"status": "ok"}

def _reflect_running_now():
    """Mirror the running flag into latest_data immediately so /api/latest is in sync
    on the very next poll (the update loop would otherwise lag ~1s, flickering the UI)."""
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]

@app.post("/api/start")
async def start_trading():
    """Begin trading. Data/prices stream continuously; this opens the gate so the
    engine may enter, reverse and exit trades."""
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    """Stop all trading. New entries, reversals and TP/SL exits halt immediately; any
    open position keeps settling to expiry so it can't get stuck."""
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    return {"ok": True, "running": False}

@app.post("/api/close-trade")
async def close_trade():
    """Close the open position immediately at the current bid and lock the market so no
    new entry happens until the next 15m window. Manual override — independent of TP/SL."""
    result = await close_active_trade_now()
    return result

@app.post("/api/test-connection")
async def test_connection():
    """Validate the saved key/seed + relayer: derive the EOA and candidate wallets and
    report which one holds pUSD (the trading wallet) and the chosen signature type."""
    result = await asyncio.to_thread(clob_trader.test_connection)
    if result.get("ok"):
        log_message(f"Connection test OK: EOA {result.get('eoa')} → trading wallet {result.get('funder')}"
                    + ("" if result.get("relayer_key_set") else "  (relayer key MISSING)"))
    else:
        log_message(f"Connection test failed: {result.get('error')}")
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"], "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

@app.get("/history.csv")
async def get_history_csv():
    """All closed trades as a downloadable CSV (one row per trade)."""
    import csv, io
    cols = [
        ("market_slug", "market"),
        ("side", "side"),
        ("entry_time", "entry_time"),
        ("exit_time", "close_time"),
        ("entry_price", "entry_price"),
        ("exit_mark", "exit_price"),
        ("open_reason", "opened_by"),
        ("exit_reason", "exit_reason"),
        ("amount", "stake_usd"),
        ("shares", "shares"),
        ("open_price", "open_px"),
        ("close_price", "close_px"),
        ("profit_loss", "profit_loss"),
        ("status", "status"),
        ("resolution", "resolution"),
        ("winning_outcome", "winning_outcome"),
        ("mode", "mode"),
    ]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([label for _, label in cols])
    for t in state["trade_history"]:
        w.writerow([t.get(key, "") for key, _ in cols])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="closed_trades_{STRATEGY_LABEL}_{ts}.csv"'},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
