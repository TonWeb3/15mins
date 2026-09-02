import asyncio
import time
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load previous state
    load_state()

    # Initial seeding
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(update_loop())
    ]

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant", lifespan=lifespan)
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
    "last_balance_refresh": 0,
    # Trading is OFF until the user presses Start on the dashboard. Data/prices still
    # stream; this flag only gates ENTRIES (open positions always settle to expiry).
    "running": False,
    # Per-window marked opens, keyed by the market's own eventStartTime (ms):
    #   {start_ms: {"chainlink": float|None, "binance": float|None,
    #               "close": float|None, "genuine": bool}}
    # "chainlink" is the SETTLEMENT strike; "binance" is the model's reference open.
    "market_opens": {},
    "last_window_start": None,
    "last_seen_price": None
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
            
        if os.path.exists("config.json"):
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
    print(formatted)
    state["logs"].append(formatted)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))

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

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = None
    if settings.POLYMARKET_SLUG:
        market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    elif settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)

        now = time.time() * 1000
        live_markets = [m for m in markets if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now]
        if live_markets:
            live_markets.sort(key=lambda x: x["endDate"])
            market = live_markets[0]

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

    try:
        up_buy, down_buy, up_book, down_book = await asyncio.gather(
            data.fetch_clob_price(up_token_id, "buy"),
            data.fetch_clob_price(down_token_id, "buy"),
            data.fetch_order_book(up_token_id),
            data.fetch_order_book(down_token_id)
        )
        up_book_summary = data.summarize_order_book(up_book)
        down_book_summary = data.summarize_order_book(down_book)
    except:
        up_buy = None
        down_buy = None
        up_book_summary = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}
        down_book_summary = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": up_buy if up_buy is not None else gamma_yes,
            "down": down_buy if down_buy is not None else gamma_no
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

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], strike_open: Optional[float], token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry"):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    #
    # `strike_open` is the SETTLEMENT strike — the Chainlink price latched at the
    # market's eventStartTime. It is what update_trades() scores the close against, so
    # it must come from the same feed as the close, NOT from the model's Binance open.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    # No authoritative strike (the Chainlink open at this market's eventStartTime was
    # never captured) => no trade. Scoring against a guessed open is worse than sitting
    # the window out, so there is deliberately no Binance/spot fallback here.
    if strike_open is None:
        return "no_strike"

    side = decision["side"]

    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    # ── Risk per trade ──────────────────────────────────────────────────────────
    # No flat price cap (EV already governs reward/risk). RISK_TYPE selects how the
    # stake (the dollars put at risk) is sized:
    #   "percent" -> RISK_VALUE% of the current balance
    #   "fixed"   -> RISK_VALUE dollars, flat
    balance = state["paper_balance"]
    risk_type = (settings.RISK_TYPE or "percent").lower()
    if risk_type == "fixed":
        amount_to_risk = float(settings.RISK_VALUE)
    else:  # "percent" (default)
        amount_to_risk = (float(settings.RISK_VALUE) / 100.0) * balance

    if amount_to_risk <= 0:
        return "stake_zero"

    # Liquidity: never outsize what the ask side of the book can absorb.
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    ask_liq_shares = ob.get("askLiquidity")
    if ask_liq_shares is not None and price > 0:
        ask_liq_usd = ask_liq_shares * price
        if ask_liq_usd < settings.MIN_BOOK_LIQUIDITY_USD:
            log_message(f"Skip {side}: thin book (${ask_liq_usd:.2f} ask liquidity)")
            return "thin_book"
        amount_to_risk = min(amount_to_risk, ask_liq_usd)  # don't outsize the book

    if balance < amount_to_risk or amount_to_risk <= 0:
        print(f"Insufficient paper balance ({balance}) or invalid risk amount ({amount_to_risk})")
        return "insufficient_balance"

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
        "side": side,
        "entry_price": price,
        "amount": amount_to_risk,
        "shares": amount_to_risk / price,
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": strike_open,       # SETTLEMENT strike: Chainlink @ eventStartTime
        "strike_source": strike_source,
        "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
        "open_reason": open_reason,        # "ev_entry" | "flip_entry"
        "close_price": None,               # frozen once, the instant the window expires
        "end_ts": end_ts,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        log_message(f"Executed PAPER trade: {side} @ {price} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, price)
        if result.get("ok"):
            resp = result.get("response") or {}
            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            trade["order_id"] = order_id
            trade["order_response"] = resp
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            log_message(f"Executed LIVE trade: {side} ${amount_to_risk:.2f} on {market.get('slug')} (order {order_id})")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    """Close the open position early and flip when a STRONG opposite signal appears.

    Opt-in (FLIP_ENABLED). Guards: the new side must clear FLIP_MIN_CONVICTION and at
    least FLIP_MIN_MINUTES_LEFT must remain, and we only flip within the same market.
    After closing here, execute_trade() opens the new side (slot is now free).
    """
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    trade = state["active_trades"][0]
    if trade["side"] == new_side:
        return  # already on the signalled side
    if str(trade.get("market_id")) != str(market.get("id")):
        return  # different market — let the old one settle on its own

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')}")
            return
        # live balance is refreshed from chain elsewhere
    else:
        state["paper_balance"] += trade["shares"] * exit_price  # proceeds from selling out

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["resolution"] = "flip_exit"
    trade["settlement_price_at_expiry"] = exit_price
    # A flip exits on the BOOK, not at expiry — so there is no window close price.
    # Record the marked open and the BTC price we bailed at, for the history table.
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")
    trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side}")
    return new_side   # truthy => the entry that follows is a flip entry, not a fresh EV entry

MARK_CAPTURE_WINDOW_MS = 20_000   # how late after eventStartTime a latch still counts


def mark_window_open(start_ms: int, window_ms: int, current_price: Optional[float],
                     spot_price: Optional[float], price_source: Optional[str]) -> Dict[str, Any]:
    """Latch this window's OPEN at the market's own eventStartTime and return its record.

    Two values are captured at the SAME instant:
      "chainlink" -> the SETTLEMENT strike, the price Polymarket resolves against
      "binance"   -> the MODEL's reference open, so fair_prob keeps measuring the
                     Binance move since the open exactly as it always has
    Mixing the feeds (Binance spot vs a Chainlink open) would inject a constant
    ~0.13% offset straight into the model, which on a 15m window is the same order
    of magnitude as the move being predicted.

    A window is only marked when `genuine` — i.e. we were already running through the
    immediately-preceding window, so the first price we see in this one really is its
    open. Otherwise "chainlink" stays None and the caller opens no trade: no API can
    fetch a past open, and scoring against a guess is worse than sitting the window out.
    """
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # On rollover, freeze the PRIOR window's close = the last price seen inside it.
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
    if start_ms not in opens:
        opens[start_ms] = {"chainlink": None, "binance": None,
                           "close": None, "genuine": observed_prev}
        for k in list(opens.keys()):           # prune old windows
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    # `since_start` must be within [0, MARK_CAPTURE_WINDOW_MS): too late and the price
    # is no longer the open; NEGATIVE means eventStartTime is still in the future (the
    # market hasn't begun) and latching would freeze a pre-open price as the strike.
    since_start = time.time() * 1000 - start_ms
    if (win["chainlink"] is None and win["genuine"] and current_price
            and 0 <= since_start < MARK_CAPTURE_WINDOW_MS):
        win["chainlink"] = current_price
        win["binance"] = spot_price
        log_message(f"Window open marked @ eventStartTime: Chainlink {current_price:.2f} "
                    f"({price_source}) / Binance {spot_price if spot_price else '-'}")
    state["last_window_start"] = start_ms
    return win


def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Strip bulky/internal scratch keys before a trade goes into trade_history.

    `_market` caches a whole Gamma market payload while the trade is open; writing
    that into state_data.json every settle would bloat the file for no benefit.
    """
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade


async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here: open and close then come from the SAME feed and no cross-feed
    # offset can flip a near-the-money result. Binance spot is a last resort only.
    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300  # if still unresolvable this long past expiry, void it

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

        # Freeze the CLOSE the instant the window ends. Polymarket settles on the
        # Chainlink value AT the close time — not whenever we happen to get around to
        # resolving (which can lag by many seconds). Capturing it once here stops
        # post-expiry drift from flipping a near-the-money win/loss.
        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        # Poll the market for the AUTHORITATIVE Polymarket resolution. Before expiry
        # this is a cheap ~30s heartbeat; ONCE EXPIRED we poll every 3s, because the
        # official outcome is what we actually want to settle on.
        #
        # This used to be a flat 15s poll with `market` reset to None each tick, which
        # meant that on the very tick where `expired` first became true `market` was
        # almost always None -> no outcome prices -> the close-vs-open fallback below
        # resolved and CLOSED the trade immediately. The "authoritative first" priority
        # was therefore dead code, and every trade got scored against our own captured
        # strike rather than Polymarket's published outcome. Caching the last fetched
        # market on the trade and polling fast after expiry fixes that.
        market = trade.get("_market")
        poll_every = 3.0 if expired else 30.0
        if trade.get("last_api_check", 0) < now_ts - poll_every:
            try:
                fetched = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                fetched = None
            trade["last_api_check"] = now_ts
            if fetched is not None:
                market = fetched
                trade["_market"] = fetched
                trade["_market_closed"] = bool(fetched.get("closed"))
        market_closed = trade.get("_market_closed", False)

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
        # 1) Authoritative: a settled Polymarket outcome trades at ~$1.
        for i, p in enumerate(outcome_prices):
            try:
                if float(p) > 0.9:
                    winning_index = i
                    resolution = "polymarket_settled"
                    break
            except Exception:
                pass

        # 2) Fallback once the window/market is over: frozen CLOSE vs STRIKE (open).
        # Both are Chainlink values now, so this mirrors how Polymarket resolves —
        # did the close finish above or below the open?
        #
        # Only used AFTER giving Polymarket AUTHORITATIVE_SETTLE_WAIT_S to publish its
        # own outcome: our strike is a best-effort snapshot, theirs is the truth.
        strike = trade.get("strike_price")   # the marked OPEN
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)   # the frozen CLOSE
        if winning_index == -1 and (expired or market_closed):
            if trade.get("expired_at") is None:
                trade["expired_at"] = now_ts
            waited = now_ts - trade["expired_at"]
            if waited < settings.AUTHORITATIVE_SETTLE_WAIT_S and not market_closed:
                remaining_active.append(trade)   # keep waiting for the official result
                continue
            if strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"
                trade["settle_wait_s"] = round(waited, 1)

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
            state["trade_history"].append(_archive(trade))
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        # Open/close context — recorded and shown in the log so the direction (and why
        # that side won) is always visible after the fact.
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
            trade["profit_loss"] = payout - trade["amount"]
            log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Profit: ${trade['profit_loss']:.2f}")
        else:
            trade["profit_loss"] = -trade["amount"]
            log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Loss: ${trade['profit_loss']:.2f}")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = trade.get("exit_reason") or "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(_archive(trade))
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def seed_kline_buffers():
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        log_message(f"Seeded Binance kline buffers (1m/5m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd(),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price

            mc_steps = max(1, __import__('math').ceil(timing["remainingMinutes"] / 5))

            # ── The settlement feed ──────────────────────────────────────────────
            # Prefer Polymarket's OWN Chainlink WS: it is the exact stream Polymarket
            # settles on, so marking the open and the close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            current_price = None
            price_source = None
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            # ── Authoritative window start = the market's own eventStartTime ──────
            # NOT the local aligned clock. eventStartTime is the exact second the
            # contract's "Price to Beat" is fixed, so a feed value captured at that
            # instant matches Polymarket's settlement. Fall back to endDate - window,
            # then the local boundary. (Polymarket exposes no numeric strike field —
            # the strike IS the Chainlink price at eventStartTime.)
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = None
            if poly_snapshot.get("ok"):
                _mkt = poly_snapshot["market"]
                _esr = _mkt.get("eventStartTime") or _mkt.get("gameStartTime")
                if _esr:
                    try:
                        event_start_ms = int(datetime.fromisoformat(str(_esr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        event_start_ms = None
                if event_start_ms is None and _mkt.get("endDate"):
                    try:
                        event_start_ms = int(datetime.fromisoformat(_mkt["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        event_start_ms = None
            if event_start_ms is None:
                event_start_ms = int(timing["startMs"])

            # ── Mark this window's OPEN at eventStartTime ─────────────────────────
            # Two values are captured at the same instant:
            #   "chainlink" -> the SETTLEMENT strike (what Polymarket resolves against)
            #   "binance"   -> the MODEL's reference open, so fair_prob keeps measuring
            #                  the Binance move since the open exactly as it always has.
            # Mixing the two feeds would inject a constant ~0.13% offset into the model.
            start_ms = event_start_ms
            win = mark_window_open(start_ms, window_ms, current_price, spot_price, price_source)

            # Settlement strike for a trade entered now: ONLY the Chainlink open latched
            # at eventStartTime. None until/unless captured => no trade this window.
            strike_open = win["chainlink"]
            strike_source = "chainlink_ws"

            # Model reference open. Prefer the 5m candle that opens EXACTLY at the
            # window start (strict equality — a `<=` scan silently picks the previous
            # candle's open in the first seconds of a window, before the new one has
            # arrived over the WS). Fall back to the Binance spot latched at the mark.
            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms:
                    model_open = c["open"]
                    break
                if c["openTime"] < start_ms:
                    break
            model_open = model_open or win.get("binance")
            target_open = model_open if strike_open is not None else None

            # Fast closed-form fair probability (replaces 1000-sim Monte Carlo —
            # backtest-verified equivalent, ~1000x cheaper, which a latency play needs).
            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
            fair_up = indicators.fair_prob_up(spot_price or 0, target_open or 0, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]

            closes = [c["close"] for c in klines_1m]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD)

            # Heiken-Ashi streaks (1m & 5m) — the exhaustion veto.
            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m))
            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 20:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))

            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None

            # ── LATENCY EDGE ─────────────────────────────────────────────────────
            # Our fast Binance-derived fair prob vs the market's (possibly stale)
            # implied prob. A positive edge = the book hasn't repriced the move yet.
            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            # Heiken-Ashi exhaustion veto (>=6 bars in one direction = don't chase).
            EB = engines.EXHAUSTION_BARS

            def _is(color, count, want):
                return color == want and (count or 0) >= EB

            ha_exhausted_green = _is(consec["color"], consec["count"], "green") or _is(consec_5m["color"], consec_5m["count"], "green")
            ha_exhausted_red = _is(consec["color"], consec["count"], "red") or _is(consec_5m["color"], consec_5m["count"], "red")

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
                "rsi": rsi_now,
                "haExhaustedGreen": ha_exhausted_green,
                "haExhaustedRed": ha_exhausted_red,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            # Entries and flips run ONLY when the user has pressed Start. Feeds, the
            # model and settlement all keep running either way, so an open position
            # always settles to expiry and can never get stranded by a Stop.
            exec_result = None
            if poly_snapshot["ok"] and state["running"]:
                flipped = await maybe_flip_position(decision, poly_snapshot, time_left_min)
                # NOTE: the trade is stamped with `strike_open` (Chainlink @ eventStartTime),
                # NOT `target_open` (the model's Binance reference). Settlement compares a
                # Chainlink close to this, so both sides must come from the same feed.
                exec_result = await execute_trade(
                    decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open,
                    poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}),
                    strike_source=strike_source, window_start_ms=start_ms,
                    open_reason="flip_entry" if flipped else "ev_entry")
            elif not state["running"]:
                exec_result = "stopped"

            await update_trades(current_prices_dict)

            # Mark open positions to market so the dashboard can show live P/L.
            open_value = 0.0
            for t in state["active_trades"]:
                mark = None
                if poly_snapshot["ok"] and str(t.get("market_id")) == str(poly_snapshot["market"].get("id")):
                    ob = (poly_snapshot.get("orderbook") or {}).get("up" if t["side"] == "UP" else "down") or {}
                    mark = ob.get("bestBid") or (market_up if t["side"] == "UP" else market_down)
                if mark:
                    t["mark_price"] = mark
                    t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                    open_value += t["shares"] * mark
                else:
                    t["unrealized_pl"] = None
                    open_value += t["amount"]   # no quote — carry at cost

            # In live mode, reflect the real on-chain USDC balance in the dashboard
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "running": state["running"],
                    "balance": state["paper_balance"],
                    "equity": state["paper_balance"] + open_value,
                    "open_value": open_value,
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,          # strike: Chainlink @ eventStartTime
                    "window_open_source": strike_source,
                    "model_open": model_open,            # the model's Binance reference open
                    "window_start_ms": start_ms
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision
                }
            }
            state["last_update_ts"] = time.time()

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
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

# Data files the dashboard is allowed to download. An explicit whitelist, NOT a path
# join on user input — this endpoint is reachable by anyone who can reach the
# dashboard, so it must not be able to serve arbitrary files (private_key lives in
# config.json).
DOWNLOADABLE = {
    "signals": ("logs/signals.csv", "text/csv"),
    "trades": ("state_data.json", "application/json"),
}


@app.get("/api/files")
async def list_files():
    """Which data files exist, how big, and when they last changed."""
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out


@app.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"15m-{base}-{stamp}{ext}")


def _reflect_running_now():
    """Mirror the running flag into latest_data immediately so /api/latest is in sync
    on the very next poll (the update loop would otherwise lag ~1s, flickering the UI)."""
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]


@app.post("/api/start")
async def start_trading():
    """Begin trading. Data/prices stream continuously; this flips the gate so the
    engine may enter/flip trades."""
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    return {"ok": True, "running": True}


@app.post("/api/stop")
async def stop_trading():
    """Stop all trading. New entries and flips halt immediately; any open position
    keeps settling to expiry so it can't get stuck."""
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    return {"ok": True, "running": False}


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
        "live": {
            "signature_type": settings.CLOB_SIGNATURE_TYPE,
            "funder": settings.CLOB_FUNDER
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
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        settings.PRIVATE_KEY = new_pk

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

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        settings.CLOB_SIGNATURE_TYPE = int(lv.get("signature_type", settings.CLOB_SIGNATURE_TYPE))
        settings.CLOB_FUNDER = lv.get("funder", settings.CLOB_FUNDER)

    # Credentials/signature may have changed — drop the cached CLOB client so the
    # next live order re-initialises with the new key/signature/funder.
    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

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

    return {"status": "ok"}

@app.post("/api/setup-allowances")
async def setup_allowances():
    import bot.allowances as allowances
    try:
        result = await allowances.ensure_allowances()
        if result.get("ok"):
            if result.get("skipped"):
                log_message(f"Allowance setup skipped: {result.get('reason')}")
            elif result.get("already_set"):
                log_message("Allowances already configured for trading wallet")
            else:
                log_message(f"Allowances configured ({len(result.get('actions', []))} tx)")
        else:
            log_message(f"Allowance setup failed: {result.get('error')}")
        return result
    except Exception as e:
        log_message(f"Allowance setup error: {e}")
        return {"ok": False, "error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"],
            "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
