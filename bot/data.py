import httpx
from .config import settings
from typing import List, Dict, Optional
from .net_utils import get_proxy_url_for

# Every call here sits on the critical path of the trading tick. Without an explicit
# timeout a single hung connection stalls the whole loop (observed: a 71s gap), which
# on a 300s window means missing a fifth of the market. Fail fast and retry next tick.
HTTP_TIMEOUT = httpx.Timeout(4.0, connect=2.0)

# Clients are POOLED and reused. Building a fresh AsyncClient per request (the previous
# behaviour) paid a new TCP + TLS handshake on every call — several per tick, and the
# dominant cost in the loop. Keep-alive + HTTP/2 multiplexing collapses that to one
# warm connection per host. Keyed by proxy so proxy settings still apply.
_clients: Dict[str, httpx.AsyncClient] = {}


def _client(target_url: str) -> httpx.AsyncClient:
    proxy = get_proxy_url_for(target_url) or ""
    c = _clients.get(proxy)
    if c is None or c.is_closed:
        c = httpx.AsyncClient(
            proxy=proxy or None,
            timeout=HTTP_TIMEOUT,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50,
                                keepalive_expiry=60.0),
        )
        _clients[proxy] = c
    return c

def to_number(x) -> Optional[float]:
    try:
        n = float(x)
        return n
    except (TypeError, ValueError):
        return None

async def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict]:
    url = f"{settings.BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    res = await _client(url).get(url, params=params)
    res.raise_for_status()
    data = res.json()
    return [{
        "openTime": int(k[0]),
        "open": to_number(k[1]),
        "high": to_number(k[2]),
        "low": to_number(k[3]),
        "close": to_number(k[4]),
        "volume": to_number(k[5]),
        "closeTime": int(k[6])
    } for k in data]

async def fetch_last_price(symbol: str) -> Optional[float]:
    url = f"{settings.BINANCE_BASE_URL}/api/v3/ticker/price"
    params = {"symbol": symbol}
    res = await _client(url).get(url, params=params)
    res.raise_for_status()
    data = res.json()
    return to_number(data.get("price"))

async def fetch_market_by_slug(slug: str) -> Optional[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/markets"
    params = {"slug": slug}
    res = await _client(url).get(url, params=params)
    res.raise_for_status()
    data = res.json()
    market = data[0] if isinstance(data, list) and data else data
    return market if market else None

async def fetch_live_events_by_series_id(series_id: str, limit: int = 20) -> List[Dict]:
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {
        "series_id": series_id,
        "active": "true",
        "closed": "false",
        "limit": limit
    }
    res = await _client(url).get(url, params=params)
    res.raise_for_status()
    data = res.json()
    return data if isinstance(data, list) else []

async def fetch_available_15m_series() -> List[Dict]:
    """The live "Up or Down 15m" series (BTC, ETH, ...) for the Settings dropdown.
    Discovered from Gamma's 15M tag; the defaults below are the known series IDs so the
    dropdown is never empty if the tag query fails."""
    url = f"{settings.GAMMA_BASE_URL}/events"
    params = {
        "active": "true",
        "closed": "false",
        "limit": 100,
        "tag_id": "102467" # 15M tag
    }
    try:
        res = await _client(url).get(url, params=params)
        res.raise_for_status()
        events = res.json()
    except Exception:
        events = []

    series_map = {}
    defaults = {
        "10192": "BTC Up or Down 15m",
        "10191": "ETH Up or Down 15m",
        "10422": "XRP Up or Down 15m",
        "10423": "SOL Up or Down 15m",
        "11328": "DOGE Up or Down 15m",
        "11329": "HYPE Up or Down 15m",
        "11330": "BNB Up or Down 15m",
        "12518": "ZEC Up or Down 15m"
    }
    for sid, name in defaults.items():
        series_map[sid] = {"series_id": sid, "title": name}

    for e in events:
        series_slug = e.get("seriesSlug", "")
        if "up-or-down-15m" in series_slug:
            sid = e.get("series_id")
            if not sid and e.get("series") and isinstance(e["series"], list) and len(e["series"]) > 0:
                sid = e["series"][0].get("id")

            if sid:
                asset = series_slug.split("-")[0].upper()
                series_map[str(sid)] = {
                    "series_id": str(sid),
                    "title": f"{asset} Up or Down 15m",
                    "slug": series_slug
                }

    return sorted(list(series_map.values()), key=lambda x: x["title"])

def flatten_event_markets(events: List[Dict]) -> List[Dict]:
    out = []
    for e in events:
        markets = e.get("markets", [])
        if isinstance(markets, list):
            out.extend(markets)
    return out

# NOTE: there is deliberately no `fetch_clob_price` here. CLOB /price?side=buy returns the
# best BID despite the name (verified live), so using it as the entry price overstated
# every edge by the full spread. Entry prices come off the book's ask side instead.

async def fetch_order_book(token_id: str) -> Dict:
    url = f"{settings.CLOB_BASE_URL}/book"
    params = {"token_id": token_id}
    res = await _client(url).get(url, params=params)
    res.raise_for_status()
    return res.json()

def normalize_levels(raw: List[Dict], ascending: bool) -> List[Dict]:
    """(price, size) levels, cleaned and sorted. Asks ascending (best = cheapest first),
    bids descending (best = highest first). The venue does not guarantee an order, so we
    impose one — walking an unsorted book would price the fill at a random level."""
    out = []
    for lvl in raw or []:
        p = to_number(lvl.get("price"))
        s = to_number(lvl.get("size"))
        if p is not None and s is not None and p > 0 and s > 0:
            out.append({"price": p, "size": s})
    out.sort(key=lambda l: l["price"], reverse=not ascending)
    return out


def walk_asks(ask_levels: List[Dict], usd_amount: float) -> Dict:
    """Spend up to `usd_amount` buying INTO the ask side, level by level.

    This is what a real marketable order does. Taking `usd / best_ask` instead (the old
    behaviour) assumes the entire stake clears at the top of the book — on a thin 15m book
    that overstates both the share count and the edge, and it is precisely wrong on the
    trades with the biggest apparent edge. Returns the true blended (VWAP) fill price.

    `filled_usd` < `usd_amount` means the book was too thin to absorb the whole stake.

    `worst_price` is the LAST (most expensive) level the fill has to reach. A marketable
    limit order must be priced against THIS, not the VWAP and not the best ask: a limit at
    the blended average cannot clear the top of the walk, so the order is killed. See
    `main.execute_trade`.
    """
    shares = 0.0
    spent = 0.0
    worst = None
    for lvl in ask_levels:
        remaining = usd_amount - spent
        if remaining <= 1e-9:
            break
        take_usd = min(lvl["price"] * lvl["size"], remaining)
        shares += take_usd / lvl["price"]
        spent += take_usd
        worst = lvl["price"]
    vwap = (spent / shares) if shares > 0 else None
    return {"shares": shares, "vwap": vwap, "filled_usd": spent, "worst_price": worst}


def walk_bids(bid_levels: List[Dict], shares: float) -> Dict:
    """Sell `shares` INTO the bid side, level by level — what we'd actually RECEIVE.
    Same reasoning as walk_asks, in reverse: valuing the whole position at the best bid
    overstates exit proceeds and would fire take-profit on a sale we can't get.

    `worst_price` is the LOWEST bid the sale has to reach — the price a marketable SELL
    limit has to sit at or below to actually clear the whole size."""
    remaining = float(shares or 0.0)
    proceeds = 0.0
    sold = 0.0
    worst = None
    for lvl in bid_levels:
        if remaining <= 1e-9:
            break
        take = min(lvl["size"], remaining)
        proceeds += take * lvl["price"]
        sold += take
        remaining -= take
        worst = lvl["price"]
    vwap = (proceeds / sold) if sold > 0 else None
    return {"proceeds": proceeds, "vwap": vwap, "sold_shares": sold, "worst_price": worst}


def summarize_order_book(book: Dict, depth_levels: int = 5) -> Dict:
    bid_levels = normalize_levels(book.get("bids", []), ascending=False)
    ask_levels = normalize_levels(book.get("asks", []), ascending=True)

    best_bid = bid_levels[0]["price"] if bid_levels else None
    best_ask = ask_levels[0]["price"] if ask_levels else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    # Midpoint = what Polymarket's site displays as the market price (real-time mark),
    # distinct from the best ask you PAY to buy or the VWAP for a full-size fill.
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2
    else:
        mid = best_ask if best_ask is not None else best_bid

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "mid": mid,
        "spread": spread,
        "bidLiquidity": sum(l["size"] for l in bid_levels[:depth_levels]),
        "askLiquidity": sum(l["size"] for l in ask_levels[:depth_levels]),
        # Full sorted levels — required to price a fill honestly (walk_asks/walk_bids).
        "askLevels": ask_levels,
        "bidLevels": bid_levels,
    }
