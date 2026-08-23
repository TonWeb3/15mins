import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODE: str = "paper"  # "paper" or "live"
    PAPER_BALANCE_USD: float = 1000.0
    PRIVATE_KEY: str = ""  # hex private key, or derived from a 12/24-word seed phrase

    # Live trading (Polymarket CLOB V2). The wallet is derived from PRIVATE_KEY (hex
    # key or seed phrase). New wallets trade via the gasless deposit-wallet flow
    # (POLY_1271); the relayer key sponsors on-chain setup (deploy/approvals).
    CLOB_MAX_SLIPPAGE: float = 0.02  # marketable-limit buffer above the quote (probability units)
    RELAYER_API_KEY: str = ""        # Polymarket relayer API key (gasless on-chain txs)

    # ── Auto-withdrawal (capital extractor) ─────────────────────────────────────
    # When the live balance reaches TRIGGER, pause entries, wait until flat, then
    # withdraw AMOUNT of pUSD to your own wallet (the key/seed EOA) and auto-resume.
    AUTO_WITHDRAW_ENABLED: bool = False
    WITHDRAW_TRIGGER_BALANCE: float = 2000.0  # withdraw once balance reaches this
    WITHDRAW_AMOUNT: float = 1000.0           # amount of pUSD to withdraw each time
    WITHDRAW_ADDRESS: str = ""                # destination wallet (blank = your own key/seed EOA)
    WITHDRAW_AUTO_RESUME: bool = True         # resume trading after the withdrawal
    WITHDRAW_RESUME_AFTER: str = "submitted"  # "submitted" or "confirmed"

    SYMBOL: str = "BTCUSDT"
    BINANCE_BASE_URL: str = "https://api.binance.com"
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL: str = "https://clob.polymarket.com"

    # A 15m window is 900s: the poll interval is how fast we SEE price cross the open.
    POLL_INTERVAL_MS: int = 500
    CANDLE_WINDOW_MINUTES: int = 15

    # ── Feed freshness (SAFETY) ─────────────────────────────────────────────────
    # A silently-stalled Binance socket (connection alive, messages stopped) freezes the
    # spot price. The market keeps moving, so the gap between our "fair" value and the
    # book grows without bound — a DEAD FEED LOOKS EXACTLY LIKE A HUGE LATENCY EDGE, and
    # the bot would fire into it with maximum confidence. Spot older than this is treated
    # as no spot at all, and entries stop until a fresh price arrives.
    MAX_SPOT_AGE_S: float = 5.0

    # The SAME failure mode applies to the order book — and worse, because the book IS
    # the thing we claim is stale. A frozen book is indistinguishable from an enormous
    # latency edge. We can't age out the book DATA (a quiet book is legitimately
    # unchanged), so we age out the SOCKET: an app-level PING is sent every 10s and any
    # inbound frame refreshes the clock. No frame for this long => the stream is not
    # trusted and we fall back to REST /book.
    MAX_BOOK_AGE_S: float = 15.0

    # Settlement/strike feed (Polymarket crypto_prices). Marking a window's close against
    # a frozen price silently mis-resolves near-the-money trades, so a price older than
    # this is not used to settle; we wait for the authoritative Polymarket resolution.
    MAX_SETTLE_PRICE_AGE_S: float = 30.0

    # Winning outcome tokens are ERC-1155 positions, not cash: they must be REDEEMED into
    # pUSD or a live win never becomes spendable balance. Redemption is gasless via the
    # relayer, same as every other on-chain action here.
    AUTO_REDEEM_ENABLED: bool = True

    # ── Risk per trade ──────────────────────────────────────────────────────────
    # "percent" = RISK_VALUE% of the balance RECORDED AT THE WINDOW OPEN; "fixed" =
    # RISK_VALUE dollars. Sizing off the recorded balance (not the live one) keeps every
    # trade in a window the same size, so the risk doesn't shrink after a loss or grow
    # after a win mid-window. See bot/risk.py.
    RISK_TYPE: str = "percent"
    RISK_VALUE: float = 10.0

    # ── Per-window risk budget (bot/risk.py) ────────────────────────────────────
    # Both caps are a percentage OF THE WINDOW'S RISK-PER-TRADE, not of the balance.
    # With TP 30% / SL 10%, the defaults below mean exactly:
    #   max loss  30% = three stop-losses  -> stop entering for this window
    #   max win   30% = one take-profit    -> stop entering for this window
    MAX_WINDOW_LOSS_PCT: float = 30.0
    MAX_WINDOW_WIN_PCT: float = 30.0
    # Stop entering for the rest of the window on the FIRST winning close, whatever the
    # dollar cap says.
    STOP_AFTER_WIN: bool = True

    # ── Above/below-the-open entry engine (bot/engines.py) ──────────────────────
    # Price above the window's open -> hold UP; below -> hold DOWN; on the wrong side,
    # sell it and reverse. No model, no EV gate.
    #
    # DEAD BAND around the open, in dollars of the underlying. Price sitting ON the open
    # is a coin flip quoted with the widest spread of the window, and the literal rule
    # flips the whole position on every tick of noise there. Measured live: two full
    # reversals ONE SECOND apart on BTC moving $3.27 (d = -1.55 then +1.72), costing ~7%
    # of the stake each in round-trip cost alone — more of that window's -$31.30 was the
    # cost of flipping than the market ever took. Inside the band there is no side, so
    # the bot simply holds. 0 = the literal rule (flip on any distance).
    MIN_MOVE_USD: float = 5.0
    MIN_BOOK_LIQUIDITY_USD: float = 20.0  # skip if the ask side can't absorb the stake
    # A Fill-Or-Kill into a book seconds from resolving is an unreliable fill, and a 30%
    # take-profit needs room to happen. Stop OPENING this many seconds before the window
    # ends (900s window, so 20s is the last ~2%). Reverses and exits still run.
    MIN_SECONDS_LEFT: float = 20.0

    # ── Take-profit / stop-loss (the ONLY discretionary exit) ───────────────────
    # Close the open position when its unrealized P/L reaches +TAKE_PROFIT_PCT or
    # -STOP_LOSS_PCT of the stake (the amount risked). A take-profit ends the window
    # (STOP_AFTER_WIN); a stop-loss blocks that direction until price is on the other side.
    TP_SL_ENABLED: bool = True
    TAKE_PROFIT_PCT: float = 30.0       # sell if position value is up this % vs the stake
    STOP_LOSS_PCT: float = 10.0         # sell if position value is down this % vs the stake

    # Polymarket — BTC "Up or Down" 15-minute series.
    POLYMARKET_SLUG: str = os.getenv("POLYMARKET_SLUG", "")
    POLYMARKET_SERIES_ID: str = os.getenv("POLYMARKET_SERIES_ID", "10192")
    POLYMARKET_SERIES_SLUG: str = os.getenv("POLYMARKET_SERIES_SLUG", "btc-up-or-down-15m")
    POLYMARKET_AUTO_SELECT_LATEST: bool = os.getenv("POLYMARKET_AUTO_SELECT_LATEST", "true").lower() == "true"
    POLYMARKET_LIVE_DATA_WS_URL: str = os.getenv("POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com")
    POLYMARKET_UP_LABEL: str = os.getenv("POLYMARKET_UP_LABEL", "Up")
    POLYMARKET_DOWN_LABEL: str = os.getenv("POLYMARKET_DOWN_LABEL", "Down")

    # Chainlink
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    POLYGON_RPC_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_RPC_URLS", "").split(",") if url.strip()]
    POLYGON_WSS_URL: str = os.getenv("POLYGON_WSS_URL", "wss://polygon-bor-rpc.publicnode.com")
    POLYGON_WSS_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_WSS_URLS", "").split(",") if url.strip()]
    CHAINLINK_BTC_USD_AGGREGATOR: str = os.getenv("CHAINLINK_BTC_USD_AGGREGATOR", "0xc907E116054Ad103354f2D350FD2514433D57F6f")

    # Alchemy — Polygon RPC used for the on-chain live-trading path (reads/setup).
    ALCHEMY_API_KEY: str = os.getenv("ALCHEMY_API_KEY", "")

    CHAINLINK_AGGREGATORS: Dict[str, str] = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "SOL": "0x39771505D18301D239916F4C88367A6010F7D2e3",
        "XRP": "0x3454796324D6469C3110996E2E10972688045F19",
        "DOGE": "0xbAf93Ba318f77363f82E8896a2E830206121D506",
        "BNB": "0x82a6C67606bdc0409f959f60608226064223A57c"
    }

    def get_aggregator(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"): s = s[:-4]
        return self.CHAINLINK_AGGREGATORS.get(s, self.CHAINLINK_BTC_USD_AGGREGATOR)

    def alchemy_rpc_url(self) -> str:
        # Polygon RPC used by the live-trading (gasless) path when a key is set.
        return f"https://polygon-mainnet.g.alchemy.com/v2/{self.ALCHEMY_API_KEY}" if self.ALCHEMY_API_KEY else ""

    # Proxy
    HTTP_PROXY: str = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))
    ALL_PROXY: str = os.getenv("ALL_PROXY", os.getenv("all_proxy", ""))

def normalize_private_key(secret: str) -> str:
    """Accept either a raw hex private key or a 12/24-word seed phrase and return a
    hex private key. EOA only — the wallet is derived from the secret, nothing else.
    Returns "" for empty input. Raises if a seed phrase can't be parsed."""
    secret = (secret or "").strip()
    if not secret:
        return ""
    # A mnemonic is several space-separated words; a private key is a single token.
    if len(secret.split()) >= 12:
        from eth_account import Account
        Account.enable_unaudited_hdwallet_features()
        return Account.from_mnemonic(secret).key.hex()
    return secret


def _is_masked_secret(v) -> bool:
    """True if the value looks like a UI mask rather than a real secret, so we never store
    a placeholder (e.g. '...', 'set', '••••') as an actual key."""
    if not isinstance(v, str):
        return False
    s = v.strip()
    return (not s) or s == "set" or "..." in s or "•" in s or "…" in s


def load_settings():
    base_settings = Settings()
    config_path = "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)
        except Exception as e:
            print(f"Warning: could not read/parse config.json: {e}")
            return base_settings

        # Each section is parsed in isolation: a single bad field logs a warning and is
        # skipped, but never drops the OTHER sections back to code defaults.
        def _section(name, fn):
            try:
                fn()
            except Exception as e:
                print(f"Warning: config.json section '{name}' skipped ({e})")

        def _top():
            if "mode" in config_data: base_settings.MODE = config_data["mode"]
            if "paper_balance_usd" in config_data: base_settings.PAPER_BALANCE_USD = float(config_data["paper_balance_usd"])
            if "private_key" in config_data and not _is_masked_secret(config_data["private_key"]):
                base_settings.PRIVATE_KEY = normalize_private_key(config_data["private_key"])
        _section("top", _top)

        def _relayer():
            rl = config_data.get("relayer", {})
            if "api_key" in rl and not _is_masked_secret(rl["api_key"]):
                base_settings.RELAYER_API_KEY = rl["api_key"]
        if "relayer" in config_data: _section("relayer", _relayer)

        def _capital():
            ce = config_data["capital_extractor"]
            if "enabled" in ce: base_settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
            if "trigger_balance" in ce: base_settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
            if "withdraw_amount" in ce: base_settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
            if "withdraw_address" in ce: base_settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
            if "auto_resume_after_withdrawal" in ce: base_settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
            if "resume_after" in ce: base_settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]
        if "capital_extractor" in config_data: _section("capital_extractor", _capital)

        def _poly():
            poly = config_data["polymarket"]
            if "gamma_base_url" in poly: base_settings.GAMMA_BASE_URL = poly["gamma_base_url"]
            if "clob_base_url" in poly: base_settings.CLOB_BASE_URL = poly["clob_base_url"]
            if "live_ws_url" in poly: base_settings.POLYMARKET_LIVE_DATA_WS_URL = poly["live_ws_url"]
            if "series_id" in poly: base_settings.POLYMARKET_SERIES_ID = str(poly["series_id"])
            if "series_slug" in poly: base_settings.POLYMARKET_SERIES_SLUG = poly["series_slug"]
            if "auto_select_latest" in poly: base_settings.POLYMARKET_AUTO_SELECT_LATEST = bool(poly["auto_select_latest"])
            if "up_label" in poly: base_settings.POLYMARKET_UP_LABEL = poly["up_label"]
            if "down_label" in poly: base_settings.POLYMARKET_DOWN_LABEL = poly["down_label"]
        if "polymarket" in config_data: _section("polymarket", _poly)

        def _trading():
            trading = config_data["trading"]
            if "symbol" in trading: base_settings.SYMBOL = trading["symbol"]
            if "binance_base_url" in trading: base_settings.BINANCE_BASE_URL = trading["binance_base_url"]
            if "candle_window_minutes" in trading: base_settings.CANDLE_WINDOW_MINUTES = int(trading["candle_window_minutes"])
            if "poll_interval_ms" in trading: base_settings.POLL_INTERVAL_MS = int(trading["poll_interval_ms"])
            if "risk_type" in trading: base_settings.RISK_TYPE = trading["risk_type"]
            if "risk_value" in trading: base_settings.RISK_VALUE = float(trading["risk_value"])
            if "max_spot_age_s" in trading: base_settings.MAX_SPOT_AGE_S = float(trading["max_spot_age_s"])
            if "max_book_age_s" in trading: base_settings.MAX_BOOK_AGE_S = float(trading["max_book_age_s"])
            if "max_settle_price_age_s" in trading: base_settings.MAX_SETTLE_PRICE_AGE_S = float(trading["max_settle_price_age_s"])
            if "auto_redeem" in trading: base_settings.AUTO_REDEEM_ENABLED = bool(trading["auto_redeem"])
        if "trading" in config_data: _section("trading", _trading)

        def _entry():
            en = config_data["entry"]
            if "min_move_usd" in en: base_settings.MIN_MOVE_USD = float(en["min_move_usd"])
            if "min_book_liquidity_usd" in en: base_settings.MIN_BOOK_LIQUIDITY_USD = float(en["min_book_liquidity_usd"])
            if "min_seconds_left" in en: base_settings.MIN_SECONDS_LEFT = float(en["min_seconds_left"])
        if "entry" in config_data: _section("entry", _entry)

        def _window_risk():
            wr = config_data["window_risk"]
            if "max_loss_pct" in wr: base_settings.MAX_WINDOW_LOSS_PCT = float(wr["max_loss_pct"])
            if "max_win_pct" in wr: base_settings.MAX_WINDOW_WIN_PCT = float(wr["max_win_pct"])
            if "stop_after_win" in wr: base_settings.STOP_AFTER_WIN = bool(wr["stop_after_win"])
        if "window_risk" in config_data: _section("window_risk", _window_risk)

        def _tpsl():
            ts = config_data["tp_sl"]
            if "enabled" in ts: base_settings.TP_SL_ENABLED = bool(ts["enabled"])
            if "take_profit_pct" in ts: base_settings.TAKE_PROFIT_PCT = float(ts["take_profit_pct"])
            if "stop_loss_pct" in ts: base_settings.STOP_LOSS_PCT = float(ts["stop_loss_pct"])
        if "tp_sl" in config_data: _section("tp_sl", _tpsl)

        def _chainlink():
            cl = config_data["chainlink"]
            if "polygon_rpc_url" in cl: base_settings.POLYGON_RPC_URL = cl["polygon_rpc_url"]
            if "polygon_wss_url" in cl: base_settings.POLYGON_WSS_URL = cl["polygon_wss_url"]
            if "btc_usd_aggregator" in cl: base_settings.CHAINLINK_BTC_USD_AGGREGATOR = cl["btc_usd_aggregator"]
            if "alchemy_api_key" in cl and not _is_masked_secret(cl["alchemy_api_key"]):
                base_settings.ALCHEMY_API_KEY = cl["alchemy_api_key"]
        if "chainlink" in config_data: _section("chainlink", _chainlink)

    return base_settings

settings = load_settings()
