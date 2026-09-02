"""
Live trading via the Polymarket CLOB.

Order placement uses the official `py-clob-client`. It is imported lazily inside
methods so the rest of the app keeps running in paper mode even when the package
or live credentials are absent.

Prerequisites for live mode (one-time, done OUTSIDE this app):
  - Fund the trading wallet (the address derived from PRIVATE_KEY, or the proxy
    `funder` address for signature_type 1/2) with USDC.e on Polygon.
  - Approve the Polymarket exchange contracts to spend USDC/CTF for that wallet.
This client does NOT set on-chain allowances; orders are rejected without them.
"""

import threading
from typing import Optional, Dict, Any
from .config import settings


class ClobTrader:
    def __init__(self):
        self.client = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._lock = threading.Lock()

    def reset(self):
        """Drop the cached client so the next call re-initialises with fresh
        credentials (call after the private key / signature settings change)."""
        with self._lock:
            self.client = None
            self.ready = False
            self.last_error = None

    def _init_client(self):
        from py_clob_client.client import ClobClient
        try:
            from py_clob_client.constants import POLYGON
        except Exception:
            POLYGON = 137  # Polygon mainnet chain id

        kwargs: Dict[str, Any] = {
            "host": settings.CLOB_BASE_URL,
            "key": settings.PRIVATE_KEY,
            "chain_id": POLYGON,
        }
        # signature_type: 0 = EOA, 1 = Email/Magic proxy, 2 = Browser proxy
        if settings.CLOB_SIGNATURE_TYPE is not None:
            kwargs["signature_type"] = int(settings.CLOB_SIGNATURE_TYPE)
        if settings.CLOB_FUNDER:
            kwargs["funder"] = settings.CLOB_FUNDER

        client = ClobClient(**kwargs)
        # Derive (or create) L2 API credentials from the private key (L1 auth)
        client.set_api_creds(client.create_or_derive_api_creds())
        self.client = client
        self.ready = True
        self.last_error = None

    def ensure_ready(self) -> bool:
        if self.ready and self.client is not None:
            return True
        with self._lock:
            if self.ready and self.client is not None:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_client()
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.client = None
                return False

    def place_market_buy(self, token_id: str, usdc_amount: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Place a Fill-Or-Kill marketable BUY for `usdc_amount` USDC of `token_id`.

        `price` is the current market quote for the side and is used as the
        order's limit (worst acceptable) price — so the fill is slippage-capped
        instead of taking the book blindly. If the market has moved past it, the
        FOK order is killed (no fill) rather than executing at a bad price.

        Synchronous (CLOB client uses blocking requests) — call from the event
        loop via `asyncio.to_thread`.
        """
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            amount = round(float(usdc_amount), 2)
            # Marketable limit: quote + a small slippage buffer, capped below $1.
            limit_price = 0.0
            if price is not None and price > 0:
                limit_price = round(min(0.99, float(price) + settings.CLOB_MAX_SLIPPAGE), 4)

            try:
                args = MarketOrderArgs(token_id=str(token_id), amount=amount, side=BUY, price=limit_price)
            except TypeError:
                # Older client without a `price` field on MarketOrderArgs
                args = MarketOrderArgs(token_id=str(token_id), amount=amount, side=BUY)

            signed = self.client.create_market_order(args)
            resp = self.client.post_order(signed, OrderType.FOK)

            ok = True
            if isinstance(resp, dict) and resp.get("success") is False:
                ok = False
            return {"ok": ok, "response": resp, "limit_price": limit_price}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_market_sell(self, token_id: str, size: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Place a Fill-Or-Kill marketable SELL of `size` shares of `token_id`.

        Used to exit a position early (e.g. close-and-flip). `price` is the current
        quote for the side; the order's limit is set just below it (minus the
        slippage buffer) so it fills into the bid rather than dumping blindly.

        Synchronous — call via `asyncio.to_thread`.
        """
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL

            amount = round(float(size), 2)
            limit_price = 0.0
            if price is not None and price > 0:
                limit_price = round(max(0.01, float(price) - settings.CLOB_MAX_SLIPPAGE), 4)

            try:
                args = MarketOrderArgs(token_id=str(token_id), amount=amount, side=SELL, price=limit_price)
            except TypeError:
                args = MarketOrderArgs(token_id=str(token_id), amount=amount, side=SELL)

            signed = self.client.create_market_order(args)
            resp = self.client.post_order(signed, OrderType.FOK)

            ok = True
            if isinstance(resp, dict) and resp.get("success") is False:
                ok = False
            return {"ok": ok, "response": resp, "limit_price": limit_price}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """Best-effort collateral (USDC) balance in dollars, or None on failure."""
        if not self.ensure_ready():
            return None
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            res = self.client.get_balance_allowance(params)
            raw = res.get("balance") if isinstance(res, dict) else None
            if raw is None:
                return None
            return float(raw) / 1_000_000  # USDC uses 6 decimals
        except Exception:
            return None


clob_trader = ClobTrader()
