"""
Live trading on Polymarket CLOB V2 via `polymarket-apis`.

Polymarket migrated to CLOB V2. New wallets trade through the gasless
**deposit-wallet** flow (signature_type=3 / POLY_1271):

  - Funds live in a **deposit wallet** derived from your EOA (deposit via Polymarket).
  - Your **private key / seed (EOA)** signs every order — it holds no funds or gas.
  - A **relayer API key** sponsors on-chain setup (wallet deploy + token approvals),
    so you never pay gas.

Legacy accounts (Polymarket proxy / Gnosis safe) are auto-detected and used as-is:
we pick whichever wallet actually holds pUSD. The EOA is derived from PRIVATE_KEY
(hex key or 12/24-word seed phrase).

All clients are synchronous — call from the event loop via `asyncio.to_thread`.
"""

import math
import threading
from typing import Optional, Dict, Any, List, Tuple
from .config import settings


def _patch_clob_models():
    """Relax over-strict fields in polymarket-apis' models. The live CLOB API omits
    some fields the library marks required (e.g. blockaid_check_enabled), which would
    otherwise crash order placement with a pydantic ValidationError."""
    try:
        import polymarket_apis.types.clob_types as ct
        changed = False
        for name in ("blockaid_check_enabled",):
            f = ct.ClobMarketInfo.model_fields.get(name)
            if f is not None and f.is_required():
                f.default = False
                changed = True
        if changed:
            ct.ClobMarketInfo.model_rebuild(force=True)
    except Exception:
        pass


_patch_clob_models()


class ClobTrader:
    def __init__(self):
        self.clob = None          # PolymarketClobClient — order placement
        self.gasless = None       # PolymarketGaslessWeb3Client — relayer on-chain ops
        self.funder: Optional[str] = None         # funded wallet (deposit/proxy/safe)
        self.signature_type: Optional[int] = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._approvals_done = False
        self._api_creds = None
        self._lock = threading.Lock()

    def reset(self):
        """Drop cached clients so the next call re-initialises with fresh
        credentials (call after the key / relayer settings change)."""
        with self._lock:
            self.clob = None
            self.gasless = None
            self.funder = None
            self.signature_type = None
            self.ready = False
            self.last_error = None
            self._approvals_done = False
            self._api_creds = None

    # ── wallet derivation / detection ───────────────────────────────────────────
    def _derive_creds(self):
        """CLOB API creds derived from the key (L1 auth). Cached. Doubles as the
        gasless client's builder_creds when no relayer key is set."""
        if self._api_creds is not None:
            return self._api_creds
        from polymarket_apis.clients.clob_client import PolymarketClobClient
        from eth_account import Account
        eoa = Account.from_key(settings.PRIVATE_KEY).address
        c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa,
                                 chain_id=137, signature_type=3)
        self._api_creds = c.create_or_derive_api_creds()
        return self._api_creds

    def _new_gasless(self, signature_type: int):
        # Gasless on-chain client. Two distinct roles:
        #   - relayer key (or builder creds): SUBMITS txs and pays the gas
        #   - rpc_url: READS chain state (pUSD balance, wallet derivation). Use Alchemy
        #     when configured, else the library default.
        from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
        kwargs = {"private_key": settings.PRIVATE_KEY, "signature_type": signature_type}
        if settings.RELAYER_API_KEY:
            kwargs["relayer_api_key"] = settings.RELAYER_API_KEY
        else:
            kwargs["builder_creds"] = self._derive_creds()
        if settings.alchemy_rpc_url():
            kwargs["rpc_url"] = settings.alchemy_rpc_url()
        return PolymarketGaslessWeb3Client(**kwargs)

    def _candidate_wallets(self, gasless) -> List[Tuple[int, str]]:
        """(signature_type, address) candidates derived from the EOA, deposit-wallet
        (V2) first, then legacy proxy / safe."""
        out: List[Tuple[int, str]] = []
        for st, getter in (
            (3, gasless.get_expected_deposit_wallet),
            (1, gasless.get_poly_proxy_wallet_address),
            (2, gasless.get_safe_proxy_wallet_address),
        ):
            try:
                out.append((st, getter()))
            except Exception:
                pass
        return out

    def _pick_funded_wallet(self, gasless) -> Tuple[int, str]:
        """Pick the trading wallet by PRIORITY ORDER, not by largest balance. Candidates are
        probed deposit(3) → proxy(1) → safe(2), and we take the FIRST one holding pUSD.

        This is deliberate: on Polymarket the trading collateral lives in the deposit/proxy
        wallet, so the priority order IS the correct selector. Picking the max-balance
        candidate would be wrong — a different wallet (e.g. the EOA or a stale proxy) can hold
        a larger balance that is NOT the account's designated trading funds. Falls back to the
        deposit wallet only when every candidate reads 0."""
        best = None  # (sig_type, addr, balance)
        for st, addr in self._candidate_wallets(gasless):
            try:
                bal = float(gasless.get_pusd_balance(address=addr))
            except Exception:
                bal = 0.0
            if bal > 0:
                return st, addr
            if best is None or bal > best[2]:
                best = (st, addr, bal)
        if best:
            return best[0], best[1]
        return 3, gasless.get_expected_deposit_wallet()

    def _init_clients(self):
        from polymarket_apis.clients.clob_client import PolymarketClobClient

        # Any gasless client can derive every candidate address; probe with deposit.
        probe = self._new_gasless(3)
        sig_type, funder = self._pick_funded_wallet(probe)

        gasless = probe if sig_type == 3 else self._new_gasless(sig_type)

        clob = PolymarketClobClient(
            private_key=settings.PRIVATE_KEY,
            address=funder,
            chain_id=137,
            signature_type=sig_type,
        )
        clob.set_api_creds(clob.create_or_derive_api_creds())

        self.gasless = gasless
        self.clob = clob
        self.funder = funder
        self.signature_type = sig_type
        self.ready = True
        self.last_error = None

    def ensure_ready(self) -> bool:
        if self.ready and self.clob is not None:
            return True
        with self._lock:
            if self.ready and self.clob is not None:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_clients()
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.clob = None
                return False

    def ensure_setup(self) -> Dict[str, Any]:
        """One-time gasless on-chain setup: deploy the deposit wallet (if needed) and
        set token approvals, sponsored by the relayer key. Required once before the
        first live order on a fresh deposit wallet."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if self._approvals_done:
            return {"ok": True, "skipped": "already_done"}
        if not settings.RELAYER_API_KEY:
            return {"ok": False, "error": "missing_relayer_api_key"}
        try:
            receipts = self.gasless.set_all_approvals()
            self._approvals_done = True
            return {"ok": True, "approvals": len(receipts or [])}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── orders ──────────────────────────────────────────────────────────────────
    def _market_order(self, token_id, amount, side: str, price) -> Dict[str, Any]:
        from polymarket_apis.types.clob_types import MarketOrderArgs, OrderType
        # Amount semantics differ by side: BUY = USDC to spend (2dp), SELL = share quantity.
        # SELL must be FLOORED, never rounded up — rounding 10.339 -> 10.34 would try to sell
        # more shares than we hold and the FOK is killed. Floor to 6dp (token precision).
        if side == "SELL":
            amt = math.floor(float(amount) * 1_000_000) / 1_000_000
        else:
            amt = round(float(amount), 2)
        args = MarketOrderArgs(
            token_id=str(token_id),
            amount=amt,
            side=side,                        # "BUY" / "SELL"
            price=round(float(price), 4) if price else 0,
            order_type=OrderType.FOK,
        )
        resp = self.clob.create_and_post_market_order(args)
        data = resp.model_dump() if hasattr(resp, "model_dump") else (resp or {})
        order_id = None
        status = ""
        explicit_fail = resp is None
        if isinstance(data, dict):
            order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
            status = str(data.get("status", "")).lower()
            if data.get("success") is False:
                explicit_fail = True
        # A Fill-Or-Kill only counts as done when the venue confirms a fill. A KILLED FOK
        # comes back as success:true, status:"unmatched"/"cancelled" with NO fill — treating
        # that as success creates a phantom position (BUY) or a false close (SELL). So we
        # trust only an explicit matched/delayed status, or (status absent) a real order id.
        FILLED = ("matched", "delayed")
        KILLED = ("unmatched", "cancelled", "canceled", "rejected", "killed", "expired", "invalid")
        if explicit_fail or status in KILLED:
            success = False
        elif status in FILLED:
            success = True
        else:
            success = bool(order_id)   # unknown/absent status: trust only with an order id
        out = {"ok": success, "response": data, "order_id": order_id, "status": status}
        if success:
            out.update(self._parse_fill(data, side))
        return out

    @staticmethod
    def _parse_fill(data: Any, side: str) -> Dict[str, Any]:
        """Actual filled size from the order response.

        A FOK fixes the USDC spent, NOT the share count — the share count depends on where
        in the book it actually matched. Everything downstream (the exit SELL size, the
        `shares x $1` payout) needs the true number, so we take it from the venue instead
        of trusting our pre-trade estimate.

        OrderPostResponse carries makingAmount/takingAmount:
            BUY  -> making = USDC paid,   taking = shares received
            SELL -> making = shares sold, taking = USDC received

        The unit scaling of these fields is not contractually guaranteed, so the result is
        validated: an implied price outside (0, 1] is impossible for a $1 binary, and we
        discard the parse rather than record a wrong share count.
        """
        if not isinstance(data, dict):
            return {}
        def _num(*keys):
            for k in keys:
                v = data.get(k)
                if v not in (None, ""):
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
            return None
        making = _num("making_amount", "makingAmount")
        taking = _num("taking_amount", "takingAmount")
        if making is None or taking is None or making <= 0 or taking <= 0:
            return {}
        shares, usdc = (taking, making) if side == "BUY" else (making, taking)
        price = usdc / shares
        if not (0.0 < price <= 1.0):
            return {}      # unit mismatch or garbage — fall back to the estimate
        return {"filled_shares": shares, "filled_usdc": usdc, "fill_price": price}

    def _ensure_setup_ok(self) -> Optional[str]:
        """Run the one-time on-chain setup before an order. Returns None if OK (or if the
        only issue is a missing relayer key — legacy/already-approved wallets don't need
        it), else an error string to abort on. Both BUY and SELL go through this so a fresh
        wallet's first order can't revert on missing CTF/exchange approvals."""
        setup = self.ensure_setup()
        if setup.get("ok"):
            return None
        if setup.get("error") == "missing_relayer_api_key":
            return None  # no relayer -> assume a legacy/pre-approved wallet
        return f"setup_failed: {setup.get('error')}"

    def place_market_buy(self, token_id: str, usdc_amount: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Fill-Or-Kill marketable BUY for `usdc_amount` USDC of `token_id`.

        `price` must be the price the fill actually has to REACH — i.e. the WORST ask level
        the stake has to walk to (`walk_asks()["worst_price"]`), not the best ask and not
        the blended VWAP. The limit is that price + slippage buffer.

        This matters because the stake is sized by walking the book. With asks of 0.85 ($10)
        then 0.90 ($40) and a $50 stake, the fill must reach 0.90; a limit derived from the
        0.85 best ask (0.87) cannot clear it and the venue kills the whole order. Quoting
        off the top of book therefore failed exactly the thin-book trades the book-walking
        logic was written to price correctly, and made live silently diverge from paper."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not price or price <= 0:
            return {"ok": False, "error": "missing_quote_price"}   # never send a market order without a quote
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            # Marketable limit = ask + buffer, but never below the ask (must cross) and
            # never above the 0.99 tick ceiling.
            limit = min(0.99, max(float(price), float(price) + settings.CLOB_MAX_SLIPPAGE))
            return self._market_order(token_id, usdc_amount, "BUY", limit)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_market_sell(self, token_id: str, size: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Fill-Or-Kill marketable SELL of `size` shares (used to exit / flip).

        `price` must be the LOWEST bid the sale has to reach (`walk_bids()["worst_price"]`),
        for the same reason as the BUY side. Limit is that price − slippage buffer
        (floored at 1¢)."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not price or price <= 0:
            return {"ok": False, "error": "missing_quote_price"}
        if not size or float(size) <= 0:
            return {"ok": False, "error": "nothing_to_sell"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            # Marketable limit = bid − buffer, but never above the bid (must cross) and
            # never below the 0.01 tick floor.
            limit = max(0.01, min(float(price), float(price) - settings.CLOB_MAX_SLIPPAGE))
            return self._market_order(token_id, size, "SELL", limit)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── redemption (winning positions -> spendable pUSD) ────────────────────────
    def get_api_creds(self) -> Optional[Dict[str, str]]:
        """CLOB API credentials in the shape the user WebSocket channel expects.
        Returns None when there is no key (paper mode), so the stream just idles."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            creds = self._derive_creds()
            return {"apiKey": creds.key, "secret": creds.secret, "passphrase": creds.passphrase}
        except Exception as e:
            self.last_error = f"creds: {type(e).__name__}: {e}"
            return None

    def get_token_balance(self, token_id: str) -> Optional[float]:
        """Outcome-token (ERC-1155) balance held by the trading wallet."""
        if not token_id or not self.ensure_ready():
            return None
        try:
            return float(self.gasless.get_token_balance(str(token_id), address=self.funder))
        except Exception:
            return None

    def enable_auto_redeem(self) -> Dict[str, Any]:
        """Authorise the CTF auto-redeem operator once, so resolved winners are swept into
        pUSD without us sending a redeem per market. Gasless."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            self.gasless.auto_redeem_enable()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def redeem_position(self, condition_id: str, up_shares: float = 0.0,
                        down_shares: float = 0.0, neg_risk: bool = False) -> Dict[str, Any]:
        """Convert resolved winning outcome tokens into pUSD.

        WITHOUT THIS A LIVE WIN NEVER BECOMES SPENDABLE BALANCE. A settled market pays out
        in ERC-1155 outcome tokens, not collateral; `get_pusd_balance` cannot see them, so
        the mirrored balance would only ever fall (every stake debits, no win credits) and
        percent-of-balance sizing would shrink after wins.

        `amounts` is ordered [outcome0, outcome1] and is only consulted for neg-risk
        markets; the plain CTF path redeems the whole position for the condition."""
        if not condition_id:
            return {"ok": False, "error": "missing_condition_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            receipt = self.gasless.redeem_position(
                condition_id, [float(up_shares or 0.0), float(down_shares or 0.0)], bool(neg_risk)
            )
            tx = None
            try:
                d = receipt.model_dump() if hasattr(receipt, "model_dump") else dict(receipt)
                tx = d.get("transaction_hash") or d.get("transactionHash") or d.get("hash")
            except Exception:
                tx = str(receipt) if receipt is not None else None
            return {"ok": True, "tx": tx}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── withdrawal (auto capital extractor) ─────────────────────────────────────
    def get_eoa_address(self) -> Optional[str]:
        """The EOA address derived from PRIVATE_KEY — the wallet you control (the
        key/seed owner). Default destination for auto-withdrawals."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            from eth_account import Account
            return Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            return None

    def withdraw_pusd(self, recipient: str, amount: float) -> Dict[str, Any]:
        """Transfer `amount` pUSD from the funded (deposit) wallet to `recipient`.
        Gasless via the relayer. Used by the auto-withdrawal state machine."""
        if not recipient:
            return {"ok": False, "error": "missing_withdraw_address"}
        if amount is None or float(amount) <= 0:
            return {"ok": False, "error": "invalid_amount"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        # Validate the recipient BEFORE sending — a malformed address must be rejected, never
        # forwarded as a raw string (a typo'd/truncated address would burn the funds).
        try:
            from eth_utils import to_checksum_address, is_address
            if not is_address(recipient):
                return {"ok": False, "error": "invalid_recipient_address"}
            addr = to_checksum_address(recipient)
        except Exception:
            return {"ok": False, "error": "invalid_recipient_address"}
        # FLOOR to 2dp — rounding up (e.g. 100.999 -> 101.00) could exceed the balance and revert.
        amt = math.floor(float(amount) * 100) / 100
        if amt <= 0:
            return {"ok": False, "error": "invalid_amount"}
        try:
            receipt = self.gasless.transfer_pusd(addr, amt)
            tx = None
            try:
                data = receipt.model_dump() if hasattr(receipt, "model_dump") else dict(receipt)
                tx = data.get("transaction_hash") or data.get("transactionHash") or data.get("hash")
            except Exception:
                tx = str(receipt) if receipt is not None else None
            return {"ok": True, "tx": tx, "amount": amt, "recipient": addr}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── diagnostics / balance ───────────────────────────────────────────────────
    def test_connection(self) -> Dict[str, Any]:
        """Derive the EOA + its candidate wallets and report pUSD balances — shows
        which wallet holds the funds and which signature type will be used. Read-only
        (no relayer key required)."""
        if not settings.PRIVATE_KEY:
            return {"ok": False, "error": "missing_private_key"}
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception as e:
            return {"ok": False, "error": f"invalid_key: {type(e).__name__}: {e}"}
        try:
            probe = self._new_gasless(3)
            wallets = []
            for st, addr in self._candidate_wallets(probe):
                try:
                    bal = float(probe.get_pusd_balance(address=addr))
                except Exception:
                    bal = None
                wallets.append({"signature_type": st, "address": addr, "pusd_balance": bal})
            sig_type, funder = self._pick_funded_wallet(probe)
            return {
                "ok": True,
                "eoa": eoa,
                "chosen_signature_type": sig_type,
                "funder": funder,
                "relayer_key_set": bool(settings.RELAYER_API_KEY),
                "wallets": wallets,
            }
        except Exception as e:
            return {"ok": False, "eoa": eoa, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """pUSD balance of the funded wallet (dollars), or None. pUSD is Polymarket's
        V2 collateral; this is the deposit wallet's tradeable balance."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            if self.ready and self.gasless is not None and self.funder:
                return float(self.gasless.get_pusd_balance(address=self.funder))
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return float(probe.get_pusd_balance(address=funder))
        except Exception:
            return None


clob_trader = ClobTrader()
