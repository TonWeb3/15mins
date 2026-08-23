"""
Per-window risk budget — the heart of this build.

Every 15-minute market window is treated as an independent session with its own
bankroll snapshot and its own win/loss budget:

  1. At the START of the window the current balance is RECORDED. Everything for the
     rest of that window is sized off that one number, so the stake does not drift as
     trades close mid-window (a percent-of-live-balance stake would shrink after each
     loss and grow after a win, which silently changes the risk you agreed to).
  2. `risk_per_trade` = RISK_VALUE% of the recorded balance (or a flat RISK_VALUE when
     RISK_TYPE is "fixed").
  3. The window has a MAX LOSS and a MAX WIN, both expressed as a percentage OF THAT
     risk-per-trade. With the defaults (SL 10%, TP 30%, max loss 30%, max win 30%)
     that is exactly "three stop-losses or one take-profit, then stop for this window".
  4. `stop_after_win` stops entries on the first TAKE-PROFIT, regardless of the
     dollar cap.

It also owns the BLOCKED SIDE: after a stop-loss that direction cannot be re-opened
until price is on the OTHER side of the open. Without that, a stop-loss taken while
price is still on the same side re-buys the same losing direction on the very next
500ms tick, and again, and burns the whole loss budget in seconds.

The risk state is rebuilt from scratch on every window roll — nothing carries over.
"""

from typing import Optional, Dict, Any, Tuple
from .config import settings


class WindowRisk:
    def __init__(self):
        self.window_start_ms: Optional[int] = None
        self.balance_at_start: float = 0.0
        self.risk_per_trade: float = 0.0
        self.realized_pl: float = 0.0
        self.wins: int = 0
        self.losses: int = 0
        self.trades: int = 0
        self.blocked: Optional[str] = None   # why entries are closed for this window
        # The side that just hit a STOP-LOSS. That direction cannot be re-opened until
        # price is on the OTHER side of the open — otherwise the bot buys the same losing
        # direction back on the very next tick, and does it again, and again.
        self.blocked_side: Optional[str] = None

    # ── window lifecycle ────────────────────────────────────────────────────────
    def roll(self, window_start_ms: int, balance: float) -> bool:
        """Start a new window and record the balance it is sized against.
        Returns True if this actually rolled (i.e. it's a new window)."""
        if self.window_start_ms == window_start_ms:
            return False
        self.window_start_ms = window_start_ms
        self.balance_at_start = float(balance or 0.0)
        self.risk_per_trade = self._stake_from(self.balance_at_start)
        self.realized_pl = 0.0
        self.wins = 0
        self.losses = 0
        self.trades = 0
        self.blocked = None
        self.blocked_side = None
        return True

    @staticmethod
    def _stake_from(balance: float) -> float:
        if (settings.RISK_TYPE or "percent").lower() == "fixed":
            return max(0.0, float(settings.RISK_VALUE))
        return max(0.0, (float(settings.RISK_VALUE) / 100.0) * float(balance or 0.0))

    def stake(self) -> float:
        """Dollars to risk on one trade — sized off the balance RECORDED AT THE WINDOW
        OPEN, not the live balance. Falls back to a live-balance sizing only if the
        window was never rolled (shouldn't happen once the loop is running)."""
        return self.risk_per_trade

    # ── budgets ─────────────────────────────────────────────────────────────────
    def loss_budget(self) -> float:
        """Max dollars this window may LOSE (a positive number)."""
        return (float(settings.MAX_WINDOW_LOSS_PCT) / 100.0) * self.risk_per_trade

    def win_budget(self) -> float:
        """Max dollars this window may WIN before it stops."""
        return (float(settings.MAX_WINDOW_WIN_PCT) / 100.0) * self.risk_per_trade

    def loss_left(self) -> float:
        return max(0.0, self.loss_budget() + min(0.0, self.realized_pl))

    def can_enter(self) -> Tuple[bool, Optional[str]]:
        """(allowed, reason_if_not). Reasons are the strings written to signals.csv."""
        if self.window_start_ms is None:
            return False, "no_window"
        if self.blocked:
            return False, self.blocked
        if self.risk_per_trade <= 0:
            return False, "stake_zero"
        return True, None

    # ── events ──────────────────────────────────────────────────────────────────
    def note_signal(self, side: Optional[str]):
        """Clear the stop-loss block once price is on the OTHER side of the open.

        This is a LEVEL check, not a cross event: it only asks where price is now, so it
        cannot be missed by a dropped tick the way waiting for a crossing would be."""
        if not side or not self.blocked_side:
            return
        if side != self.blocked_side:
            self.blocked_side = None

    def record_open(self):
        self.trades += 1

    def record_close(self, window_start_ms: Optional[int], pl: float,
                     reason: str, side: Optional[str] = None) -> Optional[str]:
        """Book a realized P/L against its own window and re-check the budgets.

        `window_start_ms` is the window the TRADE belongs to — a position held to expiry
        settles after the window has already rolled, and that P/L must not be charged to
        the fresh window's budget. Returns the blocking reason if this close closed the
        window, else None.
        """
        if window_start_ms is not None and self.window_start_ms is not None \
                and int(window_start_ms) != int(self.window_start_ms):
            return None      # belongs to a window that has already ended — not our budget

        pl = float(pl or 0.0)
        self.realized_pl += pl
        if pl > 0:
            self.wins += 1
        elif pl < 0:
            self.losses += 1

        # A stop-loss blocks THAT DIRECTION until price is on the other side of the open.
        if reason == "stop_loss":
            self.blocked_side = side

        # "Stop after a win" means the TAKE-PROFIT you were aiming for — not any close that
        # happens to end a cent up. Live: a reversal closed +$7.00 and another +$1.89, and
        # both shut the window down while the signal was plainly saying to take the other
        # side; the bot then sat out the rest of the window for a 2% gain. Reaching the win
        # BUDGET still stops it (next branch), so a run of small profits is still capped.
        if settings.STOP_AFTER_WIN and pl > 0 and reason == "take_profit":
            self.blocked = "window_win_stop"
        elif self.realized_pl >= self.win_budget() > 0:
            self.blocked = "window_win_cap"
        elif self.realized_pl <= -self.loss_budget() and self.loss_budget() > 0:
            self.blocked = "window_loss_cap"
        elif reason == "manual":
            self.blocked = "manual_close"
        return self.blocked

    # ── reporting ───────────────────────────────────────────────────────────────
    def snapshot(self) -> Dict[str, Any]:
        allowed, reason = self.can_enter()
        return {
            "window_start_ms": self.window_start_ms,
            "balance_at_start": self.balance_at_start,
            "risk_per_trade": self.risk_per_trade,
            "realized_pl": self.realized_pl,
            "wins": self.wins,
            "losses": self.losses,
            "trades": self.trades,
            "loss_budget": self.loss_budget(),
            "win_budget": self.win_budget(),
            "loss_left": self.loss_left(),
            "blocked": self.blocked,
            "can_enter": allowed,
            "block_reason": reason,
            "blocked_side": self.blocked_side,
        }


window_risk = WindowRisk()
