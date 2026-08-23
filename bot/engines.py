from typing import Dict, Any, Optional

# ─────────────────────────────────────────────────────────────────────────────
#  Entry rule — the LEVEL, not an event.
#
#      price above the open  ->  hold UP    (buy UP if we don't already hold UP)
#      price below the open  ->  hold DOWN  (buy DOWN if we don't already hold DOWN)
#      holding the wrong one ->  close it and open the other (reversal)
#
#  It is deliberately stated as a level rather than a "cross". A cross is an EVENT
#  between two ticks, so it is missed whenever the tick that would have seen it is
#  lost — a slow poll, a stalled feed, a restart mid-window — and once missed the bot
#  sits flat with the signal plainly telling it what to hold. Comparing the level every
#  tick cannot be missed: whatever side price is on, that is the side we should be on.
#
#  After a STOP-LOSS the side that lost is BLOCKED, so the bot cannot immediately buy
#  back the same losing direction tick after tick. The block clears as soon as price is
#  on the other side of the open, which is what "wait for the opposite signal" means.
#
#  A take-profit is handled by the window budget (bot/risk.py), not here: it stops the
#  window entirely.
# ─────────────────────────────────────────────────────────────────────────────


def _no_trade(reason: str, side=None, distance=None) -> Dict[str, Any]:
    # Carry the side/distance even on a no-trade so every tick can be logged.
    return {"action": "NO_TRADE", "side": side, "phase": "LEVEL", "strength": "LEVEL",
            "reason": reason, "distance": distance}


def signal_side(spot: Optional[float], strike: Optional[float],
                min_move: float = 0.0) -> Optional[str]:
    """Which side of the open price is on, or None while it is within `min_move` of it.

    The dead band is not a filter on conviction — it is there because ON the open the side
    is a coin flip priced with the window's widest spread, and flipping the position costs
    a full round trip (~7% of stake) every time. Inside the band there is no side, so a
    held position is simply kept.
    """
    if not spot or not strike or spot <= 0 or strike <= 0:
        return None
    diff = spot - strike
    if abs(diff) <= max(0.0, float(min_move or 0.0)):
        return None
    return "UP" if diff > 0 else "DOWN"


def decide_side(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Entry / reverse decision from where price sits relative to the window's open.

    Returns action:
      ENTER    — flat, and price is on a side we're allowed to take.
      REVERSE  — holding the opposite side: sell it and buy this one.
      NO_TRADE — with the reason, which is written to signals.csv every tick.
    """
    spot = inputs.get("spot")
    strike = inputs.get("strike")
    min_move = inputs.get("minMove", 0.0)
    held = inputs.get("heldSide")
    blocked = inputs.get("blockedSide")     # side that just hit a stop-loss, if any
    seconds_left = inputs.get("secondsLeft")
    min_seconds_left = inputs.get("minSecondsLeft", 20.0)

    if strike is None:
        return _no_trade("no_strike_this_window", None, None)
    if spot is None:
        return _no_trade("no_spot", None, None)

    side = signal_side(spot, strike, min_move)
    distance = spot - strike

    # Inside the dead band there is no side. A held position is KEPT (not closed) — the
    # band means "this is noise, don't act", not "get out".
    if side is None:
        return _no_trade("at_the_open", None, distance)

    # Already on the right side — nothing to do. This is the "buy UP only if there is no
    # open UP position" half of the rule.
    if held == side:
        return _no_trade("already_holding_signal_side", side, distance)

    # Holding the OTHER side: close it and take this one. A reversal is an exit first, so
    # it is not gated on time or on the stop-loss block — being on the wrong side of the
    # open is the exact thing this strategy refuses to keep paying for.
    if held:
        return {"action": "REVERSE", "side": side, "phase": "LEVEL", "strength": "LEVEL",
                "reason": "wrong_side_of_open", "distance": distance}

    # ── FLAT: entry gates ──
    # Don't re-buy the side that just stopped out; wait until price is on the other side.
    if blocked and side == blocked:
        return _no_trade(f"{side.lower()}_blocked_after_stop_loss", side, distance)
    # Too close to expiry to trust a Fill-Or-Kill fill, and too close for a take-profit to
    # have room to happen. Fails CLOSED: a missing secondsLeft blocks the entry.
    if seconds_left is None or seconds_left < min_seconds_left:
        left_txt = "unknown" if seconds_left is None else f"{seconds_left:.0f}s"
        return _no_trade(f"only_{left_txt}_left_below_{min_seconds_left:.0f}s", side, distance)

    return {"action": "ENTER", "side": side, "phase": "LEVEL", "strength": "LEVEL",
            "reason": "above_open" if side == "UP" else "below_open", "distance": distance}
