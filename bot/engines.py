from typing import Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
#  Latency-arb entry engine.
#
#  Backtest verdict: the model has NO predictive edge over the trivial "is spot
#  already above the 15m open?" baseline — that signal is fully priced by the
#  market. The only edge left is LATENCY: act on a Binance spot move before
#  Polymarket's thin book reprices.
#
#  The decision is purely a fast fair probability (from Binance spot) vs the
#  market's implied price. Enter when the gap (expected value) is large enough
#  that the book looks stale. Heiken-Ashi then has to AGREE with that direction on
#  all three timeframes (15m permission, 5m + 1m confirmation) and RSI can still
#  veto an extreme — neither ever creates or reweights the signal.
# ─────────────────────────────────────────────────────────────────────────────

# The 1m/5m Heiken-Ashi candles are DIRECTIONAL confirmation (see LAYER 2 below), not
# an exhaustion veto. There is deliberately no streak-length constant any more: a long
# same-colour run is now supporting evidence for that direction, not a reason to sit out.


def _no_ev(reason: str) -> Dict[str, Any]:
    return {"action": "NO_TRADE", "side": None, "phase": "EV", "strength": "EV", "reason": reason}


def decide_ev(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """EV gate: fair probability (Binance) vs market ask price (Polymarket).

    EV_side = p_side - ask_price_side. A positive EV beyond `evThreshold` means the
    book is underpricing the side our fast feed already favours — the latency edge.
    Heiken-Ashi must then agree with the chosen direction on 15m, 5m and 1m, and RSI
    can veto an extreme. None of them distort the probability — they only decide
    whether the side EV picked may be traded. Position sizing (percent/fixed of
    balance) is handled by the caller.
    """
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")     # ask (buy) price for the UP share, 0..1
    price_down = inputs.get("priceDown") # ask (buy) price for the DOWN share, 0..1

    if p_up is None:
        return _no_ev("missing_model_data")
    if price_up is None or price_down is None:
        return _no_ev("missing_prices")

    p_down = 1.0 - p_up
    ev_up = p_up - price_up
    ev_down = p_down - price_down

    side = "UP" if ev_up >= ev_down else "DOWN"
    p = p_up if side == "UP" else p_down
    price = price_up if side == "UP" else price_down
    ev = ev_up if side == "UP" else ev_down

    min_prob = inputs.get("minProb", 0.55)
    ev_threshold = inputs.get("evThreshold", 0.04)

    # ── LAYER 1: 15m Heiken-Ashi parent shield — MACRO PERMISSION ───────────────
    # Checked first, and on the side EV actually chose: the 15m trend decides
    # whether that direction may be traded at all.
    #
    # MANDATORY, like the 5m/1m confirmation below it. Permission must be granted
    # explicitly, so an absent key fails CLOSED — a caller that forgets to supply the
    # verdict gets no trade rather than an unshielded one.
    if side == "UP" and not inputs.get("ha15AllowUp"):
        return _no_ev(inputs.get("ha15UpReason") or "ha15_no_data")
    if side == "DOWN" and not inputs.get("ha15AllowDown"):
        return _no_ev(inputs.get("ha15DownReason") or "ha15_no_data")

    # ── LAYER 2: 1m + 5m Heiken-Ashi direction — MICRO CONFIRMATION ─────────────
    # The faster candles must point the SAME way as the side the 15m shield just
    # permitted: green confirms UP, red confirms DOWN. With the 15m above them this
    # is a three-timeframe alignment — 15m permits the direction, 5m and 1m confirm
    # the move is live right now.
    #
    # This REPLACES the old exhaustion veto, which blocked a side once its Heiken-Ashi
    # streak reached 6 bars. That rule was anti-trend: it withheld the trade exactly
    # when the timeframes agreed most strongly, which on a persistence model is the
    # best setup rather than the worst. A long green streak is now a reason to buy UP,
    # not a reason to sit out.
    #
    # MANDATORY and fails CLOSED, like the shield above: an absent or unknown colour
    # blocks the entry rather than waving it through unconfirmed.
    want = "green" if side == "UP" else "red"
    for tf in ("1m", "5m"):
        colour = inputs.get(f"ha{tf}Colour")
        if colour != want:
            return _no_ev(f"ha{tf}_{colour or 'no_data'}_blocks_{side.lower()}")

    # Don't trade into an RSI extreme, even with all three timeframes aligned. This is
    # the one COUNTER-trend check left in the stack: everything above it is confluence,
    # so without it nothing can stop a fully-aligned signal.
    rsi = inputs.get("rsi")
    if rsi is not None:
        if side == "UP" and rsi > 70:
            return _no_ev("rsi_overbought")
        if side == "DOWN" and rsi < 30:
            return _no_ev("rsi_oversold")

    # ── GATES ──
    # Too close to expiry to trust a Fill-Or-Kill fill. Now that the horizon is
    # continuous the model goes near-certain in the last seconds, so EV against any
    # stale quote looks enormous — but a FOK into a closing book is exactly the order
    # least likely to fill, and the fill it does get is the worst of the window. The
    # step-quantized horizon used to damp this by pricing 5 whole minutes of vol no
    # matter how little was left; that crutch is gone, so the guard is explicit.
    # Fails CLOSED: a missing secondsLeft blocks the entry rather than skipping it.
    seconds_left = inputs.get("secondsLeft")
    min_seconds_left = inputs.get("minSecondsLeft", 30.0)
    if seconds_left is None or seconds_left < min_seconds_left:
        left_txt = "unknown" if seconds_left is None else f"{seconds_left:.0f}s"
        return _no_ev(f"only_{left_txt}_left_below_{min_seconds_left:.0f}s")

    if p < min_prob:
        return _no_ev(f"prob_{p:.2f}_below_{min_prob:.2f}")
    if ev < ev_threshold:
        return _no_ev(f"ev_{ev:.3f}_below_{ev_threshold:.3f}")

    strength = "HIGH_CONVICTION" if p >= 0.70 else "STRONG"
    return {
        "action": "ENTER", "side": side, "phase": "EV", "strength": strength,
        "prob": p, "price": price, "ev": ev, "reason": "ev_enter"
    }
