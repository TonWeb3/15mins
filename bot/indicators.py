import pandas as pd
import numpy as np
import math
from ta.momentum import RSIIndicator
from typing import List, Optional, Dict, Tuple


def compute_rsi(closes: List[float], period: int) -> Optional[float]:
    """RSI(period). Used as a veto: don't buy UP into >70 / DOWN into <30."""
    if not closes or len(closes) < period:
        return None
    series = pd.Series(closes)
    rsi = RSIIndicator(close=series, window=period).rsi()
    if rsi.empty:
        return None
    val = rsi.iloc[-1]
    return float(val) if not pd.isna(val) else None


def compute_heiken_ashi(candles: List[Dict]) -> List[Dict]:
    """Heiken-Ashi candles. Feeds the 15m parent shield and, via count_consecutive,
    the 1m/5m directional confirmation."""
    if not candles:
        return []

    ha = []
    for i in range(len(candles)):
        c = candles[i]
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4

        if i > 0:
            prev = ha[i - 1]
            ha_open = (prev["open"] + prev["close"]) / 2
        else:
            ha_open = (c["open"] + c["close"]) / 2

        ha_high = max(c["high"], ha_open, ha_close)
        ha_low = min(c["low"], ha_open, ha_close)

        ha.append({
            "open": ha_open,
            "high": ha_high,
            "low": ha_low,
            "close": ha_close,
            "isGreen": ha_close >= ha_open,
            "body": abs(ha_close - ha_open)
        })
    return ha


def count_consecutive(ha_candles: List[Dict]) -> Dict:
    """Current Heiken-Ashi colour and how many bars it has run.

    `color` is the DIRECTIONAL signal the engine gates on; `count` is context only —
    a long streak used to veto the trade and now simply reinforces its direction."""
    if not ha_candles or len(ha_candles) < 2:
        return {"color": None, "count": None}

    last = ha_candles[-1]
    target = "green" if last["isGreen"] else "red"

    count = 0
    for i in range(len(ha_candles) - 1, -1, -1):
        c = ha_candles[i]
        color = "green" if c["isGreen"] else "red"
        if color != target:
            break
        count += 1

    return {"color": target, "count": count}


# ─────────────────────────────────────────────────────────────────────────────
#  15-minute Heiken-Ashi "parent shield" — MACRO PERMISSION, not a signal.
#
#  The 1m/5m HA colours are micro confirmation: they say the move is running right
#  now. This is the layer above them — it asks whether the 15m trend permits the side
#  at all, and vetoes both sides when the last completed 15m candle is indecisive.
#
#  It reads only the LAST COMPLETED 15m candle. A developing candle's wicks and
#  body change every tick, so filtering on one would flip its own verdict
#  mid-window — the filter has to be stable for the window it governs.
#
#  HA candles are computed by compute_heiken_ashi() above; the math is identical,
#  so there is deliberately no second implementation to drift out of sync.
#
#  NOT CONFIGURABLE, and not optional — the thresholds are hardcoded here for the
#  same reason the 1m/5m direction rule and the RSI 70/30 bounds live in engines.py:
#  they are part of the strategy's definition, not a knob to tune per run. Change
#  them here if the strategy changes.
# ─────────────────────────────────────────────────────────────────────────────

# Counter wick measured against the BODY. A green candle dragging a long lower wick
# is being fought, so it is not clean permission.
HA15_MAX_COUNTER_WICK_RATIO = 0.15
# BOTH wicks measured against the TOTAL RANGE. Above this on both sides the body is
# under 20% of the candle: the 15m has no committed direction, so neither side runs.
HA15_INDECISION_WICK_RATIO = 0.40


def evaluate_15m_ha_filter(prev_ha_candle: Dict, target_side: str,
                           max_counter_wick_ratio: float = HA15_MAX_COUNTER_WICK_RATIO,
                           indecision_ratio: float = HA15_INDECISION_WICK_RATIO) -> Tuple[bool, str]:
    """Is `target_side` permitted by the last completed 15m HA candle?

    Returns (allowed, reason). The reason is a short snake_case code so it lands
    in signals.csv alongside `rsi_overbought` / `ha1m_red_blocks_up` and stays
    greppable; the human-readable form is built by ha15_permissions().
    """
    if not prev_ha_candle:
        return False, "ha15_no_data"

    ha_open = prev_ha_candle["open"]
    ha_close = prev_ha_candle["close"]
    ha_high = prev_ha_candle["high"]
    ha_low = prev_ha_candle["low"]

    body = abs(ha_close - ha_open)
    upper_wick = ha_high - max(ha_open, ha_close)
    lower_wick = min(ha_open, ha_close) - ha_low
    total_range = ha_high - ha_low

    if total_range <= 0:
        return False, "ha15_flat"

    # 1. Indecision veto — prominent wicks on BOTH sides means the 15m has no
    #    committed direction, so neither side is permitted.
    if (upper_wick / total_range > indecision_ratio) and (lower_wick / total_range > indecision_ratio):
        return False, "ha15_indecision"

    # 2. Bullish permission. Note `body > 0` is guaranteed here: ha_close == ha_open
    #    is caught by the direction test above it.
    if target_side == "UP":
        if ha_close <= ha_open:
            return False, "ha15_red_blocks_up"
        if body > 0 and (lower_wick / body) > max_counter_wick_ratio:
            return False, "ha15_lower_wick"
        return True, "ha15_bullish"

    # 3. Bearish permission.
    if target_side == "DOWN":
        if ha_close >= ha_open:
            return False, "ha15_green_blocks_down"
        if body > 0 and (upper_wick / body) > max_counter_wick_ratio:
            return False, "ha15_upper_wick"
        return True, "ha15_bearish"

    return False, "ha15_invalid_side"


def ha15_permissions(closed_15m: List[Dict],
                     max_counter_wick_ratio: float = HA15_MAX_COUNTER_WICK_RATIO,
                     indecision_ratio: float = HA15_INDECISION_WICK_RATIO) -> Dict:
    """Evaluate both sides at once against the last COMPLETED 15m candle.

    `closed_15m` must contain only completed candles — the caller is responsible
    for dropping the developing one. At least 2 are needed: Heiken-Ashi is a
    recurrence, so the last candle's HA open depends on the one before it.

    Returns a dict the decision engine and the dashboard both read.
    """
    if not closed_15m or len(closed_15m) < 2:
        return {"allow_up": False, "allow_down": False, "reason": "ha15_no_data",
                "color": None, "detail": "Waiting for two completed 15m candles"}

    ha = compute_heiken_ashi(closed_15m)
    last = ha[-1]

    up_ok, up_reason = evaluate_15m_ha_filter(last, "UP", max_counter_wick_ratio, indecision_ratio)
    down_ok, down_reason = evaluate_15m_ha_filter(last, "DOWN", max_counter_wick_ratio, indecision_ratio)

    body = abs(last["close"] - last["open"])
    upper_wick = last["high"] - max(last["open"], last["close"])
    lower_wick = min(last["open"], last["close"]) - last["low"]
    total_range = last["high"] - last["low"]

    if up_ok:
        detail = "15m HA bullish — UP permitted"
    elif down_ok:
        detail = "15m HA bearish — DOWN permitted"
    elif up_reason == "ha15_indecision":
        detail = (f"15m HA indecision — both wicks prominent "
                  f"(upper {upper_wick:.1f} / lower {lower_wick:.1f} of {total_range:.1f} range)")
    elif up_reason == "ha15_lower_wick" or down_reason == "ha15_upper_wick":
        detail = (f"15m HA has a counter wick against its own body "
                  f"(body {body:.1f}, upper {upper_wick:.1f}, lower {lower_wick:.1f})")
    else:
        detail = "15m HA permits neither side"

    return {
        "allow_up": up_ok,
        "allow_down": down_ok,
        # The reason belonging to whichever side was blocked; UP's is reported when
        # both are blocked, since an indecision veto gives both the same code.
        "reason": up_reason if not up_ok else (down_reason if not down_ok else "ha15_bullish"),
        "up_reason": up_reason,
        "down_reason": down_reason,
        "color": "green" if last["close"] > last["open"] else ("red" if last["close"] < last["open"] else "flat"),
        "body": body,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "range": total_range,
        "detail": detail,
    }


def realized_drift_vol(candles: List[Dict], lookback: int = 300,
                       minutes_per_candle: float = 1.0):
    """(drift, sigma) of log returns, normalised to PER-MINUTE units.

    The raw statistics are per-CANDLE, so `minutes_per_candle` converts them. Under
    GBM variance grows linearly with time, so drift scales with t and sigma with
    sqrt(t):

        drift_1m = drift_per_candle / minutes_per_candle
        sigma_1m = sigma_per_candle / sqrt(minutes_per_candle)

    Everything downstream then works in minutes, which is what lets the fair-prob
    horizon be continuous instead of quantized into candle-sized steps. Doing the
    conversion here rather than at the call site keeps the two scaling laws (linear
    for drift, square-root for sigma) in one place — getting sigma's wrong is silent
    and mis-prices every probability.

    Returns (None, None) if there isn't enough data.
    """
    closes = [c["close"] for c in candles[-lookback:] if c.get("close")]
    if len(closes) < 20:
        return None, None
    arr = np.asarray(closes, dtype=float)
    rets = np.diff(np.log(arr))
    rets = rets[np.isfinite(rets)]
    if len(rets) < 10:
        return None, None
    m = float(minutes_per_candle) if minutes_per_candle and minutes_per_candle > 0 else 1.0
    return float(np.mean(rets)) / m, float(np.std(rets)) / math.sqrt(m)


# Below this many minutes left the horizon is treated as effectively settled — it also
# keeps sigma*sqrt(t) away from zero so z stays finite.
MIN_HORIZON_MINUTES = 1.0 / 60.0  # 1 second


def fair_prob_up(current_price: float, strike: float, minutes_left: float,
                 sigma_per_minute: Optional[float], drift_per_minute: float = 0.0) -> float:
    """Closed-form GBM probability that price closes ABOVE `strike` in `minutes_left`
    minutes — the core direction/edge model. The model is just persistence: "is spot
    above the open, given the volatility still to come?" Returns 0..1.

    Time is CONTINUOUS. The horizon used to be `ceil(minutes_left / 5)` whole 5-minute
    steps, which gave a 15m window only three values (3/2/1) and floored at one — so
    the last five minutes were all priced as though five full minutes of volatility
    remained. Because ceil only ever rounds UP, that overstated the vol still to come,
    which widens sd, shrinks |z| and pulls the probability toward 0.5: at one minute
    left the model read 0.64 where the honest number was 0.79, and at ten seconds 0.64
    against 0.97. Conviction was understated exactly where the persistence signal is
    strongest, and every EV with it. Scaling by sqrt(t) in minutes makes conviction
    tighten smoothly instead, and removes the step changes at the 10- and 5-minute
    boundaries that moved the fair price with no market move behind them.
    """
    if not current_price or not strike or current_price <= 0 or strike <= 0:
        return 0.5
    t = max(float(minutes_left or 0.0), MIN_HORIZON_MINUTES)
    # No usable volatility (too few candles, or flat/degenerate data) => we have NO
    # directional information, so return 0.5 (a coin flip). This used to return a hard
    # 1.0/0.0 on "is spot above the strike", which made EV = 1 - price sail through the
    # min-prob and EV gates and fire at full size on garbage data — reachable whenever
    # the strike was marked but the kline buffer had failed to seed. 0.5 is caught by
    # the min-prob gate and suppresses the entry, which is the correct fail-safe.
    if sigma_per_minute is None or sigma_per_minute <= 0:
        return 0.5
    sd = sigma_per_minute * math.sqrt(t)
    if sd <= 0:
        return 0.5
    mu = (drift_per_minute - 0.5 * sigma_per_minute ** 2) * t
    # P(S * exp(X) > K) for X ~ N(mu, sd^2)  ->  1 - Phi(z)  ->  0.5 * erfc(z/sqrt2)
    z = (math.log(strike / current_price) - mu) / sd
    prob = 0.5 * math.erfc(z / math.sqrt(2))
    return float(min(1.0, max(0.0, prob)))
