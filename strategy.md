# Strategy: Polymarket BTC "Up or Down" 15-minute — Above/Below the Open, with a per-window risk budget

This document describes how the bot decides to trade. It trades a binary option:
*"Will BTC be higher at the end of this 15-minute window than at the window's open?"*
The winning side pays **$1.00** per share, the loser $0.

Series **10192** (`btc-up-or-down-15m`). Windows are aligned to exact 15-minute
boundaries (:00/:15/:30/:45), so the local window clock and the market's `endDate` agree.

---

## 0. The idea

Take the side price is already on, and follow it.

> Price **above** the window's open → hold **UP** (buy UP if we don't already hold UP).
> Price **below** the window's open → hold **DOWN** (buy DOWN if we don't already hold DOWN).
> Holding the **wrong** side → sell it and take the other one.

It is a **level, not an event**. A "cross" happens between two ticks, so it is missed
whenever the tick that would have seen it is lost — a stalled feed, a slow poll, a restart
mid-window — and once missed the bot sits flat while the signal plainly says what to hold.
Comparing the level every tick cannot be missed: whichever side price is on is the side we
should be on.

That's the whole entry model. There is no fair-value model, no expected-value gate and
no probability threshold — the earlier closed-form GBM / latency-arbitrage engine has been
removed, along with the 1-minute volatility feed that fed it.

What replaces it is **risk control**, not signal cleverness: the position size, how much a
window may lose, how much it may win, and when the bot must sit out are all fixed in
advance, per window. A trivial signal with a hard budget is the design.

---

## 1. The pipeline

Every 500 ms ([`update_loop`](main.py)):

```
Binance spot ─► side of the open ─► ENTER / REVERSE ─► stake ─► execute ─► TP/SL ─► window budget
(fast feed)     (vs the venue's     (bot/engines.py)   (fixed    (FOK,      (+30% /   (3 losses or
                 real strike)                           for the   paper/     -10%)     1 win = stop)
                                                        window)   live)
```

---

## 2. The signal — which side of the open
[`signal_side`](bot/engines.py) / [`decide_side`](bot/engines.py).

**The open** is the market's own **Price to Beat**: the venue's `crypto_prices_chainlink`
value read at the market's exact `eventStartTime`.

> ⚠ **The 15m market settles on a TWAP, and the venue does not publish it.** Unlike the 5m
> market (which resolves on the plain Chainlink BTC/USD stream — exactly this topic), the
> 15m market's `resolutionSource` is the **60-second TWAP** stream
> `data.chain.link/streams/btc-usd-twap-60s-streams`. Probing the venue's live-data socket
> for every plausible TWAP topic returns nothing: it publishes only `crypto_prices_chainlink`
> (Chainlink spot) and `crypto_prices` (USDT spot). The strike marked here is therefore the
> Chainlink **spot** at `eventStartTime` — the closest available proxy, differing from the
> TWAP by roughly the average move over the trailing minute. This affects the price-based
> settlement *fallback* only; the authoritative paths (the venue's pushed `market_resolved`
> and Gamma's `closed` flag, §7) are what actually settle a trade and are unaffected. Verified live — the marked strike sat **0.010%** from the independent on-chain
Chainlink BTC/USD aggregator, while Binance spot was **$52** away at the same instant. That
gap is why the strike may only come from the venue's own feed.

If the bot was not running at the boundary, the strike is never marked and **that window is
simply not traded** — there is deliberately no Binance approximation. When that happens the
bot now says *why* (feed never connected / stopped / gapped / started mid-window), in the
log and on the dashboard, because "not marked" alone looks identical whether one window was
skipped or the price feed has been dead for hours.

**Spot** is the fast Binance price, re-anchored onto that strike:

```
effective_spot = strike + (binance_spot − binance_15m_open)
```

so the comparison reacts at Binance speed but is measured against the number the market
actually resolves on, and the delta is offset-free (same feed at both ends).

### The dead band around the open
`MIN_MOVE_USD` (default **$5**) is how far price must be from the open before it counts as
a side. Inside the band there is no signal, and a held position is simply **kept** — the
band means "this is noise", not "get out".

It is not a conviction filter. ON the open the side is a coin flip quoted with the widest
spread of the window, and every flip pays a full round trip. Measured live with no band:

```
14:35:01  UP @0.437
14:35:04  REVERSE -> DOWN   -$8.53   [entry cost -6.4%, market -2.1%]   d = -1.55
14:35:05  REVERSE -> UP     -$10.00  [entry cost -7.4%, market -2.6%]   d = +1.72
14:35:14  STOP LOSS         -$12.77
          window loss cap hit: -$31.30
```

Two complete position flips **one second apart** on BTC moving $3.27, and −$16.10 of that
−$31.30 was the cost of flipping — more than the market itself took. Replayed against the
same trace, a $5 band removes both noise flips (3 flips → 1) while keeping every real
signal, including the −$6.15 move that produced the +$46.01 take-profit. Set it to 0 for
the literal rule.

### Entry gates
A *new* position additionally requires:

1. **`SECONDS_LEFT ≥ MIN_SECONDS_LEFT`** (default 20s) — a Fill-Or-Kill into a book
   seconds from resolving is not a fill you can rely on, and a take-profit needs room to
   happen. Fails closed on unknown time. Exits and reversals are **not** gated by it.
2. **The direction is not blocked** — see §4. After a stop-loss that side cannot be
   re-opened until price is on the other side of the open.
3. **A fresh spot price** (age ≤ `MAX_SPOT_AGE_S`, 5s). A frozen feed keeps asserting
   whichever side it died on, so a stale spot registers **no side at all** — it can neither
   trigger an entry, nor clear a stop-loss block, nor reverse.
4. The window's risk budget still open (§4).

---

## 3. The reverse — the second half of the rule
[`maybe_reverse_position`](main.py).

The side of the open *is* the position, so the moment price is on the other side, the held
side is sold and the other bought **in the same tick**. This is not an optional "flip" mode;
it is the entry rule applied continuously.

The **close always runs** (being on the wrong side of the open is precisely what we stop
paying for), but the **re-open is conditional**: if the window's budget has closed (a
take-profit already happened, or the loss budget is exhausted), or a withdrawal is waiting for the account to go flat, or there
is less than `MIN_SECONDS_LEFT` remaining, the bot closes and stays flat. A reversal whose
re-entry is refused is reported as `reverse_close_only_<reason>`, never as a completed
reversal.

### Being on the wrong side outranks the stop-loss
They almost always fire on the **same tick** — price moving to the other side of the open is
exactly what makes the held side lose value, so by the time we're on the wrong side the
position is usually already past −`STOP_LOSS_PCT`. The reversal is therefore evaluated
**first**.

Checking the stop first (the original order) booked those closes as `stop_loss`: it
mislabelled the history (`Exit` showed **SL** where a reversal had plainly happened), spent
the loss budget under the wrong heading, and blocked a direction the bot was about to take
deliberately. Now such a close is `reverse`, shown as **REVERSAL**.

The tick that reverses also **skips the TP/SL check**: the position a reversal just opened
is under water by its own entry cost (§5) the instant it exists, and stop-checking it in the
same tick would close it before the market had any chance to move.

---

## 4. Risk management — the per-window budget
[`bot/risk.py`](bot/risk.py). **This is the core of this build.** Each 15-minute window is
an independent session:

### At the window open, the balance is RECORDED
`risk_per_trade` is derived from that snapshot, once:
`RISK_VALUE`% of it (`percent`) or a flat `RISK_VALUE` (`fixed`).

Sizing off the *recorded* balance rather than the live one is deliberate. A
percent-of-live-balance stake shrinks after every loss and grows after every win, so the
window's caps would mean a different number of trades depending on the order results
happened to arrive in. Fixed for the window, "three stop-losses" stays three stop-losses.

Equity (cash + the marked value of anything still open) is what gets recorded, so a
position still settling from the previous window doesn't understate the bankroll.

### The window has a maximum loss and a maximum win
Both are a percentage **of that window's risk-per-trade**, not of the balance:

| | default | with a $100 risk/trade | means |
|---|---|---|---|
| `MAX_WINDOW_LOSS_PCT` | 30% | −$30 | **three** stop-losses (−10% each) |
| `MAX_WINDOW_WIN_PCT` | 30% | +$30 | **one** take-profit (+30%) |
| `STOP_AFTER_WIN` | true | — | a **take-profit** ends the window |

When either is reached, **entries stop until the next 15-minute window**. Open positions
still exit and settle normally — a budget closes the door to new risk, it never traps an
existing position.

Realized P/L is charged to the window the **trade belongs to**, not the window it happens
to close in. A position held to expiry settles after its window has already rolled; that
result must not consume the fresh window's budget.

### After a stop-loss: that direction is blocked
A stop-loss **blocks the side that lost**. If an UP position stops out, the bot will not
open UP again — it waits until price is on the **other** side of the open, then trades DOWN.
Without this, a stop-loss taken while price is still above the open re-buys the same losing
direction on the very next 500 ms tick, again and again, and burns the entire loss budget
in seconds.

The block is cleared by the **level**, not by a crossing event: as soon as price is on the
other side of the open, the block is gone. The opposite side is never blocked — only the
direction that just lost.

A take-profit doesn't need this — it ends the window outright.

---

## 5. Exits — take-profit / stop-loss only
[`maybe_tp_sl`](main.py), [`close_position_at_bid`](main.py).

An open position is closed by exactly three things: **+`TAKE_PROFIT_PCT`** of the stake,
**−`STOP_LOSS_PCT`** of the stake, or the 15-minute expiry (§7). Plus a reversal (§3) and
the manual **Close Trade** button, which also ends the window.

The mark is what a **full liquidation would really return** — `walk_bids` over the whole
position, not the best bid. Valuing every share at the top of book overstates the exit and
would fire a take-profit on a sale you cannot actually get.

### They measure the MARKET, not the spread you paid to enter
Both triggers are measured from the position's **liquidation value at entry**, not from the
stake. `-10%` means the market took 10% of the stake away *after* the unavoidable round
trip; it does not mean "the bid is 10% below the ask I just paid".

This was a live failure, not a theoretical one. Measured against the stake, a position at
~50-67¢ with a 5-6¢ spread is **9-12% down the instant it fills** — more than a 10% stop —
so the stop was tripped before the market moved at all:

| entered | closed | side | entry | exit | P/L |
|---|---|---|---|---|---|
| 12:55:54 | **12:55:54** | UP | 67¢ | SL | −$16.03 |
| 1:00:00 | 1:00:02 | DOWN | 53¢ | SL | −$21.40 |
| 1:00:02 | 1:00:04 | UP | 58¢ | SL | −$11.67 |
| 1:05:00 | 1:05:04 | DOWN | 52¢ | SL | −$11.24 |

The first row entered and stopped out **in the same second** — zero seconds of market
movement. Meanwhile every trade that survived past ~9s won (+$36.00, +$32.55, +$31.28). The
old measurement also made the two exits wildly asymmetric: the stop needed **0%** of market
movement while the take-profit needed roughly **+40%** (its 30%, plus climbing back through
the spread first).

**The spread is still paid** — it is just no longer mistaken for a market move. Booked
results are therefore about one spread worse than the headline percentages on both sides:
with a 9% round trip, a `+30%` take-profit books ≈ +21% of the stake and a `−10%` stop books
≈ −19%. Set `MAX_WINDOW_LOSS_PCT` with that in mind — at those numbers a 30% window budget
is spent by roughly **1.5** stop-losses, not three.

### ⚠ The stop-loss competes with the entry cost
P/L is marked on liquidation value. You **buy by walking up the asks** and would **sell by
walking down the bids**, so a position is under water by that whole round trip — spread
*plus* the depth consumed on both sides — the instant it exists, before the market has
moved at all.

Measured on a live 15m book with a $105 stake:

| side | best bid / ask | buy VWAP | sell VWAP | instant round trip |
|---|---|---|---|---|
| Up (favoured) | 0.76 / 0.77 | 0.7745 | 0.7600 | **−1.9%** |
| Down (cheap) | 0.23 / 0.24 | 0.2400 | 0.2168 | **−9.7%** (only −4.2% of it is the spread; the rest is depth) |

That cost is charged against the *same stake the stop-loss is measured on*. A 10% stop on
the second book is already spent before the trade starts: it stops out on the next tick,
and two of those exhaust a 30% window budget in seconds. Observed live before this was
addressed — entered DOWN at 0.537 VWAP, marked at the 0.47 walked bid one second later,
−12.4%, stopped out, *with the quoted bid barely moving*.

Nothing in the bot blocks these trades — the rule is the rule. What it does do is
**report the split on every close**:

```
REVERSE: spot crossed the open - closed UP @ 0.40 (P/L $-21.57) [entry cost -2.0%, market -19.6%]
```

so a large P/L on a book whose quoted bid barely moved is always attributable. If the stop
should measure *the market going against you* rather than the cost of entering,
`STOP_LOSS_PCT` has to stay comfortably above the round trip you actually pay.

---

## 6. Sizing & execution
[`execute_trade`](main.py). One position at a time. **Paper:** debits the simulated
balance. **Live:** a slippage-capped marketable Fill-Or-Kill BUY on the CLOB
(limit = quote + `CLOB_MAX_SLIPPAGE`).

The stake is **exactly** the window's risk-per-trade — never trimmed to fit the book and
never scaled up. If the ask side cannot absorb the whole stake the trade is *skipped*
(`thin_book`).

The fill is priced by **walking the ask levels** ([`walk_asks`](bot/data.py)) for the true
blended VWAP, and a live order is quoted against the **worst level the fill has to reach**
(not the best ask, not the VWAP) — a limit derived from the top of book cannot clear the
levels the size was walked over, and the venue kills the whole FOK. Sells are quoted
against the lowest bid reached, symmetrically.

The share count comes from the venue's own fill report (`takingAmount`/`makingAmount`, or
the authenticated user WebSocket's `size_matched`), never from the pre-trade estimate: the
exit SELL must not ask for more shares than we hold, and the win payout is `shares × $1`.

---

## 7. Resolution & settlement
[`update_trades`](main.py). A position still open at expiry is resolved in priority order:

1. **`market_resolved`** pushed over the CLOB market WebSocket — the venue's own settlement.
2. **Gamma `closed` + `outcomePrices`** — REST fallback, requested at most every 15s and
   only while the pushed resolution above has not arrived.
3. **Frozen close vs strike** — only when the settlement feed is provably fresh
   (`MAX_SETTLE_PRICE_AGE_S`, 30s). Open and close are read from the **same feed**, so no
   cross-feed offset can flip a near-the-money result.
4. A **2-minute grace** before voiding (paper stake refunded), so a stuck trade can never
   block the slot across two consecutive windows.

**Live wins are redeemed** into pUSD off the trading tick ([`redeem_loop`](main.py)) —
a settled market pays out in ERC-1155 outcome tokens, and without redemption a live win
never becomes spendable balance.

---

## 7a. Dead sockets — the failure that stops everything

Every input arrives over a WebSocket, and the dangerous failure is not a socket that
*errors* (that reconnects) but one that **silently stops delivering**: the connection object
stays "open", `await ws.receive()` never returns, no exception is raised, and the reconnect
loop is never reached.

Observed: the machine slept, the Polymarket price stream hung, and the bot ran for **9.6
hours** with no price samples — so no window could ever have its strike marked and the
dashboard read *"Open (strike): — not marked (window not traded)"* forever, while otherwise
looking perfectly healthy.

Both feeds the strategy cannot run without now carry a receive-timeout watchdog: silence
for longer than the tolerance (`MAX_SETTLE_PRICE_AGE_S` for the price feed, 30s for Binance
trades) drops the socket and reconnects. `ClobBookStream` already had one; these did not.

## 7b. Transport — what is pushed, and what is left

Every input the strategy acts on arrives over a WebSocket. Nothing on the decision path is
polled.

| Input | Transport |
|---|---|
| Spot price | Binance `@trade` WS |
| Order books | CLOB market WS (snapshot + level deltas) |
| Strike / settlement price | Polymarket live-data WS (`crypto_prices_chainlink`) |
| Our fills | CLOB authenticated user WS |
| Settlement | CLOB `market_resolved`, pushed |
| Live pUSD balance | Polygon WS — `Transfer` log subscription triggers a `balanceOf` read |
| Chainlink BTC/USD | Polygon WS — `AnswerUpdated` subscription, `eth_call` for cold start |
| Dashboard | server pushes each 500ms tick over `/ws` |

**The balance is a subscription, not a timer.** pUSD is an ERC-20, so any change to the
balance emits a `Transfer` log naming our wallet. The bot subscribes to exactly those
(topic-position filtered — the unfiltered stream measured ~280 logs/second) and re-reads
`balanceOf` when one lands. The log is a *trigger*, never arithmetic: applying its
`+amount`/`-amount` would drift permanently the first time an event was missed across a
reconnect, and this number sizes every trade.

**Three REST calls remain, and all three are deliberate:**

1. **Gamma market discovery.** The venue publishes no market-lifecycle topic — probing the
   live-data socket for every plausible name returns `topic ... not found`, and CLOB topics
   there are explicitly retired. So Gamma REST is the only way to learn which window is
   trading. It is driven by the **window boundary** rather than a timer: a 15m market is
   only replaced when the current one expires, and the *next* one is prefetched ~20s early
   so the boundary tick — the one that marks the strike — never waits on a request. That is
   one request per 15 minutes instead of six per minute.
2. **Binance REST spot**, used *only* while the trade socket is stale.
3. **REST `/book`**, used *only* while the book socket has no usable snapshot.

The last two exist precisely because a socket can die. Removing them would not make the bot
more real-time; it would delete the fallback for the exact failure §7a is about.

---

## 8. Logging

[`logs/signals.csv`](logs/) records one row per tick: the strike, the effective spot, the
distance from the open, the signal side, the fill prices, and the **whole window budget**
(recorded balance, risk/trade, running P/L, wins, losses, armed) plus the exact decision and
blocking reason. If the columns change, the old file is rotated aside rather than appended
to with a mismatched header.

---

## 9. Tunable parameters

| Setting | Default | Meaning |
|---------|---------|---------|
| `CANDLE_WINDOW_MINUTES` | 15 | Market window length. |
| `POLL_INTERVAL_MS` | 500 | How fast a cross of the open is seen. |
| `RISK_TYPE` / `RISK_VALUE` | percent / 10 | Stake: % of the balance **recorded at the window open**, or flat $. |
| `MAX_WINDOW_LOSS_PCT` | 30 | Window loss cap, as % of risk/trade (30 = three 10% stop-losses). |
| `MAX_WINDOW_WIN_PCT` | 30 | Window win cap, as % of risk/trade (30 = one 30% take-profit). |
| `STOP_AFTER_WIN` | true | A **take-profit** ends the window (not an incidental small profit on a reversal). |
| `MIN_MOVE_USD` | 5 | Dead band around the open, in dollars of the underlying. 0 = the literal rule. |
| `TAKE_PROFIT_PCT` | 30 | Close at +this % of the stake. |
| `STOP_LOSS_PCT` | 10 | Close at −this % of the stake. **See the spread warning in §5.** |
| `MIN_SECONDS_LEFT` | 20 | Stop *opening* this close to expiry. |
| `MIN_BOOK_LIQUIDITY_USD` | 20 | Skip if the ask side can't absorb the full stake. |
| `MAX_SPOT_AGE_S` | 5 | Older spot registers no side at all. |
| `MAX_BOOK_AGE_S` | 15 | Distrust the book socket (fall back to REST) after this long with no frame. |
| `MAX_SETTLE_PRICE_AGE_S` | 30 | Don't settle close-vs-open on a feed older than this. |
| `AUTO_REDEEM_ENABLED` | true | Redeem winning outcome tokens into pUSD (live). |
| `CLOB_MAX_SLIPPAGE` | 0.02 | Live-order limit buffer. |

---

*This is not financial advice. The signal is the trivial persistence baseline and is fully
visible to the market — the discipline here is in the budget, not in an edge. Verify in
**paper mode** that the stop-loss is measuring market movement rather than the bid-ask
spread (§5) before risking capital. Use at your own risk; live mode trades real funds.*
