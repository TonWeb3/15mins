# Polymarket BTC 15m Assistant (Python FastAPI)

A real-time trading assistant for Polymarket **"Bitcoin Up or Down" 15-minute** markets
(series `10192`, `btc-up-or-down-15m`), ported to Python and FastAPI.

It trades the **side of the window's open**, under a hard **per-window risk budget**:

> Price above the open → hold **UP**. Below → hold **DOWN**. Holding the wrong side → sell
> it and take the other. Exits are take-profit / stop-loss only, and after a stop-loss that
> direction is blocked until price is on the other side of the open.

Every 15-minute window records the balance it is sized against, and may take only a fixed
amount of loss (default: three stop-losses) or a single win before it stops trading until
the next window. See [`strategy.md`](strategy.md) for the full rationale.

## Features

- Real-time Web Dashboard (FastAPI + Jinja2 + Alpine.js)
- Above/below-the-open entry (a level, not a crossing event) with close-and-reverse
- Per-window risk manager: recorded balance, fixed stake, max loss / max win, stop-after-win
- Take-profit / stop-loss on the live liquidation value of the position
- Trade Execution: Paper Trading simulation vs Live Mode toggle
- Data Sources: Binance, Polymarket (Gamma/CLOB), Chainlink (WebSocket + RPC)
- Proxy Support: Global HTTP/HTTPS/SOCKS proxy configuration

## Requirements

- Python **3.11+**
- pip (comes with Python)

## Local Run

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure `config.json`

Set your trading mode, risk preferences, and optional private key in `config.json`.

**Position size** comes from `trading.risk_type` (`"percent"` = `risk_value`% of the
balance **recorded when the window opened**, or `"fixed"` = `risk_value` dollars) and
`trading.risk_value`.

**The per-window budget** — the heart of this build. Both caps are a percentage *of that
window's risk-per-trade*, so with the defaults below they mean "three stop-losses or one
take-profit, then stop until the next 15-minute window":

```jsonc
"window_risk": {
  "max_loss_pct": 30,     // 30% of risk/trade = three 10% stop-losses
  "max_win_pct": 30,      // 30% of risk/trade = one 30% take-profit
  "stop_after_win": true  // a take-profit ends the window
},
"tp_sl": {
  "enabled": true,
  "take_profit_pct": 30,  // close at +30% of the stake
  "stop_loss_pct": 10     // close at −10% of the stake
}
```

**The entry rule** is tuned in the `entry` block:

```jsonc
"entry": {
  "min_move_usd": 5,              // dead band: price must be >$5 from the open to count
  "min_book_liquidity_usd": 20.0, // skip if the ask side can't absorb the stake
  "min_seconds_left": 20          // stop OPENING this close to expiry (FOK fill risk)
}
```

`min_move_usd` exists because with no band the bot reversed the position **twice in one
second** on $3 of BTC movement, paying a full round trip (~7% of stake) each time. Inside
the band there is no signal and a held position is kept. 0 = the literal rule.

A Fill-Or-Kill order into a book seconds from resolving is not a fill you can count on, and
a take-profit needs room to happen, so new *entries* stop inside `min_seconds_left`. Exits,
stop-losses and reversals still run.

All of these are also editable live on the **Settings** page.

> **Before going live, read [`strategy.md`](strategy.md) §5.** You buy by walking *up* the
> asks and sell by walking *down* the bids, so a position starts under water by that round
> trip — measured live at −1.9% on a tight book and −9.7% on a 24¢ side. That is charged
> against the same stake the stop-loss is measured on, so a 10% stop can fire seconds after
> entry *with the quoted bid barely moving*, and two of those exhaust a 30% window budget.
> Every close logs the split (`[entry cost −2.0%, market −19.6%]`) so a large P/L is always
> attributable — check a paper session before deciding where the stop belongs.

### 3) Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Access the dashboard at `http://localhost:8000`.

(Running `python main.py` directly binds **8080** instead.)

## Docker

```bash
docker build -t polymarket-assistant .
docker run -p 8000:8000 polymarket-assistant
```

## Deployment on Render

If you are seeing errors related to Node.js or `npm run start`, it is because Render is auto-detecting the old environment. **You must manually set the runtime to Python.**

### Manual Setup
1. Create a **Web Service** on Render.
2. Under **Runtime**, explicitly select **Python 3**.
3. Set the following commands:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port 8000`
4. Add any necessary environment variables (optional).

## Live Trading

Switching **Mode** to `live` (config or the Settings page) makes the bot place real
**Fill-Or-Kill market BUY** orders on the Polymarket CLOB via `polymarket-apis` (CLOB V2).

Before enabling live mode:

1. Set a **private key or 12/24-word seed** (Settings → Credentials, or `config.json`).
   It only *signs* — it holds no funds and pays no gas.
2. Set a **Relayer API key** (polymarket.com → Settings → Relayer API keys). It sponsors
   the one-time on-chain setup (deposit-wallet deploy + token approvals), gaslessly.
3. **Deposit pUSD** on polymarket.com. The funds live in the deposit wallet derived from
   your key (CLOB V2, signature type 3); legacy proxy/safe wallets are auto-detected.
4. Click **Test Connection** on the Settings page — it derives the wallet and reports which
   one holds pUSD. Approvals are then set automatically before the first live order.

Orders are placed as **slippage-capped marketable Fill-Or-Kill** orders. The limit price is
the **worst book level the order actually has to reach** plus a small buffer
(`CLOB_MAX_SLIPPAGE`, default 2¢) — not the best ask, which could not clear a fill that was
sized by walking the book. If the book moves away beyond the buffer the order is killed
rather than filled at a bad price.

The share count is taken from the venue's fill report (the order response, or the
authenticated user WebSocket), never assumed from the pre-trade estimate.

In live mode the dashboard balance reflects the real on-chain pUSD balance, updated from
chain **events** — the bot subscribes to the pUSD `Transfer` logs naming its own wallet and
re-reads the balance when one lands, so it moves the moment a fill, payout or withdrawal
settles rather than on a timer. **Winning positions are automatically redeemed** into pUSD — a settled market
pays out in outcome tokens, and without redemption a win would never become spendable
balance. Order failures are reported in the Console Log.

## Safety

This is not financial advice. Use at your own risk; live mode trades real funds.

**The edge is unproven.** The signal is the trivial persistence baseline — "price is
already above the open" — and it is fully visible to, and priced by, the market. Nothing
here claims an information edge. What the build actually enforces is **risk**: a fixed
stake per window, a hard loss budget, a hard win stop, and a direction lock after a
stop-loss. Run it in **paper mode** for a good number of windows and read the P/L split in
the log before risking capital.

**Every trade pays a round trip.** You buy by walking up the asks and sell by walking down
the bids, so a position is under water the instant it fills — measured at −1.9% on a tight
book and −9.7% on a thin one. A `+30%` take-profit therefore books roughly +21% and a
`−10%` stop books roughly −19%. Size the window budget with that in mind.

**The 15m market settles on a TWAP.** Its resolution source is the Chainlink BTC/USD
**60-second TWAP** stream, while the venue's live feed only publishes the spot Chainlink
price — so the strike this bot marks is a close proxy, not the exact settlement number. See
[`strategy.md`](strategy.md) §2.
