# Error Summary & Audit Report

This document summarizes all errors and issues identified during the audit of the **Polymarket BTC 15m Assistant** repository, along with detailed explanations of how each issue was resolved.

---

## 1. Binance WebSocket Feed Error (HTTP 451)

### Description
When connecting to Binance WebSocket streams (`wss://stream.binance.com:9443/ws/btcusdt@trade` and `wss://stream.binance.com:9443/ws/btcusdt@kline_1m`), the server rejected the connection with **HTTP Status 451 (Unavailable For Legal Reasons)** due to geographic restrictions.

### Root Cause
Binance blocks direct access to `stream.binance.com` from certain cloud server regions / IPs.

### Solution & Fix
Updated `BinanceTradeStream` and `BinanceKlineStream` in `bot/ws_data.py` to use Binance's official public data stream endpoint (`wss://data-stream.binance.vision/ws/...`) as the primary connection endpoint, while maintaining automatic fallback to `stream.binance.com:9443`.

---

## 2. Polymarket Odds & Orderbook WebSocket Stream

### Description
The bot previously relied on REST API polling (`data.fetch_clob_price` and `data.fetch_order_book`) for orderbook depth and Polymarket odds. The existing `PolymarketChainlinkStream` attempted to connect to `wss://ws-live-data.polymarket.com` with generic topics (`crypto_prices_chainlink`), which was returning invalid request body messages for market orderbooks.

### Root Cause
Polymarket CLOB V2 serves real-time market data (orderbook snapshots, price changes, best bids/asks) on a dedicated CLOB WebSocket endpoint (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) using the token asset IDs payload: `{"assets_ids": [...], "type": "market"}`.

### Solution & Fix
Implemented `PolymarketClobMarketStream` in `bot/ws_data.py`:
- Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Dynamically subscribes to the active market's UP and DOWN CLOB token IDs (`assets_ids`).
- Processes orderbook snapshots (`book`) and real-time level changes (`price_changes`).
- Integrated into `main.py` (`lifespan` and `fetch_polymarket_snapshot`) so real-time WebSocket odds and orderbook depth update live, with graceful REST fallback if WebSocket connection drops.

---

## 3. Paper Trading Mode Audit & Parity Verification

### Description
Verified that paper trading mode behaves identically to live trading mode without sending on-chain / CLOB transactions.

### Key Audit Findings & Parity Checks
1. **Risk Management & Position Sizing**:
   - `risk_type` ("percent" or "fixed") correctly calculates the dollar risk amount based on balance recorded at the start of the window.
   - Liquidity check (`MIN_BOOK_LIQUIDITY_USD`) enforces that order size does not outsize available ask depth.
2. **Order Execution & Pricing**:
   - Paper trades record entry prices and share counts based on actual orderbook ask prices.
   - Deducts stake from `state["paper_balance"]` and records trade in `state["active_trades"]`.
3. **Position Reversals & Exits**:
   - `maybe_flip_position` bails out paper trades on opposite signals and credits proceeds to `state["paper_balance"]`.
4. **Settlement & History**:
   - `update_trades` resolves trades at expiry against Chainlink / Polymarket resolution.
   - Wins payout `shares * $1.00`, losses record `-amount`, and voids refund the stake.
   - History and balance are persisted in `state_data.json` and `config.json`.

---

## 4. Endpoint Verification & Test Suite

### Description
Created an automated test suite using `pytest` and `fastapi.testclient.TestClient` to verify all FastAPI endpoints.

### File Added
- `tests/test_endpoints.py`

### Verified Endpoints
- `/health`: Health status, running state, trading mode.
- `/api/latest`: Real-time state payload (`prices`, `indicators`, `analysis`, `trading_state`).
- `/api/logs`: Diagnostic logs list.
- `/api/settings` (GET & POST): Configuration loading, masking of keys, and updating.
- `/api/start` & `/api/stop`: Toggling active trading gate.
- `/history`: Historical trade log.
- `/api/files`: File metadata listing.
- `/api/test-connection`: Wallet derivation diagnostic endpoint.

All 7 test modules passed with 100% success rate.
