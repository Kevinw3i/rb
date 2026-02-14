# API

## WebSocket
- Futures price stream: `wss://fstream.binance.com/ws/<symbol>@aggTrade`
- Spot price stream: `wss://stream.binance.com:9443/ws/<symbol>@aggTrade`
- Futures user data stream: `wss://fstream.binance.com/pm/ws/<listenKey>`
- Spot user data stream: `wss://stream.binance.com:9443/ws/<listenKey>`

## REST (signed)
- Futures base: `https://papi.binance.com` (Unified account).
  - `GET /papi/v1/um/positionRisk?symbol=<symbol>`
  - `GET /papi/v1/um/openOrders?symbol=<symbol>`
  - `GET /papi/v1/um/conditional/openOrders?symbol=<symbol>`
  - `GET /papi/v1/um/order?symbol=<symbol>&origClientOrderId=...`
  - `GET /papi/v1/um/conditional/orderHistory?symbol=<symbol>&newClientStrategyId=...`
  - `POST /papi/v1/um/order` (LIMIT + GTX + reduceOnly=true, take-profit)
  - `POST /papi/v1/um/order` (LIMIT + GTX, entry)
  - `POST /papi/v1/um/conditional/order` (STOP_MARKET + stopPrice + reduceOnly=true + workingType=CONTRACT_PRICE, stop-loss)
  - `DELETE /papi/v1/um/order?symbol=<symbol>&orderId=...`
  - `DELETE /papi/v1/um/conditional/order?symbol=<symbol>&strategyId=...`
  - `POST /papi/v1/listenKey` (start user data stream)
  - `PUT /papi/v1/listenKey` (keepalive)
  - `DELETE /papi/v1/listenKey` (close)
- Spot base: `https://api.binance.com`.
  - `GET /api/v3/account` (base-asset balance for position)
  - `GET /api/v3/openOrders?symbol=<symbol>`
  - `GET /api/v3/order?symbol=<symbol>&origClientOrderId=...`
  - `POST /api/v3/order` (LIMIT_MAKER take-profit)
  - `DELETE /api/v3/order?symbol=<symbol>&orderId=...`
  - `POST /api/v3/userDataStream` (start user data stream)
  - `PUT /api/v3/userDataStream` (keepalive)
  - `DELETE /api/v3/userDataStream` (close)
- clientOrderId prefixes: `rb-tp-` (take-profit), `rb-entry-` (entry, futures), `rb-stop-` (stop-loss, futures `newClientStrategyId`).
- Entry order quantity is derived from entry USDC amount and leverage (default 100), then floored to the LOT_SIZE step (futures).
- When both entry USDC and leverage are provided, the first entry-manage pass cancels same-price entry orders and same-stop stop-loss orders before placing refreshed orders (futures).

## REST (public)
- Futures exchange info: `GET /fapi/v1/exchangeInfo?symbol=<symbol>` (LOT_SIZE step size).
- Spot exchange info: `GET /api/v3/exchangeInfo?symbol=<symbol>` (LOT_SIZE step size).

## Authentication
- `X-MBX-APIKEY` header.
- HMAC SHA256 signature over the query string.
- `timestamp` + `recvWindow` included on each signed request.

## Logging
- Events and errors are printed to stderr.
- Log file appends EVENT lines at `RB_LOG_PATH` (default `rb.log`) unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
- Rate-limit headers are logged periodically when provided by the API.
- 429/418 responses trigger a backoff before retry.
- Stop-loss/take-profit fills trigger an exit after canceling remaining managed orders.
- If `--entry-abort` is set and price reaches that level before entry fills, entry/stop orders are canceled and the task exits.
- If an entry order already exists at startup (per entry detection), `--entry-abort` is ignored for this run.
- If `--entry-arm` is set, entry/stop orders are armed only after price reaches the arm threshold.
- For `long`, `entry-arm` must be above `entry` and arm is triggered when price touches or goes below `entry-arm`.
- For `short`, `entry-arm` must be below `entry` and arm is triggered when price touches or goes above `entry-arm`.
- Before `--entry-arm` is touched, detected entry orders are canceled; stop orders are canceled only when no position exists.
- If a position exists, stop-loss orders are preserved and synchronized without forced cancel/recreate on each manage cycle.

## Order Detection
- Entry detection mode `prefix` (default) only treats `rb-entry-` orders as entry orders.
- Entry detection mode `any` treats matching LIMIT orders (price/side) as entry orders.

## Telegram (optional)
- Base: `TG_API_BASE_URL` (default `https://api.telegram.org`).
- SendMessage: `POST /bot<TELEGRAM_BOT_TOKEN>/sendMessage`.
  - JSON: `chat_id` (from `TELEGRAM_CHAT_ID`), `text`, optional `message_thread_id` (from `TELEGRAM_MESSAGE_THREAD_ID`).
- Requests set `parse_mode=HTML` and render a bold title line (best-effort).
- Enabled only when both `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set.
- Best-effort: failures/timeouts do not affect order processing.
- Duplicate `ERROR` alerts are deduplicated for 60 seconds per `market + symbol + message` key.
- On shutdown, sender handles are dropped before worker join; buffered alerts are drained without extra rate-limit sleep, and join timeout uses at least `TG_ALERT_TIMEOUT_SECS + 1s`.
