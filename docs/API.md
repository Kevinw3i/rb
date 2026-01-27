# API

## WebSocket
- `wss://fstream.binance.com/ws/btcusdc@aggTrade`
- User data stream: `wss://fstream.binance.com/pm/ws/<listenKey>`

## REST (signed)
- Base: `https://papi.binance.com` (Unified account).
- `GET /papi/v1/um/positionRisk?symbol=BTCUSDC`
- `GET /papi/v1/um/openOrders?symbol=BTCUSDC`
- `GET /papi/v1/um/conditional/openOrders?symbol=BTCUSDC`
- `GET /papi/v1/um/order?symbol=BTCUSDC&origClientOrderId=...`
- `GET /papi/v1/um/conditional/orderHistory?symbol=BTCUSDC&newClientStrategyId=...`
- `POST /papi/v1/um/order` (LIMIT + GTX + reduceOnly=true, take-profit)
- `POST /papi/v1/um/order` (LIMIT + GTX, entry)
- `POST /papi/v1/um/conditional/order` (STOP_MARKET + stopPrice + reduceOnly=true + workingType=CONTRACT_PRICE, stop-loss)
- `DELETE /papi/v1/um/order?symbol=BTCUSDC&orderId=...`
- `DELETE /papi/v1/um/conditional/order?symbol=BTCUSDC&strategyId=...`
- `POST /papi/v1/listenKey` (start user data stream)
- `PUT /papi/v1/listenKey` (keepalive)
- `DELETE /papi/v1/listenKey` (close)
- clientOrderId prefixes: `rb-tp-` (take-profit), `rb-entry-` (entry), `rb-stop-` (stop-loss, `newClientStrategyId`).
- Entry order quantity is derived from entry USDC amount and leverage (default 100), then floored to the LOT_SIZE step.
- When both entry USDC and leverage are provided, the first entry-manage pass cancels same-price entry orders and same-stop stop-loss orders before placing refreshed orders.

## REST (public)
- Base: `https://fapi.binance.com` (exchange info for filters).
- `GET /fapi/v1/exchangeInfo?symbol=BTCUSDC` (LOT_SIZE step size used for quantity rounding).

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

## Order Detection
- Entry detection mode `prefix` (default) only treats `rb-entry-` orders as entry orders.
- Entry detection mode `any` treats matching LIMIT orders (price/side) as entry orders.
