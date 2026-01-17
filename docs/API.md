# API

## WebSocket
- `wss://fstream.binance.com/ws/btcusdc@aggTrade`

## REST (signed)
- Base: `https://papi.binance.com` (Unified account).
- `GET /papi/v1/um/positionRisk?symbol=BTCUSDC`
- `GET /papi/v1/um/openOrders?symbol=BTCUSDC`
- `POST /papi/v1/um/order` (LIMIT + GTX + reduceOnly=true)
- `DELETE /papi/v1/um/order?symbol=BTCUSDC&orderId=...`

## Authentication
- `X-MBX-APIKEY` header.
- HMAC SHA256 signature over the query string.
- `timestamp` + `recvWindow` included on each signed request.

## Logging
- Events and errors are printed to stderr.
- Log file appends EVENT/ERROR/TICK lines at `RB_LOG_PATH` (default `rb.log`) unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
