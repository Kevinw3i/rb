# UX/UI

## CLI Output
- Stdout format: `BTCUSDC / <last_price> / <YYYY-MM-DD HH:MM:SS> / <position>`
- Timezone: UTC+8.
- Position text: `LONG <qty>`, `SHORT <qty>`, or `FLAT`.

## Entry Options
- Optional entry/stop inputs: `--entry <price> --stop <price> --side <long|short> --entry-usdc <amount> [--leverage <n>] [--entry-detect <prefix|any>]`.
- Entry order size is derived from USDC amount and leverage (default 100), then floored to the LOT_SIZE step from exchange info.
- Entry is single-use: once a position is opened, no new entry orders are placed.
- Entry detection default is `prefix`; use `any` to detect web-placed LIMIT orders matching entry price/side.
- Stop-loss orders are tracked as UM conditional orders (strategyId/newClientStrategyId).

## Logging
- Stderr events use `[YYYY-MM-DD HH:MM:SS] EVENT ...` and errors use `ERROR`.
- Log file: `RB_LOG_PATH` (default `rb.log`), appends EVENT lines in UTC+8 unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
- Startup logs a warning event if trigger/order gap is >= 10%.
- Rate-limit events include `rate_limit_status` and `rate_limit_backoff`.
- Entry quantity rounding logs `entry_qty_round` (raw vs rounded) or `entry_qty_round_skip` if the rounded qty is zero.
- Exit events include `event=exit` and `event=exit_done`.
- User data stream events include `user_stream_start`, `user_stream_connected`, `user_stream_keepalive`, `user_stream_expired`.
- REST calls default to Unified account (`https://papi.binance.com`).
