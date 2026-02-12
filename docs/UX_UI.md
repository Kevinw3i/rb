# UX/UI

## CLI Output
- Stdout format: `<SYMBOL> / <last_price> / <YYYY-MM-DD HH:MM:SS> / <position>`
- Timezone: UTC+8.
- Position text: `LONG <qty>`, `SHORT <qty>`, or `FLAT`.

## Startup Requirements
- Must provide `--symbol <pair>` and `--market <futures|spot>`.
- Startup validates the symbol exists for the selected market; invalid symbols exit with a warning.

## Entry Options
- Optional entry/stop inputs (futures only): `--entry <price> --stop <price> --side <long|short> --entry-usdc <amount> [--leverage <n>] [--entry-detect <prefix|any>] [--entry-arm <price>] [--entry-abort <price>]`.
- Entry order size is derived from USDC amount and leverage (default 100), then floored to the LOT_SIZE step from exchange info.
- When both entry USDC and leverage are provided, startup cancels same-price entry/stop orders before re-placing them with the computed rounded quantity.
- Entry is single-use: once a position is opened, no new entry orders are placed.
- If `--entry-arm` is set, entry/stop management starts only after arm is touched (above if arm >= entry, below if arm < entry).
- Before arm is touched, the bot cancels detected entry orders; stop-loss orders are canceled only when no position exists.
- If a position exists before arm is touched, stop-loss protection is preserved and synchronized (no forced stop churn).
- If `--entry-abort` is set and the market price reaches that level before entry fills, the bot cancels open entry and stop-loss orders and exits.
- If an entry order already exists at startup (per entry detection), `--entry-abort` is ignored for this run.
- Entry detection default is `prefix`; use `any` to detect web-placed LIMIT orders matching entry price/side.
- Stop-loss orders are tracked as UM conditional orders (strategyId/newClientStrategyId).

## Logging
- Stderr events use `[YYYY-MM-DD HH:MM:SS] EVENT ...` and errors use `ERROR`.
- Log file: `RB_LOG_PATH` (default `rb.log`), appends EVENT lines in UTC+8 unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
- Optional Telegram alerts: set `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` to receive best-effort async notifications (non-blocking; bounded queue may drop when full).
  - Alerts are sent for: `ERROR`, `ENTRY FILLED`, `TAKE PROFIT FILLED`, `STOP FILLED`, `ENTRY ABORT TRIGGERED`.
  - Duplicate `ERROR` alerts are deduplicated for 60 seconds per `market + symbol + message` key.
  - Format: a header line (`rb <market> <symbol>`), then a bold title, then `key=value` lines (rendered via Telegram HTML parse mode).
  - On process shutdown, queued Telegram alerts are drained in fast-flush mode (no extra throttle sleep), and worker wait time is at least request timeout + 1s.
- Startup logs a warning event if trigger/order gap is >= 10%.
- Rate-limit events include `rate_limit_status` and `rate_limit_backoff`.
- Entry quantity rounding logs `entry_qty_round` (raw vs rounded) or `entry_qty_round_skip` if the rounded qty is zero.
- Startup entry cleanup logs `entry_startup_cleanup` with counts when same-price orders are canceled.
- Exit events include `event=exit` and `event=exit_done`.
- User data stream events include `user_stream_start`, `user_stream_connected`, `user_stream_keepalive`, `user_stream_expired`.
- REST calls default to the selected market's base URL (PAPI UM for futures, spot API for spot).
