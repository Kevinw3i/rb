# UX/UI

## CLI Output
- Stdout format: `BTCUSDC / <last_price> / <YYYY-MM-DD HH:MM:SS> / <position>`
- Timezone: UTC+8.
- Position text: `LONG <qty>`, `SHORT <qty>`, or `FLAT`.

## Logging
- Stderr events use `[YYYY-MM-DD HH:MM:SS] EVENT ...` and errors use `ERROR`.
- Log file: `RB_LOG_PATH` (default `rb.log`), appends EVENT/ERROR/TICK lines in UTC+8 unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
- Startup logs a warning event if trigger/order gap is >= 10%.
- REST calls default to Unified account (`https://papi.binance.com`).
