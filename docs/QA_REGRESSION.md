# QA Regression

- WebSocket reconnects after a disconnect.
- Tick output format is unchanged.
- Event logs print executed actions and are appended to the log file unless `--no-log` is set.
- Order/position EVENT logs include `current_price=<last_price>` when available.
- Startup warns on trigger/order gap >= 10% and continues.
- Reduce-only orders are placed with `GTX` and canceled on trigger exit.
- REST calls default to Unified account PAPI UM endpoints.
