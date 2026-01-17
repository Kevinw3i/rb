# TDD Checklist

- [ ] Reject missing/invalid `--trigger` or `--order` arguments.
- [ ] Trigger logic flips when order price is above/below trigger price.
- [ ] Reduce-only maker order uses `GTX` and opposite side of the position.
- [ ] Cancel reduce-only orders when trigger condition is inactive.
- [ ] Tick output format uses UTC+8 time.
- [ ] Event logs are printed for connect/reconnect, order placement, and cancellation.
- [ ] Log file appends EVENT/ERROR/TICK lines at `RB_LOG_PATH` (default `rb.log`) unless `--no-log` is set.
- [ ] Order/position EVENT logs include `current_price=<last_price>` when available.
- [ ] Warn on startup when trigger/order gap is >= 10%.
- [ ] REST requests use Unified account PAPI UM endpoints by default.
