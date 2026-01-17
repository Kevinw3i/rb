# QA Testcases

1. Missing env keys
   - Unset `BINANCE_API_KEY` or `BINANCE_API_SECRET`.
   - Expect the program to exit with a clear message.
2. No open position
   - With valid keys, run with trigger/order prices.
   - Expect tick output and no REST order activity.
3. Long position with order price > trigger
   - When latest price > trigger, expect a reduce-only GTX SELL at order price.
   - When latest price <= trigger, expect cancellation of reduce-only orders.
4. Short position with order price < trigger
   - When latest price < trigger, expect a reduce-only GTX BUY at order price.
   - When latest price >= trigger, expect cancellation of reduce-only orders.
5. Output format
   - Verify tick output uses `YYYY-MM-DD HH:MM:SS` in UTC+8.
6. Logging
   - Set `RB_LOG_PATH` to a temp file.
   - Run for a short period.
   - Expect stderr event lines and the log file to contain EVENT/ERROR/TICK entries.
7. Event logs include current price
   - With an active trigger and open position, observe EVENT lines (manage_orders/place_order/cancel_orders).
   - Expect `current_price=<last_price>` in those EVENT lines.
8. Logging disabled
   - Set `RB_LOG_PATH` to a temp file and run with `--no-log`.
   - Expect stderr event lines but no new log file writes.
9. Unified account base
   - Run with `BINANCE_BASE_URL` unset.
   - Expect REST calls to target `https://papi.binance.com` endpoints.
10. Price gap warning
   - Use `--trigger 100` and `--order 115`.
   - Expect an EVENT warning about the 10%+ gap and the program continues.
