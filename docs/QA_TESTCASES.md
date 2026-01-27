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
   - Expect stderr event lines and the log file to contain EVENT entries only.
7. Event logs include current price
   - With an active trigger and open position, observe EVENT lines (manage_orders/place_order/cancel_orders).
   - Expect `current_price=<last_price>` in those EVENT lines.
8. Entry/stop args validation
   - Run with only one of `--entry/--stop/--side`.
   - Expect a usage error requiring all three.
9. Entry order placement
   - Set `RB_ENTRY_USDC` (or `--entry-usdc`) and optionally `RB_ENTRY_LEVERAGE` (or `--leverage`), pass `--entry/--stop/--side`.
   - Expect a maker-only LIMIT entry order (GTX) at the entry price.
10. Entry qty rounding
   - Choose an `entry_usdc`/`leverage` that produces a quantity with extra decimals.
   - Expect a `entry_qty_round` EVENT and the placed quantity to match the LOT_SIZE step.
11. Entry detect any
   - Place a web LIMIT order at the entry price/side.
   - Run with `--entry/--stop/--side --entry-detect any` and no `RB_ENTRY_USDC`.
   - Expect the entry order to be detected and no new entry placement.
12. Missing entry amount when no order exists
   - Run with `--entry/--stop/--side` but without `RB_ENTRY_USDC`/`--entry-usdc`.
   - Expect an EVENT warning about missing entry amount and no entry order placement.
13. Stop-loss placement and persistence
   - After entry fills (or when a position exists), expect a reduce-only STOP_MARKET at the stop price.
   - If entry order is open while flat, expect stop-loss quantity to match the entry order qty or computed entry qty.
   - Ensure stop-loss is not canceled by the trigger/order flow.
   - Verify the stop-loss appears in UM conditional open orders and does not duplicate on re-manage ticks.
14. Logging disabled
   - Set `RB_LOG_PATH` to a temp file and run with `--no-log`.
   - Expect stderr event lines but no new log file writes.
15. Rate limit backoff
   - Simulate a 429/418 response (proxy or mock server).
   - Expect a `rate_limit_backoff` EVENT and the client waits before retrying.
16. Exit on take-profit fill
   - Let the TP order fill.
   - Expect remaining managed orders to be canceled and an `event=exit` followed by process exit.
17. Exit on stop-loss fill
   - Let the stop-loss order fill.
   - Expect remaining managed orders to be canceled and an `event=exit` followed by process exit.
18. User data stream exit
   - Trigger a TP or stop fill and observe a user data stream event.
   - Expect the same exit flow as the REST-based check.
19. Unified account base
   - Run with `BINANCE_BASE_URL` unset.
   - Expect REST calls to target `https://papi.binance.com` endpoints.
20. Price gap warning
   - Use `--trigger 100` and `--order 115`.
   - Expect an EVENT warning about the 10%+ gap and the program continues.
21. Trigger flip with cached open orders
   - Use a small trigger gap so price oscillates around the threshold within 1 second.
   - Expect reduce-only TP orders to be placed/canceled correctly without stale orders remaining.
22. Startup cleanup with same-price orders
   - Create existing entry/stop orders at the same entry/stop prices (any quantity).
   - Run with both `--entry-usdc` and `--leverage` provided.
   - Expect an `entry_startup_cleanup` EVENT, cancellations of same-price entry/stop orders, and fresh orders sized from the new rounded entry quantity.
23. Entry single-use
   - Let the entry order fill once and the position open.
   - Expect no new entry orders to be placed while the bot continues running.
