# QA Testcases

1. Missing env keys
   - Unset `BINANCE_API_KEY` or `BINANCE_API_SECRET`.
   - Expect the program to exit with a clear message.
2. Missing symbol/market
   - Omit `--symbol` or `--market`.
   - Expect a usage error requiring both.
3. Invalid symbol for market
   - Use `--symbol BAD/PAIR --market futures` or a valid futures symbol with `--market spot` if not listed.
   - Expect a warning about invalid symbol and the program exits without running.
4. No open position
   - With valid keys, run with trigger/order prices.
   - Expect tick output and no REST order activity.
5. Spot entry/stop not allowed
   - Run with `--market spot` and any `--entry/--stop/--side` options.
   - Expect a usage error stating entry/stop is unsupported for spot.
6. Spot TP order type
   - With a spot base-asset balance, run `--market spot`.
   - Expect a `LIMIT_MAKER` sell order at the order price (clientOrderId `rb-tp-`).
7. Long position with order price > trigger
   - When latest price > trigger, expect a reduce-only GTX SELL at order price.
   - When latest price <= trigger, expect cancellation of reduce-only orders.
8. Short position with order price < trigger
   - When latest price < trigger, expect a reduce-only GTX BUY at order price.
   - When latest price >= trigger, expect cancellation of reduce-only orders.
9. Output format
   - Verify tick output uses `YYYY-MM-DD HH:MM:SS` in UTC+8.
10. Logging
   - Set `RB_LOG_PATH` to a temp file.
   - Run for a short period.
   - Expect stderr event lines and the log file to contain EVENT entries only.
11. Event logs include current price
   - With an active trigger and open position, observe EVENT lines (manage_orders/place_order/cancel_orders).
   - Expect `current_price=<last_price>` in those EVENT lines.
12. Entry/stop args validation
   - Run with only one of `--entry/--stop/--side`.
   - Expect a usage error requiring all three.
13. Entry order placement
   - Set `RB_ENTRY_USDC` (or `--entry-usdc`) and optionally `RB_ENTRY_LEVERAGE` (or `--leverage`), pass `--entry/--stop/--side`.
   - Expect a maker-only LIMIT entry order (GTX) at the entry price.
14. Entry qty rounding
   - Choose an `entry_usdc`/`leverage` that produces a quantity with extra decimals.
   - Expect a `entry_qty_round` EVENT and the placed quantity to match the LOT_SIZE step.
15. Entry detect any
   - Place a web LIMIT order at the entry price/side.
   - Run with `--entry/--stop/--side --entry-detect any` and no `RB_ENTRY_USDC`.
   - Expect the entry order to be detected and no new entry placement.
16. Missing entry amount when no order exists
   - Run with `--entry/--stop/--side` but without `RB_ENTRY_USDC`/`--entry-usdc`.
   - Expect an EVENT warning about missing entry amount and no entry order placement.
17. Stop-loss placement and persistence
   - After entry fills (or when a position exists), expect a reduce-only STOP_MARKET at the stop price.
   - If entry order is open while flat, expect stop-loss quantity to match the entry order qty or computed entry qty.
   - Ensure stop-loss is not canceled by the trigger/order flow.
   - Verify the stop-loss appears in UM conditional open orders and does not duplicate on re-manage ticks.
18. Logging disabled
   - Set `RB_LOG_PATH` to a temp file and run with `--no-log`.
   - Expect stderr event lines but no new log file writes.
19. Rate limit backoff
   - Simulate a 429/418 response (proxy or mock server).
   - Expect a `rate_limit_backoff` EVENT and the client waits before retrying.
20. Exit on take-profit fill
   - Let the TP order fill.
   - Expect remaining managed orders to be canceled and an `event=exit` followed by process exit.
21. Exit on stop-loss fill
   - Let the stop-loss order fill.
   - Expect remaining managed orders to be canceled and an `event=exit` followed by process exit.
22. User data stream exit
   - Trigger a TP or stop fill and observe a user data stream event.
   - Expect the same exit flow as the REST-based check.
23. Default base URLs
   - Run with `BINANCE_BASE_URL` unset.
   - Expect futures to use `https://papi.binance.com` and spot to use `https://api.binance.com`.
24. Price gap warning
   - Use `--trigger 100` and `--order 115`.
   - Expect an EVENT warning about the 10%+ gap and the program continues.
25. Trigger flip with cached open orders
   - Use a small trigger gap so price oscillates around the threshold within 1 second.
   - Expect reduce-only TP orders to be placed/canceled correctly without stale orders remaining.
26. Startup cleanup with same-price orders
   - Create existing entry/stop orders at the same entry/stop prices (any quantity).
   - Run with both `--entry-usdc` and `--leverage` provided.
   - Expect an `entry_startup_cleanup` EVENT, cancellations of same-price entry/stop orders, and fresh orders sized from the new rounded entry quantity.
27. Entry single-use
   - Let the entry order fill once and the position open.
   - Expect no new entry orders to be placed while the bot continues running.
28. Entry abort touched before fill
   - Configure entry/stop and set `--entry-abort` beyond the entry price.
   - Before the entry fills, let price reach the abort price.
   - Expect entry orders and stop-loss orders to be canceled and the task to exit.
29. Entry abort ignored with existing entry order
   - Place an entry LIMIT order that matches detection mode before starting the bot.
   - Run with `--entry-abort`.
   - Expect `entry_abort` to be ignored (no abort on price touch).

30. Telegram alerting disabled by default
   - Ensure `TELEGRAM_BOT_TOKEN` and/or `TELEGRAM_CHAT_ID` are unset.
   - Expect `event=tg_alerting enabled=false reason=missing_env` in stderr.

31. Telegram alert on ERROR
   - Set `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` (and optionally `TELEGRAM_MESSAGE_THREAD_ID`).
   - Trigger an error (e.g. unset `BINANCE_API_KEY`).
   - Expect a Telegram message with `kind=ERROR` and the original error line.

32. Telegram failures do not affect order processing
   - Set `TELEGRAM_BOT_TOKEN` to an invalid value (or block Telegram access) while keeping Binance keys valid.
   - Expect stderr to show Telegram send errors while the bot continues to place/cancel orders normally.

33. Telegram alert on ENTRY FILLED
   - Enable Telegram alerts (`TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`).
   - Run futures entry flow so a position opens (entry fills).
   - Expect a Telegram message titled `ENTRY FILLED` including `kind=entry_filled`, `side=LONG|SHORT`, `qty=...`, and (best-effort) `client_id=...`.

34. Telegram alert on TAKE PROFIT FILLED
   - Enable Telegram alerts.
   - Let the take-profit order fill and the bot exit.
   - Expect a Telegram message titled `TAKE PROFIT FILLED` including `kind=tp_filled` (or `kind=order_filled` in spot), plus (best-effort) `side/qty/price/client_id`.

35. Telegram alert on STOP FILLED
   - Enable Telegram alerts.
   - Let the stop-loss order fill and the bot exit.
   - Expect a Telegram message titled `STOP FILLED` including `kind=stop_filled`, plus (best-effort) `side/qty/price/client_id`.

36. Telegram alert on ENTRY ABORT TRIGGERED
   - Enable Telegram alerts.
   - Configure `--entry-abort` and let price touch the abort threshold before entry fills.
   - Expect a Telegram message titled `ENTRY ABORT TRIGGERED` including `kind=entry_abort_touched` and `abort_price=...`.
