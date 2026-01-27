# PRD

## Goal
- Stream BTCUSDC USD-M futures latest price in real time.
- Print every tick with UTC+8 time and current position.
- Emit event logs to stderr (include current price for order/position actions) and optionally append EVENT logs to a log file.
- Manage a reduce-only maker take-profit order on mainnet using trigger/order prices (Unified account REST).
- Optionally manage a maker-only entry order and a reduce-only stop-loss flow.

## Inputs
- CLI: `--trigger <price>`, `--order <price>`, optional `--entry <price> --stop <price> --side <long|short> --entry-usdc <amount> [--leverage <n>] [--entry-detect <prefix|any>]`, optional `--no-log` to disable log file writes.
- Environment: `BINANCE_API_KEY`, `BINANCE_API_SECRET`, optional `BINANCE_BASE_URL` (default `https://papi.binance.com`), optional `BINANCE_EXCHANGE_BASE_URL` (default `https://fapi.binance.com`), optional `RB_LOG_PATH` (default `rb.log`), optional `RB_ENTRY_USDC` (entry notional), optional `RB_ENTRY_LEVERAGE` (default 100), optional `RB_ENTRY_DETECT` (`prefix` or `any`).
- `--entry/--stop/--side` must be provided together; entry order placement requires an entry USDC amount.

## Behavior
- If the trigger/order gap is 10% or more, print a warning and continue.
- If order price < trigger price:
  - When latest price < trigger, if a position exists, ensure a reduce-only maker order
    at the order price for 100% size on the opposite side.
  - If bot-managed reduce-only orders exist but mismatch price/size/side, cancel and replace.
  - When latest price >= trigger, if a position exists, cancel reduce-only orders.
- If order price > trigger price, invert the trigger condition and cancellation rule.
- If no position is open, do nothing.
- Trigger/order management only targets bot-managed reduce-only orders (clientOrderId `rb-tp-`).
- Open orders are cached for a short interval to avoid duplicate REST calls across trigger/entry management.
- On 429/418 responses, back off before retrying and emit rate-limit logs.
- If the take-profit or stop-loss order fills, cancel remaining managed orders and exit.
- User data stream (Portfolio Margin) listens for TP/stop fills and triggers the same exit flow.
- Optional entry/stop flow (independent of trigger/order):
  - Place a maker-only LIMIT entry order at `--entry` using `entry_usdc * leverage / entry_price`.
  - Entry quantity is floored to the symbol LOT_SIZE step from exchange info; raw vs rounded qty is logged, and placement is skipped if the rounded qty is zero.
  - When both `entry_usdc` and `leverage` are provided, the first manage cycle cancels same-price entry/stop orders (matching side) before placing fresh orders with the computed rounded quantity.
  - Entry is single-use: once a position is opened, no new entry orders are placed.
  - When the entry order is open or a position exists, ensure a reduce-only STOP_MARKET stop-loss at `--stop`.
  - Stop-loss uses UM conditional orders and is tracked via open conditional orders (strategyId/newClientStrategyId).
  - Stop-loss trigger uses the latest price (CONTRACT_PRICE), reduce-only = true, maker-only = false.
  - Stop-loss quantity uses current position size; if position is flat, it falls back to entry order quantity (or computed entry quantity).
  - If entry USDC amount is missing and no entry order exists, entry placement is skipped; stop-loss is managed only when a position exists.
  - Entry detection:
    - `prefix` (default): only orders with `clientOrderId` prefix `rb-entry-` are treated as entry orders.
    - `any`: any LIMIT order matching entry price/side is treated as an entry order (even if placed via web).

## Out of Scope
- Multi-symbol support.
