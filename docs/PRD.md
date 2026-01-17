# PRD

## Goal
- Stream BTCUSDC USD-M futures latest price in real time.
- Print every tick with UTC+8 time and current position.
- Emit event logs to stderr (include current price for order/position actions) and optionally append logs (including ticks) to a log file.
- Manage a reduce-only maker take-profit order on mainnet using trigger/order prices (Unified account REST).

## Inputs
- CLI: `--trigger <price>`, `--order <price>`, optional `--no-log` to disable log file writes.
- Environment: `BINANCE_API_KEY`, `BINANCE_API_SECRET`, optional `BINANCE_BASE_URL` (default `https://papi.binance.com`), optional `RB_LOG_PATH` (default `rb.log`).

## Behavior
- If the trigger/order gap is 10% or more, print a warning and continue.
- If order price < trigger price:
  - When latest price < trigger, if a position exists, ensure a reduce-only maker order
    at the order price for 100% size on the opposite side.
  - If reduce-only orders exist but mismatch price/size/side, cancel and replace.
  - When latest price >= trigger, if a position exists, cancel reduce-only orders.
- If order price > trigger price, invert the trigger condition and cancellation rule.
- If no position is open, do nothing.

## Out of Scope
- Opening new positions.
- Multi-symbol support.
