# Fixed Notional Ceiling Contract

This document defines the pure helper contract for fixed-notional-ceiling
semantics. It is a docs/schema/tests/helper contract only and is not wired into
runtime order flow or backtest sizing flow.

## Definition

A fixed-notional ceiling is the maximum allowed order or position notional after
risk sizing has already produced a notional cap.

It is not:

- wallet balance
- initial PAPER or LIVE size
- guaranteed order notional
- guaranteed liquidity
- profit target
- position recommendation

The ceiling caps a notional after risk sizing. It must not replace risk sizing
or create a positive notional when the percentage risk cap is zero or invalid.

## Helper Inputs

The pure helper accepts explicit values:

- `equity_quote`
- `cap_pct`
- `fixed_ceiling_enabled`
- `global_fixed_notional_ceiling`
- `per_symbol_fixed_notional_ceiling`
- `symbol`

It does not read environment variables, config modules, files, network state,
exchange state, balances, orders, private API data, or runtime state stores.

## Effective Maximum Notional

When `fixed_ceiling_enabled` is false, the helper returns:

```text
equity_quote * cap_pct
```

if both values are positive and finite. Otherwise it returns `0.0`.

When `fixed_ceiling_enabled` is true, the helper first computes the same
percentage risk cap. If that cap is not positive and finite, it returns `0.0`.
Otherwise it returns the minimum of:

- the percentage risk cap
- the matching per-symbol ceiling, when positive and finite
- the global ceiling, when positive and finite

Invalid, non-positive, and non-finite fixed ceilings are ignored.

## Symbol Alias Contract

The helper may match simple deterministic symbol aliases. For example,
`ETH/USDT` and `ETHUSDT` are treated as aliases for per-symbol ceiling lookup.
Alias matching is only for explicit per-symbol ceiling keys supplied to the
helper.

## Blocked Runtime And Backtest Wiring

This contract intentionally does not modify or import `runner.py`,
`backtest.py`, or `strategy.py`.

Runtime and backtest wiring remains blocked because connecting the helper to
order sizing would change PAPER/LIVE behavior, order notional behavior, or
backtest sizing behavior. That requires a separate owner approval gate and a
separate implementation task.

Future approval is required before any runtime import, runtime dispatch,
backtest sizing connection, PAPER/LIVE sizing change, order path change,
private API access, balance fetch, deploy, or production mutation.
