# Free precomputed signal tape foundation

This document describes the LoneWolf Fang Free Phase 1 foundation for reading
precomputed signal tapes. Phase 1 is intentionally limited to schema,
validation, reader helpers, and fast research accounting.

## Scope

- Product name: `free`
- Default root: `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`
- Environment override: `LWF_PRECOMPUTED_SIGNALS_ROOT`
- Path schema: `<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>\`
- Example: `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals\free\BTCUSDT\5m_1h\sig_xxx`

Phase 1 includes:

- signal tape root/path helpers
- manifest validation
- `trades.csv` reader validation
- `summary.json` validation
- fail-closed private/runtime field checks
- DD schema v2 compatibility and display fields
- fast long-only research accounting from an existing synthetic or produced tape

Phase 1 does not include:

- producer implementation; `precompute_signals.py` is deferred
- `backtest.py` explicit fast path; deferred to Phase 2 or later
- `runner.py` replay-only fast path; deferred to Phase 3 or later
- GUI DD display wiring; deferred to a later phase
- LIVE or PAPER runtime connection
- order fetch, order submit, balance fetch, or MEXC private API access
- strategy, indicators, exchange, risk, or order runtime logic changes
- generated signal tape body, raw market data, runtime exports, package zips, exe, setup, or installer assets

## Safety Scope

Every accepted manifest must include this exact safety scope:

```json
{
  "research_only": true,
  "paper_live_order_execution": false,
  "contains_api_key": false,
  "contains_secret": false,
  "contains_order_id": false
}
```

Manifest, summary, and trade payloads fail closed when they contain private
runtime fields or credential-like text such as MEXC API fields, secret/token
material, authorization headers, raw order payloads, balance snapshots, private
keys, or billing payloads.

## DD Schema V2

DD schema v2 keeps `max_drawdown` as a signed negative legacy compatibility
field. New display code should prefer the non-negative fields.

Required DD fields:

- `dd_schema_version`
- `dd_sign_convention`
- `max_drawdown`
- `max_dd_signed`
- `max_dd_abs`
- `max_dd_pct`
- `max_dd_peak_equity`
- `max_dd_trough_equity`
- `max_dd_ts_ms`
- `max_dd_ts_iso`
- `max_dd_display_abs`
- `max_dd_display_pct`
- `max_dd_display_label`
- `max_drawdown_legacy_note`

Compatibility rules:

- `max_drawdown == max_dd_signed`
- `max_drawdown` remains signed negative or zero
- `max_dd_abs == abs(max_dd_signed)`
- `max_dd_pct >= 0`
- `max_dd_display_abs == max_dd_abs`
- `max_dd_display_pct == max_dd_pct`
- human-facing display should prefer `max_dd_abs` and `max_dd_pct`
- legacy negative `max_drawdown` only summaries may be normalized
- positive legacy `max_drawdown` only summaries fail closed

## Accounting Foundation

`fast_backtest_signals.py` reads an existing tape directory and recomputes
long-only MVP accounting from `trades.csv`:

```text
gross_pnl = (exit_exec - entry_exec) * qty
fee = entry_exec * qty * entry_fee_rate + exit_exec * qty * exit_fee_rate
net = gross_pnl - fee
equity = equity + net
```

The fast path imports only stdlib modules plus `signal_tape`. It does not import
or call strategy, indicators, exchange, ccxt, runner, risk, LIVE/PAPER runtime,
order handling, balance handling, or MEXC private API code.

## Repository Boundary

Generated signal tape body and raw market data must not be committed to the repo
or included in migration zips. Phase 1 committed files are limited to source,
tests, and this document.
