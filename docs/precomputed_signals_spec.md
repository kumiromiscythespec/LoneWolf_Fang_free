# Free precomputed signal tape foundation

This document describes the LoneWolf Fang Free precomputed signal tape migration.
Phase 1 added the schema, validation, reader helpers, and fast research
accounting foundation. Phase 2 connects that accounting path to `backtest.py`
only behind an explicit opt-in flag. Phase 3 connects the same saved-tape
accounting helper to `runner.py` for replay-only use, also behind explicit
flags. Phase 4 adds a safe producer that converts existing research export
files into the Free signal tape format without running strategy logic. Phase 7
adds a read-only GUI picker pre-stage adapter that maps an existing selection
contract to safe display metadata only.

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
- GUI DD display wiring; deferred to a later phase
- LIVE or PAPER runtime connection
- order fetch, order submit, balance fetch, or MEXC private API access
- strategy, indicators, exchange, risk, or order runtime logic changes
- generated signal tape body, raw market data, runtime exports, package zips, exe, setup, or installer assets

## Phase 2 Backtest Explicit Fast Path

Free Phase 2 connects `backtest.py` to the saved signal tape accounting helper
with explicit CLI flags:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir <signal_set_dir>
```

Optional flags:

- `--precomputed-signals-write-report`
- `--precomputed-signals-initial-equity <float>`
- `--precomputed-signals-strict`

When `--use-precomputed-signals` is absent, the existing Free backtest path is
preserved. No signal tape is discovered or used automatically.

When `--use-precomputed-signals --precomputed-signals-dir <dir>` is present,
`backtest.py` short-circuits into `fast_backtest_signals.run_fast_backtest`.
The fast path reads only `manifest.json`, `summary.json`, and `trades.csv` from
the supplied directory, recomputes saved-tape accounting, and writes comparable
`equity_curve.csv` and `trades.csv` outputs in the normal backtest export
directory. With `--precomputed-signals-write-report`, it also writes
`fast_summary.json`.

The backtest fast path fails closed when:

- `--precomputed-signals-dir` is missing
- `manifest.json`, `summary.json`, or `trades.csv` is missing
- manifest `product` is not `free`
- manifest safety scope is not research-only
- manifest `research_only=true` is not present through the safety scope
- manifest `paper_live_order_execution=false` is not present through the safety scope
- summary `research_only=true` is absent or false
- summary `paper_live_order_execution=false` is absent or true
- manifest, summary, or trades contain forbidden private/runtime fields
- legacy `max_drawdown` is positive

The canonical root remains:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`

The environment override remains:

`LWF_PRECOMPUTED_SIGNALS_ROOT`

The Free product path is:

`<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>`

Example:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals\free\BTCUSDT\5m_1h\sig_xxx`

Phase 2 does not add a producer. `precompute_signals.py` is still deferred.
Phase 2 does not connect `runner.py`; runner replay fast path is Phase 3 or
later. Phase 2 does not connect GUI; GUI DD display remains a later phase.

Phase 2 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change
strategy, indicators, exchange, risk, order runtime logic, entry timing, exit
timing, fee logic, quantity logic, PnL formulas, or DD calculation.

Generated signal tape bodies and raw market data remain local artifacts. They
must not be committed to the repo or included in migration zips or release
packages.

## Phase 3 Runner Replay-Only Fast Path

Free Phase 3 connects `runner.py` to the saved signal tape accounting helper
for replay mode only:

```powershell
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir <signal_set_dir>
```

The existing Free replay selector remains supported:

```powershell
python runner.py --replay --use-precomputed-signals --precomputed-signals-dir <signal_set_dir>
```

Optional runner flags:

- `--precomputed-signals-write-report`
- `--precomputed-signals-initial-equity <float>`
- `--precomputed-signals-strict`

When `--use-precomputed-signals` is absent, the existing `runner.py` path is
preserved. No signal tape is discovered or used automatically.

When replay mode and `--use-precomputed-signals --precomputed-signals-dir <dir>`
are both present, `runner.py` short-circuits into
`fast_backtest_signals.run_fast_backtest`. The fast path reads only
`manifest.json`, `summary.json`, and `trades.csv` from the supplied directory,
recomputes saved-tape accounting, and writes replay-compatible
`equity_curve.csv` and `trades.csv` outputs in the normal runner export
directory. With `--precomputed-signals-write-report`, it also writes
`fast_summary.json`.

The runner fast path fails closed when:

- replay mode is not explicitly selected
- live mode is selected with `--use-precomputed-signals`
- paper mode is selected with `--use-precomputed-signals`
- `--precomputed-signals-dir` is missing
- `manifest.json`, `summary.json`, or `trades.csv` is missing
- manifest `product` is not `free`
- manifest safety scope is not research-only
- manifest `research_only=true` is not present through the safety scope
- manifest `paper_live_order_execution=false` is not present through the safety scope
- summary `research_only=true` is absent or false
- summary `paper_live_order_execution=false` is absent or true
- manifest, summary, or trades contain forbidden private/runtime fields
- legacy `max_drawdown` is positive

The canonical root remains:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`

The environment override remains:

`LWF_PRECOMPUTED_SIGNALS_ROOT`

The Free product path is:

`<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>`

Example:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals\free\BTCUSDT\5m_1h\sig_xxx`

Phase 3 does not add a producer. `precompute_signals.py` is still deferred.
Phase 3 does not connect GUI; GUI DD display remains a later phase.

Phase 3 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change
strategy, indicators, exchange, risk, order runtime logic, entry timing, exit
timing, fee logic, quantity logic, PnL formulas, signal timing, or DD
calculation. It does not launch a producer when a tape is stale or missing.

Generated real signal tape bodies and raw market data remain local artifacts.
They must not be committed to the repo or included in migration zips or release
packages.

## Phase 4 Safe Producer

Free Phase 4 adds `precompute_signals.py` as a safe producer for product
`free`.

The producer converts existing `trades.csv` and optional `equity_curve.csv`
exports into a saved signal tape bundle only. It does not run `backtest.py`,
does not run `runner.py`, does not import strategy, indicators, exchange,
`ccxt`, or runner modules, and does not reimplement signal logic. It also does
not read OHLCV or raw market data.

Supported input/output shape:

- required: `--trades-csv`, `--symbol`, `--entry-tf`, `--filter-tf`
- optional: `--equity-csv`, `--summary-json`, `--product`, `--since`,
  `--until`, `--dataset-id`, `--out-root`, `--signal-set-id`,
  `--initial-equity`, `--write-summary`, `--strict`
- `--product` defaults to `free`; any other value fails closed
- default output root:
  `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`
- environment override: `LWF_PRECOMPUTED_SIGNALS_ROOT`
- output path:
  `<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>`

Bundle files:

- `manifest.json`
- `trades.csv`
- `trades.jsonl`
- `summary.json`
- `equity_reference.csv`

The manifest safety scope is fixed to:

```json
{
  "research_only": true,
  "paper_live_order_execution": false,
  "contains_api_key": false,
  "contains_secret": false,
  "contains_order_id": false
}
```

The producer fails closed when input files are missing, when `trades.csv`
contains forbidden private/runtime fields, when required accounting columns are
missing, when a supplied summary is unsafe, or when a legacy summary has a
positive `max_drawdown`.

When `--equity-csv` is supplied, `equity_reference.csv` is derived from that
source and DD metrics are calculated from the equity curve. When `--equity-csv`
is not supplied, `equity_reference.csv` is synthesized by accumulating saved
trade `net` values from `--initial-equity`. This synthetic equity basis is
recorded in `summary.json`.

DD schema v2 and display fields remain required. `max_drawdown` is the signed
negative legacy compatibility field. Human-facing display should prefer
`max_dd_abs` and `max_dd_pct`.

The Phase 2 backtest fast path and Phase 3 runner replay fast path continue to
read existing saved tape only. They do not invoke the producer when a tape is
missing or stale.

GUI DD display remains a later phase. `APP_VERSION` is not changed by this
migration.

Phase 4 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change
strategy, indicators, exchange, risk, order runtime logic, entry timing, exit
timing, fee logic, quantity logic, PnL formulas, signal timing, or DD
calculation.

Generated real signal tape bodies and raw market data remain local artifacts.
They must not be committed to the repo or included in migration zips or release
packages.

## Phase 5 Operator Inventory / Discovery

Free Phase 5 adds `precomputed_signals_inventory.py`, a read-only operator-facing
inventory layer for existing Free precomputed signal tapes. It discovers tapes
under `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals\free` by default.
`LWF_PRECOMPUTED_SIGNALS_ROOT` can override the root, and `--root` can point to a
specific safe local inventory root.

The inventory product is `free`. Any other `--product` fails closed. The helper
does not create signal tapes, does not run the producer, does not run
`backtest.py`, and does not run `runner.py`.

Example:

```powershell
python precomputed_signals_inventory.py `
  --product free `
  --root "%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals" `
  --symbol BTC/USDT `
  --entry-tf 5m `
  --filter-tf 1h `
  --format json `
  --out C:\path\to\safe_inventory.json
```

The inventory reads only safe metadata from `manifest.json` and `summary.json`.
It checks that `trades.csv` exists and reports checksum metadata already present
in the manifest, but it does not read or emit raw `trades.csv` rows. It never
prints row-level `entry_exec`, `exit_exec`, quantity values, trade ids, order ids,
raw order payloads, balances, credentials, tokens, authorization headers, raw
billing payloads, raw market data, or generated signal tape body.

Valid rows expose safe aggregate metadata only, including product, symbol,
timeframes, `signal_set_id`, `signal_dir`, creation time, dataset id, time range,
trade count, `net_total`, `final_equity`, DD schema v2 display fields, safety
booleans, file presence, manifest hash, summary hash, and the manifest-provided
`trades.csv` checksum. Operators can pass a valid row's `signal_dir` to:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir <signal_dir>
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir <signal_dir>
```

Invalid tapes are reported with `signal_dir`, `status=invalid`, a safe reason,
and a safe error code only. Unsafe manifest or summary payloads, forbidden
private/runtime fields, missing required metadata, missing tape files, unsafe
safety flags, and positive legacy `max_drawdown` values are invalid. With
`--strict`, any invalid discovered tape makes the inventory command exit
non-zero.

Phase 5 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change GUI
files, `APP_VERSION`, strategy, indicators, exchange, risk, order runtime logic,
entry timing, exit timing, fee logic, quantity logic, PnL formulas, signal
timing, or DD calculation. GUI DD display remains a later phase. Generated real
signal tape bodies, raw market data, generated inventory outputs, package zips,
executables, installers, and release assets must not be committed to the repo or
included in migration zips.

## Phase 6 Read-Only Selection Contract / Picker Preflight

Free Phase 6 adds `precomputed_signals_selection.py`, a read-only selection
contract for a single operator-chosen `signal_dir`. This is a picker preflight
layer only. It does not connect GUI code yet, and it does not auto-discover,
create, repair, refresh, or execute a signal tape.

Example:

```powershell
python precomputed_signals_selection.py `
  --signal-dir "<signal_set_dir>" `
  --product free `
  --format json `
  --out C:\path\to\safe_selection.json
```

The product is `free`. Any other `--product` fails closed. The selected
`signal_dir` is the safe path an operator or future GUI picker can pass to:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir <signal_dir>
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir <signal_dir>
```

The selection contract reads only `manifest.json` and `summary.json` safe
metadata. It checks that `trades.csv` exists and exposes only the checksum
metadata already recorded in the manifest. It does not read raw `trades.csv`
rows and does not emit row-level `entry_exec`, `exit_exec`, quantity values,
trade ids, order ids, raw order payloads, balances, credentials, tokens,
authorization headers, raw billing payloads, raw OHLCV, raw market data, or
generated signal tape body.

Valid selection contracts expose safe aggregate metadata only, including product,
symbol, normalized symbol, entry/filter timeframes, `signal_set_id`,
`signal_dir`, creation time, dataset id, time range, trade count, `net_total`,
`final_equity`, DD schema v2 display fields, safety booleans, file presence,
manifest hash, summary hash, and the manifest-provided `trades.csv` checksum.
For valid Free selections:

- `selectable_for_backtest_fast_path=true`
- `selectable_for_runner_replay_fast_path=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

Selection contracts are not valid for LIVE or PAPER runtime use. They are only a
read-only preflight contract for saved research tape selection before explicit
backtest fast path or runner replay fast path execution.

Invalid selections expose only safe metadata: `signal_dir`, `status=invalid`,
`status_reason`, and a safe error code. Safe error codes include
`missing_signal_dir`, `missing_manifest`, `missing_summary`,
`missing_trades_csv`, `unsafe_manifest`, `unsafe_summary`, `product_mismatch`,
`symbol_mismatch`, `timeframe_mismatch`, `signal_set_id_mismatch`,
`forbidden_field`, `positive_legacy_drawdown`, and `invalid_unknown`. With
`--strict`, invalid selections fail closed with a non-zero exit code; invalid
selections are never marked selectable.

The selection CLI accepts optional expectation checks:

- `--expect-symbol`
- `--expect-entry-tf`
- `--expect-filter-tf`
- `--expect-signal-set-id`

Any mismatch fails closed with a safe error code and does not expose raw payloads.
`--out` writes only a safe JSON selection contract. Standard output contains only
a safe JSON contract or safe operator summary.

Phase 6 does not run `precompute_signals.py`, does not run `backtest.py`, does
not run `runner.py`, and does not run the inventory command. It does not connect
to GUI files. GUI DD display and picker wiring remain later phases.

Phase 6 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change
`APP_VERSION`, strategy, indicators, exchange, risk, order runtime logic, entry
timing, exit timing, fee logic, quantity logic, PnL formulas, signal timing, or
DD calculation.

Generated real signal tape bodies, raw market data, generated inventory outputs,
generated selection outputs, package zips, executables, installers, and release
assets must not be committed to the repo or included in migration zips.

## Phase 7 GUI Picker Pre-Stage / Read-Only Adapter

Free Phase 7 adds `precomputed_signals_gui_adapter.py` as a pure read-only
adapter for a future GUI picker. It is not GUI wiring. It does not change
`app/gui/main_window.py`, `app/gui/result_chart.py`,
`app/app/gui/main_window.py`, or `app/app/gui/result_chart.py`.

Example:

```powershell
python precomputed_signals_gui_adapter.py `
  --signal-dir "<signal_set_dir>" `
  --format json `
  --out C:\path\to\safe_gui_picker_item.json
```

The adapter accepts either an in-memory Phase 6 selection contract or a
single operator-provided `signal_dir`. For `signal_dir`, it delegates to the
Phase 6 read-only selection validation and then maps the result into a GUI
picker item. It does not discover tapes, does not run the inventory command,
does not run the producer, does not run `backtest.py`, and does not run
`runner.py`.

The GUI picker item is a safe display model for future backtest/replay
selection. It includes only safe metadata such as product, symbol, timeframes,
`signal_set_id`, `signal_dir`, creation time, dataset id, time range, trade
count, `net_total`, `final_equity`, DD display fields, picker title/subtitle,
and safety/selectability flags. It does not include raw `trades.csv` rows,
row-level `entry_exec`, `exit_exec`, quantity values, exact trade ids, order
ids, raw order payloads, balances, credentials, tokens, authorization headers,
raw billing payloads, raw OHLCV, raw market data, or generated signal tape body.

Valid Free GUI picker items keep these flags:

- `selectable_for_backtest_fast_path=true`
- `selectable_for_runner_replay_fast_path=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

Invalid selections fail closed as GUI picker items with `status=invalid`. They
are never selectable for backtest fast path or runner replay fast path, and they
only expose a safe reason and warning. Raw payloads are not echoed.

DD display is display-only mapping. Human-facing text uses the non-negative
display fields first:

- `max_dd_display_abs`, falling back to `max_dd_abs`
- `max_dd_display_pct`, falling back to `max_dd_pct`
- `max_drawdown` remains the signed negative legacy compatibility field
- `max_drawdown == max_dd_signed`
- `max_drawdown` is never treated as positive display DD

Japanese display wording for future GUI work should preserve this meaning:

- `最大DD（正値表示）`
- `max_drawdown は signed negative legacy compatibility field`

Example display text:

```text
Max DD (abs, display): 1,559.7350 | Max DD pct (display): 0.0244% | Legacy signed max_drawdown: -1,559.7350
```

The product remains `free`. The canonical root remains:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`

The environment override remains:

`LWF_PRECOMPUTED_SIGNALS_ROOT`

The Free product path remains:

`<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>`

Future phases can wire the GUI picker to this adapter. Phase 7 intentionally
does not connect button handlers, order controls, live/paper selectors, or
runtime execution paths.

Phase 7 does not connect to LIVE, PAPER, order creation, order fetch, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. It does not change
`APP_VERSION`, strategy, indicators, exchange, risk, order runtime logic, entry
timing, exit timing, fee logic, quantity logic, PnL formulas, signal timing, or
DD calculation.

Generated real signal tape bodies, raw market data, generated inventory outputs,
generated selection outputs, generated GUI adapter outputs, package zips,
executables, installers, and release assets must not be committed to the repo
or included in migration zips.

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
