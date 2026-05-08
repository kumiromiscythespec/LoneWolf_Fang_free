# Free precomputed signal tape foundation

This document describes the LoneWolf Fang Free precomputed signal tape migration.
Phase 1 added the schema, validation, reader helpers, and fast research
accounting foundation. Phase 2 connects that accounting path to `backtest.py`
only behind an explicit opt-in flag. Phase 3 connects the same saved-tape
accounting helper to `runner.py` for replay-only use, also behind explicit
flags. Phase 4 adds a safe producer that converts existing research export
files into the Free signal tape format without running strategy logic. Phase 7
adds a read-only GUI picker pre-stage adapter that maps an existing selection
contract to safe display metadata only. Phase 8 wires that adapter into the
Free GUI as a display-only picker panel for an operator-selected `signal_dir`.
Phase 9 adds GUI command preview text for the selected `signal_dir` without the
GUI executing it. Phase 10 adds copy UX only so an operator can copy the
explicit backtest/replay fast-path command text without adding any execution
path. Phase 12 adds read-only GUI selection diagnostics, and Phase 13 polishes
those diagnostics for operator-facing compact status, file presence, hash,
safety, fast-path, and LIVE/PAPER boundary display. Phase 14 is GUI smoke
checklist / docs-only hardening; it changes no GUI source, runtime source,
execution path, APP_VERSION, package, exe, setup, installer, signing, or release
asset. Phase 15 fixes the synthetic fixture only manual GUI runtime smoke record
format as docs/test-only; GUI runtime smoke is not executed in Phase 15. Phase
16 adds the synthetic manual smoke record schema validator and sample static
fixture as docs/tests-only; GUI runtime smoke is not executed in Phase 16.
Phase 17 fixes the future local-only dry-run design boundary as docs/tests-only:
no local dry-run implementation, no GUI source change, no runtime source
change, no command execution, no subprocess / QProcess / background worker, no
producer/backtest/runner/inventory auto-run, no LIVE/PAPER/order, and no MEXC
private API. Phase 18 fixes the future local-only dry-run request schema as
docs/tests-only with a synthetic static sample and validator; it still adds no
request builder, generated request file, local dry-run implementation, GUI
source change, runtime source change, command execution, subprocess / QProcess /
background worker, LIVE/PAPER/order connection, or MEXC private API.
Phase 19 adds the local-only dry-run request builder docs/helper as preview
only. Phase 20 adds the read-only GUI preview adapter and still does not connect
GUI source. Phase 21 adds read-only GUI source wiring of dry-run request preview
adapter so the Free GUI displays dry-run request preview only; it does not
execute dry-run, does not create request files, does not add confirm/dry-run/
execute buttons, does not use subprocess / QProcess / Popen / background
worker, does not run backtest/runner/producer/inventory, does not connect to
LIVE/PAPER/order, and keeps APP_VERSION unchanged.
Phase 22 fixes the local-only dry-run execution approval boundary as
docs/tests-only. It adds no approval UI implementation, no dry-run execution
implementation, no approval record generation, no execution audit record
generation, no GUI source change, no runtime source change, no command
execution, no subprocess / QProcess / background worker, no LIVE/PAPER/order,
and no MEXC private API. Future execution requires separate explicit approval
phase, and approval is not live/paper/order approval.
Phase 23 fixes the approval preflight checklist / approval record static sample
as docs/tests-only. It adds no approval UI implementation, no approval record
generation at runtime, no execution audit record generation, no dry-run
execution, no GUI source change, no runtime source change, no command
execution, no subprocess / QProcess / background worker, no LIVE/PAPER/order,
and no MEXC private API. The static sample is not actual approval, not runtime
output, and not an execution audit record.
Phase 24 fixes the execution audit record schema / static sample as
docs/tests-only. It adds no approval UI implementation, no dry-run execution
implementation, no approval record generation at runtime, no execution audit
record generation at runtime, no GUI source change, no runtime source change,
no command execution, no subprocess / QProcess / background worker, no
LIVE/PAPER/order, and no MEXC private API. The static sample is `not_run`,
not runtime output, not approval proof, and not order/trading record proof.

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
- GUI execution wiring; deferred to a later phase
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

## Phase 8 GUI Picker Wiring / Display-Only Connection

Free Phase 8 adds GUI picker wiring for precomputed signal tapes. The wiring is
display-only. It lets an operator explicitly select a `signal_dir` and shows a
safe summary in the existing Replay / Backtest GUI area.

The GUI reads the Phase 7 adapter schema through
`precomputed_signals_gui_adapter.build_gui_picker_item_from_signal_dir()` via a
small GUI helper. The GUI does not read raw `trades.csv` rows, generated tape
body, raw market data, raw OHLCV, exact trade ids, row-level `entry_exec` /
`exit_exec` values, row-level quantities, raw order payloads, balances,
credentials, tokens, authorization headers, or raw billing payloads.

Displayed safe fields are limited to the selected `signal_dir`, product/symbol,
entry and filter timeframes, `signal_set_id`, `dataset_id`, creation timestamp,
trade count, `net_total`, `final_equity`, DD display text, DD display amount and
percentage, adapter status, picker warning, and selection safety flags.

Valid display-only picker items preserve these flags:

- `selectable_for_backtest_fast_path=true`
- `selectable_for_runner_replay_fast_path=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

The GUI renders the selection as `Replay/backtest only` and
`Not selectable for LIVE/PAPER` (Japanese UI may show `バックテスト / リプレイ専用`
and `LIVE/PAPER には使用不可`). These flags are warning/display state only in
Phase 8. The selected signal tape is not wired to run buttons.

DD display remains display-only and uses the non-negative display fields first:

- `max_dd_display_abs`, falling back to `max_dd_abs`
- `max_dd_display_pct`, falling back to `max_dd_pct`
- `max_dd_display_label = "Max DD (abs, display)"`

`max_drawdown` remains the signed negative legacy compatibility field, and
`max_drawdown == max_dd_signed`. The GUI must not reinterpret a positive legacy
`max_drawdown` as positive display DD; positive legacy-only payloads fail closed
through the adapter/selection contract.

Phase 8 does not run `precompute_signals.py`, does not run `backtest.py`, does
not run `runner.py`, and does not run the inventory command automatically. It
does not add any button to launch producer, backtest, runner, inventory, or any
signal-tape execution path.

Phase 8 does not connect precomputed signal tape selection to LIVE, PAPER,
order creation, order fetch, order submit, balance fetch, MEXC private APIs,
exchange clients, or `ccxt`. Future phases may consider an explicit
backtest/replay action connection, but it must remain separated from
LIVE/PAPER/order paths.

Phase 8 does not change `APP_VERSION`, strategy, indicators, exchange, risk,
order runtime logic, entry timing, exit timing, fee logic, quantity logic, PnL
formulas, signal timing, or DD calculation.

Generated real signal tape bodies, raw market data, generated inventory outputs,
generated selection outputs, generated GUI adapter outputs, package zips,
executables, installers, release assets, and zip-in-zip artifacts must not be
committed to the repo or included in migration zips.

## Phase 9 GUI Command Preview Only

Free Phase 9 adds a display-only command preview to the GUI precomputed signal
tape panel. The selected safe picker item supplies only its `signal_dir` and
safe source labels; the GUI helper then builds copy-ready text plus argv lists
for the already-existing explicit fast paths.

Backtest preview:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report
```

Runner replay preview:

```powershell
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report
```

The preview model preserves these invariants:

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`
- selected picker item `not_selectable_for_live=true`
- selected picker item `not_selectable_for_paper=true`

The GUI text states `Backtest / replay preview only`,
`Execution is disabled in this panel`, and `Not selectable for LIVE/PAPER`.
Japanese UI may show `バックテスト用コマンドプレビュー`,
`リプレイ用コマンドプレビュー`, `このパネルからは実行しません`,
and `LIVE/PAPER には使用不可`.

Invalid picker items, empty `signal_dir`, or items that fail the LIVE/PAPER
exclusion flags fail closed and show a safe warning only. The preview does not
create LIVE or PAPER commands and does not wire the selected tape into existing
run buttons.

Phase 9 does not run `precompute_signals.py`, does not run `backtest.py`, does
not run `runner.py`, does not run the inventory command, and does not run the
selection or adapter command automatically. It adds no `subprocess`,
`os.system`, `QProcess`, `Popen`, multiprocessing, threading, scheduler, or
background worker path for the command preview.

Phase 9 does not connect to LIVE, PAPER, order creation, order fetch, order
submit, balance fetch, MEXC private APIs, exchange clients, or `ccxt`. It does
not change `APP_VERSION`, strategy, indicators, exchange, risk, order runtime
logic, entry timing, exit timing, fee logic, quantity logic, PnL formulas,
signal timing, or DD calculation.

The command preview does not display raw `trades.csv` rows, row-level
`entry_exec`, `exit_exec`, or `qty`, exact trade ids, order ids, raw order
payloads, balance snapshots, API keys, secrets, tokens, authorization headers,
raw billing payloads, raw market data, raw OHLCV, generated command preview
outputs, or generated real signal tape bodies. Generated real signal tape
bodies and raw market data remain repo-external and must not be committed to
the repo or included in migration zips.

## Phase 10 GUI Command Preview Copy UX Only

Free Phase 10 adds copy UX only to the GUI command preview panel. It does not
add command execution. It keeps the Phase 9 command preview as the single source
for copyable text and exposes only these operator actions:

- `Copy backtest command`
- `Copy replay command`

The copy helper accepts the Phase 9 command preview view model and returns only
safe command text for `backtest_command_text` or
`runner_replay_command_text`. The backtest preview command must explicitly
include `--use-precomputed-signals` and `--precomputed-signals-dir`. The runner
replay preview command must explicitly include `--mode replay`,
`--use-precomputed-signals`, and `--precomputed-signals-dir`.

The copy state preserves these invariants:

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`

Copy is disabled when the preview is invalid, command text is empty,
`preview_only` is not true, execution is enabled, a LIVE command is available,
or a PAPER command is available. Live, paper, order, and any other command kind
fail closed and are not copyable.

The GUI status states `Preview only`, `Execution disabled`,
`Not selectable for LIVE/PAPER`, and `This panel does not execute commands`.
After a user-triggered clipboard write it may show `Copied`. Japanese UI may
show `バックテストコマンドをコピー`, `リプレイコマンドをコピー`, `コピーしました`,
`プレビュー専用`, `このパネルからは実行しません`, and
`LIVE/PAPER には使用不可`.

Phase 10 does not run `precompute_signals.py`, does not run `backtest.py`, does
not run `runner.py`, does not run the inventory command, and does not run the
selection, adapter, or command-preview command automatically. It does not add
`subprocess`, `os.system`, `QProcess`, `Popen`, `startDetached`,
multiprocessing, threading, scheduler, or background worker execution for copy
UX.

Phase 10 does not connect precomputed signal tape selection, command preview,
or copy state to LIVE, PAPER, order creation, order fetch, order submit, balance
fetch, MEXC private APIs, exchange clients, or `ccxt`. Existing GUI LIVE,
PAPER, and order paths remain untouched and do not consume the selected signal
tape or command preview.

The copy UX does not display or copy raw `trades.csv` rows, row-level
`entry_exec`, `exit_exec`, row-level `qty`, exact trade ids, order ids, raw
order payloads, balance snapshots, API keys, secrets, tokens, authorization
headers, raw billing payloads, raw market data, raw OHLCV, generated command
preview output, generated clipboard output, or generated real signal tape
bodies. Generated real signal tape bodies and raw market data remain
repo-external and must not be committed to the repo or included in migration
zips.

Phase 10 does not change `APP_VERSION`, package, exe, setup, installer, signing,
release assets, strategy, indicators, exchange, risk, order runtime logic, entry
timing, exit timing, fee logic, quantity logic, PnL formulas, signal timing, or
DD calculation.

## Phase 11 GUI Command Preview Copy Accessibility / Operator Docs

Free Phase 11 is copy UX accessibility / layout smoke / operator docs. The GUI
command preview is preview-only and remains a read-only operator aid. The GUI
does not execute commands.

The copy buttons place safe command text on the clipboard only:

- `Copy backtest command`
- `Copy replay command`

They do not run producer, backtest, runner, inventory, selection, adapter, or
command-preview commands automatically. They do not add `subprocess`,
`os.system`, `QProcess`, `Popen`, `startDetached`, multiprocessing, threading,
scheduler, or background worker execution. The copied status is UI state only.

Phase 11 keeps these invariants visible in the copy state and GUI text:

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`
- `Preview only`
- `Execution disabled`
- `This panel does not execute commands`
- `Not selectable for LIVE/PAPER`
- `Backtest/replay only`
- `Run this command manually in a terminal if needed`

Invalid preview state, empty command text, unsafe command text,
`execution_enabled=true`, `preview_only!=true`, `live_command_available=true`,
or `paper_command_available=true` disables copy and shows a safe disabled
reason. Live / paper command kinds fail closed and are not copyable.

Operator workflow: if a copied preview command is needed, the operator manually
reviews it and runs it in an external terminal. The GUI is not the execution
surface.

Phase 11 does not generate live / paper command text. It does not connect
precomputed signal tape selection, command preview, copy state, or copy buttons
to LIVE, PAPER, order creation, order fetch, order submit, balance fetch, MEXC
API, exchange clients, or `ccxt`. Existing GUI LIVE, PAPER, and order paths
remain untouched and do not consume the selected signal tape or command
preview.

Phase 11 docs, accessibility text, layout smoke tests, and copy UX state must
not expose raw `trades.csv` rows, row-level `entry_exec`, `exit_exec`,
row-level `qty`, exact trade ids, order ids, raw order payloads, balance
snapshots, API keys, secrets, tokens, authorization headers, raw billing
payloads, raw market data, raw OHLCV, generated command preview output,
generated clipboard output, or generated real signal tape bodies. Generated real
signal tape bodies and raw market data remain repo-external and must not be
committed to the repo or included in migration zips.

Phase 11 does not change `APP_VERSION`, package, exe, setup, installer, signing,
release assets, strategy, indicators, exchange, risk, order runtime logic, entry
timing, exit timing, fee logic, quantity logic, PnL formulas, signal timing, or
DD calculation.

## Phase 12 Read-Only Selection Diagnostics Display

Free Phase 12 adds a read-only selection diagnostics display to the GUI for an
operator-selected precomputed signal tape `signal_dir`. The diagnostics are
read-only and display manifest / summary / selection safe metadata only.

The diagnostics display may show validation status, `safe_error_code`, safe
status reason, file presence, `manifest_sha256`, `summary_sha256`, the manifest
`trades_csv_sha256` metadata, safety flags, backtest/replay fast-path
availability, `not_selectable_for_live=true`, and `not_selectable_for_paper=true`.
It may show safe aggregate values such as `trade_count`, `net_total`,
`final_equity`, `max_dd_display_abs`, and `max_dd_display_pct`.

`trades.csv raw rows are not read or displayed`. Diagnostics must not display
raw trades rows, row-level `entry_exec`, `exit_exec`, row-level `qty`, exact
trade id, order id, raw order payload, balance snapshot, API key, secret, token,
authorization header, raw billing payload, raw market data, raw OHLCV,
generated diagnostics output, generated GUI adapter output, generated selection
output, generated command preview output, generated clipboard output, generated
real signal tape body, or raw market data.

The GUI does not execute commands. It does not run producer / backtest / runner
/ inventory automatically, and it does not invoke selection or adapter commands
as a background execution path. It does not connect selected signal tape state
to LIVE / PAPER / order creation, order fetch, order submit, balance fetch,
MEXC API, exchange clients, or `ccxt`. Phase 12 does not add `subprocess`,
`os.system`, `QProcess`, `Popen`, `startDetached`, threading, multiprocessing,
scheduler, or background worker execution for diagnostics.

DD display continues to prefer `max_dd_abs / max_dd_pct` and the display fields
`max_dd_display_abs / max_dd_display_pct`. `max_drawdown` remains the signed
negative legacy field.

Phase 12 does not change `APP_VERSION`, package, exe, setup, installer, signing,
release assets, strategy, indicators, exchange, risk, order runtime logic, entry
timing, exit timing, fee logic, quantity logic, PnL formulas, signal timing, or
DD calculation.

## Safety Scope

## Phase 14 GUI Smoke Checklist / Docs-Only Hardening

Free Phase 14 fixes the operator GUI smoke checklist in
`docs/precomputed_signals_gui_smoke_checklist.md`. The checklist is docs-only
hardening. It does not change GUI source, runtime source, command execution
behavior, strategy, indicators, exchange, risk, order runtime logic, signal
timing, DD calculation, APP_VERSION, package, exe, setup, installer, signing,
or release assets.

GUI smoke is optional. If performed, it is limited to visual confirmation of
`Precomputed Signal Tape`, `Selection diagnostics`, valid / invalid diagnostics,
safe error code, file presence, safety status, fast-path status,
`LIVE/PAPER: not selectable`, display-only DD labels, command preview, copy UX,
and disabled execution state. It must not start LIVE, PAPER, order creation,
order fetch, order submit, balance fetch, MEXC private API, producer, backtest,
runner, inventory, selection, adapter, subprocess, `QProcess`, scheduler,
thread, process, launch button, worker, or background worker paths.

Command preview is preview-only. Copy UX is clipboard-only. Operators may copy
safe preview text and manually run it in an external terminal if needed; the GUI
does not execute commands and does not auto-run producer/backtest/runner/
inventory.

The checklist fixes that raw trades rows are never displayed. It allows only
safe aggregate and metadata fields such as `signal_dir`, `symbol`, `entry_tf`,
`filter_tf`, `signal_set_id`, `dataset_id`, `trade_count`, `net_total`,
`final_equity`, `max_dd_display_abs`, `max_dd_display_pct`,
`max_dd_display_label`, `status`, `safe_error_code`, manifest hash, summary
hash, `trades.csv` hash from manifest, compact path, and compact hash.

Forbidden display fields remain raw trades rows, `entry_exec`, `exit_exec`,
`qty`, exact trade id, order id, raw order, balance, API key, secret, token,
authorization, raw billing, raw market data, raw OHLCV, generated real signal
tape body, generated diagnostics output, generated smoke output, generated
command preview output, and generated clipboard output.

Generated real tape body / raw market data excluded remains a hard repository
and migration zip boundary. `max_drawdown` remains signed negative legacy field.
GUI display prefers `max_dd_abs / max_dd_pct` and
`max_dd_display_abs / max_dd_display_pct`.

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

## Phase 15 Synthetic Fixture Only Manual GUI Runtime Smoke Record Format

Free Phase 15 adds
`docs/precomputed_signals_gui_manual_smoke_record.md` as a docs/test-only record
format for future manual GUI runtime smoke. Phase 15 is synthetic fixture only
manual GUI runtime smoke record format. GUI runtime smoke is not executed in
Phase 15.

The record is safe metadata only. It does not change GUI source, runtime
source, command preview behavior, copy UX behavior, diagnostics behavior,
execution paths, APP_VERSION, package/release state, setup, installer, exe,
signing, release assets, strategy, indicators, exchange, risk, order runtime
logic, signal timing, DD calculation, entry/exit logic, fee logic, PnL formula,
or quantity logic.

The record scope is synthetic fixture only and display-only:

- no command execution
- no producer/backtest/runner/inventory auto-run
- no selection or adapter auto-run
- no LIVE/PAPER/order
- no MEXC private API
- no order submit/fetch
- no balance fetch
- no subprocess / QProcess / background worker
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch button, worker, or background worker path
- APP_VERSION unchanged
- package/release not touched
- no real market data
- no generated real signal tape body
- raw trade rows are never recorded

The safe record schema requires branch, HEAD, APP_VERSION, recorded time,
operator, `signal_dir`, product, symbol, `entry_tf`, `filter_tf`,
`signal_set_id`, selection status, `safe_error_code`, expected labels checked,
disabled states checked, command preview checked, copy UX checked, diagnostics
checked, LIVE/PAPER boundary checked, raw trade row visibility check, forbidden
execution observations, `result_status`, `result_reason`, and sanitized notes.

Allowed `result_status` values are `pass`, `fail`, `blocked`, and `not_run`.
Allowed `smoke_mode` values are `synthetic_fixture_display_only`,
`docs_static_check_only`, and `not_run`. Allowed `fixture_type` values are
`synthetic_signal_tape`, `synthetic_selection_contract`,
`synthetic_picker_item`, and `none`.

Allowed smoke record fields remain aggregate `net_total`, `final_equity`,
`trade_count`, DD display fields, compact hashes, status / warnings,
`safe_error_code`, `signal_dir`, branch, commit, and APP_VERSION.

Forbidden smoke record fields are raw trades rows, `entry_exec`, `exit_exec`,
`qty`, exact trade id, order id, raw order, balance, API key, secret, token,
authorization, raw billing, raw market data, raw OHLCV, generated real signal
tape body, generated smoke output, generated diagnostics output, generated
command preview output, generated clipboard output, and screenshots containing
secrets / account / balances / orders.

Pass means expected labels are visible, expected disabled states are confirmed,
command preview is visible but not executed, copy UX is clipboard-only,
diagnostics are visible, raw trade rows are not visible, and no
LIVE/PAPER/order/private API path is observed. Fail means any command is
executed by the GUI, producer/backtest/runner/inventory auto-runs,
LIVE/PAPER/order/private API starts, balance/order fetch occurs, raw trades rows
or row-level private/runtime fields are displayed, or secrets/auth/billing/order
/ balance appears. Blocked means the synthetic fixture, safe launch, worktree,
APP_VERSION, package/release boundary, or LIVE/PAPER/order disabled state cannot
be confirmed. Not run means docs/static-only validation was chosen or GUI
runtime smoke was intentionally deferred.

Screenshots optional. Screenshots must not include secrets / balances / orders,
account details, raw trades rows, raw market data, raw OHLCV, generated real
tape body, API key, secret, token, authorization, raw billing, or raw order
payloads. Screenshots must be omitted from repo and zip unless explicitly
sanitized.

Future local-only dry-run is future phase. Explicit local-only dry-run design is
future phase. Execution button is future phase. LIVE/PAPER/order remains
separated, and packaging/release remains separated.

## Phase 16 Synthetic Manual Smoke Record Schema Validator / Static Sample

Free Phase 16 adds docs/tests-only schema validation and a static sanitized
sample fixture for the Phase 15-or-later manual GUI smoke record. GUI runtime
smoke is not executed in Phase 16. GUI runtime smoke not executed in Phase
15/16 is a fixed boundary.

The static sample lives at
`docs/precomputed_signals_gui_manual_smoke_record_sample.json`. It is
synthetic, sanitized, not generated runtime output, not a generated smoke
record, not raw GUI output, and not evidence that a GUI runtime smoke was
performed.

The schema is fixed in
`docs/precomputed_signals_gui_manual_smoke_record.md`:

- `record_type=precomputed_signal_gui_manual_smoke_record`
- `phase=free_precomputed_signals_phase15_or_later`
- `result_status` enum: `pass`, `fail`, `blocked`, `not_run`
- `smoke_mode` enum: `synthetic_fixture_display_only`,
  `docs_static_check_only`, `not_run`
- `fixture_type` enum: `synthetic_signal_tape`,
  `synthetic_selection_contract`, `synthetic_picker_item`, `none`

The validator test is docs-policy only. It checks required fields, enum values,
safe boolean types, pass/not_run invariants, forbidden record fields,
forbidden credential/private/runtime text, screenshot policy, and static docs
coverage. It does not add a production runtime validator.

Phase 16 does not change GUI source, runtime source, execution paths,
APP_VERSION, package/release state, setup, installer, exe, signing, release
assets, strategy, indicators, exchange, risk, order runtime logic, signal
timing, DD calculation, entry/exit logic, fee logic, PnL formula, or quantity
logic.

Phase 16 keeps these boundaries fixed:

- no command execution
- no producer/backtest/runner/inventory auto-run
- no selection or adapter auto-run
- no LIVE/PAPER/order
- no MEXC private API
- no order submit/fetch
- no balance fetch
- no subprocess / QProcess / background worker
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch button, worker, or background worker path
- APP_VERSION unchanged
- package/release not touched
- generated real tape body / raw market data excluded
- generated smoke record excluded
- generated runtime output excluded
- screenshots are optional and must not be committed unless sanitized in a
  future phase
- raw trade rows are never recorded
- `max_drawdown` remains signed negative legacy field
- GUI display prefers `max_dd_abs / max_dd_pct`

The sample and validator must not include raw trades rows, `entry_exec`,
`exit_exec`, `qty`, exact trade id, order id, raw order, balance, API key,
secret, token, authorization, raw billing, raw market data, raw OHLCV,
generated real signal tape body, generated smoke output, generated diagnostics
output, generated command preview output, generated clipboard output,
screenshots, account details, private keys, or package artifacts.

Future local-only dry-run design is future phase. Explicit local-only dry-run
design is future phase. Execution button is future phase. LIVE/PAPER/order and
packaging/release remain separated.

## Phase 17 Future Local-Only Dry-Run Design Boundary

Free Phase 17 is future local-only dry-run design boundary. It is
docs/tests-only and changes no GUI source, runtime source, execution path,
APP_VERSION, package, exe, installer, setup, signing, release asset, strategy,
indicator, exchange, risk, order runtime logic, entry/exit timing, fee logic,
PnL formula, quantity logic, signal timing, or DD calculation.

The authoritative design boundary is
`docs/precomputed_signals_gui_local_dry_run_design.md`.

Phase 17 keeps these boundaries fixed:

- no local dry-run implementation
- no GUI source change
- no runtime source change
- no command execution
- no subprocess / QProcess / background worker
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, worker, or background execution path
- no producer/backtest/runner/inventory auto-run
- no selection or adapter auto-run
- command preview remains preview-only
- copy UX remains clipboard-only
- GUI diagnostics remain read-only
- manual smoke record remains display-only
- no LIVE/PAPER/order
- no MEXC private API
- APP_VERSION unchanged
- package/release not touched
- generated real tape body / raw market data excluded
- raw trade rows are never displayed or recorded
- future local-only dry-run requires explicit operator confirmation
- future local-only dry-run is not live/paper/order
- future execution must be a separate phase

Future local-only dry-run may only be an explicit operator-confirmed action
using an already selected, validated precomputed signal tape. It may only use a
future local saved-tape fast path. It must not use LIVE, PAPER, order submit,
order fetch, balance fetch, private API, MEXC private API, raw market data, or
producer auto-run.

Allowed `dry_run_mode` values are `backtest_fast_path_local_only` and
`runner_replay_fast_path_local_only`. Not allowed values are `live`, `paper`,
`order_submit`, `order_fetch`, `balance_fetch`, `private_api`,
`producer_auto_run`, `inventory_auto_scan`, and `background_worker`.

The future dry-run request schema is docs-only in Phase 17 and must include
`request_type=precomputed_signal_local_dry_run_request`, `signal_dir`,
`product`, `symbol`, `entry_tf`, `filter_tf`, `signal_set_id`, `dry_run_mode`,
`output_dir`, confirmation flags, LIVE/PAPER exclusion flags, safety flags,
command preview text, allowed artifacts, forbidden artifacts, `status`, and
`status_reason`. `operator_confirmed=false by default`; it may become true only
after explicit future confirmation.

Allowed future artifacts are safe summary JSON, fast-path `equity_curve.csv`,
fast-path `trades.csv` only if redacted / synthetic / saved-tape compatible,
`fast_summary.json`, safe local metadata logs, and safe metadata-only manual
records.

Forbidden future artifacts include raw market data, raw OHLCV, raw trades rows,
`entry_exec`, `exit_exec`, `qty`, trade id, order id, raw order, order,
balance snapshot, balance, API key, secret, token, authorization, raw billing,
screenshots containing secrets / balances / orders / account details, package
zip, exe, installer, setup binary, release assets, runtime dirs copied into the
repo, exports dirs copied into the repo, and zip-in-zip artifacts.

Required future fail-closed conditions include invalid selection, missing
`signal_dir`, missing manifest / missing summary / missing `trades.csv`, unsafe
manifest, unsafe summary, forbidden field, positive legacy max_drawdown,
live/paper request, order/balance/private API request, output path under
release/package artifact area, missing operator confirmation, background
execution request, producer auto-run request, inventory auto-scan request,
package/release mutation, and APP_VERSION mutation.

Phase 15/16 manual smoke record remains display-only. A dry-run result record
is a separate future artifact and must not be reused as order/runtime proof.
Generated dry-run request files, generated dry-run output, screenshots, real
signal tape bodies, and raw market data must not be committed or included in
Phase 17 migration zips.

## Phase 18 Local-Only Dry-Run Request Schema

Free Phase 18 is local-only dry-run request schema docs/test-only. It freezes
the future request schema, enum values, default safety flags, fail-closed
reasons, and a sanitized static sample fixture. It changes no GUI source,
runtime source, execution path, APP_VERSION, package, exe, installer, setup,
signing, release asset, strategy, indicator, exchange, risk, order runtime
logic, entry/exit timing, fee logic, PnL formula, quantity logic, signal timing,
or DD calculation.

The authoritative request schema is
`docs/precomputed_signals_gui_local_dry_run_request_schema.md`. The static
sample fixture is
`docs/precomputed_signals_gui_local_dry_run_request_sample.json`.

Phase 18 keeps these boundaries fixed:

- Phase 18 is docs/tests-only
- no local dry-run implementation
- no request builder implementation
- no generated request file
- no GUI source change
- no runtime source change
- no command execution
- no subprocess / QProcess / background worker
- no producer/backtest/runner/inventory auto-run
- no selection or adapter auto-run
- no LIVE/PAPER/order
- no MEXC private API
- APP_VERSION unchanged
- package/release not touched
- generated real tape body / raw market data excluded
- raw trade rows are never included
- operator_confirmed=false by default
- future local-only dry-run requires explicit operator confirmation
- future local-only dry-run is not live/paper/order
- future execution must be a separate phase

The required request type is
`precomputed_signal_local_dry_run_request`. Required identity fields are
`schema_version`, `request_schema_version`, `request_type`, `phase`, and
`product`. Required selection fields are `signal_dir`, `symbol`, `entry_tf`,
`filter_tf`, and `signal_set_id`. Required dry-run fields are `dry_run_mode`,
`output_dir`, and `command_text_preview`. Required confirmation fields are
`operator_confirmation_required`, `operator_confirmed`,
`preview_only_before_confirmation`, and
`execution_enabled_after_confirmation`. Required safety fields are
`not_selectable_for_live`, `not_selectable_for_paper`,
`safety_research_only`, and `paper_live_order_execution`. Required preflight
fields are `selection_contract_valid`, `manifest_present`, `summary_present`,
`trades_csv_present`, `manifest_hash_present`, `summary_hash_present`,
`trades_csv_hash_from_manifest_present`,
`positive_legacy_max_drawdown_rejected`, `forbidden_fields_rejected`,
`live_paper_order_rejected`, `private_api_rejected`,
`background_execution_rejected`, and `package_release_artifacts_excluded`.
Required status fields are `status`, `status_reason`, and
`fail_closed_reasons`.

Allowed `dry_run_mode` values are `backtest_fast_path_local_only` and
`runner_replay_fast_path_local_only`. Not allowed values are `live`, `paper`,
`order_submit`, `order_fetch`, `balance_fetch`, `private_api`,
`producer_auto_run`, `inventory_auto_scan`, and `background_worker`.

Required defaults are `product=free`,
`operator_confirmation_required=true`, `operator_confirmed=false`,
`operator_confirmed=false by default`, `preview_only_before_confirmation=true`,
`execution_enabled_after_confirmation=false`,
`execution_enabled_after_confirmation=false in Phase 18 sample`,
`not_selectable_for_live=true`, `not_selectable_for_paper=true`,
`safety_research_only=true`, and `paper_live_order_execution=false`. Allowed
`status` values are `draft`, `valid_preview`, `blocked`, `invalid`, and
`not_run`; the recommended sample status is `not_run`. The request is not
executable in Phase 18.

Allowed `allowed_artifacts` enum values are `safe_summary_json`,
`fast_path_equity_curve_csv`, `fast_path_trades_csv_safe_condition`,
`fast_summary_json`, `safe_metadata_log`, and
`sanitized_manual_smoke_record`.

Forbidden `forbidden_artifacts` enum values are `raw_market_data`,
`raw_ohlcv`, `raw_trades_rows`, `entry_exec`, `exit_exec`, `qty`, `trade_id`,
`order_id`, `raw_order`, `balance_snapshot`, `api_key`, `secret`, `token`,
`authorization`, `raw_billing`, `package_zip`, `exe`, `installer`,
`release_asset`, and `screenshots_with_secrets_or_balances_or_orders`.

Allowed `fail_closed_reasons` enum values are `invalid_selection`,
`missing_signal_dir`, `missing_manifest`, `missing_summary`,
`missing_trades_csv`, `unsafe_manifest`, `unsafe_summary`, `forbidden_field`,
`positive_legacy_max_drawdown`, `live_or_paper_requested`,
`order_or_balance_requested`, `private_api_requested`,
`missing_operator_confirmation`, `background_execution_requested`,
`output_path_in_package_release_area`, `generated_artifact_policy_violation`,
and `unknown_safety_violation`.

`command_text_preview` is preview-only and may show only the local saved-tape
fast path forms:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir "<signal_dir>"
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir "<signal_dir>"
```

`command_text_preview` must not include live, paper, order, secrets, token,
auth, balance, private API, or any execution instruction. It must not be
executed in Phase 18.

Future dry-run must require explicit operator confirmation. Confirmation text
must state local-only, not LIVE/PAPER/order, no private API, no order/balance
fetch, `signal_dir`, product `free`, symbol/timeframe, `output_dir`, and the
generated artifacts policy. Confirmation is separate from copy command UX, and
missing confirmation fails closed.

Phase 15/16 manual smoke record remains display-only. The local dry-run request
is a separate future artifact. Manual smoke record is not runtime/order proof,
and dry-run request is not execution proof. Screenshots remain optional and
sanitized only.

Phase 18 does not implement request builder. Phase 18 does not create request
files. Phase 18 does not execute local dry-run. Future Phase 19 may add request
builder docs/helper if explicitly approved. Future runtime dry-run execution
must be separate and explicitly approved. LIVE/PAPER/order remains permanently
separated.

## Phase 19 Local-Only Dry-Run Request Builder

Free Phase 19: local-only dry-run request builder docs/helper adds
`precomputed_signals_local_dry_run_request.py` and
`tests/test_precomputed_signals_local_dry_run_request_builder.py`. The request
builder creates a safe request preview only. It builds a request dict from an
existing read-only selection contract or selected `signal_dir`, formats a safe
text preview, and may write a safe JSON preview to a user-specified path.

The request builder does not execute dry-run. It does not run
backtest/runner/producer/inventory, does not use subprocess / QProcess /
background worker, does not add a GUI execution button, does not change GUI
source, and does not change runtime execution source. It does not connect to
LIVE/PAPER/order, MEXC private API, balance fetch, order fetch, or submit paths.
APP_VERSION remains unchanged.

Phase 19 request identity is:

- `request_type=precomputed_signal_local_dry_run_request`
- `request_schema_version=free_precomputed_local_dry_run_request_v1`
- `phase=free_precomputed_signals_phase19_request_builder`
- `product=free`

Allowed `dry_run_mode` values remain:

- `backtest_fast_path_local_only`
- `runner_replay_fast_path_local_only`

Required defaults remain:

- `operator_confirmation_required=true`
- `operator_confirmed=false by default`
- `preview_only_before_confirmation=true`
- `execution_enabled_after_confirmation=false`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

The helper fails closed for invalid selection, missing `signal_dir`, missing
manifest, missing summary, missing `trades.csv`, unsafe manifest, unsafe
summary, forbidden fields, positive legacy max_drawdown, LIVE/PAPER request,
order/balance request, private API request, missing operator confirmation,
background execution request, output paths under package/release areas, and
generated artifact policy violations. Future execution still requires separate
approval/phase.

Output directory safety rules:

- safe examples include
  `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals_dry_runs\free\<signal_set_id>`
  and test `tmp_path`.
- unsafe examples include package zip, exe, installer, setup/signing, release
  asset, raw market data, raw OHLCV, and generated runtime output areas inside
  the repo.

Allowed artifacts remain `safe_summary_json`, `fast_path_equity_curve_csv`,
`fast_path_trades_csv_safe_condition`, `fast_summary_json`,
`safe_metadata_log`, and `sanitized_manual_smoke_record`. Forbidden artifacts
remain `raw_market_data`, `raw_ohlcv`, `raw_trades_rows`, `entry_exec`,
`exit_exec`, `qty`, `trade_id`, `order_id`, `raw_order`,
`balance_snapshot`, `api_key`, `secret`, `token`, `authorization`,
`raw_billing`, `package_zip`, `exe`, `installer`, `release_asset`, and
`screenshots_with_secrets_or_balances_or_orders`.

The request builder output must not include raw trade rows, `entry_exec`,
`exit_exec`, `qty`, trade id, order id, raw order payload, balance snapshot, API
key, secret, token, authorization, raw billing, generated real tape body, raw
market data, generated dry-run outputs, screenshots, package zips, exe,
installers, or release assets. Generated request output not committed remains a
hard repo and migration zip rule.

## Phase 20 Local-Only Dry-Run GUI Preview Adapter

Free Phase 20: request builder read-only GUI preview adapter adds
`precomputed_signals_local_dry_run_gui_adapter.py` and
`tests/test_precomputed_signals_local_dry_run_gui_adapter.py`. The adapter
creates GUI preview item only from Phase 19 request builder output. It is a
read-only view-model helper for future GUI display and is not connected to the
GUI in Phase 20.

The adapter does not execute dry-run. It does not run
backtest/runner/producer/inventory, does not use subprocess / QProcess /
background worker, does not add a GUI execution button, does not change GUI
source, and does not change runtime execution source. GUI source is not
connected in Phase 20. It does not connect to LIVE/PAPER/order, MEXC private
API, balance fetch, order fetch, or submit paths. APP_VERSION remains
unchanged.

Phase 20 preview identity is:

- `gui_preview_schema_version=free_precomputed_local_dry_run_gui_preview_v1`
- `source_request_schema_version=free_precomputed_local_dry_run_request_v1`
- `request_type=precomputed_signal_local_dry_run_request`
- `phase=free_precomputed_signals_phase20_gui_preview_adapter`
- `product=free`

Required Phase 20 defaults remain:

- `operator_confirmed=false`
- `execution_enabled_after_confirmation=false`
- `operator_confirmation_required=true`
- `preview_only_before_confirmation=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

Required preview labels and warnings are:

- `Local dry-run request preview`
- `Preview only`
- `Execution disabled`
- `Operator confirmation required`
- `Not LIVE/PAPER/order`
- `No private API / no balance fetch / no order fetch`
- `This preview does not execute commands`
- `Future execution requires separate approved phase`

Allowed / forbidden artifacts are shown as safe labels only. The allowed label
summarizes safe summary, equity curve, and fast summary. The forbidden label
summarizes raw market data, raw trades rows, orders, balances, and secrets as
policy warnings only. Raw trade rows / entry_exec / exit_exec / qty / trade id
are not shown. API key, secret, token, authorization, raw order, balance
snapshot, raw billing, generated real tape body, raw market data, generated
dry-run GUI preview output, screenshots, package zips, exe, installers, and
release assets are not included.

The adapter fails closed for non-free product, invalid request type, invalid
dry-run mode, unsafe command preview text, `operator_confirmed=true`,
`execution_enabled_after_confirmation=true`, LIVE/PAPER/order text, order /
balance / private API text, unsafe output directory text, unexpected preview
fields, unknown fail-closed reasons, package/release paths, and generated
artifact policy violations. The adapter display is preview-only and adapter
display is not LIVE/PAPER/order.

Generated GUI preview output is not committed. Generated dry-run GUI preview
output is not committed. Future execution still requires separate
approval/phase.

## Phase 21 Local-Only Dry-Run GUI Preview Wiring

Free Phase 21: read-only GUI source wiring of dry-run request preview adapter
connects the Phase 20 local-only dry-run GUI preview adapter to Free GUI source
as display-only UI. The GUI displays dry-run request preview only for the
selected precomputed signal tape.

The GUI helper builds two in-memory preview items from a valid picker item:

- `backtest_fast_path_local_only` shown as `Backtest fast path local-only`.
- `runner_replay_fast_path_local_only` shown as
  `Runner replay fast path local-only`.

The read-only panel shows `Local dry-run request preview`, `Preview only`,
`Execution disabled`, `Operator confirmation required`,
`Operator confirmed: false`, `Future execution requires separate approved
phase`, `Not LIVE/PAPER/order`, `No private API / no balance fetch / no order
fetch`, and `request is not executable in this panel`. It also shows safe
display fields: `signal_dir`, `symbol`, `entry_tf`, `filter_tf`,
`signal_set_id`, `output_dir`, `command_text_preview`,
`allowed_artifacts_label`, `forbidden_artifacts_label`,
`fail_closed_reasons_label`, `status`, and `status_reason`.

Phase 21 keeps these defaults fixed:

- `operator_confirmed=false`
- `execution_enabled_after_confirmation=false`
- `preview_only_before_confirmation=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

Phase 21 does not implement dry-run execution, does not generate request files,
does not generate dry-run GUI preview output, does not run
backtest/runner/producer/inventory, does not use subprocess / QProcess / Popen /
startDetached / threading / multiprocessing / scheduler / background worker,
and does not add confirm, approve, run, execute, start, or dry-run buttons.

Phase 21 does not connect selected precomputed signal tape state to LIVE/PAPER/
order, MEXC private API, balance fetch, order fetch, submit paths, strategy,
indicators, exchange, risk, order runtime logic, DD calculation, entry/exit
timing, fee logic, PnL formulas, quantity logic, or signal timing. Runtime
execution source is unchanged. APP_VERSION unchanged. Package, exe, setup,
installer, signing, upload, and release assets are not touched.

Allowed / forbidden artifacts are shown as safe labels only. Generated dry-run
GUI preview output is not committed and is not included in migration zips. Raw
trade rows, `entry_exec`, `exit_exec`, `qty`, exact trade id, order id, raw
order payload, balance snapshot, API key, secret, token, authorization, raw
billing, raw market data, raw OHLCV, generated real tape body, screenshots,
package zips, exe, installers, setup binaries, runtime dirs, exports dirs, and
zip-in-zip artifacts are excluded from repo and zip.

## Phase 22 Local-Only Dry-Run Approval Boundary

Free Phase 22 is local-only dry-run execution approval boundary and is
docs/tests-only. The canonical boundary document is
`docs/precomputed_signals_gui_local_dry_run_approval_boundary.md`.

Phase 22 has no approval UI implementation, no dry-run execution
implementation, no approval record generation, no execution audit record
generation, no GUI source change, no runtime source change, no command
execution, no subprocess / QProcess / background worker, no producer/backtest/
runner/inventory auto-run, no LIVE/PAPER/order, no MEXC private API,
APP_VERSION unchanged, and package/release not touched.

Explicit operator approval required means a future operator confirmation for
local-only saved-tape backtest/replay fast path only. Approval is not
live/paper/order approval, not private API approval, not command copy approval,
not manual smoke record approval, and not release/package approval.

Future required pre-approval gates include valid selection contract, valid
request schema, `product == free`, dry_run_mode in allowed enum,
`operator_confirmed=false before confirmation`, `execution_enabled=false before
confirmation`, `not_selectable_for_live=true`, `not_selectable_for_paper=true`,
`safety_research_only=true`, `paper_live_order_execution=false`, manifest
present, summary present, `trades.csv` present, manifest hash present, summary
hash present, `trades.csv` hash from manifest present, forbidden fields
rejected, positive legacy max_drawdown rejected, output_dir safe, worktree
status recorded, package/release artifacts excluded, no background execution,
no private API, and no order/balance path.

Allowed execution modes remain `backtest_fast_path_local_only` and
`runner_replay_fast_path_local_only`. Not allowed modes remain `live`, `paper`,
`order_submit`, `order_fetch`, `balance_fetch`, `private_api`,
`producer_auto_run`, `inventory_auto_scan`, `background_worker`,
`release_packaging`, and `external_network`.

Future approval record schema uses
`precomputed_signal_local_dry_run_approval_record`,
`approval_scope=local_saved_tape_backtest_replay_only`, allowed
`approved_actions` of `approve_backtest_fast_path_local_only` and
`approve_runner_replay_fast_path_local_only`, required `forbidden_actions` for
LIVE/PAPER/order/private API/background/package release behavior, and status
values `draft`, `approved`, `rejected`, `blocked`, `invalid`, and `not_run`.
Phase 22 must not create approval record files.

Future execution audit record boundary uses
`precomputed_signal_local_dry_run_execution_record` and is separate from the
approval record. It must include `approval_record_hash`, `request_hash`,
started/finished timestamps, `dry_run_mode`, `result_status`,
`output_artifact_manifest`, `safe_summary`, `fail_closed_reason`,
`no_live_paper_order_assertion`, `no_private_api_assertion`, and
`no_background_execution_assertion`. It must not include raw trades rows,
`entry_exec`, `exit_exec`, `qty`, trade id, order id, raw order, balance, API
key, secret, token, authorization, raw billing, raw market data, or raw OHLCV.
Phase 22 must not create execution audit records.

Allowed artifacts after future approval are safe summary JSON,
`fast_summary.json`, `equity_curve.csv`, `trades.csv` only if safe-condition /
saved-tape-compatible and explicitly documented, local-only output manifest,
approval record, execution audit record, and sanitized manual smoke record.
Forbidden artifacts include raw market data, raw OHLCV, raw trades rows,
`entry_exec`, `exit_exec`, `qty`, `trade_id`, `order_id`, raw_order,
balance_snapshot, api_key, secret, token, authorization, raw_billing,
screenshots with secrets/balances/orders/account details, package_zip, exe,
installer, release_asset, runtime dirs copied into repo, and zip inside zip.

Future fail-closed conditions include operator confirmation missing,
confirmation text mismatch, invalid approval_scope, invalid request, invalid
selection, `product != free`, live/paper/order/private API requested,
background execution requested, unsafe output path, forbidden fields detected,
positive legacy max_drawdown detected, approval record missing in a future
execution phase, approval record must match request hash, unexpected worktree
status, and package/release artifact path involved.

Phase 9 command preview remains preview-only. Phase 10 copy UX remains
clipboard-only. Phase 15/16 manual smoke record remains display-only proof, not
execution proof. Phase 18 request schema is planning artifact, not execution
proof. Phase 19 request builder creates preview only. Phase 20 GUI preview
adapter creates display item only. Phase 21 GUI wiring displays preview only.
Phase 22 defines approval boundary only. Future execution requires separate
explicit approval phase.

## Phase 23 Local-Only Dry-Run Approval Checklist And Static Sample

Free Phase 23 is approval preflight checklist / approval record static sample
docs-tests and is docs/tests-only. The canonical checklist document is
`docs/precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md`.
The sanitized static sample is
`docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`.

Phase 23 has no approval UI implementation, no approval record generation at
runtime, no execution audit record generation, no dry-run execution, no GUI
source change, no runtime source change, no command execution, no subprocess /
QProcess / background worker, no producer/backtest/runner/inventory auto-run,
no LIVE/PAPER/order, no MEXC private API, APP_VERSION unchanged, and
package/release not touched.

The required preflight checklist fixes request type
`precomputed_signal_local_dry_run_request`, request schema valid,
request_hash present, selection_hash present, selection contract valid,
`product == free`, signal_dir present, symbol present, entry_tf / filter_tf
present, dry_run_mode allowed as `backtest_fast_path_local_only` or
`runner_replay_fast_path_local_only`, `not_selectable_for_live == true`,
`not_selectable_for_paper == true`, `safety_research_only == true`,
`paper_live_order_execution == false`, `operator_confirmation_required == true`,
manifest present, summary present, `trades.csv` present, manifest hash present,
summary hash present, `trades.csv` hash from manifest present, forbidden fields
rejected, positive legacy max_drawdown rejected, output_dir safe,
allowed_artifacts reviewed, forbidden_artifacts reviewed, worktree status
recorded, package/release artifacts excluded, no background execution, no
private API, no order/balance path, and no raw trade rows displayed or recorded.

The approval record schema uses
`record_type=precomputed_signal_local_dry_run_approval_record`,
`approval_schema_version=free_precomputed_local_dry_run_approval_record_v1`,
`approval_scope=local_saved_tape_backtest_replay_only`, approved_actions of
`approve_backtest_fast_path_local_only` and
`approve_runner_replay_fast_path_local_only`, and required forbidden_actions of
`live`, `paper`, `order_submit`, `order_fetch`, `balance_fetch`,
`private_api`, `producer_auto_run`, `inventory_auto_scan`,
`background_worker`, and `package_release`. Status values are `draft`,
`approved`, `rejected`, `blocked`, `invalid`, and `not_run`.

The Phase 23 static sample status is `not_run`, operator_confirmed is `false`,
and preflight_passed is `false`. request_hash / selection_hash are synthetic
placeholders. The static sample is sanitized, not generated runtime output, not
actual approval, not dry-run execution, and not an execution audit record.

Future confirmation text requirements include local-only, not LIVE/PAPER/order,
no MEXC private API, no balance fetch, no order fetch, no order submit,
selected precomputed signal tape only, product free, signal_dir, symbol,
entry_tf / filter_tf, dry_run_mode, output_dir, generated artifacts policy, and
operator understanding that this is not release/package approval.

Fail-closed reasons include `missing_request_hash`, `missing_selection_hash`,
`invalid_request`, `invalid_selection`, `invalid_approval_scope`,
`missing_confirmation_text`, `missing_operator_confirmation`,
`forbidden_action_requested`, `missing_forbidden_action`,
`live_or_paper_requested`, `order_or_balance_requested`,
`private_api_requested`, `background_execution_requested`,
`package_release_action_requested`, `unsafe_output_dir`, `forbidden_field`,
`positive_legacy_max_drawdown`, `worktree_status_unrecorded`, and
`unknown_safety_violation`.

Execution audit boundary: approval record is not execution audit record.
Approval record does not prove execution. Execution audit record must be
generated only in a future execution phase. Phase 23 must not create execution
audit record. Future execution audit must reference approval_record_hash and
request_hash, keep selection_hash linkage, and must assert no
LIVE/PAPER/order/private API/background execution.

Phase 18 request schema is planning artifact. Phase 19 request builder creates
preview only. Phase 20 GUI preview adapter creates display item only. Phase 21
GUI wiring displays preview only. Phase 22 approval boundary defines future
approval rules. Phase 23 provides static approval sample/checklist only. Future
execution requires separate explicit approval and implementation phase.

## Phase 24 Local-Only Dry-Run Execution Audit Schema And Static Sample

Free Phase 24 is execution audit record schema / static sample docs-tests and
is docs/tests-only. The canonical schema document is
`docs/precomputed_signals_gui_local_dry_run_execution_audit_schema.md`. The
sanitized static sample is
`docs/precomputed_signals_gui_local_dry_run_execution_audit_sample.json`.

Phase 24 has no approval UI implementation, no dry-run execution
implementation, no approval record generation at runtime, no execution audit
record generation at runtime, no GUI source change, no runtime source change,
no command execution, no subprocess / QProcess / background worker, no
producer/backtest/runner/inventory auto-run, no LIVE/PAPER/order, no MEXC
private API, APP_VERSION unchanged, and package/release not touched.

The execution audit record schema uses
`record_type=precomputed_signal_local_dry_run_execution_record` and
`execution_audit_schema_version=free_precomputed_local_dry_run_execution_audit_v1`.
It is a future artifact only for future approved local saved-tape
backtest/replay fast path execution. Execution audit record is not approval
record. Approval record does not prove execution. Execution audit record is not
order/trading record, not LIVE/PAPER trading record, and not release/package
proof.

approval_record_hash / request_hash linkage is mandatory. approval_record_hash
/ request_hash / selection_hash linkage is mandatory. Missing
approval_record_hash, missing request_hash, missing selection_hash,
approval_record_hash mismatch, request_hash mismatch, selection_hash mismatch,
or approval_scope mismatch must fail closed.

Allowed dry_run_mode values remain `backtest_fast_path_local_only` and
`runner_replay_fast_path_local_only`. Allowed result_status values are `pass`,
`fail`, `blocked`, `invalid`, and `not_run`. The Phase 24 static sample uses
`result_status=not_run`, null execution timestamps, zero duration, synthetic
approval_record_hash / request_hash / selection_hash placeholders, an empty
output_artifact_manifest, and sanitized notes that state no execution occurred.

The output artifact manifest schema contains safe local metadata only:
`artifact_type`, `path`, `sha256`, `size_bytes`, `safe_to_archive`,
`contains_raw_market_data`, `contains_raw_trade_rows`,
`contains_order_or_balance`, `contains_secret_or_auth`, and `notes_sanitized`.
Allowed artifact types are `safe_summary_json`, `fast_summary_json`,
`equity_curve_csv`, `fast_path_trades_csv_safe_condition`,
`safe_metadata_log`, `sanitized_manual_smoke_record`, and
`local_output_manifest`. Forbidden artifact types are `raw_market_data`,
`raw_ohlcv`, `raw_trades_rows`, `entry_exec_rows`, `exit_exec_rows`,
`qty_rows`, `order_payload`, `balance_snapshot`, `api_key`, `secret`, `token`,
`authorization`, `raw_billing`, `package_zip`, `exe`, `installer`,
`release_asset`, and `screenshot_with_sensitive_data`.

Required assertions are no LIVE/PAPER/order/private API/background execution,
no private API, no order/balance path, forbidden fields absent, raw trade rows
absent, generated artifacts policy checked, approval_record_hash verified, and
request_hash verified. Fail-closed reasons include `none`,
`approval_record_missing`, `approval_record_hash_mismatch`, `request_missing`,
`request_hash_mismatch`, `selection_hash_mismatch`, `approval_scope_mismatch`,
`invalid_request`, `invalid_selection`, `operator_confirmation_missing`,
`live_or_paper_requested`, `order_or_balance_requested`,
`private_api_requested`, `background_execution_requested`, `unsafe_output_dir`,
`forbidden_field_detected`, `positive_legacy_max_drawdown`,
`output_artifact_policy_violation`, `raw_trade_rows_detected`,
`missing_no_live_paper_order_assertion`,
`missing_no_private_api_assertion`,
`missing_no_background_execution_assertion`, and
`unknown_safety_violation`.

Phase 18 request schema is planning artifact. Phase 19 request builder creates
preview only. Phase 20 GUI preview adapter creates display item only. Phase 21
GUI wiring displays preview only. Phase 22 approval boundary defines future
approval rules. Phase 23 approval static sample/checklist does not execute
anything. Phase 24 execution audit schema defines future audit only. Future
execution requires separate explicit approval and implementation phase.

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
