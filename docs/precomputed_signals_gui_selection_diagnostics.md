# Free Phase 12 precomputed signal GUI selection diagnostics

Free Phase 12 adds a read-only selection diagnostics display for the explicitly
selected precomputed signal tape `signal_dir`.

## Contract

- Free Phase 12 is a read-only selection diagnostics display.
- The diagnostics are read-only and are derived from manifest / summary /
  selection safe metadata.
- The diagnostics panel displays validation status, safe invalid reason,
  safe error code, safety flags, fast-path availability, file presence, and
  manifest / summary hash metadata.
- `trades.csv raw rows are not read or displayed`.
- `manifest_sha256`, `summary_sha256`, and the manifest
  `trades_csv_sha256` metadata may be displayed.
- The GUI does not execute commands.
- The GUI does not run producer / backtest / runner / inventory automatically.
- The GUI does not connect to LIVE / PAPER / order execution.
- The GUI does not connect to MEXC API, balance fetch, order fetch, or order
  submit paths.
- The GUI does not add `subprocess`, `os.system`, `QProcess`, `Popen`,
  `startDetached`, threading, multiprocessing, scheduler, or background worker
  execution for diagnostics.

## Display Fields

Allowed diagnostics fields are limited to safe aggregate and metadata values:

- `signal_dir`
- `status`
- `status_reason`
- `safe_error_code`
- `product`
- `symbol`
- `symbol_normalized`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `dataset_id`
- `created_at_utc`
- `since_ms`
- `until_ms`
- `trade_count`
- `net_total`
- `final_equity`
- `max_dd_display_abs`
- `max_dd_display_pct`
- `max_dd_display_label`
- `max_drawdown_legacy_note`
- `safety_research_only`
- `safety_paper_live_order_execution`
- `selectable_for_backtest_fast_path`
- `selectable_for_runner_replay_fast_path`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `tape_files_present`
- `manifest_sha256`
- `summary_sha256`
- `trades_csv_sha256_from_manifest`
- `diagnostics_warning`
- `picker_warning`

The diagnostics text states `Selection diagnostics`, `LIVE/PAPER: not
selectable`, `No raw trade rows are displayed`, and `This panel does not execute
commands`.

## Safety

Diagnostics must not include raw trades rows, `entry_exec`, `exit_exec`,
row-level `qty`, exact trade id, order id, raw order payload, balance snapshot,
API key, secret, token, authorization header, raw billing payload, raw market
data, raw OHLCV, generated diagnostics output, generated GUI adapter output,
generated selection output, generated command preview output, generated
clipboard output, generated real signal tape body, or raw market data.

The selected picker item remains research-only:

- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `selectable_for_backtest_fast_path=true` only for valid safe selections
- `selectable_for_runner_replay_fast_path=true` only for valid safe selections

Invalid selections show only safe status, safe error code, status reason, and
warning text. They do not expose raw payloads.

## DD Display

DD display prefers `max_dd_abs / max_dd_pct` and the GUI-facing
`max_dd_display_abs / max_dd_display_pct` fields. `max_drawdown` remains the
signed negative legacy field and is not used as the positive display value.

## Out Of Scope

Phase 12 does not change `APP_VERSION`, package zips, exe, setup, installer,
signing, release assets, strategy, indicators, exchange, risk, order runtime
logic, entry timing, exit timing, fee logic, quantity logic, PnL formulas,
signal timing, or DD calculation.

Generated real signal tape body, raw market data, generated inventory output,
generated selection output, generated GUI adapter output, generated command
preview output, generated clipboard output, generated diagnostics output,
package zips, executables, installers, runtime dirs, exports dirs, and zip-in-zip
artifacts must not be committed to the repo or included in migration zips.
