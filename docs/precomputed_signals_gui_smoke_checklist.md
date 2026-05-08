# Free Phase 14 GUI smoke checklist

Free Phase 14 is GUI smoke checklist / docs-only hardening for the precomputed
signal tape GUI. It fixes the operator checklist and static documentation
policy only.

## Scope

- Phase 14 is GUI smoke checklist / docs-only hardening.
- No runtime source change.
- No GUI source change.
- No execution path change.
- No LIVE/PAPER/order connection.
- The GUI does not execute commands.
- The GUI does not auto-run producer/backtest/runner/inventory.
- The GUI does not connect to LIVE/PAPER/order.
- No subprocess / QProcess / background worker is part of this phase.
- APP_VERSION unchanged.
- Package/release not touched.
- Generated real tape body / raw market data excluded.
- `max_drawdown` remains signed negative legacy field.
- GUI display prefers `max_dd_abs / max_dd_pct` and
  `max_dd_display_abs / max_dd_display_pct`.
- Raw trade rows are never displayed.

## Pre-Smoke Requirements

- Confirm the Free repo branch, HEAD, and worktree status before any check.
- Treat `LoneWolf_Fang_Free_Package.zip` as an existing untracked package
  artifact; do not touch it and do not stage it.
- APP_VERSION must remain unchanged.
- Use only committed test fixtures or synthetic tape fixtures.
- Do not place real market data or generated real tape body in the repo.
- Do not create package, exe, setup, installer, signing, upload, runtime export,
  generated smoke output, generated diagnostics output, generated inventory
  output, generated selection output, generated GUI adapter output, generated
  command preview output, generated clipboard output, or zip-in-zip artifacts.

## Safe GUI Smoke Boundary

GUI smoke is optional. When it is performed, it is limited to visual inspection
of the precomputed signal tape panel, selection diagnostics, command preview,
copy UX, warnings, and disabled states.

- GUI smoke must not start LIVE.
- GUI smoke must not start PAPER.
- GUI smoke must not submit/fetch orders.
- GUI smoke must not fetch balance.
- GUI smoke must not call MEXC private API.
- GUI smoke must not auto-run producer.
- GUI smoke must not auto-run backtest.
- GUI smoke must not auto-run runner.
- GUI smoke must not auto-run inventory scan.
- GUI smoke must not launch selection or adapter commands in the background.
- Command preview is preview-only.
- Copy UX is clipboard-only.
- The panel is a preview/copy surface only.
- Operators run copied commands manually in an external terminal only if needed.

Phase 14 adds no `subprocess`, `os.system`, `QProcess`, `Popen`,
`startDetached`, threading, multiprocessing, scheduler, launch button, worker,
or background worker execution path.

## Expected Visible Labels

The following visible labels or equivalent safe status text are expected for
manual smoke without needing screenshots:

- `Precomputed Signal Tape`
- `Selection diagnostics`
- `Selection diagnostics: valid`
- `Selection diagnostics: invalid`
- `Safe error code`
- `Files: manifest OK / summary OK / trades.csv OK`
- `Safety: research-only OK / no live-paper execution OK`
- `Fast path: Backtest OK / Replay OK`
- `LIVE/PAPER: not selectable`
- `Max DD (abs, display)`
- `Max DD pct (display)`
- `No raw trade rows are displayed`
- `This panel does not execute commands`
- `Preview only`
- `Execution disabled`
- `Copy backtest command`
- `Copy replay command`
- `Copied`
- `Run this command manually in a terminal if needed`

## Expected Disabled And Unavailable States

- `live_command_available=false`
- `paper_command_available=false`
- `execution_enabled=false`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- Invalid selection disables command copy.
- Missing manifest shows a safe error code only.
- Missing summary shows a safe error code only.
- Missing `trades.csv` shows a safe error code only.
- LIVE/PAPER command text is unavailable.
- Order execution, order fetch, order submit, and balance fetch remain
  unavailable from the precomputed signal tape panel.

## Fields That May Be Displayed

- `signal_dir`
- `symbol`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `dataset_id`
- `trade_count`
- `net_total`
- `final_equity`
- `max_dd_display_abs`
- `max_dd_display_pct`
- `max_dd_display_label`
- `status`
- `safe_error_code`
- manifest hash from manifest / safe metadata
- summary hash from manifest / safe metadata
- `trades.csv` hash from manifest
- compact path
- compact hash

## Fields That Must Not Be Displayed

- raw trades rows
- `entry_exec`
- `exit_exec`
- `qty`
- exact trade id
- order id
- raw order
- balance
- API key
- secret
- token
- authorization
- raw billing
- raw market data
- raw OHLCV
- generated real signal tape body
- generated diagnostics output
- generated smoke output
- generated command preview output
- generated clipboard output

## Operator Pass Criteria

Pass when all of these are true:

- The GUI panel shows the expected labels, warnings, disabled states, and
  diagnostics text.
- Valid selection shows `Selection diagnostics: valid`.
- Invalid selection shows `Selection diagnostics: invalid` and a safe error
  code only.
- File presence, safety, fast-path availability, and LIVE/PAPER exclusion are
  visible.
- DD display uses `Max DD (abs, display)` and `Max DD pct (display)`.
- Copy buttons are preview clipboard helpers only.
- Screenshots are optional; text confirmation is enough.
- Static/helper tests pass even when GUI runtime smoke is not performed.

## Operator Fail Criteria

Fail and stop when any of these occur:

- LIVE starts.
- PAPER starts.
- An order path starts.
- MEXC private API is called.
- Balance fetch, order fetch, or order submit is attempted.
- Producer, backtest, runner, inventory, selection, or adapter starts
  automatically.
- A subprocess, `QProcess`, `Popen`, `startDetached`, thread, process,
  scheduler, worker, or background worker is launched by the smoke path.
- Raw trades rows or forbidden private/runtime fields are visible.
- APP_VERSION, package, release, setup, installer, signing, or upload state is
  changed.
- Real market data, generated real tape body, or generated smoke output is
  added to the repo or migration zip.

Stop conditions are intentionally strict: if any live/paper/order path starts,
the smoke fails immediately and the operator stops the check.

## Runtime Smoke Boundary

GUI runtime smoke is optional for Phase 14. If it is run, it is limited to
visual display checks for selection diagnostics, command preview, copy UX, and
disabled states. It must not use real market data or generated real tape body.
It must not start any LIVE/PAPER/order path, and it must not execute or schedule
commands from the GUI.

## Static And Helper Test Boundary

When GUI runtime smoke is not performed, the Phase 14 static docs-policy test
records that:

- The checklist exists.
- GUI smoke is optional.
- LIVE/PAPER/order startup is forbidden.
- Producer/backtest/runner/inventory auto-run is forbidden.
- Command preview is preview-only.
- Copy UX is clipboard-only.
- No subprocess / QProcess / background worker boundary is documented.
- APP_VERSION unchanged and package/release not touched boundaries are
  documented.
- Expected labels, warnings, disabled states, displayed fields, forbidden
  fields, pass criteria, fail criteria, and future phase boundaries are fixed.
- GUI source has no Phase 14 added execution, network, LIVE/PAPER, or order
  connection lines.

## Future Phase Boundary

Future execution button is not part of Phase 14. Backtest/replay execution from the GUI requires a future explicit confirmation / dry-run / local-only phase.
LIVE/PAPER/order must remain separated from precomputed signal tape selection,
diagnostics, command preview, and copy UX.

## Phase 15 Manual Smoke Record Boundary

Free Phase 15 is synthetic fixture only manual GUI runtime smoke record format.
It is docs/test-only. GUI runtime smoke is not executed in Phase 15.

The Phase 15 record format is fixed in
`docs/precomputed_signals_gui_manual_smoke_record.md`. It keeps future manual
GUI runtime smoke records synthetic fixture only and safe metadata only.

Phase 15 does not change GUI source, runtime source, execution paths,
APP_VERSION, package/release state, setup, installer, exe, signing, release
assets, strategy, indicators, exchange, risk, order runtime logic, signal
timing, or DD calculation.

The record format preserves these boundaries:

- no command execution
- no producer/backtest/runner/inventory auto-run
- no selection or adapter auto-run
- no LIVE/PAPER/order
- no MEXC private API
- no subprocess / QProcess / background worker
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch button, worker, or background worker path
- APP_VERSION unchanged
- package/release not touched
- no real market data
- no generated real signal tape body
- raw trade rows are never recorded
- `max_drawdown` remains signed negative legacy field
- GUI display prefers `max_dd_abs / max_dd_pct`

Phase 15 record `result_status` is limited to `pass`, `fail`, `blocked`, and
`not_run`. `smoke_mode` is limited to `synthetic_fixture_display_only`,
`docs_static_check_only`, and `not_run`. `fixture_type` is limited to
`synthetic_signal_tape`, `synthetic_selection_contract`,
`synthetic_picker_item`, and `none`.

Screenshots optional. Screenshots must not include secrets / balances / orders,
account details, raw trades rows, raw market data, or generated real tape body.
Screenshots must be omitted from repo and zip unless explicitly sanitized.

Future local-only dry-run is future phase. Execution button is future phase.
LIVE/PAPER/order and packaging/release remain separated.
