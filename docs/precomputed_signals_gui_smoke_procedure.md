# Free Phase 14 GUI smoke procedure

Free Phase 14 is GUI smoke checklist / docs-only hardening for the precomputed
signal tape GUI picker. It updates documentation and static policy tests only.
The GUI source and runtime source remain unchanged, and the diagnostics remain
read-only.

GUI smoke is optional. Run it only when a local GUI check is useful and keep the
scope to selected signal tape display, diagnostics text, command preview text,
and copy control visibility.

## Safety boundary

- GUI smoke does not start LIVE / PAPER / order execution.
- GUI smoke does not submit/fetch orders.
- GUI smoke does not fetch balance.
- GUI smoke does not call MEXC private API.
- GUI smoke does not start producer / backtest / runner / inventory
  automatically.
- GUI smoke does not start selection or adapter commands in the background.
- GUI smoke does not connect to MEXC API, balance fetch, order fetch, order
  submit, exchange clients, or `ccxt`.
- GUI smoke does not execute command preview text.
- Command preview is preview-only.
- Copy UX is clipboard-only.
- Command preview/copy is not execution; copy buttons only copy safe preview
  text after an operator action.
- The selected signal tape remains `not_selectable_for_live=true` and
  `not_selectable_for_paper=true`.
- `live_command_available=false`, `paper_command_available=false`, and
  `execution_enabled=false` remain expected disabled states.
- APP_VERSION unchanged by this smoke procedure.
- Package/release not touched.
- Generated real signal tape body / raw market data must not be added to the
  repo, migration zip, package, exe, setup, installer, runtime dirs, exports
  dirs, or zip-in-zip artifacts.
- The phrase `generated real signal tape body / raw market data` remains a hard
  exclusion for repo and migration zip contents.
- The phrase `generated real tape body / raw market data excluded` is a hard
  exclusion for repo and migration zip contents.
- Phase 14 adds no subprocess / QProcess / background worker path and no
  `os.system`, `Popen`, `startDetached`, threading, multiprocessing, scheduler,
  worker, or launch button path.

## Manual check items

- `Precomputed Signal Tape` visible
- selection diagnostics visible
- valid/invalid status visible
- `Selection diagnostics: valid` visible for a valid fixture
- `Selection diagnostics: invalid` visible for an invalid fixture
- `Safe error code` visible for invalid selection
- file presence compact display visible
- hash compact display visible
- no raw trade rows visible
- `Files: manifest OK / summary OK / trades.csv OK` visible
- `Safety: research-only OK / no live-paper execution OK` visible
- `Fast path: Backtest OK / Replay OK` visible for a valid fixture
- Backtest/Replay command preview visible
- Copy buttons visible but no execution
- Not selectable for LIVE/PAPER visible
- `Max DD (abs, display)` visible
- `Max DD pct (display)` visible
- `Preview only` visible
- `Execution disabled` visible
- `Copy backtest command` visible
- `Copy replay command` visible
- `Copied` may appear only after user-triggered clipboard copy
- `This panel does not execute commands` visible
- `Run this command manually in a terminal if needed` visible
- selected `signal_dir` display is compact, while full path is available only
  as safe details / tooltip text

## Expected operator boundary

The GUI is a display and copy surface only. If an operator wants to run a
previewed Backtest or Replay command, they manually review the copied command
and run it in an external terminal. The GUI does not run producer / backtest /
runner / inventory, does not launch selection or adapter commands, and does not
start any background worker, scheduler, subprocess, `QProcess`, `Popen`,
`startDetached`, threading, or multiprocessing path for Phase 14 smoke.

Diagnostics display manifest / summary / selection safe metadata only. Raw
`trades.csv` rows, `entry_exec`, `exit_exec`, row-level `qty`, exact trade id,
raw order payload, balance snapshot, API key, secret, token, authorization
header, raw billing payload, raw market data, generated diagnostics output,
generated GUI adapter output, generated selection output, generated command
preview output, generated clipboard output, and generated real signal tape body
are not displayed.

## Pass / fail criteria

Pass when the expected labels, warnings, disabled states, valid/invalid
diagnostics, safe error code, display-only DD fields, and copy-only controls are
visible and no execution path starts. Screenshots are optional.

Fail and stop if any LIVE, PAPER, order, MEXC private API, balance fetch, order
fetch, order submit, producer, backtest, runner, inventory, selection, adapter,
subprocess, `QProcess`, `Popen`, `startDetached`, thread, process, scheduler,
worker, or background worker path starts. Fail if raw trades rows or forbidden
private/runtime fields are displayed.

## Future phase boundary

Future execution button is not part of Phase 14. Backtest/replay execution from
the GUI requires a future explicit confirmation / dry-run / local-only phase.
LIVE/PAPER/order must remain separated.

## Phase 15 manual smoke record format

Free Phase 15 is synthetic fixture only manual GUI runtime smoke record format.
It is docs/test-only. GUI runtime smoke is not executed in Phase 15.

Use `docs/precomputed_signals_gui_manual_smoke_record.md` as the only Phase 15
record template. The template is safe metadata only and screenshots optional.
It records branch, HEAD, APP_VERSION, worktree status, synthetic fixture path,
`signal_dir`, safe status, checked labels, checked disabled states, and whether
any forbidden execution or private/network path was observed.

The Phase 15 record procedure is constrained to synthetic fixture only
display-only smoke. It documents:

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

The allowed `result_status` enum is `pass`, `fail`, `blocked`, and `not_run`.
The allowed `smoke_mode` enum is `synthetic_fixture_display_only`,
`docs_static_check_only`, and `not_run`. The allowed `fixture_type` enum is
`synthetic_signal_tape`, `synthetic_selection_contract`,
`synthetic_picker_item`, and `none`.

Screenshots must not include secrets / balances / orders, account details, raw
trade rows, raw market data, raw OHLCV, generated real tape body, raw order
payloads, API key, secret, token, authorization, or raw billing. Screenshots
must be omitted from repo and zip unless explicitly sanitized.

Future local-only dry-run is future phase. Execution button is future phase.
LIVE/PAPER/order and packaging/release remain separated.

## Phase 16 schema validator and static sample

Free Phase 16 is synthetic manual smoke record schema validator / sample static
fixture. Phase 16 is docs/tests-only. GUI runtime smoke is not executed in
Phase 16, and GUI runtime smoke not executed in Phase 15/16 remains an explicit
procedure boundary.

Use `docs/precomputed_signals_gui_manual_smoke_record_sample.json` only as a
static sanitized sample. It is not generated runtime output, not a generated
smoke record, not a screenshot reference, and not proof of a GUI runtime smoke
run.

The schema validator is a docs-policy test only. It validates required fields,
allowed enum values, safe boolean types, pass/not_run invariants, forbidden
field policy, screenshot policy, and the no-execution/no-network boundaries.
It does not add a production runtime validator and does not call producer,
backtest, runner, inventory, selection, adapter, LIVE, PAPER, order, balance,
MEXC private API, subprocess, `QProcess`, scheduler, thread, process, worker,
or background worker paths.

Future local-only dry-run design is future phase. APP_VERSION, package/release,
setup, installer, exe, signing, release assets, generated real tape body, raw
market data, generated smoke records, and screenshots remain outside Phase 16.
