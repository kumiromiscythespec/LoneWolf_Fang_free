# Free Phase 13 GUI smoke procedure

Free Phase 13 is operator-facing diagnostics polish for the precomputed signal
tape GUI picker. The diagnostics remain read-only.

GUI smoke is optional. Run it only when a local GUI check is useful and keep the
scope to selected signal tape display, diagnostics text, command preview text,
and copy control visibility.

## Safety boundary

- GUI smoke does not start LIVE / PAPER / order execution.
- GUI smoke does not start producer / backtest / runner / inventory
  automatically.
- GUI smoke does not connect to MEXC API, balance fetch, order fetch, order
  submit, exchange clients, or `ccxt`.
- GUI smoke does not execute command preview text.
- Command preview/copy is not execution; copy buttons only copy safe preview
  text after an operator action.
- The selected signal tape remains `not_selectable_for_live=true` and
  `not_selectable_for_paper=true`.
- APP_VERSION is not changed by this smoke procedure.
- Generated real signal tape body / raw market data must not be added to the
  repo, migration zip, package, exe, setup, installer, runtime dirs, exports
  dirs, or zip-in-zip artifacts.
- The phrase `generated real signal tape body / raw market data` is a hard
  exclusion for repo and migration zip contents.

## Manual check items

- selection diagnostics visible
- valid/invalid status visible
- file presence compact display visible
- hash compact display visible
- no raw trade rows visible
- Backtest/Replay command preview visible
- Copy buttons visible but no execution
- Not selectable for LIVE/PAPER visible
- `This panel does not execute commands` visible
- selected `signal_dir` display is compact, while full path is available only
  as safe details / tooltip text

## Expected operator boundary

The GUI is a display and copy surface only. If an operator wants to run a
previewed Backtest or Replay command, they manually review the copied command
and run it in an external terminal. The GUI does not run producer / backtest /
runner / inventory, does not launch selection or adapter commands, and does not
start any background worker, scheduler, subprocess, `QProcess`, `Popen`,
`startDetached`, threading, or multiprocessing path for Phase 13 diagnostics.

Diagnostics display manifest / summary / selection safe metadata only. Raw
`trades.csv` rows, `entry_exec`, `exit_exec`, row-level `qty`, exact trade id,
raw order payload, balance snapshot, API key, secret, token, authorization
header, raw billing payload, raw market data, generated diagnostics output,
generated GUI adapter output, generated selection output, generated command
preview output, generated clipboard output, and generated real signal tape body
are not displayed.
