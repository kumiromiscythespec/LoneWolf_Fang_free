# Free Phase 8/9/10 precomputed signal GUI picker wiring

Free Phase 8 wires the precomputed signal tape GUI picker as a display-only
connection. Free Phase 9 keeps that connection display-only and adds
command preview text for the selected safe `signal_dir`. Free Phase 10 adds
copy UX only for those preview command strings.

## Behavior

- The GUI lets an operator explicitly choose a `signal_dir`.
- The GUI builds a safe picker item through the Phase 7 adapter schema.
- The GUI displays symbol, timeframe, dataset, trade count, `net_total`,
  `final_equity`, DD display fields, and picker safety flags.
- The GUI displays backtest/replay command previews for a valid selected
  `signal_dir`.
- The GUI exposes copy helpers for `Copy backtest command` and
  `Copy replay command` only.
- The GUI keeps the selected `signal_dir`, safe picker item, status, and warning
  in memory only.

## Safety Boundary

- The GUI does not read raw `trades.csv` rows.
- The GUI does not display `entry_exec`, `exit_exec`, row-level quantity, exact
  trade id, order id, raw order payload, balance snapshot, API key, secret,
  token, authorization header, raw billing payload, raw market data, raw OHLCV,
  or generated signal tape body.
- The GUI does not run `precompute_signals.py`, `backtest.py`, `runner.py`, or
  `precomputed_signals_inventory.py` automatically.
- The GUI does not connect the selected tape to LIVE, PAPER, order creation,
  order fetch, order submit, balance fetch, MEXC private APIs, exchange clients,
  or `ccxt`.
- The command preview does not include raw trade rows, `entry_exec`,
  `exit_exec`, row-level `qty`, exact trade ids, order ids, raw orders,
  balances, API keys, secrets, tokens, authorization headers, raw billing, raw
  market data, raw OHLCV, generated command preview outputs, or generated real
  signal tape bodies.
- No producer/backtest/runner/inventory launch button is added.
- The copy UX writes only preview command text to the clipboard after an
  operator action. It does not execute the command and does not invoke producer,
  backtest, runner, inventory, selection, or adapter commands automatically.

## Flags

Valid picker items are display-only candidates for later explicit replay or
backtest work:

- `selectable_for_backtest_fast_path=true`
- `selectable_for_runner_replay_fast_path=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

The GUI displays the warning as `Replay/backtest only` and
`Not selectable for LIVE/PAPER`.

## Phase 9 Command Preview Only

The GUI command preview is generated from the already selected safe picker item
and its `signal_dir`. It is a view model for display/copy only and is never used
as an execution path.

Backtest command preview:

```powershell
python backtest.py --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report
```

Runner replay command preview:

```powershell
python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report
```

The preview view model exposes safe text and argv fields only:

- `signal_dir`
- `backtest_command_text`
- `runner_replay_command_text`
- `backtest_argv`
- `runner_replay_argv`
- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`
- `warning`
- `source_symbol`
- `source_tf_pair`

The panel text states `Backtest / replay preview only`,
`Execution is disabled in this panel`, and `Not selectable for LIVE/PAPER`.
Japanese UI may show `バックテスト用コマンドプレビュー`,
`リプレイ用コマンドプレビュー`, `このパネルからは実行しません`,
and `LIVE/PAPER には使用不可`.

The GUI does not execute the preview command. It does not run producer,
backtest, runner, inventory, selection, or adapter commands automatically. It
does not add `subprocess`, `os.system`, `QProcess`, `Popen`, multiprocessing,
threading, scheduler, or background worker execution for this preview. Existing
run/live/paper/order paths do not consume the selected signal tape or the
preview view model.

Invalid picker items, empty `signal_dir`, `status != valid`, or missing
`not_selectable_for_live=true` / `not_selectable_for_paper=true` fail closed and
show a safe warning without command text. The GUI never generates LIVE or PAPER
commands from a selected precomputed signal tape.

## Phase 10 Command Copy UX Only

Free Phase 10 adds copy-ready controls next to the existing command preview:

- `Copy backtest command`
- `Copy replay command`

The copy state is derived from the existing command preview view model and
preserves these invariants:

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`

The copy helper returns only `backtest_command_text` or
`runner_replay_command_text`. The backtest command must include
`--use-precomputed-signals` and `--precomputed-signals-dir`. The runner replay
command must include `--mode replay`, `--use-precomputed-signals`, and
`--precomputed-signals-dir`.

Invalid preview state, empty command text, `execution_enabled=true`,
`preview_only!=true`, `live_command_available=true`, or
`paper_command_available=true` disables copy. Live and paper command kinds fail
closed and are never copyable.

The UI status states `Preview only`, `Execution disabled`,
`Not selectable for LIVE/PAPER`, `This panel does not execute commands`, and
`Copied` after a successful user-triggered clipboard write. Japanese UI may show
`バックテストコマンドをコピー`, `リプレイコマンドをコピー`, `コピーしました`,
`プレビュー専用`, `このパネルからは実行しません`, and
`LIVE/PAPER には使用不可`.

Phase 10 does not add any run, execute, start, producer, backtest, runner,
inventory, scheduler, worker, subprocess, `os.system`, `QProcess`, `Popen`,
`startDetached`, threading, or multiprocessing execution path. Existing
run/live/paper/order paths do not consume the selected signal tape, command
preview, or copy state.

The copy UX does not expose raw `trades.csv` rows, row-level `entry_exec`,
`exit_exec`, row-level `qty`, exact trade ids, order ids, raw order payloads,
balance snapshots, API keys, secrets, tokens, authorization headers, raw
billing payloads, raw market data, raw OHLCV, generated command preview output,
generated clipboard output, or generated real signal tape bodies.

Phase 10 does not connect to LIVE, PAPER, order creation, order fetch, order
submit, balance fetch, MEXC private APIs, exchange clients, or `ccxt`. It does
not change `APP_VERSION`, strategy, indicators, exchange, risk, order runtime
logic, entry timing, exit timing, fee logic, quantity logic, PnL formulas,
signal timing, or DD calculation.

## Drawdown Display

DD display uses the non-negative display fields first:

- `max_dd_display_abs`, falling back to `max_dd_abs`
- `max_dd_display_pct`, falling back to `max_dd_pct`
- `max_dd_display_label = "Max DD (abs, display)"`

`max_drawdown` remains the signed negative legacy compatibility field, and
`max_drawdown == max_dd_signed`. A positive legacy-only `max_drawdown` must not
be adopted as GUI display DD.

## Root

The canonical root remains:

`%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals`

The environment override remains:

`LWF_PRECOMPUTED_SIGNALS_ROOT`

The Free product path remains:

`<root>\free\<symbol_normalized>\<entry_tf>_<filter_tf>\<signal_set_id>`

## Out Of Scope

Phase 8 does not change `APP_VERSION`, strategy, indicators, exchange, risk,
order runtime logic, entry timing, exit timing, fee logic, quantity logic, PnL
formulas, signal timing, or DD calculation. Phase 9 and Phase 10 keep the same
boundary.

Generated real signal tape bodies, raw market data, generated inventory outputs,
generated selection outputs, generated GUI adapter outputs, package zips,
executables, installers, release assets, and zip-in-zip artifacts must not be
committed to the repo or included in migration zips. Generated command preview
outputs and generated clipboard/copy outputs must also stay out of the repo and
migration zips.

Future phases may refine explicit backtest/replay workflows, but any execution
wiring must remain explicit and must stay separated from LIVE/PAPER/order paths.
