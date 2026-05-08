# Free Phase 8 precomputed signal GUI picker wiring

Free Phase 8 wires the precomputed signal tape GUI picker as a display-only
connection.

## Behavior

- The GUI lets an operator explicitly choose a `signal_dir`.
- The GUI builds a safe picker item through the Phase 7 adapter schema.
- The GUI displays symbol, timeframe, dataset, trade count, `net_total`,
  `final_equity`, DD display fields, and picker safety flags.
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
- No producer/backtest/runner/inventory launch button is added.

## Flags

Valid picker items are display-only candidates for later explicit replay or
backtest work:

- `selectable_for_backtest_fast_path=true`
- `selectable_for_runner_replay_fast_path=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`

The GUI displays the warning as `Replay/backtest only` and
`Not selectable for LIVE/PAPER`.

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
formulas, signal timing, or DD calculation.

Generated real signal tape bodies, raw market data, generated inventory outputs,
generated selection outputs, generated GUI adapter outputs, package zips,
executables, installers, release assets, and zip-in-zip artifacts must not be
committed to the repo or included in migration zips.

Future phases may consider explicit backtest/replay action wiring from this
picker item, but that work must stay separated from LIVE/PAPER/order paths.
