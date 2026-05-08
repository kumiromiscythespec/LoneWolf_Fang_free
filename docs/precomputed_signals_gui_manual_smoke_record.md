# Free Phase 15 GUI manual smoke record format

Free Phase 15 is synthetic fixture only manual GUI runtime smoke record format
for the precomputed signal tape GUI. It is docs/test-only. GUI runtime smoke is
not executed in Phase 15.

This document fixes how a future operator may record a safe manual GUI runtime
smoke result without changing GUI source, runtime source, or execution
behavior.

## Scope

- Phase 15 is synthetic fixture only manual GUI runtime smoke record format.
- Phase 15 is docs/test-only.
- GUI runtime smoke is not executed in Phase 15.
- No GUI source change.
- No runtime source change.
- No execution path change.
- No LIVE/PAPER/order connection.
- Manual smoke records are safe metadata only.
- Synthetic fixture only.
- No real market data.
- No generated real signal tape body.
- No command execution.
- No producer/backtest/runner/inventory auto-run.
- No selection or adapter auto-run.
- No LIVE/PAPER/order.
- No MEXC private API.
- No subprocess / QProcess / background worker.
- No `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, or launch worker.
- APP_VERSION unchanged.
- Package/release not touched.
- Raw trade rows are never recorded.
- `max_drawdown` remains signed negative legacy field.
- GUI display prefers `max_dd_abs / max_dd_pct`.

## Smoke Mode

The record describes a future manual display check only:

- record type: `manual_gui_runtime_smoke`
- fixture scope: `synthetic_fixture_only`
- display scope: `display_only`
- no command execution
- no producer auto-run
- no backtest auto-run
- no runner auto-run
- no inventory auto-run
- no selection auto-run
- no adapter auto-run
- no MEXC private API
- no order submit/fetch
- no balance fetch

The GUI remains a display and copy surface. Operators may inspect labels,
disabled states, diagnostics, command preview text, and clipboard-only copy UX,
but the GUI must not run the previewed command.

## Required Pre-Smoke Checks

Before a future manual GUI runtime smoke is recorded, the operator records safe
metadata only:

- branch
- HEAD
- APP_VERSION
- worktree status
- package zip not staged
- generated outputs not staged
- synthetic fixture path
- `signal_dir` points to synthetic fixture only
- LIVE/PAPER/order disabled / not started
- network/private API not used

The operator must stop and mark the record `blocked` when the branch, HEAD,
APP_VERSION, worktree status, package artifact boundary, generated output
boundary, synthetic fixture source, or LIVE/PAPER/order disabled state cannot be
confirmed.

## Safe Record Schema

Required fields:

- `schema_version`
- `record_type`
- `phase`
- `repo`
- `branch`
- `head_commit`
- `app_version`
- `recorded_at_utc`
- `operator`
- `smoke_mode`
- `fixture_type`
- `fixture_origin`
- `signal_dir`
- `product`
- `symbol`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `selection_status`
- `safe_error_code`
- `expected_labels_checked`
- `disabled_states_checked`
- `command_preview_checked`
- `copy_ux_checked`
- `diagnostics_checked`
- `live_paper_boundary_checked`
- `raw_trade_rows_not_visible`
- `command_execution_observed`
- `producer_auto_run_observed`
- `backtest_auto_run_observed`
- `runner_auto_run_observed`
- `order_path_observed`
- `balance_fetch_observed`
- `private_api_observed`
- `result_status`
- `result_reason`
- `notes_sanitized`

Allowed `result_status` enum:

- `pass`
- `fail`
- `blocked`
- `not_run`

Allowed `smoke_mode` enum:

- `synthetic_fixture_display_only`
- `docs_static_check_only`
- `not_run`

Allowed `fixture_type` enum:

- `synthetic_signal_tape`
- `synthetic_selection_contract`
- `synthetic_picker_item`
- `none`

The template below is valid without screenshots:

```yaml
schema_version: 1
record_type: manual_gui_runtime_smoke
phase: 15
repo: LoneWolf_Fang_free
branch: ""
head_commit: ""
app_version: ""
recorded_at_utc: ""
operator: ""
smoke_mode: not_run
fixture_type: none
fixture_origin: docs_static_check_only
signal_dir: ""
product: free
symbol: ""
entry_tf: ""
filter_tf: ""
signal_set_id: ""
selection_status: ""
safe_error_code: ""
expected_labels_checked: false
disabled_states_checked: false
command_preview_checked: false
copy_ux_checked: false
diagnostics_checked: false
live_paper_boundary_checked: false
raw_trade_rows_not_visible: true
command_execution_observed: false
producer_auto_run_observed: false
backtest_auto_run_observed: false
runner_auto_run_observed: false
order_path_observed: false
balance_fetch_observed: false
private_api_observed: false
result_status: not_run
result_reason: "GUI runtime smoke intentionally deferred for docs/test-only Phase 15."
notes_sanitized: ""
```

## Fields Allowed In Smoke Record

Only safe metadata may be recorded:

- aggregate `net_total`
- aggregate `final_equity`
- aggregate `trade_count`
- DD display fields
- compact hashes
- status / warnings
- safe_error_code
- `signal_dir`
- branch / commit / app_version
- synthetic fixture origin
- expected label and disabled-state check results

## Fields Forbidden In Smoke Record

The smoke record must not include:

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
- screenshot containing secrets / account / balances / orders

Do not record `entry_exec / exit_exec / qty / trade id` values. Do not record
`API key / secret / token / authorization / raw order / balance / raw billing`
values. Do not include raw market data, raw OHLCV, generated real signal tape
body, generated smoke output, generated smoke record, generated diagnostics
output, generated command preview output, generated clipboard output, order
payloads, account details, or screenshots with private/account data.

## Expected Labels Checklist

Future manual GUI runtime smoke records may mark the following labels as
checked. Screenshots are not required.

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
- `Preview only`
- `Execution disabled`
- `This panel does not execute commands`
- `Copy backtest command`
- `Copy replay command`
- `Copied`
- `Run this command manually in a terminal if needed`
- `No raw trade rows are displayed`

## Expected Disabled States

- `preview_only=true`
- `execution_enabled=false`
- `live_command_available=false`
- `paper_command_available=false`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `command_execution_observed=false`
- `producer_auto_run_observed=false`
- `backtest_auto_run_observed=false`
- `runner_auto_run_observed=false`
- `order_path_observed=false`
- `balance_fetch_observed=false`
- `private_api_observed=false`

## Pass / Fail / Blocked Criteria

Pass criteria:

- expected labels visible
- expected disabled states confirmed
- command preview visible but not executed
- copy UX works as clipboard-only
- diagnostics visible
- no raw trade rows visible
- no LIVE/PAPER/order/private API observed

Fail criteria:

- any command executed by GUI
- producer/backtest/runner/inventory auto-runs
- selection or adapter auto-runs
- LIVE/PAPER/order/private API path starts
- balance/order fetch occurs
- raw trade rows / `entry_exec` / `exit_exec` / `qty` / trade id displayed
- secrets/auth/billing/order/balance appears

Blocked criteria:

- no synthetic fixture available
- GUI cannot launch safely
- worktree contains unexpected staged files
- package/release artifact confusion
- APP_VERSION mismatch
- operator cannot confirm LIVE/PAPER/order disabled state

Not run criteria:

- docs/static-only validation chosen
- GUI runtime smoke intentionally deferred

## Screenshot Policy

- screenshots optional
- screenshots must not include secrets / balances / orders
- screenshots must not include account details
- screenshots must not include raw trade rows
- screenshots must be omitted from repo and zip unless explicitly sanitized
- Phase 15 does not require screenshots

Screenshots are never a substitute for the safe metadata record. If a
screenshot would expose secrets, balances, orders, account details, raw trades
rows, raw market data, or generated real tape body, omit it.

## Future Phase Boundary

- future local-only dry-run is future phase
- explicit local-only dry-run design is future phase
- execution button is future phase
- LIVE/PAPER/order remains separated
- packaging/release remains separated

Phase 15 does not design, implement, or test a dry-run executor. It does not add
Run, Execute, or Start controls, and it does not connect the precomputed signal
tape GUI to existing LIVE/PAPER/order runtime paths.
