# Free Phase 15/16 GUI manual smoke record schema

Free Phase 15 is synthetic fixture only manual GUI runtime smoke record format
for the precomputed signal tape GUI. It is docs/test-only. GUI runtime smoke is
not executed in Phase 15.

Free Phase 16 is synthetic manual smoke record schema validator / sample static
fixture. It is also docs/tests-only. GUI runtime smoke is not executed in Phase
16, and GUI runtime smoke not executed in Phase 15/16 is an explicit migration
boundary.

This document fixes how a future operator may record a safe manual GUI runtime
smoke result without changing GUI source, runtime source, or execution
behavior.

## Scope

- Phase 15 is synthetic fixture only manual GUI runtime smoke record format.
- Phase 15 is docs/test-only.
- GUI runtime smoke is not executed in Phase 15.
- Phase 16 is docs/tests-only synthetic manual smoke record schema validation.
- GUI runtime smoke is not executed in Phase 16.
- The static sample record is not a generated runtime output.
- The static sample record is sanitized and synthetic.
- No screenshots are required.
- No GUI source change.
- No runtime source change.
- No execution path change.
- No LIVE/PAPER/order connection.
- Manual smoke records are safe metadata only.
- Synthetic fixture only.
- No real market data.
- No generated real signal tape body.
- No generated smoke record.
- No generated smoke output.
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

- record type: `precomputed_signal_gui_manual_smoke_record`
- phase: `free_precomputed_signals_phase15_or_later`
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

The safe schema is fixed for docs/tests-only validation. A record is valid only
when `record_type` is `precomputed_signal_gui_manual_smoke_record` and `phase`
is `free_precomputed_signals_phase15_or_later`.

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

Safe boolean fields:

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

Safe boolean expectations for `result_status=pass`:

- `expected_labels_checked=true`
- `disabled_states_checked=true`
- `command_preview_checked=true`
- `copy_ux_checked=true`
- `diagnostics_checked=true`
- `live_paper_boundary_checked=true`
- `raw_trade_rows_not_visible=true`
- `command_execution_observed=false`
- `producer_auto_run_observed=false`
- `backtest_auto_run_observed=false`
- `runner_auto_run_observed=false`
- `order_path_observed=false`
- `balance_fetch_observed=false`
- `private_api_observed=false`

For `result_status=not_run`, forbidden execution observations must remain false:
`command_execution_observed=false`, `producer_auto_run_observed=false`,
`backtest_auto_run_observed=false`, `runner_auto_run_observed=false`,
`order_path_observed=false`, `balance_fetch_observed=false`, and
`private_api_observed=false`.

The template below is valid without screenshots:

```yaml
schema_version: 1
record_type: precomputed_signal_gui_manual_smoke_record
phase: free_precomputed_signals_phase15_or_later
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

## Static Sample Fixture

`docs/precomputed_signals_gui_manual_smoke_record_sample.json` is a static
sanitized sample fixture for docs-policy validation only. It is not generated by
GUI runtime smoke, not generated runtime output, and not a generated smoke
record.

The sample uses `smoke_mode=docs_static_check_only` and a synthetic safe
fixture origin. It may include aggregate metadata such as `trade_count`,
`net_total`, `final_equity`, `max_dd_display_abs`, `max_dd_display_pct`,
`selection_status`, and `safe_error_code`. It must not include screenshots,
screenshot paths, raw output, generated real signal tape body, raw market data,
raw OHLCV, raw trades rows, `entry_exec`, `exit_exec`, `qty`, exact trade id,
order id, raw order, balance, API key, secret, token, authorization, raw
billing, account details, private keys, or generated smoke output.

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
- Phase 16 does not require screenshots
- screenshots are optional and must not be committed unless sanitized in a
  future phase
- screenshots must not be included in the Phase 16 repo diff or repo-external
  migration zip

Screenshots are never a substitute for the safe metadata record. If a
screenshot would expose secrets, balances, orders, account details, raw trades
rows, raw market data, or generated real tape body, omit it.

## Future Phase Boundary

- future local-only dry-run is future phase
- future local-only dry-run design is future phase
- explicit local-only dry-run design is future phase
- execution button is future phase
- LIVE/PAPER/order remains separated
- packaging/release remains separated

Phase 15 does not design, implement, or test a dry-run executor. It does not add
Run, Execute, or Start controls, and it does not connect the precomputed signal
tape GUI to existing LIVE/PAPER/order runtime paths.

Phase 16 also does not design, implement, or test a dry-run executor. It does
not add command execution, producer/backtest/runner/inventory auto-run,
subprocess, `QProcess`, background worker, LIVE/PAPER/order connection, MEXC
private API access, APP_VERSION changes, package/release changes, generated
runtime output, generated smoke records, or screenshots.

## Phase 17 Local-Only Dry-Run Relationship

Free Phase 17 is future local-only dry-run design boundary. It is
docs/tests-only and does not change the manual smoke record schema.

Phase 17 preserves these manual smoke record boundaries:

- no local dry-run implementation
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
- raw trade rows are never displayed or recorded
- manual smoke record remains display-only
- future local-only dry-run requires explicit operator confirmation
- future local-only dry-run is not live/paper/order
- future execution must be a separate phase

The Phase 15/16 manual smoke record remains display-only and synthetic fixture
only. It must not be reused as local dry-run proof, LIVE/PAPER proof, order
proof, balance proof, private API proof, or runtime execution proof.

Any future local-only dry-run result record is a separate future artifact. It
must remain safe metadata only unless a later approved phase defines a stricter
schema. Phase 17 must not create generated dry-run request files, generated
dry-run output, screenshots, command preview output, diagnostics output,
inventory output, selection output, adapter output, or package/release assets.

Future local-only dry-run request schema work must keep
`request_type=precomputed_signal_local_dry_run_request`,
`operator_confirmed=false by default`, `not_selectable_for_live=true`, and
`not_selectable_for_paper=true`. Confirmation must state that the action is
local-only and not LIVE/PAPER/order, and must show `signal_dir`, product `free`,
symbol/timeframe, and output directory.
