# Free Phase 22 GUI local-only dry-run approval boundary

Free Phase 22 is local-only dry-run execution approval boundary for the
precomputed signal tape GUI. Phase 22 is docs/tests-only and defines the
future explicit operator approval required before any later local saved-tape
dry-run execution phase can exist.

Free Phase 23 extends this boundary as approval preflight checklist / approval
record static sample docs-tests. Phase 23 is docs/tests-only and adds only
`docs/precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md`,
`docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`, and
tests that validate the static sample and docs boundary.

Phase 22 does not make local-only dry-run executable. It does not add approval
UI, confirm controls, execute controls, request generation, approval record
generation, execution audit record generation, subprocess launch paths, private
API paths, LIVE/PAPER/order paths, package/release changes, or APP_VERSION
changes.

## Scope

- Phase 22 is local-only dry-run execution approval boundary.
- Phase 23 is approval preflight checklist / approval record static sample docs-tests.
- Phase 22 is docs/tests-only.
- Phase 23 is docs/tests-only.
- no GUI source change.
- no runtime source change.
- no approval UI implementation.
- no dry-run execution implementation.
- no execution button.
- no command execution.
- no approval record generation.
- no approval record generation at runtime.
- no execution audit record generation.
- no subprocess / QProcess / background worker.
- no producer/backtest/runner/inventory auto-run.
- no request generation.
- no generated dry-run outputs.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- no private API.
- APP_VERSION unchanged.
- package/release not touched.
- raw trade rows are never included.
- generated real tape body / raw market data excluded.
- generated approval record / generated execution audit record excluded.
- future local-only dry-run requires explicit operator approval.
- approval is not live/paper/order approval.
- approval record is not execution audit record.
- future execution must be a separate phase.
- future execution requires separate explicit approval phase.

## Approval Definition

Approval is a future explicit operator confirmation for local-only dry-run.
The approval boundary is intentionally narrow:

- approval is not LIVE approval.
- approval is not PAPER approval.
- approval is not order approval.
- approval is not private API approval.
- approval is not live/paper/order approval.
- approval is only for local saved-tape backtest/replay fast path.
- approval must be separate from command copy UX.
- approval must be separate from manual smoke record.
- approval must be separate from release/package approval.
- approval cannot make LIVE, PAPER, order, balance, fetch, submit, private API,
  packaging, or release behavior selectable.

The approval scope is local saved-tape backtest/replay only. It can never grant
permission to connect selected precomputed signal tape state to LIVE/PAPER/order
buttons, MEXC private API, balance fetch, order fetch, submit paths, strategy,
indicators, exchange, risk, order runtime logic, or release packaging.

## Current Phase 22 Boundary

Current Phase 22 boundary:

- no approval UI.
- no approval record generated.
- no execution audit record generated.
- no dry-run request execution.
- no generated dry-run outputs.
- no subprocess / QProcess / background worker.
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch worker, or background execution path.
- no producer/backtest/runner/inventory auto-run.
- no selection/adapter/request builder auto-run.
- command preview remains preview-only.
- command copy UX remains clipboard-only.
- GUI local dry-run preview remains read-only.
- GUI source remains unchanged.
- runtime source remains unchanged.
- APP_VERSION unchanged.
- package/release not touched.
- no LIVE/PAPER/order.
- no MEXC private API.

Phase 22 must not create approval record files. Phase 22 must not create
execution audit records. Phase 22 must not create generated dry-run request
files, generated dry-run outputs, screenshots, command preview outputs,
diagnostics outputs, inventory outputs, selection outputs, adapter outputs,
package zips, exe, installer, setup binaries, release assets, runtime exports,
or zip-in-zip artifacts.

Phase 23 must not create generated approval records. The Phase 23 JSON fixture
is a static sanitized sample, not generated runtime output, not actual
operator approval, not dry-run execution proof, and not an execution audit
record.

## Phase 23 Approval Preflight Checklist

The canonical Phase 23 checklist is
`docs/precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md`.
The checklist fixes the preflight gate names, approval record schema, static
sample rules, confirmation text requirements, fail-closed reasons, execution
audit boundary, and relationship to previous phases.

Phase 23 is docs/tests-only. It has no approval UI implementation, no approval
record generation at runtime, no execution audit record generation, no dry-run
execution, no GUI source change, no runtime source change, no command
execution, no subprocess / QProcess / background worker, no producer/backtest/
runner/inventory auto-run, no LIVE/PAPER/order, no MEXC private API,
APP_VERSION unchanged, and package/release not touched.

Required preflight checklist items include:

- `request_type == precomputed_signal_local_dry_run_request`.
- request schema valid.
- request_hash present.
- selection_hash present.
- selection contract valid.
- `product == free`.
- signal_dir present.
- symbol present.
- entry_tf / filter_tf present.
- dry_run_mode is allowed.
- `dry_run_mode` is `backtest_fast_path_local_only` or
  `runner_replay_fast_path_local_only`.
- `not_selectable_for_live == true`.
- `not_selectable_for_paper == true`.
- `safety_research_only == true`.
- `paper_live_order_execution == false`.
- `operator_confirmation_required == true`.
- `operator_confirmed == true` only in a future actual approval record, not in
  the Phase 23 sample because the sample is `not_run`.
- manifest present.
- summary present.
- trades.csv present.
- manifest hash present.
- summary hash present.
- trades.csv hash from manifest present.
- forbidden fields rejected.
- positive legacy max_drawdown rejected.
- output_dir safe.
- allowed_artifacts reviewed.
- forbidden_artifacts reviewed.
- worktree status recorded.
- package/release artifacts excluded.
- no background execution.
- no private API.
- no order/balance path.
- no raw trade rows displayed or recorded.

## Phase 23 Static Approval Record Sample

The static sample approval record is
`docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`.

It is a sanitized static fixture only:

- `record_type = precomputed_signal_local_dry_run_approval_record`.
- `approval_schema_version = free_precomputed_local_dry_run_approval_record_v1`.
- `approval_scope = local_saved_tape_backtest_replay_only`.
- `status = not_run`.
- `operator_confirmed = false`.
- `preflight_passed = false`.
- request_hash / selection_hash are synthetic placeholders.
- approved_actions contain only future local saved-tape approval actions.
- forbidden_actions include LIVE/PAPER/order/private API/background/package
  release actions.
- no approval or execution occurred.
- no runtime source generated the record.

The sample must not include generated approval record output, generated
execution audit record output, generated dry-run output, raw market data, raw
OHLCV, raw trade row payloads, row-level execution prices, exact row
identifiers, raw order payloads, account payloads, private credentials, raw
billing data, screenshot paths, package zips, exe, installer, release assets,
runtime dirs, exports dirs, or zip-in-zip artifacts.

## Required Operator Confirmation Text

Future required operator confirmation text must include every item below before
any later phase can approve a local-only saved-tape dry-run:

- This is local-only.
- This is not LIVE/PAPER/order.
- No MEXC private API.
- No balance fetch.
- No order fetch.
- No order submit.
- Uses selected precomputed signal tape only.
- Product: free.
- `signal_dir`.
- `symbol`.
- `entry_tf`.
- `filter_tf`.
- `dry_run_mode`.
- `output_dir`.
- operator understands generated artifacts policy.
- operator understands execution is local saved-tape fast path only.

The text must be exact enough for audit review and must not be inferred from
selecting a tape, copying a command, opening diagnostics, viewing manual smoke
records, or viewing the GUI preview. Before future confirmation,
`operator_confirmed=false`, `execution_enabled=false`, and
`execution_enabled_after_confirmation=false` remain fixed.

## Future Required Pre-Approval Gates

All future required pre-approval gates must pass before any future execution:

- valid selection contract.
- valid request schema.
- `product == free`.
- `dry_run_mode in allowed enum`.
- `operator_confirmed=false before confirmation`.
- `execution_enabled=false before confirmation`.
- `execution_enabled_after_confirmation=false before confirmation`.
- `not_selectable_for_live=true`.
- `not_selectable_for_paper=true`.
- `safety_research_only=true`.
- `paper_live_order_execution=false`.
- manifest present.
- summary present.
- `trades.csv` present.
- manifest hash present.
- summary hash present.
- `trades.csv` hash from manifest present.
- forbidden fields rejected.
- positive legacy max_drawdown rejected.
- output_dir safe.
- worktree status recorded.
- package/release artifacts excluded.
- APP_VERSION unchanged unless future release phase explicitly approves.
- no background execution.
- no private API.
- no order/balance path.

Every gate is fail-closed. A later executor must stop before launch when a gate
is missing, false, mismatched, unsafe, or unreviewed.

## Allowed Execution Modes

Allowed execution modes:

- `backtest_fast_path_local_only`.
- `runner_replay_fast_path_local_only`.

These are the only future local saved-tape candidates. They are local-only and
must use the operator-selected precomputed signal tape directory.

## Not Allowed Modes

Not allowed modes:

- `live`.
- `paper`.
- `order_submit`.
- `order_fetch`.
- `balance_fetch`.
- `private_api`.
- `producer_auto_run`.
- `inventory_auto_scan`.
- `background_worker`.
- `release_packaging`.
- `external_network`.

`live/paper/order/private_api/background_worker` are not allowed. LIVE/PAPER/
order, private API, balance fetch, order fetch, order submit, producer auto-run,
inventory auto-scan, background worker, release packaging, and external network
behavior remain outside this boundary.

## Approval Record Schema

Future approval record is a separate artifact and is not generated in Phase 22.
It is not a request file, not an execution record, not a manual smoke record,
and not release approval.

Required record identity:

- `request_type`: `precomputed_signal_local_dry_run_approval_record`.
- `record_type`: `precomputed_signal_local_dry_run_approval_record`.

Approval record schema fields:

- `schema_version`.
- `approval_schema_version`.
- `record_type`.
- `phase`.
- `repo`.
- `branch`.
- `head_commit`.
- `app_version`.
- `created_at_utc`.
- `operator`.
- `product`.
- `signal_dir`.
- `symbol`.
- `entry_tf`.
- `filter_tf`.
- `signal_set_id`.
- `dry_run_mode`.
- `output_dir`.
- `request_hash`.
- `selection_hash`.
- `confirmation_text`.
- `operator_confirmed`.
- `approval_scope`.
- `approved_actions`.
- `forbidden_actions`.
- `preflight_passed`.
- `preflight_summary`.
- `fail_closed_reasons`.
- `allowed_artifacts`.
- `forbidden_artifacts`.
- `status`.
- `status_reason`.
- `notes_sanitized`.

approval_scope enum:

- `local_saved_tape_backtest_replay_only`.

approved_actions allowed:

- `approve_backtest_fast_path_local_only`.
- `approve_runner_replay_fast_path_local_only`.

forbidden_actions required:

- `live`.
- `paper`.
- `order_submit`.
- `order_fetch`.
- `balance_fetch`.
- `private_api`.
- `producer_auto_run`.
- `inventory_auto_scan`.
- `background_worker`.
- `package_release`.

status enum:

- `draft`.
- `approved`.
- `rejected`.
- `blocked`.
- `invalid`.
- `not_run`.

Phase 22 must not create approval record files.

## Execution Audit Record Boundary

Future execution audit record is separate from approval record and is not
generated in Phase 22.

Required record identity:

- `record_type`: `precomputed_signal_local_dry_run_execution_record`.

Purpose:

- record future local-only dry-run result if a future approved phase executes it.

Future execution audit record must include:

- `approval_record_hash`.
- `request_hash`.
- `execution_started_at_utc`.
- `execution_finished_at_utc`.
- `dry_run_mode`.
- `result_status`.
- `output_artifact_manifest`.
- `safe_summary`.
- `fail_closed_reason`.
- `no_live_paper_order_assertion`.
- `no_private_api_assertion`.
- `no_background_execution_assertion`.

Future execution audit record must not include:

- raw trades rows.
- `entry_exec`.
- `exit_exec`.
- `qty`.
- trade id.
- order id.
- raw order.
- balance.
- API key.
- secret.
- token.
- authorization.
- raw billing.
- raw market data.
- raw OHLCV.

Phase 22 must not create execution audit records.

## Allowed Artifacts

Allowed artifacts after future approval:

- safe summary JSON.
- `fast_summary.json`.
- `equity_curve.csv`.
- `trades.csv` only if safe-condition / saved-tape-compatible and explicitly
  documented.
- local-only output manifest.
- approval record.
- execution audit record.
- sanitized manual smoke record.

Allowed artifacts must remain local-only, sanitized, and outside package/release
assets unless a future phase explicitly defines a safe migration summary.

## Forbidden Artifacts

Forbidden artifacts:

- raw market data.
- raw OHLCV.
- raw trades rows.
- `entry_exec`.
- `exit_exec`.
- `qty`.
- `trade_id`.
- `order_id`.
- raw_order.
- balance_snapshot.
- api_key.
- secret.
- token.
- authorization.
- raw_billing.
- screenshots with secrets/balances/orders/account details.
- package_zip.
- exe.
- installer.
- release_asset.
- runtime dirs copied into repo.
- zip inside zip.

Forbidden artifact labels may appear in docs/tests as policy identifiers only.
They must not appear as generated values, payloads, screenshots, rows, account
details, private API data, package assets, or release artifacts.

## Future Fail-Closed Conditions

Future fail-closed conditions:

- operator confirmation missing.
- confirmation text mismatch.
- approval_scope invalid.
- request invalid.
- selection invalid.
- `product != free`.
- live/paper/order/private API requested.
- background execution requested.
- output path unsafe.
- forbidden fields detected.
- positive legacy max_drawdown detected.
- approval record missing in future execution phase.
- approval record does not match request hash.
- approval record must match request hash.
- worktree status unexpected.
- package/release artifact path involved.

Every fail-closed condition stops before execution and returns only a safe,
sanitized status reason.

## Existing Phases Relationship

- Phase 9 command preview remains preview-only.
- Phase 10 copy UX remains clipboard-only.
- Phase 15/16 manual smoke record remains display-only proof, not execution proof.
- Phase 18 request schema is planning artifact, not execution proof.
- Phase 19 request builder creates preview only.
- Phase 20 GUI preview adapter creates display item only.
- Phase 21 GUI wiring displays preview only.
- Phase 22 defines approval boundary only.
- future execution requires separate explicit approval phase.

Future execution must not be introduced through command preview, copy UX,
selection diagnostics, request builder preview, GUI preview adapter, GUI preview
wiring, manual smoke record, package/release approval, or APP_VERSION changes.
