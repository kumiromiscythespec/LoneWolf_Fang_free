# Free Phase 23 GUI local-only dry-run approval preflight checklist

Free Phase 23 is approval preflight checklist / approval record static sample
docs-tests for the precomputed signal tape GUI. Phase 23 is docs/tests-only and
fixes the checklist and sanitized static approval record sample needed before
any later local-only saved-tape dry-run execution phase can be designed.

Phase 23 does not implement approval UI, confirm controls, approve controls,
execute controls, dry-run execution, command execution, approval record
generation at runtime, execution audit record generation, subprocess launch
paths, QProcess launch paths, background workers, private API paths,
LIVE/PAPER/order paths, package/release changes, or APP_VERSION changes.

Free Phase 24 fixes the execution audit record schema / static sample
docs-tests in
`docs/precomputed_signals_gui_local_dry_run_execution_audit_schema.md` and
`docs/precomputed_signals_gui_local_dry_run_execution_audit_sample.json`.
Phase 24 is docs/tests-only. It does not implement dry-run execution and does
not generate an execution audit record at runtime.

## Scope

- Phase 23 is approval preflight checklist / approval record static sample docs-tests.
- Phase 24 is execution audit record schema / static sample docs-tests.
- Phase 23 is docs/tests-only.
- Phase 24 is docs/tests-only.
- no GUI source change.
- no runtime source change.
- no approval UI implementation.
- no dry-run execution implementation.
- no approval record generation at runtime.
- no execution audit record generation.
- no execution audit record generation at runtime.
- no dry-run execution.
- no command execution.
- no subprocess / QProcess / background worker.
- no producer/backtest/runner/inventory auto-run.
- no selection/adapter/request builder auto-run.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- APP_VERSION unchanged.
- package/release not touched.
- raw trade rows are never included.
- generated real tape body / raw market data excluded.
- generated approval record / generated execution audit record excluded.
- future execution must be a separate phase.
- future execution requires separate explicit approval and implementation phase.

## Approval Scope

The only approval scope is:

- `approval_scope = local_saved_tape_backtest_replay_only`

Approval is narrow and local:

- approval is not LIVE approval.
- approval is not PAPER approval.
- approval is not order approval.
- approval is not private API approval.
- approval is not release/package approval.
- approval only covers future local saved-tape fast-path dry-run.
- approval is separate from command preview/copy UX.
- approval is separate from manual GUI smoke record.
- approval is separate from execution audit record.
- approval cannot make LIVE, PAPER, order, balance, fetch, submit, private API,
  packaging, or release behavior selectable.

## Required Preflight Checklist

All required preflight checklist items must pass before any future approval can
be considered valid:

- `request_type == precomputed_signal_local_dry_run_request`
- request schema valid.
- request_hash present.
- selection_hash present.
- selection contract valid.
- `product == free`.
- signal_dir present.
- symbol present.
- entry_tf / filter_tf present.
- dry_run_mode is allowed.
- dry_run_mode in `backtest_fast_path_local_only`.
- dry_run_mode in `runner_replay_fast_path_local_only`.
- `not_selectable_for_live == true`.
- `not_selectable_for_paper == true`.
- `safety_research_only == true`.
- `paper_live_order_execution == false`.
- `operator_confirmation_required == true`.
- `operator_confirmed == true` only in a future actual approval record, not in
  the Phase 23 sample unless the sample is marked `not_run`.
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

Every checklist item is fail-closed. A missing, false, mismatched, unsafe, or
unreviewed item blocks approval and does not produce execution.

## Approval Record Schema

The approval record schema is documented for a future phase. Phase 23 includes
only a sanitized static sample fixture.

Required identity:

- `record_type`: `precomputed_signal_local_dry_run_approval_record`
- `approval_schema_version`: `free_precomputed_local_dry_run_approval_record_v1`

Required fields:

- `schema_version`
- `approval_schema_version`
- `record_type`
- `phase`
- `repo`
- `branch`
- `head_commit`
- `app_version`
- `created_at_utc`
- `operator`
- `product`
- `signal_dir`
- `symbol`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `dry_run_mode`
- `output_dir`
- `request_hash`
- `selection_hash`
- `confirmation_text`
- `operator_confirmed`
- `approval_scope`
- `approved_actions`
- `forbidden_actions`
- `preflight_passed`
- `preflight_summary`
- `fail_closed_reasons`
- `allowed_artifacts`
- `forbidden_artifacts`
- `status`
- `status_reason`
- `notes_sanitized`

approval_scope enum:

- `local_saved_tape_backtest_replay_only`

approved_actions allowed:

- `approve_backtest_fast_path_local_only`
- `approve_runner_replay_fast_path_local_only`

forbidden_actions required:

- `live`
- `paper`
- `order_submit`
- `order_fetch`
- `balance_fetch`
- `private_api`
- `producer_auto_run`
- `inventory_auto_scan`
- `background_worker`
- `package_release`

status enum:

- `draft`
- `approved`
- `rejected`
- `blocked`
- `invalid`
- `not_run`

Phase 23 sample status:

- `not_run`

Phase 23 sample operator_confirmed:

- `false`

Phase 23 uses `status=not_run` and `operator_confirmed=false` because this
phase is static sample / docs-tests only and does not perform actual operator
approval.

## Static Sample Approval Record

The static approval record sample is:

- `docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`

Sample requirements:

- static sanitized sample.
- not generated runtime output.
- no approval UI.
- no actual approval.
- no execution.
- `status = not_run`.
- `operator_confirmed = false`.
- `preflight_passed = false`.
- `approval_scope = local_saved_tape_backtest_replay_only`.
- approved_actions may list future allowed action examples while status remains
  `not_run`.
- forbidden_actions must include all required forbidden actions.
- request_hash / selection_hash are synthetic placeholders.
- confirmation_text is sanitized sample text.
- signal_dir is synthetic example path.
- output_dir is synthetic safe local path.
- notes_sanitized states no approval or execution occurred.

The sample must not include generated approval records, generated execution
audit records, generated dry-run outputs, raw trades rows, row-level execution
prices, exact row identifiers, raw order payloads, account payloads, private
credentials, raw billing data, raw market data, raw OHLCV, screenshot paths, or
account details. Forbidden artifact labels may appear in docs/tests as policy
identifiers only and must not appear as generated payload values.

## Confirmation Text Requirements

Future confirmation text must include:

- local-only.
- not LIVE/PAPER/order.
- no MEXC private API.
- no balance fetch.
- no order fetch.
- no order submit.
- selected precomputed signal tape only.
- product free.
- signal_dir.
- symbol.
- entry_tf / filter_tf.
- dry_run_mode.
- output_dir.
- generated artifacts policy.
- operator understands this is not release/package approval.

Future confirmation text must be exact enough for audit review and must not be
inferred from selecting a tape, copying a command, opening diagnostics, viewing
manual smoke records, or viewing the GUI preview.

## Approval Preflight Pass/Fail

`preflight_passed` can be:

- `true`
- `false`

Phase 23 static sample:

- `preflight_passed = false`
- `status = not_run`
- `operator_confirmed = false`

Approval can be considered passed only in a future phase if:

- all checklist items pass.
- `operator_confirmed=true`.
- approval_scope valid.
- approved_actions subset of allowed.
- forbidden_actions complete.
- request_hash present.
- selection_hash present.
- no fail_closed_reasons.

## Fail-Closed Reasons

Allowed fail-closed reason identifiers:

- `missing_request_hash`
- `missing_selection_hash`
- `invalid_request`
- `invalid_selection`
- `invalid_approval_scope`
- `missing_confirmation_text`
- `missing_operator_confirmation`
- `forbidden_action_requested`
- `missing_forbidden_action`
- `live_or_paper_requested`
- `order_or_balance_requested`
- `private_api_requested`
- `background_execution_requested`
- `package_release_action_requested`
- `unsafe_output_dir`
- `forbidden_field`
- `positive_legacy_max_drawdown`
- `worktree_status_unrecorded`
- `unknown_safety_violation`

Every fail-closed reason blocks approval before execution and returns only a
safe, sanitized status reason.

## Execution Audit Boundary

Execution audit record is not approval record. Approval record does not prove
execution. Execution audit record must be generated only in a future execution
phase. Phase 23 must not create execution audit record.

Phase 24 defines the future execution audit schema and static sanitized
`not_run` sample only. Phase 24 is docs/tests-only, adds no approval UI
implementation, adds no dry-run execution implementation, and adds no execution
audit record generation at runtime. Execution audit record is not order/trading
record.

Future execution audit must reference:

- `approval_record_hash`
- `request_hash`
- `selection_hash`

approval_record_hash / request_hash linkage is mandatory. approval_record_hash
/ request_hash / selection_hash linkage is mandatory. Missing
approval_record_hash, missing request_hash, missing selection_hash, or
approval_scope mismatch must fail closed.

Future execution audit must assert:

- no LIVE/PAPER/order/private API/background execution.
- no private API.
- no balance fetch.
- no order fetch.
- no order submit.
- no generated real signal tape body in repo or package.

Phase 23 must not create approval record generation at runtime and must not
create execution audit record generation. The static sample is not execution
proof and is not approval proof.

Phase 24 output artifact manifest schema allows only safe local artifact
metadata and forbids raw market data, raw trades rows, order payload, balance
snapshot, package zip, exe, installer, release asset, screenshots, credentials,
and private or account payloads.

## Relationship To Existing Phases

- Phase 18 request schema is planning artifact.
- Phase 19 request builder creates preview only.
- Phase 20 GUI preview adapter creates display item only.
- Phase 21 GUI wiring displays preview only.
- Phase 22 approval boundary defines future approval rules.
- Phase 23 provides static approval sample/checklist only.
- Phase 23 approval static sample/checklist does not execute anything.
- Phase 24 execution audit schema defines future audit only.
- future execution requires separate explicit approval and implementation phase.

Future execution must not be introduced through command preview, copy UX,
selection diagnostics, request builder preview, GUI preview adapter, GUI preview
wiring, manual smoke record, approval checklist docs, execution audit schema
docs, package/release approval, or APP_VERSION changes.
