# Free Phase 24 GUI local-only dry-run execution audit schema

Free Phase 24 is execution audit record schema / static sample docs-tests for
the precomputed signal tape GUI. Phase 24 is docs/tests-only and fixes the
schema for a future execution audit record that can exist only after a separate
future approved local saved-tape dry-run execution phase.

Phase 24 does not implement execution. It does not add approval UI, confirm
controls, approve controls, execute controls, dry-run execution, command
execution, approval record generation at runtime, execution audit record
generation at runtime, subprocess launch paths, QProcess launch paths,
background workers, private API paths, LIVE/PAPER/order paths, package/release
changes, or APP_VERSION changes.

## Scope

- Phase 24 is execution audit record schema / static sample docs-tests.
- Phase 24 is docs/tests-only.
- no GUI source change.
- no runtime source change.
- no approval UI implementation.
- no dry-run execution implementation.
- no approval record generation at runtime.
- no execution audit record generation at runtime.
- no command execution.
- no subprocess / QProcess / background worker.
- no producer/backtest/runner/inventory auto-run.
- no selection/adapter/request builder auto-run.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- no private API.
- APP_VERSION unchanged.
- package/release not touched.
- raw trade rows are never included.
- generated real tape body / raw market data excluded.
- generated approval record / generated execution audit record excluded.
- execution audit record is not approval record.
- execution audit record is not order/trading record.
- future execution must be separate explicit approval and implementation phase.

## Audit Record Purpose

The execution audit record is a future artifact. It is only for future approved
local saved-tape backtest/replay fast path execution. It is not created by
Phase 24.

The execution audit record is not an approval record, not an order record, not
a LIVE/PAPER trading record, not release/package proof, and not proof that
Phase 24 executed anything. A future execution audit record must reference
`approval_record_hash` and `request_hash`, and it must retain the
`selection_hash` linkage for the selected precomputed signal tape.

Every future execution audit record must assert:

- no LIVE/PAPER/order/private API/background execution.
- no private API.
- no balance fetch.
- no order fetch.
- no order submit.
- no generated real signal tape body in repo or package.

## Relationship To Approval Record

Approval record authorizes a future local-only dry-run. Execution audit record
records a future execution result after that separate execution phase exists.
Approval record does not prove execution.

Execution audit record must reference approval record. Execution audit record
must fail closed if `approval_record_hash` is missing. Execution audit record
must fail closed if `request_hash` is missing. Execution audit record must fail
closed if `selection_hash` is missing. Execution audit record must fail closed
if approval_scope mismatch is detected.

approval_record_hash / request_hash linkage is mandatory. approval_record_hash
/ request_hash / selection_hash linkage is mandatory. The future approval
record must match the request and selected tape before any execution audit
record can claim a completed local-only dry-run result.
approval_record_hash / request_hash / selection_hash linkage is mandatory.

The Phase 24 sample is a static `not_run` sample. It does not prove execution,
does not prove approval, does not prove a dry-run result, and was not generated
by runtime code.

## Execution Audit Record Schema

Required identity:

- `record_type`: `precomputed_signal_local_dry_run_execution_record`
- `execution_audit_schema_version`:
  `free_precomputed_local_dry_run_execution_audit_v1`

Required fields:

- `schema_version`
- `execution_audit_schema_version`
- `record_type`
- `phase`
- `repo`
- `branch`
- `head_commit`
- `app_version`
- `created_at_utc`
- `approval_record_hash`
- `request_hash`
- `selection_hash`
- `approval_scope`
- `product`
- `signal_dir`
- `symbol`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `dry_run_mode`
- `output_dir`
- `execution_started_at_utc`
- `execution_finished_at_utc`
- `execution_duration_ms`
- `result_status`
- `result_reason`
- `fail_closed_reason`
- `output_artifact_manifest`
- `safe_summary`
- `no_live_paper_order_assertion`
- `no_private_api_assertion`
- `no_background_execution_assertion`
- `no_order_or_balance_path_assertion`
- `forbidden_fields_absent`
- `raw_trade_rows_absent`
- `generated_artifacts_policy_checked`
- `approval_record_hash_verified`
- `request_hash_verified`
- `notes_sanitized`

approval_scope enum:

- `local_saved_tape_backtest_replay_only`

dry_run_mode enum:

- `backtest_fast_path_local_only`
- `runner_replay_fast_path_local_only`

result_status enum:

- `pass`
- `fail`
- `blocked`
- `invalid`
- `not_run`

Phase 24 sample rules:

- `result_status = not_run`
- `execution_started_at_utc = null`
- `execution_finished_at_utc = null`
- `execution_duration_ms = 0`
- `approval_record_hash` is a synthetic placeholder.
- `request_hash` is a synthetic placeholder.
- `selection_hash` is a synthetic placeholder.
- `output_artifact_manifest` is an empty safe list or synthetic placeholder.
- `safe_summary` is a static sanitized summary.
- all required no_* assertions are true.
- `notes_sanitized` states no execution occurred.

## Required Assertions

These fields must be true:

- `no_live_paper_order_assertion`
- `no_private_api_assertion`
- `no_background_execution_assertion`
- `no_order_or_balance_path_assertion`
- `forbidden_fields_absent`
- `raw_trade_rows_absent`
- `generated_artifacts_policy_checked`
- `approval_record_hash_verified`
- `request_hash_verified`

In the Phase 24 sample, these assertions are static schema assertions only.
The sample result remains `not_run`, and the notes state no execution occurred.

## Output Artifact Manifest Schema

output artifact manifest schema is fixed as a safe local metadata list. The
`output_artifact_manifest` value must be a list. Each item must contain:

- `artifact_type`
- `path`
- `sha256`
- `size_bytes`
- `safe_to_archive`
- `contains_raw_market_data`
- `contains_raw_trade_rows`
- `contains_order_or_balance`
- `contains_secret_or_auth`
- `notes_sanitized`

allowed artifact types:

- `safe_summary_json`
- `fast_summary_json`
- `equity_curve_csv`
- `fast_path_trades_csv_safe_condition`
- `safe_metadata_log`
- `sanitized_manual_smoke_record`
- `local_output_manifest`

forbidden artifact types:

- `raw_market_data`
- `raw_ohlcv`
- `raw_trades_rows`
- `entry_exec_rows`
- `exit_exec_rows`
- `qty_rows`
- `order_payload`
- `balance_snapshot`
- `api_key`
- `secret`
- `token`
- `authorization`
- `raw_billing`
- `package_zip`
- `exe`
- `installer`
- `release_asset`
- `screenshot_with_sensitive_data`

The Phase 24 sample output_artifact_manifest can be empty. If a future sample
uses a placeholder item, it must not point to a real generated file and must
not include screenshots. Future execution output artifacts must remain local,
sanitized, and outside package/release assets unless a separate phase defines a
safe migration summary.

## Allowed Result Status Meaning

- `pass`: future execution completed and safe assertions passed.
- `fail`: future execution ran but failed safely.
- `blocked`: future execution did not run due preflight / approval / safety block.
- `invalid`: audit record invalid.
- `not_run`: static docs/test sample or future non-executed placeholder.

Phase 24 sample status is `not_run` only.

## Fail-Closed Reasons

Allowed fail-closed reason identifiers:

- `none`
- `approval_record_missing`
- `approval_record_hash_mismatch`
- `request_missing`
- `request_hash_mismatch`
- `selection_hash_mismatch`
- `approval_scope_mismatch`
- `invalid_request`
- `invalid_selection`
- `operator_confirmation_missing`
- `live_or_paper_requested`
- `order_or_balance_requested`
- `private_api_requested`
- `background_execution_requested`
- `unsafe_output_dir`
- `forbidden_field_detected`
- `positive_legacy_max_drawdown`
- `output_artifact_policy_violation`
- `raw_trade_rows_detected`
- `missing_no_live_paper_order_assertion`
- `missing_no_private_api_assertion`
- `missing_no_background_execution_assertion`
- `unknown_safety_violation`

Every fail-closed reason blocks or invalidates the audit record and returns
only a safe sanitized status reason.

## Static Sample Execution Audit Record

The static sample execution audit record is:

- `docs/precomputed_signals_gui_local_dry_run_execution_audit_sample.json`

Sample requirements:

- static sanitized sample.
- not generated runtime output.
- no execution.
- no approval UI.
- no dry-run execution.
- `result_status = not_run`.
- approval_record_hash / request_hash / selection_hash are synthetic
  placeholders.
- output_artifact_manifest is empty or synthetic safe placeholder.
- no screenshots.
- no generated output.
- no raw market data.
- no raw trade rows.
- no row-level execution price payloads.
- no row-level quantity payloads.
- no exact row identifiers.
- no order / balance / API key / secret / token / authorization / raw billing.
- notes_sanitized states no execution occurred.

## Forbidden Fields

Audit sample and future audit record must not include:

- raw trades rows.
- `entry_exec`.
- `exit_exec`.
- `qty`.
- exact trade id.
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
- screenshot path with sensitive content.
- account details.

Forbidden labels may appear in docs/tests as policy identifiers only. They must
not appear as generated values, payload rows, screenshots, account details,
private API data, package assets, or release artifacts.

## Future Execution Boundary

Phase 24 does not implement execution. Phase 24 does not generate audit record.
Future execution requires separate explicit approval and implementation phase.
Future execution must use a prior approval record. Future execution must
generate execution audit record only after execution.

LIVE/PAPER/order remains permanently separated. Package/release remains
separated. Command preview, copy UX, diagnostics, request builder preview, GUI
preview adapter, and GUI preview wiring remain non-executing surfaces.

## Relationship To Existing Phases

- Phase 18 request schema is planning artifact.
- Phase 19 request builder creates preview only.
- Phase 20 GUI preview adapter creates display item only.
- Phase 21 GUI wiring displays preview only.
- Phase 22 approval boundary defines future approval rules.
- Phase 23 approval static sample/checklist does not execute anything.
- Phase 24 execution audit schema defines future audit only.
- future execution requires separate explicit approval and implementation phase.

Future execution must not be introduced through command preview, copy UX,
selection diagnostics, request builder preview, GUI preview adapter, GUI preview
wiring, manual smoke record, approval checklist docs, execution audit schema
docs, package/release approval, or APP_VERSION changes.
