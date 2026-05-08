# Free Phase 18 GUI local-only dry-run request schema

Free Phase 18 fixes the local-only dry-run request schema for the precomputed
signal tape GUI as docs/tests-only. The schema is a future design artifact. It
does not create a request builder, request file, executor, GUI command path, or
runtime dry-run path.

Free Phase 19: local-only dry-run request builder docs/helper adds
`precomputed_signals_local_dry_run_request.py`. The request builder creates a
safe request preview only. It builds a request dict, formats a safe text
preview, and may write a safe JSON preview to a user-specified path. The request
builder does not execute dry-run, does not run backtest/runner/producer/inventory,
does not use subprocess / QProcess / background worker, and does not connect to
LIVE/PAPER/order or private API. Future execution still requires separate
approval/phase.

## Scope

- Phase 18 is local-only dry-run request schema docs/test-only.
- Free Phase 19: local-only dry-run request builder docs/helper.
- request builder creates a safe request preview only.
- request builder does not execute dry-run.
- request builder does not run backtest/runner/producer/inventory.
- request builder does not use subprocess / QProcess / background worker.
- request builder does not create generated dry-run output.
- generated request output not committed.
- no GUI source change.
- no runtime source change.
- no dry-run implementation.
- no local dry-run implementation.
- no request builder implementation.
- no dry-run request generation.
- no generated request file.
- no command execution.
- no subprocess / QProcess / background worker.
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch worker, or background execution path.
- no producer/backtest/runner/inventory auto-run.
- no selection or adapter auto-run.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- no MEXC API private balance/order fetch.
- APP_VERSION unchanged.
- package/release not touched.
- generated real tape body / raw market data excluded.
- raw trade rows are never included.
- generated dry-run request is not created.
- screenshots are not included.
- future local-only dry-run requires explicit operator confirmation.
- future local-only dry-run is not live/paper/order.
- future execution must be a separate phase.
- future execution still requires separate approval/phase.

## Request Definition

The future request type is:

- `request_type`: `precomputed_signal_local_dry_run_request`

The request schema fields are:

- `schema_version`: integer schema family for the request object.
- `request_schema_version`: string version for this docs/test-only schema.
  Phase 19 builder output uses `free_precomputed_local_dry_run_request_v1`.
- `request_type`: must be `precomputed_signal_local_dry_run_request`.
- `phase`: phase label, for this phase
  `free_precomputed_signals_phase18_local_dry_run_request_schema_docs_only`.
  Phase 19 builder output uses
  `free_precomputed_signals_phase19_request_builder`.
- `product`: must be `free`.
- `signal_dir`: selected precomputed signal tape directory.
- `symbol`: selected symbol.
- `symbol_normalized`: selected normalized symbol.
- `entry_tf`: selected entry timeframe.
- `filter_tf`: selected filter timeframe.
- `signal_set_id`: selected signal set id.
- `selection_status`: read-only selection contract status.
- `selection_status_reason`: sanitized selection contract status reason.
- `selection_safe_error_code`: sanitized selection contract safe error code.
- `dry_run_mode`: future local saved-tape fast path mode.
- `output_dir`: future local output directory.
- `requested_at_utc`: UTC timestamp string for the request artifact.
- `operator_confirmation_required`: must be `true`.
- `operator_confirmed`: defaults to `false`.
- `preview_only_before_confirmation`: must be `true`.
- `execution_enabled_after_confirmation`: must be `false` in Phase 18.
- `not_selectable_for_live`: must be `true`.
- `not_selectable_for_paper`: must be `true`.
- `safety_research_only`: must be `true`.
- `paper_live_order_execution`: must be `false`.
- `command_text_preview`: preview-only command text.
- `allowed_artifacts`: policy list of safe future artifact types.
- `forbidden_artifacts`: policy list of forbidden artifact types.
- `preflight`: nested fail-closed preflight flags.
- `fail_closed_reasons`: policy list of fail-closed reason ids.
- `status`: request status enum.
- `status_reason`: sanitized status reason.
- `notes_sanitized`: sanitized free-form note.

## Required Field Groups

identity:

- `schema_version`
- `request_schema_version`
- `request_type`
- `phase`
- `product`

selection:

- `signal_dir`
- `symbol`
- `symbol_normalized`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `selection_status`
- `selection_status_reason`
- `selection_safe_error_code`

dry-run:

- `dry_run_mode`
- `output_dir`
- `command_text_preview`

confirmation:

- `operator_confirmation_required`
- `operator_confirmed`
- `preview_only_before_confirmation`
- `execution_enabled_after_confirmation`

safety:

- `not_selectable_for_live`
- `not_selectable_for_paper`
- `safety_research_only`
- `paper_live_order_execution`

preflight:

- `selection_contract_valid`
- `manifest_present`
- `summary_present`
- `trades_csv_present`
- `manifest_hash_present`
- `summary_hash_present`
- `trades_csv_hash_from_manifest_present`
- `positive_legacy_max_drawdown_rejected`
- `forbidden_fields_rejected`
- `live_paper_order_rejected`
- `private_api_rejected`
- `background_execution_rejected`
- `package_release_artifacts_excluded`

status:

- `status`
- `status_reason`
- `fail_closed_reasons`

## Allowed dry_run_mode Enum

Allowed `dry_run_mode` values:

- `backtest_fast_path_local_only`
- `runner_replay_fast_path_local_only`

## Not Allowed Modes

These modes and behaviors are not allowed:

- `live`
- `paper`
- `order_submit`
- `order_fetch`
- `balance_fetch`
- `private_api`
- `producer_auto_run`
- `inventory_auto_scan`
- `background_worker`

LIVE/PAPER/order, order submit, order fetch, balance fetch, private API,
producer auto-run, inventory auto-scan, and background worker execution remain
outside the local-only dry-run boundary.

## Required Default Values

Required defaults:

- `product = free`
- `operator_confirmation_required = true`
- `operator_confirmed = false`
- `operator_confirmed=false by default`
- `preview_only_before_confirmation = true`
- `execution_enabled_after_confirmation = false`
- `execution_enabled_after_confirmation = false in Phase 18 sample`
- `execution_enabled_after_confirmation=false in Phase 19 builder output`
- `not_selectable_for_live = true`
- `not_selectable_for_paper = true`
- `safety_research_only = true`
- `paper_live_order_execution = false`
- `status = draft or blocked or not_run`
- Phase 19 builder default status is `valid_preview` when the selected tape is
  valid and the request preview is safe.
- request is not executable in Phase 18.
- request is not executable in Phase 19.

Allowed `status` enum:

- `draft`
- `valid_preview`
- `blocked`
- `invalid`
- `not_run`

Recommended sample status:

- `not_run`

Recommended Phase 19 builder status:

- `valid_preview`

## Allowed Artifacts

Allowed `allowed_artifacts` enum values:

- `safe_summary_json`
- `fast_path_equity_curve_csv`
- `fast_path_trades_csv_safe_condition`
- `fast_summary_json`
- `safe_metadata_log`
- `sanitized_manual_smoke_record`

Allowed artifacts are future local-only dry-run outputs only. They must remain
outside package/release assets and must not be committed or included in
migration zips unless a future phase explicitly defines a sanitized summary.

## Forbidden Artifacts

Forbidden `forbidden_artifacts` enum values:

- `raw_market_data`
- `raw_ohlcv`
- `raw_trades_rows`
- `entry_exec`
- `exit_exec`
- `qty`
- `trade_id`
- `order_id`
- `raw_order`
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
- `screenshots_with_secrets_or_balances_or_orders`

These values are policy identifiers only when they appear under
`forbidden_artifacts`; the request body must not contain raw trade rows, entry
or exit execution rows, quantities, trade ids, order ids, raw order payloads,
balance snapshots, secrets, tokens, authorization headers, raw billing, raw
market data, raw OHLCV, screenshots, package zips, exe, installers, or release
assets.

## Fail-Closed Reasons

Allowed `fail_closed_reasons` enum values:

- `invalid_selection`
- `missing_signal_dir`
- `missing_manifest`
- `missing_summary`
- `missing_trades_csv`
- `unsafe_manifest`
- `unsafe_summary`
- `forbidden_field`
- `positive_legacy_max_drawdown`
- `live_or_paper_requested`
- `order_or_balance_requested`
- `private_api_requested`
- `missing_operator_confirmation`
- `background_execution_requested`
- `output_path_in_package_release_area`
- `generated_artifact_policy_violation`
- `unknown_safety_violation`

Any unknown safety issue must fail closed before execution. Phase 18 has no
executor, so the sample remains `not_run`. Phase 19 has no executor either;
the builder output remains a preview/planning artifact.

## Command Preview Rules

- `command_text_preview` is preview-only.
- `command_text_preview` must not be executed in Phase 18.
- `command_text_preview` may include:
  - `python backtest.py --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report`
  - `python runner.py --mode replay --use-precomputed-signals --precomputed-signals-dir "<signal_dir>" --precomputed-signals-write-report`
- `command_text_preview` must not include live / paper / order.
- `command_text_preview` must not include secrets / token / auth / order /
  balance.
- Copying command text is not confirmation.
- A Phase 18 request is not executable.
- A Phase 19 request builder preview is not executable.

## Phase 19 Request Builder Helper Boundary

Free Phase 19: local-only dry-run request builder docs/helper is local-only and
preview-only. The helper may use the existing read-only selection contract for a
selected `signal_dir`; it must not run producer, backtest, runner, inventory, or
any GUI command path. It must not import or call strategy, indicators, exchange,
ccxt, risk, order runtime logic, or private API code.

The helper output is a request dict / safe JSON preview only. The helper may
write safe JSON only to a user-specified path such as a temporary test path or a
local app-data preview path. Generated request output not committed remains a
hard rule, and generated dry-run request files must not be included in the repo
or migration zip.

Required Phase 19 builder defaults:

- `operator_confirmation_required=true`
- `operator_confirmed=false by default`
- `preview_only_before_confirmation=true`
- `execution_enabled_after_confirmation=false`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

Phase 19 builder validation fails closed when product is not `free`, the
`dry_run_mode` is not allowed, the selection contract is invalid, `signal_dir`
is missing, manifest/summary/trades metadata is missing, positive legacy
`max_drawdown` is detected, forbidden fields appear, operator confirmation is
already true, execution is enabled, LIVE/PAPER/order text is requested, private
API text is requested, background execution is requested, or `output_dir` points
at package/release, setup/installer/exe, raw market data, or other generated
artifact policy areas.

The request builder output must not include raw trade rows, `entry_exec`,
`exit_exec`, `qty`, trade id, order id, raw order payload, balance snapshot, API
key, secret, token, authorization, raw billing, generated real signal tape body,
raw market data, or screenshots.

Safe output directory examples:

- `%LOCALAPPDATA%\LoneWolfFang\data\precomputed_signals_dry_runs\free\<signal_set_id>`
- a test `tmp_path`

Unsafe output directory examples:

- package zip, exe, installer, setup, signing, or release asset areas
- raw market data or raw OHLCV areas
- repo paths intended for generated runtime output

## Operator Confirmation Rules

Future dry-run must require explicit operator confirmation. The default is:

- `operator_confirmation_required=true`
- `operator_confirmed=false by default`
- missing confirmation fails closed.

Future confirmation text must state:

- local-only.
- not LIVE/PAPER/order.
- no private API.
- no order/balance fetch.
- `signal_dir`.
- product `free`.
- symbol/timeframe.
- `output_dir`.
- generated artifacts policy.

confirmation is separate from copy command UX. Selecting a tape, viewing
diagnostics, copying a command, or viewing command text must not imply
confirmation.

## Relationship To Phase 15/16 Manual Smoke Record

- Phase 15/16 manual smoke record remains display-only.
- manual smoke record remains display-only.
- local dry-run request is a separate future artifact.
- manual smoke record is not runtime/order proof.
- dry-run request is not execution proof.
- screenshots remain optional and sanitized only.
- screenshots are not part of the Phase 18 sample request.

## Future Phase Boundary

- Phase 18 does not implement request builder.
- Phase 18 does not create request files.
- Phase 18 does not execute local dry-run.
- Phase 18 does not implement local dry-run execution.
- Phase 18 does not add GUI source change.
- Phase 18 does not add runtime source change.
- Phase 18 does not add command execution.
- Phase 18 does not add subprocess / QProcess / background worker.
- Phase 18 does not add producer/backtest/runner/inventory auto-run.
- Phase 18 does not add LIVE/PAPER/order.
- Phase 18 does not add MEXC private API.
- Phase 19 adds request builder docs/helper only.
- Phase 19 request builder creates a safe request preview only.
- Phase 19 request builder does not execute dry-run.
- Phase 19 request builder does not run backtest/runner/producer/inventory.
- Phase 19 request builder does not use subprocess / QProcess / background worker.
- Phase 19 request builder keeps generated request output not committed.
- future runtime dry-run execution must be separate and explicitly approved.
- future execution still requires separate approval/phase.
- LIVE/PAPER/order remains permanently separated.

The static sample fixture
`docs/precomputed_signals_gui_local_dry_run_request_sample.json` is synthetic
and sanitized. It was not generated by a dry-run request builder and does not
prove execution.
