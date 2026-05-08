# Free Phase 17 GUI local-only dry-run design boundary

Free Phase 17 is future local-only dry-run design boundary for the
precomputed signal tape GUI. This phase is docs/tests-only.

This document fixes the safety boundary for a possible future local-only
dry-run feature before any implementation exists. It does not add execution.
Phase 18 extends this boundary with a docs/test-only request schema and a
synthetic static sample fixture. The request schema is documented in
`docs/precomputed_signals_gui_local_dry_run_request_schema.md`; the sample is
`docs/precomputed_signals_gui_local_dry_run_request_sample.json`.
Free Phase 19: local-only dry-run request builder docs/helper adds a helper that
creates a safe request preview only. It does not add dry-run execution.
Free Phase 20: request builder read-only GUI preview adapter adds a helper that
converts the Phase 19 request builder output into a GUI preview item only. It
does not connect GUI source and does not add dry-run execution.
Free Phase 21: read-only GUI source wiring of dry-run request preview adapter
connects that preview item to the Free GUI as display-only text. It does not add
dry-run execution, request file generation, confirmation controls, or runtime
execution paths.
Free Phase 22: local-only dry-run execution approval boundary fixes the future
operator approval boundary as docs/tests-only. It adds no approval UI
implementation, no dry-run execution implementation, no approval record
generation, no execution audit record generation, no GUI source change, no
runtime source change, no command execution, no subprocess / QProcess /
background worker, no LIVE/PAPER/order, and no MEXC private API. The boundary
is detailed in
`docs/precomputed_signals_gui_local_dry_run_approval_boundary.md`.
Free Phase 23: approval preflight checklist / approval record static sample
docs-tests fixes the required preflight checklist and static sanitized approval
record sample in
`docs/precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md`
and
`docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`. It
adds no approval UI implementation, no approval record generation at runtime,
no execution audit record generation, no dry-run execution, no GUI source
change, no runtime source change, no command execution, no subprocess /
QProcess / background worker, no LIVE/PAPER/order, and no MEXC private API.
Free Phase 24: execution audit record schema / static sample docs-tests fixes
the future execution audit record schema and sanitized static `not_run` sample
in `docs/precomputed_signals_gui_local_dry_run_execution_audit_schema.md` and
`docs/precomputed_signals_gui_local_dry_run_execution_audit_sample.json`. It
adds no approval UI implementation, no dry-run execution implementation, no
approval record generation at runtime, no execution audit record generation at
runtime, no GUI source change, no runtime source change, no command execution,
no subprocess / QProcess / background worker, no LIVE/PAPER/order, and no MEXC
private API.

## Scope

- Phase 17 is future local-only dry-run design boundary.
- Free Phase 19: local-only dry-run request builder docs/helper.
- Free Phase 20: request builder read-only GUI preview adapter.
- Free Phase 21: read-only GUI source wiring of dry-run request preview adapter.
- Free Phase 22: local-only dry-run execution approval boundary.
- Free Phase 23: approval preflight checklist / approval record static sample
  docs-tests.
- Free Phase 24: execution audit record schema / static sample docs-tests.
- docs/tests-only.
- Phase 22 is docs/tests-only.
- Phase 23 is docs/tests-only.
- Phase 24 is docs/tests-only.
- no approval UI implementation.
- no dry-run execution implementation.
- no approval record generation.
- no approval record generation at runtime.
- no execution audit record generation.
- no execution audit record generation at runtime.
- Phase 17-20 no GUI source change; Phase 21 adds read-only/display-only GUI
  source wiring only.
- Phase 22 no GUI source change.
- Phase 23 no GUI source change.
- Phase 24 no GUI source change.
- no runtime source change.
- Phase 22 no runtime source change.
- Phase 23 no runtime source change.
- Phase 24 no runtime source change.
- no local dry-run implementation.
- no dry-run implementation.
- GUI displays dry-run request preview only.
- GUI does not create request files.
- GUI does not execute dry-run.
- GUI does not run backtest/runner/producer/inventory.
- GUI does not use subprocess / QProcess / Popen / background worker.
- request builder creates a safe request preview only.
- adapter creates GUI preview item only.
- adapter does not execute dry-run.
- adapter does not run backtest/runner/producer/inventory.
- adapter does not use subprocess / QProcess / background worker.
- adapter display is preview-only.
- adapter display is not LIVE/PAPER/order.
- request builder does not execute dry-run.
- request builder does not run backtest/runner/producer/inventory.
- request builder does not use subprocess / QProcess / background worker.
- no execution button.
- no command execution.
- no subprocess / QProcess / background worker.
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch worker, or background execution path.
- no producer/backtest/runner/inventory auto-run.
- no selection or adapter auto-run.
- GUI source is not connected in Phase 20.
- GUI source is display-only connected in Phase 21.
- no confirm button in Phase 21.
- no dry-run button in Phase 21.
- no live/paper/order connection.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- APP_VERSION unchanged.
- package/release not touched.
- generated real tape body / raw market data excluded.
- raw trade rows are never displayed or recorded.
- raw trade rows / entry_exec / exit_exec / qty / trade id are not shown.
- allowed / forbidden artifacts are shown as safe labels only.
- generated GUI preview output is not committed.
- generated dry-run GUI preview output is not committed.
- future local-only dry-run requires explicit operator confirmation.
- explicit operator approval required.
- approval is not live/paper/order approval.
- approval record is not execution audit record.
- execution audit record is not approval record.
- execution audit record is not order/trading record.
- future local-only dry-run is not live/paper/order.
- future execution must be a separate phase.
- future execution still requires separate approval/phase.
- future execution requires separate explicit approval phase.
- future execution requires separate explicit approval and implementation phase.

Phase 17 must not create generated dry-run output, generated dry-run request
files, screenshots, command preview output, diagnostics output, inventory
output, selection output, adapter output, package zips, exe, installer, setup,
release assets, runtime exports, or zip-in-zip artifacts.

## Definition

Future local-only dry-run is a possible explicit operator-confirmed action. It
may only use an already selected, validated precomputed signal tape and may only
run local saved-tape backtest/replay fast path in a future phase.

Future local-only dry-run must not:

- use LIVE.
- use PAPER.
- use order execution.
- use private API.
- fetch balance.
- fetch orders.
- submit orders.
- call MEXC private API.
- use raw market data unless a future phase explicitly defines a local fixture
  boundary.
- auto-run producer unless a separate future phase explicitly permits it.
- auto-run inventory scan.
- run in the background.
- touch package/release assets.

The only future execution candidates are local saved-tape fast paths that
already read an existing selected tape directory. They remain separate from
LIVE/PAPER/order forever.

## Current Phase 17 Boundary

Current Phase 17 boundary:

- no dry-run is executed.
- no command is executed.
- no subprocess / QProcess / background worker is added.
- no producer/backtest/runner/inventory auto-run.
- no selection or adapter auto-run.
- command preview remains preview-only.
- copy UX remains clipboard-only.
- GUI diagnostics remain read-only.
- manual smoke record remains display-only.
- manual smoke record remains synthetic fixture only.
- no LIVE/PAPER/order.
- no MEXC private API.
- no generated dry-run request.
- no generated dry-run output.
- no GUI runtime smoke is required or executed by this phase.

Phase 17 docs and tests may describe future request fields, allowed modes,
artifacts, and fail-closed conditions. They must not create actual request files
or execute any future request.

## Current Phase 18 Request Schema Boundary

Free Phase 18 is local-only dry-run request schema docs/test-only. It adds no
GUI source, no runtime source, no local dry-run implementation, no request
builder implementation, no generated request file, and no command execution.

Phase 18 fixes these request schema defaults:

- `request_type=precomputed_signal_local_dry_run_request`
- `product=free`
- `operator_confirmation_required=true`
- `operator_confirmed=false by default`
- `preview_only_before_confirmation=true`
- `execution_enabled_after_confirmation=false`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`
- `status=not_run` for the static sample

Phase 18 keeps these request schema boundaries fixed:

- Phase 18 does not implement request builder.
- Phase 18 does not create request files.
- Phase 18 does not execute local dry-run.
- Phase 18 does not add subprocess / QProcess / background worker.
- Phase 18 does not add producer/backtest/runner/inventory auto-run.
- Phase 18 does not add LIVE/PAPER/order.
- future runtime dry-run execution must be separate and explicitly approved.

The Phase 18 static request sample is synthetic and sanitized. It is not a
generated dry-run request, not generated runtime output, not a command execution
record, not runtime/order proof, and not live/paper/order proof.

## Current Phase 19 Request Builder Boundary

Free Phase 19: local-only dry-run request builder docs/helper adds
`precomputed_signals_local_dry_run_request.py` and focused tests. The helper is
local-only, read-only with respect to signal selection, and preview-only. The
request builder creates a safe request preview only: a request dict, safe text
preview, or safe JSON preview written only to a user-specified path.

The request builder does not execute dry-run, does not run
backtest/runner/producer/inventory, does not use subprocess / QProcess /
background worker, does not add a GUI execution button, and does not touch
runtime execution source. It does not connect to LIVE/PAPER/order, MEXC private
API, balance fetch, order fetch, or submit paths.

Phase 19 keeps these defaults fixed:

- `operator_confirmed=false by default`
- `execution_enabled_after_confirmation=false`
- `operator_confirmation_required=true`
- `preview_only_before_confirmation=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

Allowed `dry_run_mode` values remain only:

- `backtest_fast_path_local_only`
- `runner_replay_fast_path_local_only`

The builder fails closed for invalid selection, missing signal directory,
missing manifest, missing summary, missing `trades.csv`, unsafe manifest,
unsafe summary, forbidden field, positive legacy max_drawdown, LIVE/PAPER
request, order/balance request, private API request, missing operator
confirmation, background execution request, package/release output path, and
generated artifact policy violation.

Allowed artifacts remain safe summaries, fast-path equity curve CSV,
fast-path trades CSV only under the safe condition, fast summary JSON, safe
metadata log, and sanitized manual smoke record. Forbidden artifacts remain raw
market data, raw OHLCV, raw trades rows, `entry_exec`, `exit_exec`, `qty`, trade
id, order id, raw order, balance snapshot, API key, secret, token,
authorization, raw billing, package zip, exe, installer, release asset, and
screenshots containing secrets, balances, or orders.

The builder output must not include generated real tape body, raw market data,
raw trade rows, `entry_exec`, `exit_exec`, `qty`, trade id, order payloads,
balance data, secrets, generated dry-run output, screenshots, package zips, exe,
installers, or release assets. Generated request output not committed remains a
hard rule for repo diffs and migration zips.

Future execution still requires separate approval/phase. Phase 19 does not make
local-only dry-run executable.

## Current Phase 20 GUI Preview Adapter Boundary

Free Phase 20: request builder read-only GUI preview adapter adds
`precomputed_signals_local_dry_run_gui_adapter.py` and focused tests. The
adapter creates GUI preview item only from a valid Phase 19 local-only dry-run
request dict or from an existing selected `signal_dir` through the Phase 19
request builder. The adapter is read-only and display-only.

The adapter does not execute dry-run, does not run
backtest/runner/producer/inventory, does not use subprocess / QProcess /
background worker, does not add a GUI execution button, does not connect GUI
source, and does not touch runtime execution source. GUI source is not connected
in Phase 20. It does not connect to LIVE/PAPER/order, MEXC private API, balance
fetch, order fetch, or submit paths.

Phase 20 keeps these defaults fixed:

- `operator_confirmed=false`
- `execution_enabled_after_confirmation=false`
- `operator_confirmation_required=true`
- `preview_only_before_confirmation=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

The preview labels must make the boundary visible:

- `Local dry-run request preview`
- `Preview only`
- `Execution disabled`
- `Operator confirmation required`
- `Not LIVE/PAPER/order`
- `No private API / no balance fetch / no order fetch`
- `This preview does not execute commands`
- `Future execution requires separate approved phase`

Allowed artifacts are shown as safe labels only: safe summary, equity curve,
and fast summary. Forbidden artifacts are shown as safe labels only: raw market
data, raw trades rows, orders, balances, and secrets. These labels are policy
warnings, not payloads. Raw trade rows / entry_exec / exit_exec / qty / trade
id are not shown, and API key / secret / token / authorization / raw order /
balance snapshot / raw billing are not shown.

The adapter validates the request and preview fail-closed. It rejects non-free
product, invalid request type, invalid dry-run mode, `operator_confirmed=true`,
`execution_enabled_after_confirmation=true`, LIVE/PAPER/order command text,
balance/order/private API command text, unsafe output directory text, unknown
fail-closed reasons, unexpected payload fields, raw trade rows, order payloads,
balance payloads, secrets, generated artifact policy paths, and package/release
paths.

The Phase 20 adapter display is preview-only and adapter display is not
LIVE/PAPER/order. Generated GUI preview output is not committed. Generated
dry-run GUI preview output is not committed. Future execution still requires
separate approval/phase.

## Current Phase 21 GUI Source Wiring Boundary

Free Phase 21: read-only GUI source wiring of dry-run request preview adapter
connects the Phase 20 adapter to Free GUI source as a display-only panel under
the selected precomputed signal tape section. The GUI displays dry-run request
preview only and keeps the preview state in memory.

The Phase 21 panel shows two preview sections/cards when the picker item is
valid: `Backtest fast path local-only` and `Runner replay fast path local-only`.
Each section shows safe labels for `signal_dir`, `symbol`, `entry_tf`,
`filter_tf`, `signal_set_id`, `output_dir`, `command_text_preview`,
`allowed_artifacts_label`, `forbidden_artifacts_label`,
`fail_closed_reasons_label`, `status`, and `status_reason`.

Phase 21 keeps these defaults fixed in the GUI:

- `operator_confirmed=false`
- `execution_enabled_after_confirmation=false`
- `preview_only_before_confirmation=true`
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `operator confirmation required`
- future execution requires separate approved phase

Phase 21 does not create request files, does not create generated dry-run GUI
preview output, does not execute dry-run, does not run backtest / runner /
producer / inventory, and does not use subprocess / QProcess / Popen /
startDetached / threading / multiprocessing / scheduler / background worker.
It adds no confirm button, no approve button, no execute button, no start
button, and no dry-run button.

Phase 21 does not connect selected precomputed signal tape state to LIVE/PAPER/
order, MEXC private API, balance fetch, order fetch, submit paths, strategy,
indicators, exchange, risk, or order runtime logic. Runtime execution source is
unchanged. APP_VERSION unchanged. Package, exe, setup, installer, signing,
upload, and release assets are not touched.

The GUI display excludes raw trade rows, `entry_exec`, `exit_exec`, `qty`, exact
trade id, order id, raw order payload, balance snapshot, API key, secret, token,
authorization, raw billing, raw market data, raw OHLCV, generated real tape
body, screenshots, package zips, exe, installer, setup binary, runtime dirs,
exports dirs, and zip-in-zip artifacts.

## Current Phase 22 Approval Boundary

Free Phase 22 is local-only dry-run execution approval boundary. Phase 22 is
docs/tests-only and is defined in
`docs/precomputed_signals_gui_local_dry_run_approval_boundary.md`.

Phase 22 keeps the GUI local dry-run preview read-only and does not implement
approval UI. It does not implement dry-run execution, does not generate approval
records, does not generate execution audit records, does not change GUI source,
does not change runtime source, does not execute commands, does not use
subprocess / QProcess / background worker, does not auto-run producer/backtest/
runner/inventory, does not connect to LIVE/PAPER/order, and does not call MEXC
private API.

The approval boundary states that explicit operator approval required applies
only to future local saved-tape backtest/replay fast paths. Approval is not
live/paper/order approval, not private API approval, not command copy approval,
not manual smoke record approval, and not release/package approval. Phase 22
must not create approval record files. Phase 22 must not create execution audit
records. Future execution requires separate explicit approval phase.

## Current Phase 23 Approval Preflight Checklist And Static Sample

Free Phase 23 is approval preflight checklist / approval record static sample
docs-tests. Phase 23 is docs/tests-only and is defined by:

- `docs/precomputed_signals_gui_local_dry_run_approval_preflight_checklist.md`
- `docs/precomputed_signals_gui_local_dry_run_approval_record_sample.json`

Phase 23 has no approval UI implementation, no approval record generation at
runtime, no execution audit record generation, no dry-run execution, no GUI
source change, no runtime source change, no command execution, no subprocess /
QProcess / background worker, no producer/backtest/runner/inventory auto-run,
no LIVE/PAPER/order, no MEXC private API, APP_VERSION unchanged, and
package/release not touched.

The Phase 23 approval preflight checklist requires request_hash present,
selection_hash present, valid request schema, valid selection contract,
`product == free`, allowed local-only dry_run_mode, manifest / summary /
trades.csv presence, manifest / summary / trades.csv hash coverage, forbidden
fields rejected, positive legacy max_drawdown rejected, safe output_dir,
allowed_artifacts reviewed, forbidden_artifacts reviewed, worktree status
recorded, package/release artifacts excluded, no background execution, no
private API, no order/balance path, and no raw trade rows displayed or
recorded.

The Phase 23 static approval record sample uses
`record_type=precomputed_signal_local_dry_run_approval_record`,
`approval_schema_version=free_precomputed_local_dry_run_approval_record_v1`,
`approval_scope=local_saved_tape_backtest_replay_only`, `status=not_run`,
`operator_confirmed=false`, and `preflight_passed=false`. request_hash and
selection_hash are synthetic placeholders. The sample is sanitized and static;
it is not generated runtime output, not actual approval, not dry-run execution,
and not an execution audit record.

Approval record is not execution audit record. Approval record does not prove
execution. Future execution must be a separate phase and must reference an
approval_record_hash and request_hash only after a separately approved
implementation exists.

## Current Phase 24 Execution Audit Schema And Static Sample

Free Phase 24 is execution audit record schema / static sample docs-tests.
Phase 24 is docs/tests-only and is defined by:

- `docs/precomputed_signals_gui_local_dry_run_execution_audit_schema.md`
- `docs/precomputed_signals_gui_local_dry_run_execution_audit_sample.json`

Phase 24 has no approval UI implementation, no dry-run execution
implementation, no approval record generation at runtime, no execution audit
record generation at runtime, no GUI source change, no runtime source change,
no command execution, no subprocess / QProcess / background worker, no
producer/backtest/runner/inventory auto-run, no LIVE/PAPER/order, no MEXC
private API, APP_VERSION unchanged, and package/release not touched.

The Phase 24 execution audit schema uses
`record_type=precomputed_signal_local_dry_run_execution_record`,
`execution_audit_schema_version=free_precomputed_local_dry_run_execution_audit_v1`,
`approval_scope=local_saved_tape_backtest_replay_only`, allowed dry_run_mode
values of `backtest_fast_path_local_only` and
`runner_replay_fast_path_local_only`, and result_status values of `pass`,
`fail`, `blocked`, `invalid`, and `not_run`. The Phase 24 static sample is
`not_run` only.

approval_record_hash / request_hash linkage is mandatory. approval_record_hash
/ request_hash / selection_hash linkage is mandatory. Missing
approval_record_hash, request_hash, selection_hash, or approval_scope mismatch
must fail closed. Execution audit record is not approval record. Execution
audit record is not order/trading record.

The output artifact manifest schema allows only safe local artifact metadata:
`safe_summary_json`, `fast_summary_json`, `equity_curve_csv`,
`fast_path_trades_csv_safe_condition`, `safe_metadata_log`,
`sanitized_manual_smoke_record`, and `local_output_manifest`. Forbidden
artifact types include `raw_market_data`, `raw_ohlcv`, `raw_trades_rows`,
`entry_exec_rows`, `exit_exec_rows`, `qty_rows`, `order_payload`,
`balance_snapshot`, `api_key`, `secret`, `token`, `authorization`,
`raw_billing`, `package_zip`, `exe`, `installer`, `release_asset`, and
`screenshot_with_sensitive_data`.

Required Phase 24 assertions include no LIVE/PAPER/order/private API/background
execution, no private API, no order/balance path, forbidden fields absent, raw
trade rows absent, generated artifact policy checked, approval_record_hash
verified, and request_hash verified. Future execution must be separate explicit
approval and implementation phase.

## Required Future Operator Confirmation

Future dry-run must require explicit operator confirmation. The confirmation
must be separate from command preview copy UX. The confirmation must fail closed
if the selection is invalid. Future confirmation must fail closed if the
selection is invalid.

The future confirmation text must say:

- this is local-only.
- this is not LIVE/PAPER/order.
- No MEXC private API.
- No balance fetch.
- No order fetch.
- No order submit.
- Uses selected precomputed signal tape only.
- must fail closed if the selection is invalid.
- future local-only dry-run is not live/paper/order.
- `signal_dir`.
- product `free`.
- `symbol`.
- `entry_tf`.
- `filter_tf`.
- `dry_run_mode`.
- `not_selectable_for_live=true`.
- `not_selectable_for_paper=true`.
- output directory.
- operator understands generated artifacts policy.
- operator understands execution is local saved-tape fast path only.

Future confirmation must not be implied by selecting a tape, copying a command,
opening diagnostics, or viewing a preview. The default state is unconfirmed:
`operator_confirmed=false`, `execution_enabled=false`, and
`execution_enabled_after_confirmation=false`.

## Required Future Preflight Checks

Future dry-run must fail closed unless every preflight is true:

- `signal_dir` exists.
- selection contract valid.
- valid request schema.
- `product == free`.
- dry_run_mode in allowed enum.
- `operator_confirmed=false before confirmation`.
- `execution_enabled=false before confirmation`.
- `execution_enabled_after_confirmation=false before confirmation`.
- `safety_scope research_only=true`.
- `paper_live_order_execution=false`.
- `not_selectable_for_live=true`.
- `not_selectable_for_paper=true`.
- `manifest.json` present.
- `summary.json` present.
- `trades.csv` present.
- manifest / summary / trades hash metadata present.
- manifest / summary hash metadata present.
- positive legacy max_drawdown rejected.
- forbidden fields rejected.
- no raw trades rows displayed.
- output dir is repo-external or safe runtime export dir.
- package/release assets not touched.
- APP_VERSION unchanged.
- worktree state is reported.
- generated artifacts excluded from commit/zip unless explicitly summarized.
- operator confirmation present.
- no background execution requested.
- approval record must match request hash in a future execution phase.

Fail closed examples include missing manifest / unsafe manifest / forbidden
field / positive legacy max_drawdown.

## Allowed dry_run_mode enum

Allowed dry_run_mode enum:

- `backtest_fast_path_local_only`
- `runner_replay_fast_path_local_only`

## Not Allowed Modes

These values and behaviors are not allowed:

- `live` (not allowed)
- `paper` (not allowed)
- `order_submit` (not allowed)
- `order_fetch` (not allowed)
- `balance_fetch` (not allowed)
- `private_api` (not allowed)
- `producer_auto_run` (not allowed)
- `inventory_auto_scan` (not allowed)
- `background_worker` (not allowed)

LIVE/PAPER/order, private API, order fetch, order submit, balance fetch,
producer auto-run, inventory auto-scan, and background worker execution must
remain outside the local-only dry-run boundary.

## Required Future Dry-Run Request Schema

dry-run request schema fields:

- `schema_version`
- `request_type`
- `phase`
- `product`
- `signal_dir`
- `symbol`
- `entry_tf`
- `filter_tf`
- `signal_set_id`
- `dry_run_mode`
- `output_dir`
- `requested_at_utc`
- `operator_confirmation_required`
- `operator_confirmed`
- `preview_only_before_confirmation`
- `execution_enabled_after_confirmation`
- `not_selectable_for_live`
- `not_selectable_for_paper`
- `safety_research_only`
- `paper_live_order_execution`
- `command_text_preview`
- `allowed_artifacts`
- `forbidden_artifacts`
- `status`
- `status_reason`

Required request type:

- `precomputed_signal_local_dry_run_request`

Required defaults and flags:

- `operator_confirmation_required=true`
- `operator_confirmed=false by default`
- `operator_confirmed=true` only after explicit future confirmation.
- `preview_only_before_confirmation=true`
- `execution_enabled_after_confirmation=false` in Phase 17 because no executor
  exists.
- `not_selectable_for_live=true`
- `not_selectable_for_paper=true`
- `safety_research_only=true`
- `paper_live_order_execution=false`

Phase 17 must not create actual request files.

## Allowed Future Output Artifacts

Allowed artifacts for a future local-only dry-run are limited to safe local
outputs:

- safe summary JSON.
- `equity_curve.csv` generated by fast path.
- `trades.csv` generated by fast path only if already redacted / synthetic /
  saved-tape compatible.
- `fast_summary.json`.
- local log with safe metadata only.
- manual smoke record with safe metadata only.

Allowed artifacts must remain outside package/release assets and must not be
committed or zipped unless a future phase explicitly summarizes them safely.

## Forbidden Future Output Artifacts

Forbidden artifacts:

- raw market data.
- raw OHLCV.
- raw trades rows.
- `entry_exec`.
- `exit_exec`.
- `qty`.
- trade id.
- raw trades rows with entry_exec / exit_exec / qty / trade id.
- order id.
- raw order.
- order.
- balance snapshot.
- balance.
- API key.
- secret.
- token.
- authorization.
- raw billing.
- screenshots containing secrets / balances / orders / account details.
- screenshots containing raw trades rows.
- package zip.
- exe.
- installer.
- setup binary.
- release assets.
- runtime dirs copied into repo.
- exports dirs copied into repo.
- zip-in-zip.

API key / secret / token / authorization / raw billing / order / balance are
forbidden in docs, tests, static samples, generated artifacts, repo zips, and
manual records.

## Required Future Fail-Closed Conditions

fail-closed conditions:

- invalid selection.
- missing `signal_dir`.
- missing manifest / missing summary / missing `trades.csv`.
- missing manifest.
- unsafe manifest.
- unsafe summary.
- forbidden field.
- positive legacy max_drawdown.
- missing manifest / unsafe manifest / forbidden field / positive legacy
  max_drawdown fail closed.
- live/paper requested.
- LIVE/PAPER/order requested.
- order/balance/private API requested.
- output path under release/package artifact area.
- operator confirmation missing.
- background execution requested.
- producer auto-run requested.
- inventory auto-scan requested.
- package/release asset mutation requested.
- APP_VERSION mutation requested.

Every fail-closed condition must stop before execution. Future implementations
must report a safe status and sanitized reason only.

## Output Artifact Exclusion Policy

Generated real tape body / raw market data excluded remains a hard policy for
repo commits and migration zips.

Generated artifacts are excluded from commit/zip unless explicitly summarized.
Generated dry-run requests, generated dry-run output, screenshots, command
preview output, diagnostics output, inventory output, selection output, adapter
output, clipboard output, generated smoke output, generated smoke records,
package zip, exe, installer, setup binary, release assets, runtime dirs, export
dirs, and zip-in-zip artifacts must not be included in the Phase 17 repo diff
or repo-external zip.

## Future Implementation Split

- Phase 17 only defines design boundary.
- Phase 18 adds dry-run request schema docs/test only.
- Phase 18 does not implement request builder.
- Phase 18 does not create request files.
- Phase 18 does not execute local dry-run.
- Phase 19 adds local-only dry-run preview request builder docs/helper.
- Phase 19 request builder creates a safe request preview only.
- Phase 19 request builder does not execute dry-run.
- Phase 19 request builder does not run backtest/runner/producer/inventory.
- Phase 19 request builder does not use subprocess / QProcess / background worker.
- Phase 20 adds request builder read-only GUI preview adapter docs/tests/helper.
- Phase 20 adapter creates GUI preview item only.
- Phase 20 adapter does not execute dry-run.
- Phase 20 GUI source is not connected.
- Phase 21 adds read-only GUI source wiring of dry-run request preview adapter.
- Phase 21 GUI displays dry-run request preview only.
- Phase 21 GUI does not create request files.
- Phase 21 GUI does not execute dry-run.
- Phase 21 GUI does not add confirm / dry-run / execute buttons.
- Phase 21 runtime execution source is unchanged.
- Phase 21 APP_VERSION unchanged.
- Phase 22 defines approval boundary only.
- Phase 22 is docs/tests-only.
- Phase 22 no approval UI implementation.
- Phase 22 no dry-run execution implementation.
- Phase 22 no approval record generation.
- Phase 22 no execution audit record generation.
- Phase 23 provides static approval sample/checklist only.
- Phase 23 is docs/tests-only.
- Phase 23 no approval UI implementation.
- Phase 23 no approval record generation at runtime.
- Phase 23 no execution audit record generation.
- Phase 23 no dry-run execution.
- Phase 24 defines execution audit schema/static sample only.
- Phase 24 is docs/tests-only.
- Phase 24 no approval UI implementation.
- Phase 24 no dry-run execution implementation.
- Phase 24 no approval record generation at runtime.
- Phase 24 no execution audit record generation at runtime.
- Phase 24 no GUI source change.
- Phase 24 no runtime source change.
- approval record is not execution audit record.
- execution audit record is not approval record.
- execution audit record is not order/trading record.
- future runtime dry-run execution must be separate and explicitly approved.
- LIVE/PAPER/order remains permanently separated.
- future execution must be a separate phase.
- future execution still requires separate approval/phase.
- future execution requires separate explicit approval phase.
- future execution requires separate explicit approval and implementation phase.

Any future executor must have a new explicit approval boundary. It must not be
introduced through command preview, copy UX, selection diagnostics, manual smoke
record schema, checklist docs, or execution audit schema docs.

## Manual Smoke Record Relationship

- Phase 15/16 manual smoke record remains display-only.
- manual smoke record remains display-only.
- dry-run result record is a separate future artifact.
- manual GUI smoke record must not be reused as order/runtime proof.
- screenshots remain optional and sanitized only.
- screenshots must not be included unless explicitly sanitized in a future
  phase.

The manual smoke record is safe metadata only. It is not proof that a local-only
dry-run, LIVE/PAPER run, order path, producer, backtest, runner, inventory scan,
selection command, adapter command, or private API path executed.
