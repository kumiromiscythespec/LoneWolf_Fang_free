# Free Phase 17 GUI local-only dry-run design boundary

Free Phase 17 is future local-only dry-run design boundary for the
precomputed signal tape GUI. This phase is docs/tests-only.

This document fixes the safety boundary for a possible future local-only
dry-run feature before any implementation exists. It does not add execution.

## Scope

- Phase 17 is future local-only dry-run design boundary.
- docs/tests-only.
- no GUI source change.
- no runtime source change.
- no local dry-run implementation.
- no dry-run implementation.
- no execution button.
- no command execution.
- no subprocess / QProcess / background worker.
- no `os.system`, `Popen`, `startDetached`, threading, multiprocessing,
  scheduler, launch worker, or background execution path.
- no producer/backtest/runner/inventory auto-run.
- no selection or adapter auto-run.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- APP_VERSION unchanged.
- package/release not touched.
- generated real tape body / raw market data excluded.
- raw trade rows are never displayed or recorded.
- future local-only dry-run requires explicit operator confirmation.
- future local-only dry-run is not live/paper/order.
- future execution must be a separate phase.

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

## Required Future Operator Confirmation

Future dry-run must require explicit operator confirmation. The confirmation
must be separate from command preview copy UX. The confirmation must fail closed
if the selection is invalid. Future confirmation must fail closed if the
selection is invalid.

The future confirmation text must say:

- this is local-only.
- this is not LIVE/PAPER/order.
- must fail closed if the selection is invalid.
- future local-only dry-run is not live/paper/order.
- `signal_dir`.
- product `free`.
- `symbol`.
- `entry_tf`.
- `filter_tf`.
- `not_selectable_for_live=true`.
- `not_selectable_for_paper=true`.
- output directory.

Future confirmation must not be implied by selecting a tape, copying a command,
opening diagnostics, or viewing a preview. The default state is unconfirmed.

## Required Future Preflight Checks

Future dry-run must fail closed unless every preflight is true:

- `signal_dir` exists.
- selection contract valid.
- `product == free`.
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
- future Phase 18 may add dry-run request schema docs/test only.
- future Phase 19 may add local-only dry-run preview request builder.
- future runtime dry-run execution must be separate and explicitly approved.
- LIVE/PAPER/order remains permanently separated.
- future execution must be a separate phase.

Any future executor must have a new explicit approval boundary. It must not be
introduced through command preview, copy UX, selection diagnostics, manual smoke
record schema, or checklist docs.

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
