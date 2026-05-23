# Free Phase 25 pre-execution readiness index

Free Phase 25 is pre-execution handoff / readiness index docs-tests for the
Free precomputed signal tape and local-only dry-run boundary. Phase 25 is
docs/tests-only. It turns the Phase 1-24 work into a single readiness index
before any runtime local dry-run execution phase exists.

Phase 25 does not move into execution. Runtime dry-run execution requires
explicit new approval. Packaging/release remains paused while Pro plan changes
are in progress.

## Scope

- Phase 25 is pre-execution handoff / readiness index docs-tests.
- Phase 25 is docs/tests-only.
- no GUI source change.
- no runtime source change.
- no dry-run execution implementation.
- no approval UI implementation.
- no approval UI.
- no execution UI.
- no execution audit runtime generation.
- no approval/audit record runtime generation.
- no command execution.
- no producer/backtest/runner/inventory auto-run.
- no selection/adapter/request builder auto-run.
- no subprocess / QProcess / background worker.
- no LIVE/PAPER/order.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- no private API path.
- no balance fetch.
- no order fetch.
- no order submit.
- no APP_VERSION bump.
- no package/release/signing/upload.
- generated artifacts remain excluded.
- future runtime execution requires separate explicit approval.

## Current Implemented Capabilities

The Phase 1-24 completed phases table is the handoff inventory. Each item is
implemented only within its stated boundary.

| Phase | Completed capability | Fixed boundary |
| --- | --- | --- |
| Phase 1 | signal tape reader / validator / fast accounting foundation | research-only saved tape helpers; no producer, GUI execution, LIVE/PAPER/order, raw market data in repo, or package artifact |
| Phase 2 | backtest.py explicit --use-precomputed-signals path | opt-in fast path only; no automatic discovery, LIVE/PAPER/order, runtime strategy/risk/order changes, or producer |
| Phase 3 | runner.py replay-only --use-precomputed-signals path | replay-only opt-in fast path; live/paper modes fail closed; no producer or GUI execution |
| Phase 4 | safe producer from existing trades.csv / equity_curve.csv | converts existing research exports only; no strategy, indicators, exchange, ccxt, runner, OHLCV, private API, or raw market data read |
| Phase 5 | operator-facing inventory / discovery | read-only discovery; no auto-run, no execution, no LIVE/PAPER/order, no raw rows in docs or generated migration artifacts |
| Phase 6 | read-only selection contract | selection metadata contract only; no execution, no producer, no inventory auto-run, no GUI command launch |
| Phase 7 | GUI picker adapter | read-only adapter; display-safe DD mapping only; no GUI source wiring, execution, or order path |
| Phase 8 | GUI display-only picker wiring | operator-selected signal_dir display only; no execution button, command launch, LIVE/PAPER/order connection, or APP_VERSION change |
| Phase 9 | command preview only | explicit command text preview only; no command execution and no subprocess/QProcess/background worker |
| Phase 10 | copy UX only | clipboard copy only; no execution, approval, confirm, start, run, dry-run, or order path |
| Phase 11 | accessibility / operator docs | layout/accessibility smoke docs only; no runtime or package/release change |
| Phase 12 | read-only selection diagnostics display | diagnostics display only; no command execution, producer/backtest/runner/inventory auto-run, or LIVE/PAPER/order |
| Phase 13 | diagnostics polish | compact status/hash/safety display polish only; no execution, package/release, APP_VERSION, or order runtime changes |
| Phase 14 | GUI smoke checklist docs | docs-only hardening; GUI runtime smoke checklist only; no GUI runtime smoke executed by the phase |
| Phase 15 | manual GUI smoke record format | synthetic fixture only manual runtime smoke record format; no generated smoke record or runtime execution |
| Phase 16 | manual smoke record schema/static sample | static sample validator only; no GUI runtime smoke execution, screenshots, or generated smoke records |
| Phase 17 | local-only dry-run design boundary | design boundary only; no local dry-run implementation, GUI/runtime source change, command execution, subprocess/QProcess/background worker, LIVE/PAPER/order, or MEXC private API |
| Phase 18 | local-only dry-run request schema/static sample | docs/test-only schema and sanitized static sample; no request builder, generated request file, GUI/runtime source change, or execution |
| Phase 19 | local-only dry-run request builder docs/helper | safe request preview helper only; no dry-run execution, producer/backtest/runner/inventory auto-run, subprocess/QProcess/background worker, or runtime request file generation |
| Phase 20 | local-only dry-run GUI preview adapter | converts request builder output to display item only; no GUI source connection, execution, or LIVE/PAPER/order path |
| Phase 21 | read-only GUI preview wiring | display-only preview wiring; no request files, confirm/dry-run/execute buttons, command execution, background worker, or APP_VERSION change |
| Phase 22 | approval boundary docs | docs/test-only future approval boundary; no approval UI implementation, approval record generation, execution audit generation, dry-run execution, command execution, or private API |
| Phase 23 | approval preflight checklist / approval record static sample | docs/test-only checklist and static not_run approval sample; no runtime approval record generation or execution |
| Phase 24 | execution audit record schema / static sample | docs/test-only future audit schema and static not_run sample; no runtime audit generation, approval proof, execution proof, or order/trading proof |

## Current Non-Capabilities / Not Implemented

Current non-capabilities / not implemented:

- no runtime local dry-run execution.
- no GUI dry-run button.
- no approval UI.
- no confirm/approve/execute button.
- no command execution from GUI.
- no producer/backtest/runner/inventory auto-run from GUI.
- no LIVE/PAPER/order connection.
- no MEXC private API.
- no balance/order fetch.
- no subprocess / QProcess / background worker.
- no generated approval record at runtime.
- no generated execution audit record at runtime.
- no approval/audit record runtime generation.
- no packaging/release/signing/upload.
- no packaging/release/signing/upload phase in Phase 25.
- no APP_VERSION bump.

## Required Pre-Execution Gates

Required pre-execution gates before any future runtime dry-run execution:

- user explicitly approves moving beyond docs/tests-only.
- implementation phase explicitly named.
- current branch / HEAD recorded.
- worktree state reviewed.
- APP_VERSION policy decided.
- package/release artifacts excluded.
- selected signal tape exists.
- selection contract valid.
- request schema valid.
- request builder output valid.
- GUI preview adapter output valid.
- approval preflight passed.
- approval record generated in future phase only.
- approval_record_hash / request_hash / selection_hash linked.
- output_dir safe.
- generated artifact policy accepted.
- execution audit schema ready.
- no LIVE/PAPER/order path.
- no private API path.
- no background execution.
- fail-closed conditions covered.
- rollback / cleanup plan for generated local-only artifacts.
- repo-external output location decided.

## Required Future Approval Flow

Required future approval flow:

- approval is local_saved_tape_backtest_replay_only.
- approval is not LIVE/PAPER/order/private API/release/package approval.
- approval requires explicit operator confirmation.
- approval must show signal_dir, product, symbol, timeframe, dry_run_mode, output_dir.
- approval must show no private API / no balance fetch / no order fetch / no order submit.
- approval must show allowed/forbidden artifacts.
- approval must fail closed if any required field/check is missing.
- approval record and execution audit record are separate.

Future approval must not add a generic run, execute, start, dry-run, confirm, or
approve UI without a separately named and approved implementation phase. It
must not approve LIVE, PAPER, orders, MEXC private API, release, package,
signing, upload, background execution, or producer auto-run.

## Required Future Execution Flow

Required future execution flow is high-level only and allowed only if explicitly
approved in a future phase:

- validate selection.
- validate request.
- validate approval record.
- verify approval_record_hash / request_hash / selection_hash linkage.
- run local saved-tape fast path only.
- write allowed artifacts only to safe output_dir.
- generate execution audit record.
- verify no live/paper/order/private API/background execution.
- do not touch package/release assets.
- do not commit generated artifacts.

Phase 25 does not implement this flow.

## Required Future Fail-Closed List

Required future fail-closed list:

- missing approval.
- missing approval_record_hash.
- missing request_hash.
- invalid selection.
- invalid request.
- invalid approval_scope.
- operator confirmation missing.
- live/paper requested.
- order/balance requested.
- private API requested.
- background execution requested.
- unsafe output_dir.
- forbidden field.
- positive legacy max_drawdown.
- output artifact policy violation.
- package/release path involved.
- worktree status unexpected.
- unknown safety violation.

Any unknown safety violation fails closed before execution and emits only a
sanitized status reason.

## Allowed Artifacts Summary

Allowed artifacts summary for a future explicitly approved local-only execution
phase:

- safe summary JSON.
- fast_summary.json.
- equity_curve.csv.
- safe-condition trades.csv.
- local output manifest.
- approval record.
- execution audit record.
- sanitized manual smoke record.
- safe metadata log.

Allowed future generated artifacts must remain repo-external or explicitly
ignored unless a later phase says otherwise.

## Forbidden Artifacts Summary

Forbidden artifacts summary:

- raw market data.
- raw OHLCV.
- raw trades rows.
- entry_exec.
- exit_exec.
- qty.
- trade_id.
- order_id.
- raw_order.
- balance_snapshot.
- API key.
- secret.
- token.
- authorization.
- raw billing.
- screenshots with secrets/balances/orders/account details.
- package zip.
- exe.
- installer.
- release asset.
- zip inside zip.
- generated artifacts committed to repo.

Forbidden means the payload, generated file, screenshot, package artifact, or
release artifact must not be committed, staged, archived in the Phase 25 zip, or
included in later migration zips unless a separate approved phase changes the
policy with sanitized evidence only.

## Stop / Pause Recommendation

- safe to pause after Phase 25.
- It is safe to pause after Phase 25.
- runtime dry-run execution requires explicit new approval.
- Runtime dry-run execution should not be implemented without explicit new approval.
- packaging/release remains paused while Pro plan changes are in progress.
- Packaging/release should remain paused while Pro plan changes are in progress.
- LoneWolf_Fang_Free_Package.zip do not stage unless packaging phase approved.
- LoneWolf_Fang_Free_Package.zip remains unrelated / do not stage unless packaging phase is explicitly approved.
- Next implementation phase must be explicitly named and approved.

## Handoff Summary

- latest Free commit at Phase 25 start:
  `c1ea7ba5555796e5929081ac017fdc1ea19ac3b6`
  (`test(gui): document free dry run execution audit record`).
- current branch: `publish/pairs-live-api-20260423-free`.
- current untracked artifact: `LoneWolf_Fang_Free_Package.zip`, unrelated and
  not to be staged in Phase 25.
- repo-external zip convention:
  `C:\Users\yu_ki\AppData\Local\LoneWolfFang\data\free_precomputed_signals_phase25_pre_execution_readiness_index_<timestamp>.zip`.
- no-order boundary: no LIVE/PAPER/order, no MEXC private API, no balance fetch,
  no order fetch, no order submit, no GUI execution path.
- APP_VERSION status: unchanged in Phase 25; no APP_VERSION bump.
- worktree note: Phase 25 docs/tests may be committed; package zip / exe /
  installer / release asset must not be staged.

Next safe choices:

- pause migration.
- run docs/test-only review.
- prepare explicit local-only dry-run implementation prompt only after approval.
- package/release phase only after Pro plan changes settle.
