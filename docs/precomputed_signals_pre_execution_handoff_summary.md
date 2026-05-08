# Free Phase 25 pre-execution handoff summary

## Short Overview

Free Phase 25 is pre-execution handoff / readiness index docs-tests. Phase 25 is
docs/tests-only and records the state of the Free precomputed signal tape,
GUI display-only surfaces, local-only dry-run request boundary, approval
boundary, and execution audit boundary before any runtime execution work starts.

Phase 25 has no GUI source change, no runtime source change, no dry-run
execution implementation, no approval UI, no command execution, no subprocess /
QProcess / background worker, no LIVE/PAPER/order, no MEXC private API, no
approval/audit record runtime generation, no APP_VERSION bump, and no
package/release/signing/upload.

## Completed Phases Table

| Phase | Completed item | Handoff note |
| --- | --- | --- |
| Phase 1 | signal tape reader / validator / fast accounting foundation | base saved-tape schema and fast accounting only |
| Phase 2 | backtest.py explicit --use-precomputed-signals path | opt-in local saved-tape backtest fast path |
| Phase 3 | runner.py replay-only --use-precomputed-signals path | replay-only fast path; live/paper fail closed |
| Phase 4 | safe producer from existing trades.csv / equity_curve.csv | converts existing exports only |
| Phase 5 | operator-facing inventory / discovery | read-only inventory/discovery |
| Phase 6 | read-only selection contract | selection contract and preflight only |
| Phase 7 | GUI picker adapter | safe display metadata adapter |
| Phase 8 | GUI display-only picker wiring | display-only signal picker panel |
| Phase 9 | command preview only | text preview only |
| Phase 10 | copy UX only | clipboard-only copy surface |
| Phase 11 | accessibility / operator docs | docs and layout smoke hardening |
| Phase 12 | read-only selection diagnostics display | diagnostics display only |
| Phase 13 | diagnostics polish | operator-facing diagnostics polish |
| Phase 14 | GUI smoke checklist docs | docs-only GUI smoke checklist |
| Phase 15 | manual GUI smoke record format | synthetic fixture only manual smoke record format |
| Phase 16 | manual smoke record schema/static sample | static sample and validator |
| Phase 17 | local-only dry-run design boundary | design boundary only |
| Phase 18 | local-only dry-run request schema/static sample | request schema docs/test-only |
| Phase 19 | local-only dry-run request builder docs/helper | safe request preview helper |
| Phase 20 | local-only dry-run GUI preview adapter | display item adapter only |
| Phase 21 | read-only GUI preview wiring | GUI preview display-only wiring |
| Phase 22 | approval boundary docs | future approval boundary only |
| Phase 23 | approval preflight checklist / approval record static sample | static approval sample/checklist only |
| Phase 24 | execution audit record schema / static sample | future audit schema/static not_run sample only |

## Safety Invariants

- docs/tests-only.
- no GUI source change.
- no runtime source change.
- no dry-run execution implementation.
- no approval UI.
- no command execution.
- no subprocess / QProcess / background worker.
- no producer/backtest/runner/inventory auto-run.
- no LIVE/PAPER/order.
- no MEXC private API.
- no approval/audit record runtime generation.
- no APP_VERSION bump.
- no package/release/signing/upload.
- generated artifacts remain excluded.
- future runtime execution requires separate explicit approval.

## Open Decisions

- whether a future implementation phase should exist at all.
- the exact future implementation phase name.
- APP_VERSION policy for any future release phase.
- repo-external output location for future generated local-only artifacts.
- generated artifact retention / cleanup window.
- whether safe-condition trades.csv may be retained outside the repo.
- whether any future packaging/release phase starts after Pro plan changes settle.

## Blocked Items

Blocked until separate explicit approval:

- runtime local dry-run execution.
- GUI dry-run button.
- approval UI.
- confirm/approve/execute button.
- command execution from GUI.
- producer/backtest/runner/inventory auto-run from GUI.
- generated approval record at runtime.
- generated execution audit record at runtime.
- package/release/signing/upload.
- APP_VERSION bump.

Blocked permanently by boundary:

- LIVE/PAPER/order connection from the precomputed signal tape GUI surface.
- MEXC private API path.
- balance fetch.
- order fetch.
- order submit.
- raw market data or raw OHLCV committed to repo or Phase 25 zip.

## Explicit Approvals Needed

Future work needs separate explicit approval for:

- moving beyond docs/tests-only.
- naming an implementation phase.
- approving local_saved_tape_backtest_replay_only.
- generating a future approval record.
- generating a future execution audit record.
- accepting generated artifact policy.
- deciding APP_VERSION policy.
- starting package/release/signing/upload, after Pro plan changes settle.

Approval is not LIVE/PAPER/order/private API/release/package approval.

## Artifact Policy

Allowed future generated artifacts:

- safe summary JSON.
- fast_summary.json.
- equity_curve.csv.
- safe-condition trades.csv.
- local output manifest.
- approval record.
- execution audit record.
- sanitized manual smoke record.
- safe metadata log.

Allowed artifacts must remain repo-external or explicitly ignored unless a later
phase says otherwise.

Forbidden artifacts:

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
- order.
- balance.
- screenshots with secrets/balances/orders/account details.
- package zip.
- exe.
- installer.
- release asset.
- zip inside zip.
- generated artifacts committed to repo.

## Test Command Summary

Phase 25 verification is docs/tests-only:

```powershell
python -m compileall -q app tests
pytest tests\test_signal_tape_schema.py tests\test_fast_backtest_signals_no_network.py tests\test_fast_backtest_signals_accounting.py tests\test_precomputed_signals_dd_schema.py tests\test_precomputed_signals_backtest_cli.py tests\test_precomputed_signals_runner_replay_cli.py tests\test_precompute_signals_safe_producer.py tests\test_precomputed_signals_inventory.py tests\test_precomputed_signals_selection_contract.py tests\test_precomputed_signals_gui_adapter.py tests\test_precomputed_signals_gui_picker_wiring.py tests\test_precomputed_signals_gui_command_preview.py tests\test_precomputed_signals_gui_command_copy_ux.py tests\test_precomputed_signals_gui_command_copy_accessibility.py tests\test_precomputed_signals_gui_selection_diagnostics.py tests\test_precomputed_signals_gui_selection_diagnostics_polish.py tests\test_precomputed_signals_gui_smoke_checklist_docs.py tests\test_precomputed_signals_gui_manual_smoke_record_docs.py tests\test_precomputed_signals_gui_manual_smoke_record_schema.py tests\test_precomputed_signals_gui_local_dry_run_design_docs.py tests\test_precomputed_signals_gui_local_dry_run_request_schema_docs.py tests\test_precomputed_signals_local_dry_run_request_builder.py tests\test_precomputed_signals_local_dry_run_gui_adapter.py tests\test_precomputed_signals_local_dry_run_gui_preview_wiring.py tests\test_precomputed_signals_gui_local_dry_run_approval_boundary_docs.py tests\test_precomputed_signals_gui_local_dry_run_approval_record_docs.py tests\test_precomputed_signals_gui_local_dry_run_execution_audit_docs.py tests\test_precomputed_signals_pre_execution_readiness_index_docs.py
git diff --check
git diff --cached --check
```

GUI runtime smoke is not executed in Phase 25 because this is docs/tests-only
and no GUI runtime path should be exercised.

## Zip Convention

Repo-external zip convention:

`C:\Users\yu_ki\AppData\Local\LoneWolfFang\data\free_precomputed_signals_phase25_pre_execution_readiness_index_<timestamp>.zip`

The zip may include only Phase 25 committed docs/tests, changed files manifest,
safe summary, test summary, pre-execution readiness summary, Free migration
summary, and commit hash summary.

The zip must not include .git, .venv, node_modules, build cache, raw market data,
generated real signal tape body, generated inventory output, generated selection
output, generated GUI adapter output, generated command preview output,
generated clipboard output, generated diagnostics output, generated smoke
output, generated smoke record, generated dry-run request, generated approval
record, generated execution audit record, screenshots, API key, secret, token,
authorization, raw billing, raw order payload, balance snapshot, private key,
apk, exe, setup binary, package zip, runtime dirs, exports dirs, or zip inside
zip.

## Worktree Status Note

- latest Free commit at Phase 25 start:
  `c1ea7ba5555796e5929081ac017fdc1ea19ac3b6`.
- latest Free commit field for the completed Phase 25 commit is recorded in the
  repo-external commit hash summary after commit.
- branch field: `publish/pairs-live-api-20260423-free`.
- worktree note field: expected unrelated untracked
  `LoneWolf_Fang_Free_Package.zip`.
- LoneWolf_Fang_Free_Package.zip do not stage unless packaging phase approved.
- package zip / exe / installer not staged.
- APP_VERSION unchanged.

## Recommended Next Step

It is safe to pause after Phase 25. Runtime dry-run execution should not be
implemented without explicit new approval. Packaging/release should remain
paused while Pro plan changes are in progress.

Next safe choices:

- pause migration.
- run docs/test-only review.
- prepare explicit local-only dry-run implementation prompt only after approval.
- package/release phase only after Pro plan changes settle.
