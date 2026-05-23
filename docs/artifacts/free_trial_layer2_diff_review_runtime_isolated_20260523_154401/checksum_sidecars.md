<!-- BUILD_ID: 2026-05-23_free_trial_layer2_diff_review_runtime_isolated_v1 -->

# Checksum Sidecars

Reviewed source artifacts:

- Layer 2 implementation ZIP:
  `C:\Users\yu_ki\AppData\Local\LoneWolfFang\data\free_trial_layer2_implementation_20260523_084926.zip`
- Layer 2 implementation ZIP SHA256:
  `6D255014C557CC02F0133D52C809D1B17069CBEFE9A97327A3A0C7947015A796`
- Layer 2 implementation ZIP size: `13661` bytes.
- Layer 2 implementation ZIP entries: `14`.
- Layer 2 implementation `.zip.sha256` matched.
- Layer 2 implementation `.zip.sha256.json` has a UTF-8 BOM, so
  `python -m json.tool` failed. This is recorded as a sidecar encoding note,
  not a ZIP hash failure.

- Runtime isolation ZIP:
  `C:\Users\yu_ki\AppData\Local\LoneWolfFang\data\runtime_dirty_diff_isolation_before_layer2_review_20260523_152229.zip`
- Runtime isolation ZIP SHA256:
  `F70C65B3E802C29A702A8315EE7584FEAC72F84401A09A6F58F4A5343AFF7545`
- Runtime isolation ZIP size: `34240` bytes.
- Runtime isolation ZIP entries: `24`.
- Runtime isolation `.zip.sha256` matched.
- Runtime isolation `.zip.sha256.json` parsed successfully with
  `python -m json.tool`.

This review packet ZIP sidecars are created externally in:
`C:\Users\yu_ki\AppData\Local\LoneWolfFang\data`.

## Required Boolean Flags

```text
free_trial_layer2_diff_review_runtime_isolated_complete=true
review_only=true
runtime_dirty_diff_isolated=true
target_worktree=C:\LoneWolf_Fang_free
implementation_changes_made=false
layer2_implementation_approval_already_used=true
commit_allowed=false
push_allowed=false
pr_creation_allowed=false
deploy_allowed=false
billing_operation_allowed=false
runtime_connected=false
paper_enabled=false
live_enabled=false
order_execution_allowed=false
private_api_allowed=false
fetch_balance_allowed=false
cancel_allowed=false
cloudflare_mutation_allowed=false
d1_apply_allowed=false
queue_r2_kv_mutation_allowed=false
production_behavior_change_allowed=false
aggressive_page_status_change_allowed=false
aggressive_page_planned_allowed=false
cleanup_allowed=false
delete_allowed=false
move_allowed=false
reset_allowed=false
restore_allowed=false
stash_apply_allowed=false
stash_pop_allowed=false
stash_drop_allowed=false
git_fetch_allowed=false
git_pull_allowed=false
zip_extraction_allowed=false
auto_recovery_allowed=false
owner_review_required=true
safe_summary_only=true
raw_patch_contents_printed=false
```
