<!-- BUILD_ID: 2026-05-23_free_trial_layer2_diff_review_runtime_isolated_v1 -->

# Test Summary

Focused tests run:

```text
python -m pytest tests/test_free_trial_layer2_site_cta.py tests/test_free_aggressive_trial_ui_copy_cta.py -q
```

Result:

```text
30 passed in 0.26s
```

Static checks run:

```text
git -C C:\LoneWolf_Fang_free diff --check
git -C C:\LoneWolf_Fang_free diff --cached --check
git -C C:\LoneWolf_Fang_free diff --name-status -- backtest.py runner.py strategy.py
git -C C:\LoneWolf_Fang_free diff --stat -- backtest.py runner.py strategy.py
```

Results:

- `git diff --check`: passed.
- `git diff --cached --check`: passed.
- Runtime target active diff: empty.
- Runtime target staged diff: false because there were no staged files.

Broad runtime, backtest, replay, sweep, Monte Carlo, deploy, billing,
Cloudflare, D1, private API, PAPER, LIVE, order, cancel, and fetch_balance
checks were not run because they are outside the approved review-only scope.

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
