<!-- BUILD_ID: 2026-05-23_free_trial_layer2_ui_cta_impl_v1 -->

# Test Summary

Focused tests run:

```text
python -m pytest tests/test_free_trial_layer2_site_cta.py tests/test_free_aggressive_trial_ui_copy_cta.py -q
```

Result:

```text
30 passed in 0.49s
```

Additional static checks:

```text
git diff --check
git diff --cached --check
BUILD_ID marker scan
changed-file trailing whitespace scan
focused forbidden CTA/copy/runtime/billing/deploy linkage scan
```

Results:

- `git diff --check` passed with pre-existing CRLF warnings for `backtest.py`,
  `runner.py`, and `strategy.py`.
- `git diff --cached --check` passed.
- BUILD_ID marker scan passed.
- Changed-file trailing whitespace scan passed.
- Focused implementation safety scan passed.

Broad runtime, backtest, replay, sweep, Monte Carlo, deploy, billing,
Cloudflare, D1, private API, PAPER, LIVE, order, cancel, and fetch_balance
checks were not run because they are outside the approved Layer 2 scope.
