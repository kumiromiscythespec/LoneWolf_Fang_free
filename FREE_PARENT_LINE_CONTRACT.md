# Free Parent-Line Contract

Date: 2026-04-19

## Scope

- Keep `LoneWolf_Fang_free` as free-only.
- Do not reintroduce `LIVE`, billing, desktop activation, seat keys, or account-gated startup.
- Follow the shared market-data, fallback, auto-prepare, and GUI close contracts that are safe for free.

## Runtime Contract

- Product runtime and user config remain under `%LOCALAPPDATA%\LoneWolfFang\data\free\...`.
- Shared chart data and precomputed indicators use `%LOCALAPPDATA%\LoneWolfFang\data\market_data` as the primary root.
- Repo-root `market_data\` remains a legacy read fallback.
- Backtest / replay / pipeline startup may attempt one automatic chart download or precompute generation pass when required inputs are missing.
- If auto-prepare still fails, the failure must stop safely and include symbol, timeframe, prepare window, and searched paths.
- GUI close during active backtest / replay / pipeline work must confirm close and request graceful stop instead of hard-closing.
- Free UI must not add LIVE-only stop choosers or paid-only controls.

## Adoption Note

- Fixed-notional ceiling follow-up is not adopted here.
- The free tree already has runtime auto-prepare and fallback work, but no clearly isolated report-only ceiling hook was found that avoids changing signal or sizing behavior.
