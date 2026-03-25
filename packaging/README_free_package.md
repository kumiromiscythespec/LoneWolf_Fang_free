# BUILD_ID: 2026-03-25_free_package_readme_v1

# LoneWolf Fang Free Package

This package is the local installer zip source distribution for `LoneWolf_Fang_free`.

- Run `Install_LoneWolf_Fang_Free.cmd` first.
- The installer validates the local payload, probes Python, runs GUI import checks, creates a desktop shortcut, and writes an install receipt.
- The desktop shortcut launches `Launch_LoneWolf_Fang_Free_GUI.vbs`.
- Free includes the GUI plus `PAPER`, `REPLAY`, and `BACKTEST`.
- `LIVE` is not included in the free package flow.
- `market_data/` is not bundled.
- `runtime/` is bundled as skeleton-only directories with `.gitkeep` files.
- Standard includes additional capabilities beyond free.
