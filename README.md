# LoneWolf Fang Free

Free Windows package for `PAPER`, `REPLAY`, and `BACKTEST`.
`LIVE`, billing flows, desktop activation, and seat-key startup gates are not part of the free product.

## English

### Current Contract

- Free stays research-only and non-LIVE.
- Official Windows artifacts at repo top are:
  - `LoneWolfFangFreeLauncher.exe`
  - `LoneWolf_Fang_Free_Setup.exe`
- The native setup bootstrap is the preferred install / repair / update entrypoint.
- The native launcher is the preferred GUI entrypoint.
- `Launch_LoneWolf_Fang_Free_GUI.vbs` and `Launch_LoneWolf_Fang_Free_GUI.cmd` remain compatibility shims.

### Install / Repair / Update

Use these entrypoints in this order:

1. `LoneWolf_Fang_Free_Setup.exe`
2. `Install_LoneWolf_Fang_Free.cmd` when you are already inside an unpacked local package

The installer validates the package, resolves a usable Python base, creates or refreshes the shared free venv, runs the GUI import check, creates the desktop shortcut, and writes `.install_receipt.json`.

The desktop shortcut now prefers `LoneWolfFangFreeLauncher.exe` and falls back safely to the VBS/CMD shims when needed.

### Launch

After installation, use the desktop shortcut `LoneWolf Fang Free`.

Manual launch remains available through:

- `LoneWolfFangFreeLauncher.exe`
- `Launch_LoneWolf_Fang_Free_GUI.vbs`
- `Launch_LoneWolf_Fang_Free_GUI.cmd`
- `Launch_LoneWolf_Fang_Free_GUI.ps1`

### Data / Runtime Contract

- Product runtime and user config stay under `%LOCALAPPDATA%\LoneWolfFang\data\free\...`.
- Shared chart data and precomputed indicators use `%LOCALAPPDATA%\LoneWolfFang\data\market_data` as the primary root.
- Repo-root `market_data\` remains a legacy read fallback.
- `BACKTEST`, `REPLAY`, and pipeline preparation may perform one automatic prepare pass when data is missing.
- If auto-prepare still fails, the stop message includes symbol, timeframe, prepare window, and searched paths.
- Closing the GUI during active `BACKTEST`, `REPLAY`, or pipeline work asks for confirmation and requests a graceful stop.

### Free Boundary

Free includes:

- GUI
- `PAPER`
- `REPLAY`
- `BACKTEST`

Free does not include:

- `LIVE`
- billing flows
- desktop activation
- seat keys
- standard-only distribution or account-gated startup

### Packaging Notes

- `market_data/` is not bundled.
- `runtime/` is bundled as skeleton-only directories.
- Sign only the repo-top or packaged package-root copies of `LoneWolfFangFreeLauncher.exe` and `LoneWolf_Fang_Free_Setup.exe`.
- Deep outputs under `launcher_native/` and `setup_bootstrap/` are intermediate build artifacts, not release artifacts.

## 日本語

### 現在の free 契約

- free は調査・検証用の non-LIVE パッケージです。
- repo top の official Windows artifacts は次の 2 つです。
  - `LoneWolfFangFreeLauncher.exe`
  - `LoneWolf_Fang_Free_Setup.exe`
- インストール / 修復 / 更新の起点は `LoneWolf_Fang_Free_Setup.exe` を優先します。
- GUI 起動の正規エントリは `LoneWolfFangFreeLauncher.exe` です。
- `Launch_LoneWolf_Fang_Free_GUI.vbs` と `Launch_LoneWolf_Fang_Free_GUI.cmd` は互換 shim として残します。

### インストール / 修復 / 更新

次の順で使ってください。

1. `LoneWolf_Fang_Free_Setup.exe`
2. 展開済みローカル package 内では `Install_LoneWolf_Fang_Free.cmd`

desktop shortcut は `LoneWolfFangFreeLauncher.exe` を優先し、native launcher が見つからない場合だけ VBS/CMD shim に安全にフォールバックします。

### 起動

インストール後は desktop shortcut `LoneWolf Fang Free` を使って起動します。

手動起動も可能です。

- `LoneWolfFangFreeLauncher.exe`
- `Launch_LoneWolf_Fang_Free_GUI.vbs`
- `Launch_LoneWolf_Fang_Free_GUI.cmd`
- `Launch_LoneWolf_Fang_Free_GUI.ps1`

### データ / ランタイム契約

- product runtime と user config は `%LOCALAPPDATA%\LoneWolfFang\data\free\...` 配下を使います。
- chart data と precomputed indicators の primary root は `%LOCALAPPDATA%\LoneWolfFang\data\market_data` です。
- repo root の `market_data\` は legacy read fallback として残します。
- `BACKTEST` / `REPLAY` / pipeline prepare では、必要データ欠落時に 1 回だけ auto-prepare を試行します。
- auto-prepare が失敗した場合は、symbol / timeframe / prepare window / searched paths を含む安全停止メッセージを返します。
- GUI を active な `BACKTEST` / `REPLAY` / pipeline 実行中に閉じる場合は confirm-close と graceful stop を使います。

### free 境界

free に含まれるもの:

- GUI
- `PAPER`
- `REPLAY`
- `BACKTEST`

free に含めないもの:

- `LIVE`
- billing 導線
- desktop activation
- seat key
- standard-only 機能や配布導線

### packaging 補足

- `market_data/` は bundle しません。
- `runtime/` は skeleton directory のみを bundle します。
- 署名対象は repo top または package root に置かれた `LoneWolfFangFreeLauncher.exe` と `LoneWolf_Fang_Free_Setup.exe` です。
- `launcher_native/` と `setup_bootstrap/` 配下の publish 生成物は中間 build artifact です。
