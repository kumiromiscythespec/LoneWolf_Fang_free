# BUILD_ID: 2026-04-20_free_package_readme_native_launcher_shortcut_v1

# LoneWolf Fang Free Package

This package is the local-installer zip source distribution for `LoneWolf_Fang_free`.

## Official Windows Artifacts

- The official free Windows artifacts are the repo-top copies of `LoneWolfFangFreeLauncher.exe` and `LoneWolf_Fang_Free_Setup.exe`.
- Package build stages and zips those same two filenames at package root after the repo-top copies are present.
- `LoneWolfFangFreeLauncher.exe` is the preferred Windows GUI entrypoint.
- `LoneWolf_Fang_Free_Setup.exe` is the preferred install / repair / update entrypoint.
- `Launch_LoneWolf_Fang_Free_GUI.vbs` and `Launch_LoneWolf_Fang_Free_GUI.cmd` remain compatibility shims.
- Desktop shortcut and setup start-after-install both prefer `LoneWolfFangFreeLauncher.exe`, then fall back to `Launch_LoneWolf_Fang_Free_GUI.vbs`, then `Launch_LoneWolf_Fang_Free_GUI.cmd`.

## Signing Policy

- Sign only the repo-top copy or packaged package-root copy of `LoneWolfFangFreeLauncher.exe` and `LoneWolf_Fang_Free_Setup.exe`.
- Do not sign intermediate outputs under `launcher_native\` or `setup_bootstrap\`.

## Native Artifact Sources

`packaging\build_package.ps1` resolves native artifacts in this order:

1. Repo-root copies:
   - `.\LoneWolfFangFreeLauncher.exe`
   - `.\LoneWolf_Fang_Free_Setup.exe`
2. Publish outputs:
   - `.\launcher_native\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolfFangFreeLauncher.exe`
   - `.\setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe`

When repo-top copies are absent but publish outputs exist, `packaging\build_package.ps1` materializes the official repo-top copies before staging and zip creation.

## Package Build

Publish the native artifacts from the repository root when you want them bundled:

```powershell
dotnet publish .\launcher_native\LoneWolfFangFreeLauncher.csproj /p:PublishProfile=win-x64-single-file
dotnet publish .\setup_bootstrap\LoneWolfFangFreeSetup.csproj -c Release -p:PublishProfile=Properties\PublishProfiles\win-x64-single-file.pubxml
```

Build the free package:

```powershell
.\packaging\build_package.ps1
```

Fail fast if either official native artifact is missing from every supported source candidate:

```powershell
.\packaging\build_package.ps1 -NativeArtifactsPolicy error
```

The build verifies native artifact presence in:

- the repo top
- the staged package root
- the final zip

## Release Asset

- `packaging\build_package.ps1` keeps the timestamped archive and also writes `packaging\dist\LoneWolf_Fang_Free_Package.zip`.
- Upload `LoneWolf_Fang_Free_Package.zip` to the latest release of `kumiromiscythespec/LoneWolf_Fang_free`.
- `LoneWolf_Fang_Free_Setup.exe` can run standalone and download that fixed asset name from the latest release when the local package root is not bundled beside the setup executable.
- For local simulation, you can override the download target with `--package-url <zip-url>` or `LWF_FREE_SETUP_PACKAGE_URL`.

## End-User Entry Order

- Run `LoneWolf_Fang_Free_Setup.exe` first when it is bundled.
- Use `Install_LoneWolf_Fang_Free.cmd` only when you are already inside an unpacked local package.
- After installation, use the desktop shortcut `LoneWolf Fang Free`.
- Manual launch remains available through `LoneWolfFangFreeLauncher.exe`, `Launch_LoneWolf_Fang_Free_GUI.vbs`, or `Launch_LoneWolf_Fang_Free_GUI.cmd`.

## Package Scope

- Free includes the GUI plus `PAPER`, `REPLAY`, and `BACKTEST`.
- `LIVE` is not included in the free package flow.
- `market_data/` is not bundled.
- `runtime/` is bundled as skeleton-only directories with `.gitkeep` files.

## Free Runtime Contract

- Runtime and user configs remain product-scoped under `%LOCALAPPDATA%\LoneWolfFang\data\free\...`.
- Shared chart and precomputed data use `%LOCALAPPDATA%\LoneWolfFang\data\market_data` as the primary root.
- Repo-root `market_data\` remains a legacy read fallback for older local datasets.
- Free keeps the non-LIVE contract: no account requirement, no billing flow, no desktop activation, and no seat-key gate for `PAPER`, `REPLAY`, or `BACKTEST`.
- Missing chart data or precomputed inputs trigger one automatic prepare attempt before the app stops with the missing symbol / timeframe / period details.
- GUI close during active `BACKTEST`, `REPLAY`, or pipeline work uses confirm-close plus graceful stop.
