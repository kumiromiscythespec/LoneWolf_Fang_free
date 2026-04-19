# BUILD_ID: 2026-04-20_free_launcher_native_build_readme_contract_v1

# Free Native Launcher Build

## Purpose

- `launcher_native/` contains the native Windows GUI launcher for free.
- The official published artifact name is fixed to `LoneWolfFangFreeLauncher.exe`.
- The native launcher is the preferred Windows GUI entrypoint for free.
- The launcher delegates to `Launch_LoneWolf_Fang_Free_GUI.ps1`, so the free runtime contract stays in one place.

## Runtime Behavior

- Resolves the free package root from the executable location.
- Calls `Launch_LoneWolf_Fang_Free_GUI.ps1`.
- Uses the shared free venv under `%LOCALAPPDATA%\LoneWolfFang\venvs\free`.
- Uses shared market data under `%LOCALAPPDATA%\LoneWolfFang\data\market_data`.
- Logs validation and launch results to `%LOCALAPPDATA%\LoneWolfFang\logs\launcher_free.log`.

## Build

```powershell
dotnet build .\launcher_native\LoneWolfFangFreeLauncher.csproj -c Release
```

## Publish

```powershell
dotnet publish .\launcher_native\LoneWolfFangFreeLauncher.csproj /p:PublishProfile=win-x64-single-file
```

Expected publish output:

```text
launcher_native\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolfFangFreeLauncher.exe
```

## Package Integration

- `packaging\build_package.ps1` treats `LoneWolfFangFreeLauncher.exe` as an official repo-top free artifact.
- Source resolution order is:
  - `.\LoneWolfFangFreeLauncher.exe`
  - `.\launcher_native\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolfFangFreeLauncher.exe`
- When only the publish output exists, package build materializes the official repo-top copy before staging and zip creation.
- Desktop shortcut and setup start-after-install prefer `LoneWolfFangFreeLauncher.exe`.
- `Launch_LoneWolf_Fang_Free_GUI.vbs` and `Launch_LoneWolf_Fang_Free_GUI.cmd` remain compatibility shims.

## Signing Policy

- Sign only the repo-top copy or packaged package-root copy.
- Do not sign the intermediate publish output under `launcher_native\`.

## Validation

```powershell
.\LoneWolfFangFreeLauncher.exe --check-only
```

Expected result:

- exit code `0` when the free package root and shared venv are ready
- `%LOCALAPPDATA%\LoneWolfFang\logs\launcher_free.log` records the validation result
