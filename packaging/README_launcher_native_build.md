# BUILD_ID: 2026-03-30_free_launcher_native_build_readme_v2

# Free Native Launcher Build

## Purpose

- `launcher_native/` contains the native Windows GUI launcher project for free distribution.
- The publish artifact name is fixed to `LoneWolfFangFreeLauncher.exe`.
- The launcher resolves the free app root from its own location, validates the bundled Python runtime, logs to `runtime\logs\launcher_native.log`, and starts `pythonw.exe -m app.cli.app_main --gui`.

## Project Files

- `launcher_native/LoneWolfFangFreeLauncher.csproj`
- `launcher_native/Program.cs`
- `launcher_native/Properties/PublishProfiles/win-x64-single-file.pubxml`

## Prerequisites

- Windows x64
- .NET 8 SDK
- Free package layout markers present beside the launcher:
  - `app\app\`
  - `python_runtime\`
  - `runtime\`
  - `configs\`

Check the SDK first:

```powershell
dotnet --list-sdks
```

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

The published `LoneWolfFangFreeLauncher.exe` embeds the official free icon from `app\logos\lwf_logo.ico` via `ApplicationIcon`.

## Package Integration

- `packaging\build_package.ps1` treats `LoneWolfFangFreeLauncher.exe` as an official free repo-top artifact and then bundles the same filename at package root.
- The build resolves the launcher from:
  - `.\LoneWolfFangFreeLauncher.exe`
  - `.\launcher_native\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolfFangFreeLauncher.exe`
- When the publish output is used, package build materializes `.\LoneWolfFangFreeLauncher.exe` at repo top before staging and zip creation.
- Existing `Launch_LoneWolf_Fang_Free_GUI.cmd` and `Launch_LoneWolf_Fang_Free_GUI.vbs` remain compatibility shims and prefer the native launcher first when it is bundled.
- The future desktop shortcut target is `LoneWolfFangFreeLauncher.exe`, but the current shortcut remains on the VBS shim during staged migration.

## Signing Policy

- The published launcher already includes the official free `.ico`.
- Sign only the repo-top copy or bundled package-root copy.
- Do not sign the intermediate file under `launcher_native\`.

## Validation

Check-only from the published package root:

```powershell
.\LoneWolfFangFreeLauncher.exe --check-only
$LASTEXITCODE
```

Expected success:

- exit code `0`
- `runtime\logs\launcher_native.log` contains `check_result=ok`

Launch from the published package root:

```powershell
.\LoneWolfFangFreeLauncher.exe
```

Expected result:

- no console window
- GUI starts through `pythonw.exe`, or `python.exe` when `pythonw.exe` is unavailable
- `runtime\logs\launcher_native.log` contains `launch_result=started`

## Notes

- The launcher does not modify trading logic, billing, or general GUI behavior.
- Package build can warn or fail fast when the launcher publish artifact is missing from every supported source candidate.
- Apply code signing only to the repo-top or packaged package-root artifact when signing is introduced later.
