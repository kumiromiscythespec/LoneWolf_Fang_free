# BUILD_ID: 2026-04-20_free_setup_bootstrap_build_readme_contract_v1

# Free Setup Bootstrap Build

## Purpose

- `setup_bootstrap/` contains the native free setup bootstrap.
- The bootstrap is the preferred install / repair / update entrypoint for free.
- It remains a thin orchestration layer around `packaging/install_free_local.ps1`.
- The official free setup artifact name is fixed to `LoneWolf_Fang_Free_Setup.exe`.

## Runtime Behavior

- Uses the local package root when bundled beside the setup executable.
- Otherwise downloads the fixed latest-release asset `LoneWolf_Fang_Free_Package.zip`.
- Supports install directory selection, desktop shortcut toggle, start-after-install, and bilingual Japanese / English UI.
- Queries Python processes under the selected install target before install and can stop them from the native GUI flow.
- Start-after-install prefers `LoneWolfFangFreeLauncher.exe`, then the VBS/CMD launcher shims.

## Build

```powershell
dotnet build .\setup_bootstrap\LoneWolfFangFreeSetup.csproj -c Release
```

## Publish

```powershell
dotnet publish .\setup_bootstrap\LoneWolfFangFreeSetup.csproj -c Release -p:PublishProfile=Properties\PublishProfiles\win-x64-single-file.pubxml
```

Expected publish output:

```text
setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe
```

## Package Integration

- `packaging\build_package.ps1` prefers the repo-top `LoneWolf_Fang_Free_Setup.exe`.
- If only the publish output exists, package build materializes the official repo-top copy before staging and zip creation.
- Package staging keeps `LoneWolf_Fang_Free_Setup.exe` at package root.
- The fixed release ZIP name is `LoneWolf_Fang_Free_Package.zip`.
- Desktop shortcut creation now prefers the native launcher and falls back safely to the compatibility shims.

## Signing Policy

- Sign only the repo-top setup EXE or packaged package-root copy.
- Do not sign intermediate outputs under `setup_bootstrap\`.

## Supported Arguments

- `--check-only`
- `--dry-run`
- `--start-after-install`
- `--desktop-shortcut auto|skip`
- `--install-dir <path>`
- `--force`
- `--language auto|ja|en`

## Logging

- Setup bootstrap log: `%LOCALAPPDATA%\LoneWolfFang\logs\setup_bootstrap_free.log`
- Backend installer log: `%LOCALAPPDATA%\LoneWolfFang\logs\installer_free.log`

## Quick Validation

```powershell
.\setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe --check-only
```

```powershell
.\setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe --dry-run --desktop-shortcut skip
```
