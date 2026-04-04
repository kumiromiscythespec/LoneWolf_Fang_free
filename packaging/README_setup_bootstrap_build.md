# BUILD_ID: 2026-04-03_setup_bootstrap_native_python_gate_readme_free_v1

# Free Setup Bootstrap Build

## Purpose
- `setup_bootstrap/` contains the shared phase-2 GUI bootstrap design used by both standard and free, while keeping product-specific artifact names and installer targets.
- The bootstrap remains a thin orchestration layer around the existing PowerShell backend and does not replace `packaging/install_free_local.ps1`.
- The official free setup artifact name is fixed as `LoneWolf_Fang_Free_Setup.exe` so signing and latest-release downloads stay stable.
- Phase-2 preinstall Python process query and stop are now handled by a C# native implementation, so the bootstrap does not rely on hidden PowerShell inline scripts for the preinstall gate.

## Project Files
- `setup_bootstrap/LoneWolfFangFreeSetup.csproj`
- `setup_bootstrap/Program.cs`
- `setup_bootstrap/BootstrapCliOptions.cs`
- `setup_bootstrap/BootstrapRuntime.cs`
- `setup_bootstrap/BootstrapTypes.cs`
- `setup_bootstrap/SetupWizardForm.cs`
- `setup_bootstrap/FailureDialog.cs`
- `setup_bootstrap/Resources/Strings.resx`
- `setup_bootstrap/Resources/Strings.ja.resx`
- `setup_bootstrap/Properties/PublishProfiles/win-x64-single-file.pubxml`

## UI Behavior
- WinForms provides the phase-2 MSI-like setup wizard while the installer backend remains PowerShell.
- The wizard supports install directory selection, desktop shortcut toggle, start-after-install toggle, bilingual Japanese / English UI, an expandable details log, and a failure dialog with `Open log folder`.
- Before backend install starts, the wizard queries Python processes under the selected install target, shows PID / path / command line details, stops them in the GUI flow, and rechecks that no matching Python remains.
- The preinstall gate uses native C# WMI process inspection plus `Process.Kill(...)`, and hidden PowerShell is reserved for backend installer execution with `powershell.exe -File`.
- The setup bootstrap itself does not use `powershell.exe -Command` for the preinstall gate, which helps reduce Defender false positive risk from hidden inline script execution patterns.
- The setup executable defaults to Japanese when the OS UI culture is `ja*`; otherwise it defaults to English.

## Build
From the repository root:

```powershell
dotnet build .\setup_bootstrap\LoneWolfFangFreeSetup.csproj -c Release
```

## Publish
From the repository root:

```powershell
dotnet publish .\setup_bootstrap\LoneWolfFangFreeSetup.csproj -c Release -p:PublishProfile=Properties\PublishProfiles\win-x64-single-file.pubxml
```

Published output:

```text
setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe
```

The published `LoneWolf_Fang_Free_Setup.exe` embeds the official free icon from `app\logos\lwf_logo.ico` via `ApplicationIcon`.

## Package Integration
- The official free setup artifact is the repo-top `LoneWolf_Fang_Free_Setup.exe`.
- `packaging\build_package.ps1` prefers the repo-top setup artifact and otherwise uses `setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe`.
- Package staging places `LoneWolf_Fang_Free_Setup.exe` at package root and the final zip keeps the same filename.
- The final latest-release asset name is fixed as `LoneWolf_Fang_Free_Package.zip`.
- The shared bootstrap still resolves product-specific config, free installer script, and free latest-release asset names at runtime.
- The existing package and publish flow stays intact while the Defender-sensitive preinstall gate no longer uses hidden `powershell.exe -Command`.

## Official Artifacts
- Setup EXE: `LoneWolf_Fang_Free_Setup.exe`
- Package ZIP: `LoneWolf_Fang_Free_Package.zip`
- Sign only the repo-top setup EXE or the packaged package-root copy.
- Do not sign intermediate outputs under `setup_bootstrap\`.

## Supported Bootstrap Arguments
- `--check-only`
- `--dry-run`
- `--start-after-install`
- `--desktop-shortcut auto|skip`
- `--install-dir <path>`
- `--force`
- `--language auto|ja|en`

`--check-only` keeps the preinstall Python query in validation mode only. `--dry-run` prints the installer arguments and, when matching Python is detected, reports `would_stop_python_process` lines without killing them.

## Logging
- Log files remain under `%LOCALAPPDATA%\LoneWolfFang\logs`.
- The setup bootstrap log is `setup_bootstrap_free.log`.
- The backend installer log remains `installer_free.log`.
- Interactive failures expose an `Open log folder` button in both the wizard and the failure dialog.

## Quick Validation
Check only:

```powershell
.\setup_bootstrap\bin\Release\net8.0-windows\LoneWolf_Fang_Free_Setup.exe --check-only
```

Dry run:

```powershell
.\setup_bootstrap\bin\Release\net8.0-windows\LoneWolf_Fang_Free_Setup.exe --dry-run --desktop-shortcut skip
```

Publish validation:

```powershell
.\setup_bootstrap\bin\Release\net8.0-windows\publish\win-x64-single-file\LoneWolf_Fang_Free_Setup.exe --check-only
```

Package validation:

```powershell
.\packaging\build_package.ps1
```
