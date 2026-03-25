# BUILD_ID: 2026-03-25_free_local_installer_v1
[CmdletBinding()]
param(
    [string]$InstallRoot = "",
    [string]$DesktopDirOverride = "",
    [switch]$SkipShortcut,
    [switch]$NonInteractive,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-03-25_free_local_installer_v1'
$ScriptRoot = $PSScriptRoot
$RepoRoot = Split-Path -Parent $ScriptRoot

function Read-JsonFile {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "JSON file not found: $Path"
    }

    return (Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json)
}

function Resolve-InstallRootPath {
    param(
        [string]$ExplicitRoot,
        [object]$Config,
        [string]$RepoRootPath
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitRoot)) {
        return [System.IO.Path]::GetFullPath($ExplicitRoot)
    }

    $mode = [string]$Config.install_root_mode
    $name = [string]$Config.install_root_name

    switch ($mode) {
        'repo_root' {
            return [System.IO.Path]::GetFullPath($RepoRootPath)
        }
        'user_profile' {
            return [System.IO.Path]::GetFullPath((Join-Path $HOME $name))
        }
        'localappdata' {
            return [System.IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA $name))
        }
        default {
            return [System.IO.Path]::GetFullPath($RepoRootPath)
        }
    }
}

function Ensure-Directory {
    param([string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }

    if (-not (Test-Path -LiteralPath $Path)) {
        $null = New-Item -ItemType Directory -Path $Path -Force
    }
}

function Confirm-OverwriteIfNeeded {
    param(
        [string]$TargetRoot,
        [object]$Config,
        [switch]$ForceInstall,
        [switch]$NoPrompt
    )

    if ($ForceInstall -or $NoPrompt) {
        return
    }

    $overwriteMode = [string]$Config.overwrite_mode
    if ($overwriteMode -ne 'prompt') {
        return
    }

    $receiptRelative = [string]$Config.receipt_relative_path
    if ([string]::IsNullOrWhiteSpace($receiptRelative)) {
        $receiptRelative = '.install_receipt.json'
    }
    $receiptPath = Join-Path $TargetRoot $receiptRelative
    if (-not (Test-Path -LiteralPath $receiptPath)) {
        return
    }

    Write-Host ''
    Write-Host '[WARN] An install receipt already exists for this payload.'
    Write-Host ("Receipt: {0}" -f $receiptPath)
    $answer = Read-Host 'Continue and refresh the local install receipt and shortcut? [y/N]'
    if ($answer -notin @('y', 'Y', 'yes', 'YES')) {
        throw 'Installation cancelled by user.'
    }
}

function Write-InstallReceipt {
    param(
        [string]$TargetRoot,
        [object]$Config,
        [string]$PythonExe,
        [bool]$GuiImportCheckPassed,
        [string]$ShortcutPath,
        [string]$ShortcutTarget
    )

    $receiptRelative = [string]$Config.receipt_relative_path
    if ([string]::IsNullOrWhiteSpace($receiptRelative)) {
        $receiptRelative = '.install_receipt.json'
    }

    $receiptPath = Join-Path $TargetRoot $receiptRelative
    $receiptDir = Split-Path -Parent $receiptPath
    Ensure-Directory -Path $receiptDir

    $receipt = [ordered]@{
        build_id         = [string]$Config.build_id
        product          = [string]$Config.product
        installed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        install_root     = $TargetRoot
        shortcut_path    = $ShortcutPath
        shortcut_target  = $ShortcutTarget
        python_exe       = $PythonExe
        gui_import_check = [ordered]@{
            required = [bool]$Config.require_gui_import_check
            passed   = [bool]$GuiImportCheckPassed
        }
    }

    ($receipt | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $receiptPath -Encoding UTF8
    return $receiptPath
}

function Assert-FreePayload {
    param(
        [string]$TargetRoot,
        [object]$Config
    )

    $requiredFiles = @(
        'Launch_LoneWolf_Fang_Free_GUI.cmd',
        'Launch_LoneWolf_Fang_Free_GUI.vbs',
        'app\app\cli\app_main.py',
        'app\app\gui\main_window.py',
        'config.py',
        'runner.py',
        'backtest.py'
    )

    foreach ($relativePath in $requiredFiles) {
        $fullPath = Join-Path $TargetRoot $relativePath
        if (-not (Test-Path -LiteralPath $fullPath)) {
            throw "Required free payload file was not found: $relativePath"
        }
    }

    $runtimeDirs = @(
        'runtime',
        'runtime\archives',
        'runtime\exports',
        'runtime\exports\runs',
        'runtime\logs',
        'runtime\state',
        'configs\user'
    )
    foreach ($relativeDir in $runtimeDirs) {
        Ensure-Directory -Path (Join-Path $TargetRoot $relativeDir)
    }

    $shortcutTarget = [string]$Config.shortcut_relative_target
    if ([string]::IsNullOrWhiteSpace($shortcutTarget)) {
        throw 'shortcut_relative_target is missing in install_config_free.json'
    }

    $shortcutTargetPath = Join-Path $TargetRoot $shortcutTarget
    if (-not (Test-Path -LiteralPath $shortcutTargetPath)) {
        throw "Shortcut target was not found: $shortcutTarget"
    }

    $shortcutIcon = [string]$Config.shortcut_icon_relative_path
    if (-not [string]::IsNullOrWhiteSpace($shortcutIcon)) {
        $shortcutIconPath = Join-Path $TargetRoot $shortcutIcon
        if (-not (Test-Path -LiteralPath $shortcutIconPath)) {
            throw "Shortcut icon was not found: $shortcutIcon"
        }
    }
}

function Resolve-PythonForFree {
    param([string]$TargetRoot)

    $candidates = @(
        (Join-Path $TargetRoot 'python_runtime\python.exe'),
        (Join-Path $TargetRoot 'python_runtime\Scripts\python.exe'),
        (Join-Path $TargetRoot '.venv\Scripts\python.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python313\python.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python312\python.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python311\python.exe')
    )

    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }

    throw @"
python.exe was not found for LoneWolf Fang Free.
Checked:
  $(($candidates | ForEach-Object { $_ }) -join "`n  ")
"@
}

function Run-FreeGuiImportChecks {
    param(
        [string]$TargetRoot,
        [string]$PythonExe
    )

    $checks = @(
        'import app.cli.app_main',
        'import app.gui.main_window',
        'from PySide6.QtWidgets import QApplication'
    )

    $env:PYTHONUTF8 = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    $env:LWF_HOME = $TargetRoot
    $env:LWF_RUNTIME_ROOT = (Join-Path $TargetRoot 'runtime')
    $env:LWF_CONFIGS_ROOT = (Join-Path $TargetRoot 'configs')
    $env:LWF_MARKET_DATA_ROOT = (Join-Path $TargetRoot 'market_data')
    $env:PYTHONPATH = "{0};{1}" -f (Join-Path $TargetRoot 'app'), [string]($env:PYTHONPATH)

    Push-Location $TargetRoot
    try {
        foreach ($code in $checks) {
            Write-Host ("[CHECK] python -c ""{0}""" -f $code)
            & $PythonExe -c $code
            if ($LASTEXITCODE -ne 0) {
                throw "GUI import check failed: $code"
            }
        }
    }
    finally {
        Pop-Location
    }
}

function New-DesktopShortcut {
    param(
        [string]$DesktopDirectory,
        [object]$Config,
        [string]$TargetRoot
    )

    Ensure-Directory -Path $DesktopDirectory

    $shortcutName = [string]$Config.shortcut_name
    if ([string]::IsNullOrWhiteSpace($shortcutName)) {
        $shortcutName = 'LoneWolf Fang Free GUI'
    }

    $shortcutPath = Join-Path $DesktopDirectory ($shortcutName + '.lnk')
    $targetPath = Join-Path $TargetRoot ([string]$Config.shortcut_relative_target)
    $iconRelative = [string]$Config.shortcut_icon_relative_path
    $iconPath = ''
    if (-not [string]::IsNullOrWhiteSpace($iconRelative)) {
        $iconCandidate = Join-Path $TargetRoot $iconRelative
        if (Test-Path -LiteralPath $iconCandidate) {
            $iconPath = [System.IO.Path]::GetFullPath($iconCandidate)
        }
    }

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $targetPath
    $shortcut.WorkingDirectory = $TargetRoot
    $shortcut.Description = [string]$Config.shortcut_description
    if (-not [string]::IsNullOrWhiteSpace($iconPath)) {
        $shortcut.IconLocation = "$iconPath,0"
    }
    $shortcut.Save()

    return [pscustomobject]@{
        shortcut_path   = $shortcutPath
        shortcut_target = $targetPath
    }
}

$configPath = Join-Path $ScriptRoot 'install_config_free.json'
$config = Read-JsonFile -Path $configPath
$targetRoot = Resolve-InstallRootPath -ExplicitRoot $InstallRoot -Config $config -RepoRootPath $RepoRoot

Write-Host '== LoneWolf Fang Free Installer =='
Write-Host ("BUILD_ID        : {0}" -f $BUILD_ID)
Write-Host ("Install Root    : {0}" -f $targetRoot)
if (-not [string]::IsNullOrWhiteSpace($DesktopDirOverride)) {
    Write-Host ("Desktop Override: {0}" -f ([System.IO.Path]::GetFullPath($DesktopDirOverride)))
}
Write-Host ''

Ensure-Directory -Path $targetRoot
Confirm-OverwriteIfNeeded -TargetRoot $targetRoot -Config $config -ForceInstall:$Force -NoPrompt:$NonInteractive
Assert-FreePayload -TargetRoot $targetRoot -Config $config

$pythonExe = ''
if ([bool]$config.probe_python_before_shortcut) {
    $pythonExe = Resolve-PythonForFree -TargetRoot $targetRoot
    Write-Host ("[OK] Python     : {0}" -f $pythonExe)
}

$guiImportCheckPassed = $false
if ([bool]$config.require_gui_import_check) {
    Run-FreeGuiImportChecks -TargetRoot $targetRoot -PythonExe $pythonExe
    $guiImportCheckPassed = $true
}

$shortcutPath = ''
$shortcutTarget = Join-Path $targetRoot ([string]$config.shortcut_relative_target)

if (-not $SkipShortcut) {
    $desktopDir = if ([string]::IsNullOrWhiteSpace($DesktopDirOverride)) {
        [Environment]::GetFolderPath('Desktop')
    }
    else {
        [System.IO.Path]::GetFullPath($DesktopDirOverride)
    }

    $shortcutInfo = New-DesktopShortcut -DesktopDirectory $desktopDir -Config $config -TargetRoot $targetRoot
    $shortcutPath = [string]$shortcutInfo.shortcut_path
    $shortcutTarget = [string]$shortcutInfo.shortcut_target
    Write-Host ("[OK] Shortcut   : {0}" -f $shortcutPath)
}
else {
    Write-Host '[SKIP] Desktop shortcut creation was skipped.'
}

$receiptPath = Write-InstallReceipt `
    -TargetRoot $targetRoot `
    -Config $config `
    -PythonExe $pythonExe `
    -GuiImportCheckPassed $guiImportCheckPassed `
    -ShortcutPath $shortcutPath `
    -ShortcutTarget $shortcutTarget

Write-Host ("[OK] Receipt    : {0}" -f $receiptPath)
Write-Host ''
Write-Host '[OK] LoneWolf Fang Free local installation completed successfully.'
if (-not $SkipShortcut) {
    Write-Host 'Use the desktop shortcut "LoneWolf Fang Free GUI" to start the GUI.'
}
