# BUILD_ID: 2026-04-02_free_shared_python_launcher_v2
[CmdletBinding()]
param(
    [switch]$CheckOnly,
    [switch]$Foreground
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-04-02_free_shared_python_launcher_v2'
$ScriptRoot = $PSScriptRoot
$RepoRoot = $ScriptRoot
$ProductName = 'LoneWolf Fang free'
$AppModule = 'app.cli.app_main'
$AppEntryPath = Join-Path $RepoRoot 'app\app\cli\app_main.py'
$SharedRoot = Join-Path $env:LOCALAPPDATA 'LoneWolfFang'
$SharedStatePath = Join-Path $SharedRoot 'config\shared_python.json'
$VenvRoot = Join-Path $SharedRoot 'venvs\free'
$DataRoot = Join-Path $SharedRoot 'data\free'
$LogPath = Join-Path $SharedRoot 'logs\launcher_free.log'

function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        $null = New-Item -ItemType Directory -Path $Path -Force
    }
}

function Write-LauncherLog {
    param([string]$Level,[string]$Message)
    Ensure-Directory -Path (Split-Path -Parent $LogPath)
    $line = '{0} [{1}] {2}' -f (Get-Date).ToString('yyyy-MM-ddTHH:mm:sszzz'), $Level, (($Message -replace "`r?`n", ' ').Trim())
    Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value $line
    if ($Level -eq 'ERROR') {
        Write-Host ("[ERROR] {0}" -f $Message)
    }
}

function Show-LauncherError {
    param([string]$Message)
    Add-Type -AssemblyName PresentationFramework
    [System.Windows.MessageBox]::Show($Message, $ProductName, 'OK', 'Error') | Out-Null
}

function Read-SharedState {
    if (-not (Test-Path -LiteralPath $SharedStatePath)) {
        return $null
    }
    try {
        return (Get-Content -LiteralPath $SharedStatePath -Raw -Encoding UTF8 | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

function Resolve-BasePython {
    $sharedState = Read-SharedState
    $candidates = @()
    if (($null -ne $sharedState) -and ($null -ne $sharedState.PSObject.Properties['preferred_python'])) {
        $candidates += [string]$sharedState.preferred_python
    }
    $candidates += (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python311\python.exe')
    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $py) {
        try {
            $resolved = (& $py.Source -3.11 -c 'import sys; print(sys.executable)' 2>$null | Select-Object -Last 1)
            if (-not [string]::IsNullOrWhiteSpace($resolved) -and (Test-Path -LiteralPath $resolved)) {
                return [System.IO.Path]::GetFullPath($resolved)
            }
        }
        catch {
        }
    }
    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($null -eq $python) { $python = Get-Command python -ErrorAction SilentlyContinue }
    if ($null -ne $python) { return [System.IO.Path]::GetFullPath($python.Source) }
    return ''
}

function Ensure-DataDirectories {
    foreach ($dir in @(
        $DataRoot,
        (Join-Path $DataRoot 'runtime'),
        (Join-Path $DataRoot 'runtime\archives'),
        (Join-Path $DataRoot 'runtime\exports'),
        (Join-Path $DataRoot 'runtime\exports\runs'),
        (Join-Path $DataRoot 'runtime\logs'),
        (Join-Path $DataRoot 'runtime\state'),
        (Join-Path $DataRoot 'configs'),
        (Join-Path $DataRoot 'configs\user'),
        (Join-Path $DataRoot 'market_data'),
        (Join-Path $DataRoot 'market_data\precomputed_indicators')
    )) {
        Ensure-Directory -Path $dir
    }
}

function Build-ArgumentString {
    param([string[]]$ArgumentList)
    return ((@($ArgumentList) | ForEach-Object { if ([string]::IsNullOrWhiteSpace($_)) { '""' } elseif (($_).IndexOfAny(@(' ', "`t", '"')) -lt 0) { [string]$_ } else { '"' + ([string]$_).Replace('"', '\"') + '"' } }) -join ' ')
}

function Invoke-PythonCapture {
    param([string]$PythonExe,[string[]]$ArgumentList,[hashtable]$EnvironmentMap)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PythonExe
    $psi.Arguments = Build-ArgumentString -ArgumentList $ArgumentList
    $psi.WorkingDirectory = $RepoRoot
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    foreach ($entry in $EnvironmentMap.GetEnumerator()) { $psi.Environment[$entry.Key] = [string]$entry.Value }
    $process = [System.Diagnostics.Process]::Start($psi)
    if ($null -eq $process) { throw "Failed to start Python process: $PythonExe" }
    $stdout = $process.StandardOutput.ReadToEndAsync(); $stderr = $process.StandardError.ReadToEndAsync(); $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = [int]$process.ExitCode; StdOut = [string]$stdout.Result; StdErr = [string]$stderr.Result }
}

function Invoke-ForegroundLaunch {
    param([string]$PythonExe,[string[]]$ArgumentList,[hashtable]$EnvironmentMap)
    $originals = @{}
    foreach ($entry in $EnvironmentMap.GetEnumerator()) {
        $originals[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
        [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, 'Process')
    }
    try {
        & $PythonExe @ArgumentList
        return $LASTEXITCODE
    }
    finally {
        foreach ($name in $originals.Keys) {
            [Environment]::SetEnvironmentVariable($name, $originals[$name], 'Process')
        }
    }
}

function Start-BackgroundLaunch {
    param([string]$PythonExe,[string[]]$ArgumentList,[hashtable]$EnvironmentMap)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PythonExe
    $psi.Arguments = Build-ArgumentString -ArgumentList $ArgumentList
    $psi.WorkingDirectory = $RepoRoot
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    foreach ($entry in $EnvironmentMap.GetEnumerator()) { $psi.Environment[$entry.Key] = [string]$entry.Value }
    return [System.Diagnostics.Process]::Start($psi)
}

if (-not (Test-Path -LiteralPath $AppEntryPath)) {
    $message = "App files are missing. Please reinstall $ProductName.`nInstall root: $RepoRoot`nExpected: $AppEntryPath"
    Write-LauncherLog -Level 'ERROR' -Message $message
    Show-LauncherError -Message $message
    exit 4
}

$basePython = Resolve-BasePython
$venvPython = Join-Path $VenvRoot 'Scripts\python.exe'
$venvPythonw = Join-Path $VenvRoot 'Scripts\pythonw.exe'
if ((-not (Test-Path -LiteralPath $venvPython)) -or (-not (Test-Path -LiteralPath $venvPythonw))) {
    $message = @"
The product environment is missing.

Install root: $RepoRoot
Base Python: $basePython
Expected venv: $VenvRoot

Run $ProductName Setup to create or repair the venv.
"@
    Write-LauncherLog -Level 'ERROR' -Message $message
    Show-LauncherError -Message $message
    exit 3
}

Ensure-DataDirectories
$envMap = @{ PYTHONPATH = (Join-Path $RepoRoot 'app'); PYTHONUTF8 = '1'; PYTHONIOENCODING = 'utf-8'; LWF_HOME = $RepoRoot; LWF_RUNTIME_ROOT = (Join-Path $DataRoot 'runtime'); LWF_CONFIGS_ROOT = (Join-Path $DataRoot 'configs'); LWF_MARKET_DATA_ROOT = (Join-Path $DataRoot 'market_data') }
$importResult = Invoke-PythonCapture -PythonExe $venvPython -ArgumentList @('-c', 'import keyring; from PySide6.QtWidgets import QApplication; import app.cli.app_main') -EnvironmentMap $envMap
$importSummary = (($importResult.StdOut + ' ' + $importResult.StdErr).Trim() -replace '\s+', ' ')
Write-LauncherLog -Level 'INFO' -Message ("build_id=$BUILD_ID install_root=$RepoRoot base_python=$basePython venv_root=$VenvRoot data_root=$DataRoot target=$AppModule import_exit_code={0} import_summary={1}" -f $importResult.ExitCode, $importSummary)
if ($importResult.ExitCode -ne 0) {
    $message = @"
Launcher validation failed.

Install root: $RepoRoot
Base Python: $basePython
Venv: $VenvRoot
Target: $AppModule
Log: $LogPath
"@
    Write-LauncherLog -Level 'ERROR' -Message 'import check failed for free launcher'
    Show-LauncherError -Message $message
    exit 5
}

if ($CheckOnly) {
    Write-LauncherLog -Level 'INFO' -Message 'check_only=ok'
    exit 0
}

$launchPython = $(if ($Foreground) { $venvPython } else { $venvPythonw })
$argumentList = @('-m', $AppModule, '--gui')
$commandText = $launchPython + ' ' + ($argumentList -join ' ')
Write-LauncherLog -Level 'INFO' -Message ("launch_command=$commandText")

if ($Foreground) {
    $exitCode = Invoke-ForegroundLaunch -PythonExe $launchPython -ArgumentList $argumentList -EnvironmentMap $envMap
    Write-LauncherLog -Level 'INFO' -Message ("foreground_exit_code=$exitCode")
    exit $exitCode
}

$process = Start-BackgroundLaunch -PythonExe $launchPython -ArgumentList $argumentList -EnvironmentMap $envMap
if ($null -eq $process) {
    $message = "Launcher could not start the GUI process. Install root: $RepoRoot"
    Write-LauncherLog -Level 'ERROR' -Message $message
    Show-LauncherError -Message $message
    exit 6
}
Write-LauncherLog -Level 'INFO' -Message ("launch_process_id={0}" -f $process.Id)
exit 0

