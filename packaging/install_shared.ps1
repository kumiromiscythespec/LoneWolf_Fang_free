# BUILD_ID: 2026-04-03_install_root_python_precheck_skip_v1
[CmdletBinding()]
param(
    [string]$InstallerBuildId = "",
    [string]$ConfigPath = "",
    [string]$ManifestPath = "",
    [string]$PackageCommonPath = "",
    [string]$InstallRoot = "",
    [string]$DesktopDirectoryOverride = "",
    [switch]$Force,
    [switch]$SkipInstallRootPythonPrecheck,
    [switch]$SkipDesktopShortcut,
    [switch]$NonInteractive
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptRoot = $PSScriptRoot
$RepoRoot = Split-Path -Parent $ScriptRoot
$script:BuildId = if ([string]::IsNullOrWhiteSpace($InstallerBuildId)) { '2026-04-03_install_root_python_precheck_skip_v1' } else { $InstallerBuildId }
$script:SharedRoot = Join-Path $env:LOCALAPPDATA 'LoneWolfFang'
$script:SharedStatePath = Join-Path $script:SharedRoot 'config\shared_python.json'
$script:LogPath = Join-Path ([System.IO.Path]::GetTempPath()) 'LoneWolfFang_installer.log'
$script:ProductCode = 'product'
$script:ProductDisplayName = 'LoneWolf Fang'
$script:PreferredMinor = '3.11'

function Ensure-Directory {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }
    if (-not (Test-Path -LiteralPath $Path)) {
        $null = New-Item -ItemType Directory -Path $Path -Force
    }
}

function Read-JsonFile {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "JSON file not found: $Path"
    }
    return (Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json)
}

function Get-ConfigValue {
    param(
        [object]$Config,
        [string[]]$Names,
        $Default = $null
    )
    foreach ($name in $Names) {
        if (($null -ne $Config) -and ($null -ne $Config.PSObject.Properties[$name])) {
            return $Config.$name
        }
    }
    return $Default
}

function Get-ConfigString {
    param(
        [object]$Config,
        [string[]]$Names,
        [string]$Default = ''
    )
    return [string](Get-ConfigValue -Config $Config -Names $Names -Default $Default)
}

function Get-ConfigArray {
    param(
        [object]$Config,
        [string[]]$Names
    )
    $value = Get-ConfigValue -Config $Config -Names $Names -Default $null
    if ($null -eq $value) {
        return @()
    }
    if (($value -is [System.Collections.IEnumerable]) -and (-not ($value -is [string]))) {
        return @($value | ForEach-Object { [string]$_ })
    }
    return @([string]$value)
}

function Sanitize-LogText {
    param([string]$Value)
    return ([string]$Value).Replace("`r", ' ').Replace("`n", ' ').Trim()
}

function Get-OutputSummary {
    param([string]$Value)
    $text = Sanitize-LogText -Value $Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return '<empty>'
    }
    if ($text.Length -gt 600) {
        return $text.Substring(0, 600) + ' ...'
    }
    return $text
}

function Quote-CommandArgument {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return '""'
    }
    if ($Value.IndexOfAny(@(' ', "`t", '"')) -lt 0) {
        return $Value
    }
    return '"' + ($Value.Replace('"', '\"')) + '"'
}

function Build-ArgumentString {
    param([string[]]$ArgumentList)
    return ((@($ArgumentList) | ForEach-Object { Quote-CommandArgument -Value ([string]$_) }) -join ' ')
}

function Format-Command {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList
    )
    $parts = New-Object 'System.Collections.Generic.List[string]'
    $null = $parts.Add((Quote-CommandArgument -Value $FilePath))
    foreach ($arg in @($ArgumentList)) {
        $null = $parts.Add((Quote-CommandArgument -Value ([string]$arg)))
    }
    return ($parts -join ' ')
}

function Write-InstallLog {
    param(
        [string]$Level,
        [string]$Message
    )
    Ensure-Directory -Path (Split-Path -Parent $script:LogPath)
    $line = '{0} [{1}] {2}' -f (Get-Date).ToString('yyyy-MM-ddTHH:mm:sszzz'), $Level, (Sanitize-LogText -Value $Message)
    Add-Content -LiteralPath $script:LogPath -Encoding UTF8 -Value $line
}

function Invoke-ProcessCapture {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory = '',
        [hashtable]$EnvironmentMap = $null,
        [string]$StepName = 'process',
        [switch]$IgnoreExitCode
    )

    $resolvedWorkingDirectory = if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) { $RepoRoot } else { $WorkingDirectory }
    $commandText = Format-Command -FilePath $FilePath -ArgumentList $ArgumentList
    Write-InstallLog -Level 'INFO' -Message ("step=$StepName command=$commandText")

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $FilePath
    $psi.Arguments = Build-ArgumentString -ArgumentList $ArgumentList
    $psi.WorkingDirectory = $resolvedWorkingDirectory
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    if ($null -ne $EnvironmentMap) {
        foreach ($entry in $EnvironmentMap.GetEnumerator()) {
            $psi.Environment[$entry.Key] = [string]$entry.Value
        }
    }

    $process = [System.Diagnostics.Process]::Start($psi)
    if ($null -eq $process) {
        throw "Failed to start process: $commandText"
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()
    $stdout = [string]$stdoutTask.Result
    $stderr = [string]$stderrTask.Result

    Write-InstallLog -Level 'INFO' -Message ("step=$StepName exit_code={0} stdout_summary={1} stderr_summary={2}" -f $process.ExitCode, (Get-OutputSummary -Value $stdout), (Get-OutputSummary -Value $stderr))

    if (($process.ExitCode -ne 0) -and (-not $IgnoreExitCode)) {
        throw "Step '$StepName' failed with exit code $($process.ExitCode)."
    }

    return [pscustomobject]@{
        ExitCode = [int]$process.ExitCode
        StdOut = $stdout
        StdErr = $stderr
        CommandText = $commandText
    }
}
function Get-ProgramFilesRoot {
    foreach ($candidate in @($env:ProgramW6432, $env:ProgramFiles)) {
        if (-not [string]::IsNullOrWhiteSpace($candidate)) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }
    return [System.IO.Path]::GetFullPath((Join-Path $HOME 'AppData\Local\Programs'))
}

function Resolve-ConfiguredInstallRoot {
    param([object]$Config)
    $mode = (Get-ConfigString -Config $Config -Names @('install_root_mode') -Default 'programfiles').Trim().ToLowerInvariant()
    $name = Get-ConfigString -Config $Config -Names @('install_root_name') -Default $script:ProductDisplayName
    switch ($mode) {
        'repo_root' { return [System.IO.Path]::GetFullPath($RepoRoot) }
        'user_profile' { return [System.IO.Path]::GetFullPath((Join-Path $HOME $name)) }
        'localappdata' { return [System.IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA $name)) }
        default { return [System.IO.Path]::GetFullPath((Join-Path (Get-ProgramFilesRoot) $name)) }
    }
}

function Resolve-InstallRootPath {
    param(
        [string]$ExplicitRoot,
        [string]$DefaultRoot,
        [switch]$NoPrompt
    )
    if (-not [string]::IsNullOrWhiteSpace($ExplicitRoot)) {
        return [System.IO.Path]::GetFullPath($ExplicitRoot)
    }
    if ($NoPrompt) {
        return [System.IO.Path]::GetFullPath($DefaultRoot)
    }
    Write-Host ''
    Write-Host ("Source Package : {0}" -f $RepoRoot)
    Write-Host ("Default Install: {0}" -f $DefaultRoot)
    $answer = Read-Host 'Install directory (press Enter to use default)'
    if ([string]::IsNullOrWhiteSpace($answer)) {
        $answer = $DefaultRoot
    }
    return [System.IO.Path]::GetFullPath($answer)
}

function Confirm-OverwriteIfNeeded {
    param(
        [string]$TargetRoot,
        [switch]$ForceInstall,
        [switch]$NoPrompt
    )
    if ($ForceInstall -or $NoPrompt) {
        return
    }
    if (-not (Test-Path -LiteralPath $TargetRoot)) {
        return
    }
    $items = @(Get-ChildItem -LiteralPath $TargetRoot -Force -ErrorAction SilentlyContinue)
    if ($items.Count -eq 0) {
        return
    }
    if ([string]::Equals([System.IO.Path]::GetFullPath($TargetRoot), [System.IO.Path]::GetFullPath($RepoRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
        return
    }
    Write-Host ''
    Write-Host '[WARN] Install target already contains files.'
    Write-Host ("Target: {0}" -f $TargetRoot)
    $answer = Read-Host 'Continue and refresh files? [y/N]'
    if ($answer -notin @('y', 'Y', 'yes', 'YES')) {
        throw 'Installation cancelled by user.'
    }
}

function Get-NormalizedDirectoryPrefix {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return ''
    }
    try {
        $fullPath = [System.IO.Path]::GetFullPath($Path)
    }
    catch {
        return ''
    }
    $separator = [string][System.IO.Path]::DirectorySeparatorChar
    $altSeparator = [string][System.IO.Path]::AltDirectorySeparatorChar
    if ((-not $fullPath.EndsWith($separator)) -and (-not $fullPath.EndsWith($altSeparator))) {
        $fullPath = $fullPath + $separator
    }
    return $fullPath
}

function Write-InstallRootPythonProcessDetails {
    param(
        [object[]]$Processes,
        [string]$Prefix
    )
    foreach ($process in @($Processes)) {
        $exePath = if ([string]::IsNullOrWhiteSpace([string]$process.ExecutablePath)) { '<unknown>' } else { [string]$process.ExecutablePath }
        Write-InstallLog -Level 'INFO' -Message ("{0} pid={1} ppid={2} name={3} executable_path={4} command_line={5}" -f $Prefix, $process.ProcessId, $process.ParentProcessId, $process.Name, $exePath, (Get-OutputSummary -Value ([string]$process.CommandLine)))
    }
}

function Get-InstallRootPythonProcesses {
    param([string]$TargetRoot)

    if ([string]::IsNullOrWhiteSpace($TargetRoot)) {
        return @()
    }

    $targetRootFull = ''
    try {
        $targetRootFull = [System.IO.Path]::GetFullPath($TargetRoot)
    }
    catch {
        return @()
    }

    if (-not (Test-Path -LiteralPath $targetRootFull -PathType Container)) {
        return @()
    }

    $runtimeRootPrefix = Get-NormalizedDirectoryPrefix -Path (Join-Path $targetRootFull 'python_runtime')
    $commandLinePattern = '(^|[\s"''=])' + [regex]::Escape($targetRootFull) + '([\\\s"'']|$)'
    $matched = New-Object 'System.Collections.Generic.List[object]'
    $processes = @(Get-CimInstance -ClassName Win32_Process -Filter "Name = 'python.exe' OR Name = 'pythonw.exe'" -ErrorAction SilentlyContinue)

    foreach ($process in @($processes | Sort-Object ProcessId)) {
        $exePath = [string]$process.ExecutablePath
        if (-not [string]::IsNullOrWhiteSpace($exePath)) {
            try {
                $exePath = [System.IO.Path]::GetFullPath($exePath)
            }
            catch {
            }
        }

        $commandLine = [string]$process.CommandLine
        $runtimeMatch = (-not [string]::IsNullOrWhiteSpace($exePath)) -and (-not [string]::IsNullOrWhiteSpace($runtimeRootPrefix)) -and $exePath.StartsWith($runtimeRootPrefix, [System.StringComparison]::OrdinalIgnoreCase)
        $commandLineMatch = (-not [string]::IsNullOrWhiteSpace($commandLine)) -and [regex]::IsMatch($commandLine, $commandLinePattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)

        if (-not ($runtimeMatch -or $commandLineMatch)) {
            continue
        }

        $null = $matched.Add([pscustomobject]@{
            ProcessId = [int]$process.ProcessId
            ParentProcessId = [int]$process.ParentProcessId
            Name = [string]$process.Name
            ExecutablePath = $exePath
            CommandLine = $commandLine
        })
    }

    return $matched.ToArray()
}

function Stop-InstallRootPythonProcesses {
    param(
        [string]$TargetRoot,
        [object[]]$Processes,
        [string]$Action
    )

    $processList = @($Processes)
    if ($processList.Count -eq 0) {
        return
    }

    Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_action={0} target_root={1} matched_count={2}" -f $Action, $TargetRoot, $processList.Count)

    foreach ($process in $processList) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
            Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_stop_requested pid={0} ppid={1} name={2}" -f $process.ProcessId, $process.ParentProcessId, $process.Name)
        }
        catch {
            Write-InstallLog -Level 'WARN' -Message ("python_process_precheck_stop_failed pid={0} ppid={1} name={2} reason={3}" -f $process.ProcessId, $process.ParentProcessId, $process.Name, $_.Exception.Message)
        }
    }

    $remaining = @()
    for ($attempt = 1; $attempt -le 5; $attempt++) {
        Start-Sleep -Milliseconds 500
        $remaining = @(Get-InstallRootPythonProcesses -TargetRoot $TargetRoot)
        Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_post_kill attempt={0} remaining_count={1}" -f $attempt, $remaining.Count)
        if ($remaining.Count -eq 0) {
            return
        }
    }

    Write-InstallRootPythonProcessDetails -Processes $remaining -Prefix 'python_process_precheck_remaining'
    Write-InstallLog -Level 'ERROR' -Message ("python_process_precheck_failed target_root={0} reason=python_processes_still_running remaining_count={1}" -f $TargetRoot, $remaining.Count)
    throw 'Failed to stop running Python processes under the install target. Close them manually and try again.'
}

function Invoke-InstallRootPythonPrecheck {
    param(
        [string]$TargetRoot,
        [switch]$ForceInstall,
        [switch]$NoPrompt
    )

    Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_started target_root={0}" -f $TargetRoot)
    $processes = @(Get-InstallRootPythonProcesses -TargetRoot $TargetRoot)
    Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_matched_count={0}" -f $processes.Count)

    if ($processes.Count -eq 0) {
        return
    }

    Write-InstallRootPythonProcessDetails -Processes $processes -Prefix 'python_process_precheck_match'

    if ($ForceInstall -or $NoPrompt) {
        Stop-InstallRootPythonProcesses -TargetRoot $TargetRoot -Processes $processes -Action 'auto_kill'
        return
    }

    Write-Host ''
    Write-Host '[WARN] Detected running Python processes under the install target.'
    Write-Host ("Install Target   : {0}" -f $TargetRoot)
    Write-Host ("Matched Processes: {0}" -f $processes.Count)
    Write-Host ''
    foreach ($process in $processes) {
        $exePath = if ([string]::IsNullOrWhiteSpace([string]$process.ExecutablePath)) { '<unknown>' } else { [string]$process.ExecutablePath }
        Write-Host ("PID={0} PPID={1} NAME={2}" -f $process.ProcessId, $process.ParentProcessId, $process.Name)
        Write-Host ("Path : {0}" -f $exePath)
        Write-Host ("Cmd  : {0}" -f (Get-OutputSummary -Value ([string]$process.CommandLine)))
        Write-Host ''
    }

    $answer = Read-Host 'Detected running Python processes under the install target. Stop them and continue? [y/N]'
    if ($answer -notin @('y', 'Y', 'yes', 'YES')) {
        Write-InstallLog -Level 'ERROR' -Message ("python_process_precheck_failed target_root={0} reason=user_declined_stop" -f $TargetRoot)
        throw 'Installation cancelled because running Python processes were detected under the install target.'
    }

    Stop-InstallRootPythonProcesses -TargetRoot $TargetRoot -Processes $processes -Action 'user_confirmed_kill'
}
function Read-SharedState {
    if (-not (Test-Path -LiteralPath $script:SharedStatePath)) {
        return $null
    }
    try {
        return (Get-Content -LiteralPath $script:SharedStatePath -Raw -Encoding UTF8 | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

function ConvertTo-PythonExePath {
    param([string]$CandidatePath)
    if ([string]::IsNullOrWhiteSpace($CandidatePath)) {
        return ''
    }
    $path = [System.IO.Path]::GetFullPath($CandidatePath)
    if ($path.EndsWith('pythonw.exe', [System.StringComparison]::OrdinalIgnoreCase)) {
        return [System.IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $path) 'python.exe'))
    }
    return $path
}

function Get-PythonInfo {
    param(
        [string]$PythonExe,
        [string]$StepName
    )
    $code = 'import json, os, sys; print(json.dumps({"executable": os.path.abspath(sys.executable), "version": sys.version.split()[0], "major": sys.version_info[0], "minor": sys.version_info[1]}))'
    $result = Invoke-ProcessCapture -FilePath $PythonExe -ArgumentList @('-c', $code) -StepName $StepName
    $jsonLine = ($result.StdOut -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 1)
    if ([string]::IsNullOrWhiteSpace($jsonLine)) {
        throw "Python probe returned no output: $PythonExe"
    }
    return ($jsonLine | ConvertFrom-Json)
}

function Resolve-BasePython {
    $checked = New-Object 'System.Collections.Generic.List[string]'
    $sharedState = Read-SharedState
    $savedPython = ''
    if (($null -ne $sharedState) -and ($null -ne $sharedState.PSObject.Properties['preferred_python'])) {
        $savedPython = ConvertTo-PythonExePath -CandidatePath ([string]$sharedState.preferred_python)
    }

    $candidateRows = @(
        [pscustomobject]@{ Source = 'saved_state'; Path = $savedPython },
        [pscustomobject]@{ Source = 'localappdata_python311'; Path = (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python311\python.exe') }
    )

    foreach ($candidate in @($candidateRows)) {
        $candidatePath = ConvertTo-PythonExePath -CandidatePath ([string]$candidate.Path)
        if ([string]::IsNullOrWhiteSpace($candidatePath)) {
            continue
        }
        if (-not (Test-Path -LiteralPath $candidatePath)) {
            $null = $checked.Add("$($candidate.Source):$candidatePath (missing)")
            continue
        }
        try {
            $info = Get-PythonInfo -PythonExe $candidatePath -StepName ("probe_python_{0}" -f [string]$candidate.Source)
            if (("{0}.{1}" -f [int]$info.major, [int]$info.minor) -eq $script:PreferredMinor) {
                $pythonwPath = Join-Path (Split-Path -Parent $candidatePath) 'pythonw.exe'
                return [pscustomobject]@{ Path = [string]$info.executable; Pythonw = $(if (Test-Path -LiteralPath $pythonwPath) { [System.IO.Path]::GetFullPath($pythonwPath) } else { '' }); Version = [string]$info.version; Source = [string]$candidate.Source }
            }
            $null = $checked.Add("$($candidate.Source):$candidatePath (version $($info.version))")
        }
        catch {
            $null = $checked.Add("$($candidate.Source):$candidatePath (probe failed)")
        }
    }

    $pyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $pyCommand) {
        $pyResult = Invoke-ProcessCapture -FilePath $pyCommand.Source -ArgumentList @('-3.11', '-c', 'import sys; print(sys.executable)') -StepName 'probe_python_py311' -IgnoreExitCode
        if ($pyResult.ExitCode -eq 0) {
            $resolved = ($pyResult.StdOut -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 1)
            $candidatePath = ConvertTo-PythonExePath -CandidatePath $resolved
            if ((-not [string]::IsNullOrWhiteSpace($candidatePath)) -and (Test-Path -LiteralPath $candidatePath)) {
                try {
                    $info = Get-PythonInfo -PythonExe $candidatePath -StepName 'probe_python_py311_resolved'
                    if (("{0}.{1}" -f [int]$info.major, [int]$info.minor) -eq $script:PreferredMinor) {
                        $pythonwPath = Join-Path (Split-Path -Parent $candidatePath) 'pythonw.exe'
                        return [pscustomobject]@{ Path = [string]$info.executable; Pythonw = $(if (Test-Path -LiteralPath $pythonwPath) { [System.IO.Path]::GetFullPath($pythonwPath) } else { '' }); Version = [string]$info.version; Source = 'py-3.11' }
                    }
                    $null = $checked.Add("py-3.11:$candidatePath (version $($info.version))")
                }
                catch {
                    $null = $checked.Add("py-3.11:$candidatePath (probe failed)")
                }
            }
            else {
                $null = $checked.Add("py-3.11:$resolved (missing)")
            }
        }
        else {
            $null = $checked.Add(("py-3.11 (exit {0})" -f $pyResult.ExitCode))
        }
    }
    else {
        $null = $checked.Add('py.exe (missing)')
    }

    foreach ($commandName in @('python.exe', 'python', 'pythonw.exe', 'pythonw')) {
        $command = Get-Command $commandName -ErrorAction SilentlyContinue
        if ($null -eq $command) {
            $null = $checked.Add("path:$commandName (missing)")
            continue
        }
        $candidatePath = ConvertTo-PythonExePath -CandidatePath ([string]$command.Source)
        if (-not (Test-Path -LiteralPath $candidatePath)) {
            $null = $checked.Add("path:$commandName -> $candidatePath (missing)")
            continue
        }
        try {
            $info = Get-PythonInfo -PythonExe $candidatePath -StepName ("probe_python_{0}" -f ($commandName -replace '[^a-zA-Z0-9]', '_'))
            if (("{0}.{1}" -f [int]$info.major, [int]$info.minor) -eq $script:PreferredMinor) {
                $pythonwPath = Join-Path (Split-Path -Parent $candidatePath) 'pythonw.exe'
                return [pscustomobject]@{ Path = [string]$info.executable; Pythonw = $(if (Test-Path -LiteralPath $pythonwPath) { [System.IO.Path]::GetFullPath($pythonwPath) } else { '' }); Version = [string]$info.version; Source = ("path:{0}" -f $commandName) }
            }
            $null = $checked.Add("path:$commandName -> $candidatePath (version $($info.version))")
        }
        catch {
            $null = $checked.Add("path:$commandName -> $candidatePath (probe failed)")
        }
    }

    throw @"
User-scope Python 3.11 was not found.

Checked:
  $(($checked.ToArray()) -join "`n  ")

Install Python 3.11 for the current user so that this path exists:
  %LocalAppData%\Programs\Python\Python311\python.exe
"@
}

function Assert-PathWithinRoot {
    param(
        [string]$Path,
        [string]$Root
    )
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($Root)
    if (-not $fullRoot.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $fullRoot = $fullRoot + [System.IO.Path]::DirectorySeparatorChar
    }
    if (-not $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to modify path outside root. path=$fullPath root=$fullRoot"
    }
}
function Ensure-Venv {
    param(
        [string]$BasePython,
        [string]$VenvRoot,
        [string[]]$RequirementFiles
    )

    $venvPython = Join-Path $VenvRoot 'Scripts\python.exe'
    $venvPythonw = Join-Path $VenvRoot 'Scripts\pythonw.exe'
    $recreateReason = ''

    if ((-not (Test-Path -LiteralPath $venvPython)) -or (-not (Test-Path -LiteralPath $venvPythonw))) {
        $recreateReason = 'missing_python_executables'
    }
    else {
        try {
            $venvInfo = Get-PythonInfo -PythonExe $venvPython -StepName 'probe_existing_venv'
            if (("{0}.{1}" -f [int]$venvInfo.major, [int]$venvInfo.minor) -ne $script:PreferredMinor) {
                $recreateReason = ("existing_venv_version_{0}" -f [string]$venvInfo.version)
            }
        }
        catch {
            $recreateReason = 'existing_venv_probe_failed'
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($recreateReason) -and (Test-Path -LiteralPath $VenvRoot)) {
        Assert-PathWithinRoot -Path $VenvRoot -Root (Join-Path $script:SharedRoot 'venvs')
        Write-InstallLog -Level 'INFO' -Message ("venv_reset_reason=$recreateReason venv_root=$VenvRoot")
        Remove-Item -LiteralPath $VenvRoot -Recurse -Force
    }

    if (-not (Test-Path -LiteralPath $venvPython)) {
        Ensure-Directory -Path (Split-Path -Parent $VenvRoot)
        Invoke-ProcessCapture -FilePath $BasePython -ArgumentList @('-m', 'venv', $VenvRoot) -StepName 'create_product_venv' | Out-Null
    }

    $upgradeResult = Invoke-ProcessCapture -FilePath $venvPython -ArgumentList @('-m', 'pip', '--disable-pip-version-check', 'install', '--upgrade', 'pip', 'setuptools', 'wheel') -WorkingDirectory $RepoRoot -StepName 'upgrade_venv_tools'

    $installArgs = @('-m', 'pip', '--disable-pip-version-check', 'install', '--upgrade')
    foreach ($requirementFile in @($RequirementFiles)) {
        if (-not (Test-Path -LiteralPath $requirementFile)) {
            throw "Requirement file not found: $requirementFile"
        }
        $installArgs += @('-r', $requirementFile)
    }
    $installResult = Invoke-ProcessCapture -FilePath $venvPython -ArgumentList $installArgs -WorkingDirectory $RepoRoot -StepName 'install_product_requirements'

    return [pscustomobject]@{
        Python = $venvPython
        Pythonw = $venvPythonw
        UpgradeCommand = $upgradeResult.CommandText
        InstallCommand = $installResult.CommandText
    }
}

function Ensure-DataDirectories {
    param([string]$DataRoot)
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

function Get-AppEnvironmentMap {
    param(
        [string]$TargetRoot,
        [string]$DataRoot,
        [string[]]$PythonPathRelativeEntries
    )
    $pythonPathEntries = New-Object 'System.Collections.Generic.List[string]'
    foreach ($entry in @($PythonPathRelativeEntries)) {
        if ([string]::IsNullOrWhiteSpace($entry) -or ($entry -eq '.')) {
            $null = $pythonPathEntries.Add([System.IO.Path]::GetFullPath($TargetRoot))
        }
        else {
            $null = $pythonPathEntries.Add([System.IO.Path]::GetFullPath((Join-Path $TargetRoot $entry)))
        }
    }
    return @{
        PYTHONPATH = ($pythonPathEntries -join ';')
        PYTHONUTF8 = '1'
        PYTHONIOENCODING = 'utf-8'
        LWF_HOME = $TargetRoot
        LWF_RUNTIME_ROOT = (Join-Path $DataRoot 'runtime')
        LWF_CONFIGS_ROOT = (Join-Path $DataRoot 'configs')
        LWF_MARKET_DATA_ROOT = (Join-Path $DataRoot 'market_data')
    }
}

function Copy-PackagePayload {
    param(
        [object]$Plan,
        [string]$TargetRoot
    )

    foreach ($relativeDir in @($Plan.skeleton_dirs)) {
        Ensure-Directory -Path (Join-NormalizedPath -BasePath $TargetRoot -RelativePath $relativeDir)
    }

    if ([string]::Equals([System.IO.Path]::GetFullPath($RepoRoot), [System.IO.Path]::GetFullPath($TargetRoot), [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-InstallLog -Level 'INFO' -Message 'payload_copy=skipped reason=source_equals_target'
        return 0
    }

    $copiedCount = 0
    foreach ($candidate in @($Plan.candidate_files)) {
        $destinationPath = Join-NormalizedPath -BasePath $TargetRoot -RelativePath ([string]$candidate.relative_path)
        Ensure-Directory -Path (Split-Path -Parent $destinationPath)
        Copy-Item -LiteralPath ([string]$candidate.source_path) -Destination $destinationPath -Force
        $copiedCount++
    }

    foreach ($relativePath in @($Plan.reinclude_after_exclude)) {
        $sourcePath = Join-NormalizedPath -BasePath $Plan.repo_root -RelativePath $relativePath
        if (-not (Test-Path -LiteralPath $sourcePath)) {
            continue
        }
        $destinationPath = Join-NormalizedPath -BasePath $TargetRoot -RelativePath $relativePath
        Ensure-Directory -Path (Split-Path -Parent $destinationPath)
        Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Force
    }

    Write-InstallLog -Level 'INFO' -Message ("payload_copy_count=$copiedCount")
    return $copiedCount
}

function Run-LauncherImportCheck {
    param(
        [string]$VenvPython,
        [hashtable]$EnvironmentMap,
        [string]$WorkingDirectory
    )
    $code = @"
import json
import keyring
from PySide6.QtWidgets import QApplication
import app.cli.app_main
from app.security.keyring_store import describe_backend
print(json.dumps(describe_backend(), ensure_ascii=False))
"@
    $result = Invoke-ProcessCapture -FilePath $VenvPython -ArgumentList @('-c', $code) -WorkingDirectory $WorkingDirectory -EnvironmentMap $EnvironmentMap -StepName 'launcher_import_check'
    $jsonLine = ($result.StdOut -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Last 1)
    $backend = $null
    if (-not [string]::IsNullOrWhiteSpace($jsonLine)) {
        try {
            $backend = ($jsonLine | ConvertFrom-Json)
        }
        catch {
            $backend = $null
        }
    }
    return [pscustomobject]@{
        ExitCode = [int]$result.ExitCode
        Summary = Get-OutputSummary -Value ($result.StdOut + ' ' + $result.StdErr)
        Backend = $backend
    }
}
function Write-SharedState {
    param(
        [object]$BasePythonInfo,
        [string]$InstallTarget,
        [string]$VenvRoot,
        [string]$DataRoot
    )
    Ensure-Directory -Path (Split-Path -Parent $script:SharedStatePath)
    $existing = Read-SharedState
    $root = [ordered]@{}
    if ($null -ne $existing) {
        foreach ($property in $existing.PSObject.Properties) {
            if ($property.Name -ne 'products') {
                $root[$property.Name] = $property.Value
            }
        }
    }
    $products = [ordered]@{}
    if (($null -ne $existing) -and ($null -ne $existing.PSObject.Properties['products'])) {
        foreach ($property in $existing.products.PSObject.Properties) {
            $products[$property.Name] = $property.Value
        }
    }
    $products[$script:ProductCode] = [ordered]@{ install_root = $InstallTarget; venv_root = $VenvRoot; data_root = $DataRoot; updated_at_utc = (Get-Date).ToUniversalTime().ToString('o'); installer_build_id = $script:BuildId }
    $root['preferred_python'] = [string]$BasePythonInfo.Path
    $root['preferred_pythonw'] = [string]$BasePythonInfo.Pythonw
    $root['python_version'] = [string]$BasePythonInfo.Version
    $root['updated_at_utc'] = (Get-Date).ToUniversalTime().ToString('o')
    $root['selector_build_id'] = $script:BuildId
    $root['products'] = $products
    ($root | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $script:SharedStatePath -Encoding UTF8
}

function Write-InstallReceipt {
    param(
        [object]$Config,
        [string]$InstallTarget,
        [object]$BasePythonInfo,
        [string]$VenvRoot,
        [string]$DataRoot,
        [string]$ShortcutPath,
        [int]$CopiedFileCount,
        [object]$ImportCheck,
        [string[]]$InstallCommands
    )
    $receiptRelative = Get-ConfigString -Config $Config -Names @('receipt_relative_path') -Default '.install_receipt.json'
    $receiptPath = Join-Path $InstallTarget $receiptRelative
    Ensure-Directory -Path (Split-Path -Parent $receiptPath)
    $receipt = [ordered]@{ build_id = $script:BuildId; installed_at_utc = (Get-Date).ToUniversalTime().ToString('o'); product = $script:ProductCode; product_display_name = $script:ProductDisplayName; source_root = $RepoRoot; install_root = $InstallTarget; base_python = [string]$BasePythonInfo.Path; base_pythonw = [string]$BasePythonInfo.Pythonw; base_python_source = [string]$BasePythonInfo.Source; python_version = [string]$BasePythonInfo.Version; venv_root = $VenvRoot; data_root = $DataRoot; shortcut_path = $ShortcutPath; copied_file_count = [int]$CopiedFileCount; install_log = $script:LogPath; package_install_commands = @($InstallCommands); import_check = [ordered]@{ exit_code = [int]$ImportCheck.ExitCode; summary = [string]$ImportCheck.Summary }; keyring_backend = $(if ($null -ne $ImportCheck.Backend) { [string]$ImportCheck.Backend.backend } else { '' }); keyring_module = $(if ($null -ne $ImportCheck.Backend) { [string]$ImportCheck.Backend.module } else { '' }); keyring_secure = $(if ($null -ne $ImportCheck.Backend) { [bool]$ImportCheck.Backend.secure } else { $false }) }
    ($receipt | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $receiptPath -Encoding UTF8
    return $receiptPath
}

function New-DesktopShortcut {
    param(
        [object]$Config,
        [string]$TargetRoot,
        [string]$DesktopOverride
    )
    $desktopDir = if ([string]::IsNullOrWhiteSpace($DesktopOverride)) { [Environment]::GetFolderPath('Desktop') } else { [System.IO.Path]::GetFullPath($DesktopOverride) }
    if ([string]::IsNullOrWhiteSpace($desktopDir)) { return '' }
    Ensure-Directory -Path $desktopDir
    $shortcutName = Get-ConfigString -Config $Config -Names @('shortcut_name') -Default $script:ProductDisplayName
    $shortcutPath = Join-Path $desktopDir ($shortcutName + '.lnk')
    $targetPath = Join-Path $TargetRoot (Get-ConfigString -Config $Config -Names @('shortcut_relative_target') -Default '')
    $iconRelative = Get-ConfigString -Config $Config -Names @('shortcut_icon_relative_path') -Default ''
    $iconPath = ''
    if (-not [string]::IsNullOrWhiteSpace($iconRelative)) { $iconCandidate = Join-Path $TargetRoot $iconRelative; if (Test-Path -LiteralPath $iconCandidate) { $iconPath = [System.IO.Path]::GetFullPath($iconCandidate) } }
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $targetPath
    $shortcut.WorkingDirectory = $TargetRoot
    $shortcut.Description = Get-ConfigString -Config $Config -Names @('shortcut_description') -Default ('Launch ' + $script:ProductDisplayName)
    if (-not [string]::IsNullOrWhiteSpace($iconPath)) { $shortcut.IconLocation = "$iconPath,0" }
    $shortcut.Save()
    return $shortcutPath
}

try {
    if ([string]::IsNullOrWhiteSpace($ConfigPath) -or [string]::IsNullOrWhiteSpace($ManifestPath) -or [string]::IsNullOrWhiteSpace($PackageCommonPath)) {
        throw 'ConfigPath, ManifestPath, and PackageCommonPath are required.'
    }
    if (-not (Test-Path -LiteralPath $PackageCommonPath)) {
        throw "Installer support file not found: $PackageCommonPath"
    }
    . $PackageCommonPath

    $config = Read-JsonFile -Path $ConfigPath
    $script:ProductCode = Get-ConfigString -Config $config -Names @('product_code') -Default 'product'
    $script:ProductDisplayName = Get-ConfigString -Config $config -Names @('product_display_name') -Default 'LoneWolf Fang'
    $script:PreferredMinor = Get-ConfigString -Config $config -Names @('python_preferred_minor') -Default '3.11'
    $script:LogPath = Join-Path $script:SharedRoot ('logs\installer_' + $script:ProductCode + '.log')

    $defaultInstallRoot = Resolve-ConfiguredInstallRoot -Config $config
    $targetRoot = Resolve-InstallRootPath -ExplicitRoot $InstallRoot -DefaultRoot $defaultInstallRoot -NoPrompt:$NonInteractive
    $targetRoot = [System.IO.Path]::GetFullPath($targetRoot)
    $venvRoot = Join-Path $script:SharedRoot ('venvs\' + (Get-ConfigString -Config $config -Names @('venv_name') -Default $script:ProductCode))
    $dataRoot = Join-Path $script:SharedRoot ('data\' + (Get-ConfigString -Config $config -Names @('data_name') -Default $script:ProductCode))

    Ensure-Directory -Path $targetRoot
    Confirm-OverwriteIfNeeded -TargetRoot $targetRoot -ForceInstall:$Force -NoPrompt:$NonInteractive
    if ($SkipInstallRootPythonPrecheck) {
        Write-InstallLog -Level 'INFO' -Message ("python_process_precheck_skipped target_root={0} reason=skip_install_root_python_precheck" -f $targetRoot)
    }
    else {
        Invoke-InstallRootPythonPrecheck -TargetRoot $targetRoot -ForceInstall:$Force -NoPrompt:$NonInteractive
    }

    $plan = Get-PackagePlan -RepoRoot $RepoRoot -ManifestPath $ManifestPath
    if (@($plan.missing_required).Count -gt 0) {
        throw ('Required package files are missing: ' + (@($plan.missing_required) -join ', '))
    }

    Write-InstallLog -Level 'INFO' -Message ("build_id=$($script:BuildId) product=$($script:ProductCode) install_root=$targetRoot venv_root=$venvRoot data_root=$dataRoot manifest=$ManifestPath")
    Write-Host ('== {0} Installer ==' -f $script:ProductDisplayName)
    Write-Host ('BUILD_ID        : {0}' -f $script:BuildId)
    Write-Host ('Package Root    : {0}' -f $RepoRoot)
    Write-Host ('Install Root    : {0}' -f $targetRoot)
    Write-Host ('Installer Log   : {0}' -f $script:LogPath)
    Write-Host ''

    $copiedFileCount = Copy-PackagePayload -Plan $plan -TargetRoot $targetRoot
    Ensure-DataDirectories -DataRoot $dataRoot

    $basePythonInfo = Resolve-BasePython
    Write-InstallLog -Level 'INFO' -Message ("selected_python_path={0} selected_python_source={1} selected_python_version={2}" -f $basePythonInfo.Path, $basePythonInfo.Source, $basePythonInfo.Version)

    $requirementFiles = @(); foreach ($relativePath in @(Get-ConfigArray -Config $config -Names @('pip_requirement_files'))) { $requirementFiles += [System.IO.Path]::GetFullPath((Join-Path $targetRoot $relativePath)) }
    $envMap = Get-AppEnvironmentMap -TargetRoot $targetRoot -DataRoot $dataRoot -PythonPathRelativeEntries @(Get-ConfigArray -Config $config -Names @('pythonpath_relative_entries'))
    $venvInfo = Ensure-Venv -BasePython $basePythonInfo.Path -VenvRoot $venvRoot -RequirementFiles $requirementFiles
    $importCheck = Run-LauncherImportCheck -VenvPython $venvInfo.Python -EnvironmentMap $envMap -WorkingDirectory $targetRoot

    Write-SharedState -BasePythonInfo $basePythonInfo -InstallTarget $targetRoot -VenvRoot $venvRoot -DataRoot $dataRoot

    $shortcutPath = ''; if (-not $SkipDesktopShortcut) { $shortcutPath = New-DesktopShortcut -Config $config -TargetRoot $targetRoot -DesktopOverride $DesktopDirectoryOverride; if (-not [string]::IsNullOrWhiteSpace($shortcutPath)) { Write-InstallLog -Level 'INFO' -Message ("desktop_shortcut={0}" -f $shortcutPath) } }
    $receiptPath = Write-InstallReceipt -Config $config -InstallTarget $targetRoot -BasePythonInfo $basePythonInfo -VenvRoot $venvRoot -DataRoot $dataRoot -ShortcutPath $shortcutPath -CopiedFileCount $copiedFileCount -ImportCheck $importCheck -InstallCommands @($venvInfo.UpgradeCommand, $venvInfo.InstallCommand)

    Write-Host ('Base Python     : {0}' -f $basePythonInfo.Path)
    Write-Host ('Venv Root       : {0}' -f $venvRoot)
    Write-Host ('Data Root       : {0}' -f $dataRoot)
    Write-Host ('Receipt         : {0}' -f $receiptPath)
    if (-not [string]::IsNullOrWhiteSpace($shortcutPath)) { Write-Host ('Desktop Shortcut: {0}' -f $shortcutPath) }
    Write-Host ''
    Write-Host ('[OK] {0} installation completed successfully.' -f $script:ProductDisplayName)
    exit 0
}
catch {
    $message = if ($null -ne $_.Exception) { $_.Exception.Message } else { [string]$_ }
    Write-InstallLog -Level 'ERROR' -Message ("install_failed product={0} requested_install_root={1} reason={2}" -f $script:ProductCode, $InstallRoot, $message)
    Write-Host ''
    Write-Host ('[FAILED] {0} installation did not complete.' -f $script:ProductDisplayName)
    Write-Host ('Reason          : {0}' -f $message)
    if (($null -ne $_.Exception) -and ($_.Exception -is [System.UnauthorizedAccessException])) {
        Write-Host 'Hint            : Run Setup as Administrator for Program Files, or choose a writable install directory.'
    }
    elseif (([string]$message) -match 'Access to the path|access is denied|permission') {
        Write-Host 'Hint            : Run Setup as Administrator for Program Files, or choose a writable install directory.'
    }
    Write-Host ('Installer Log   : {0}' -f $script:LogPath)
    exit 1
}



