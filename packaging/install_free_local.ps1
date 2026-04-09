# BUILD_ID: 2026-04-03_free_user_scope_installer_wrapper_skip_precheck_v1
[CmdletBinding()]
param(
    [string]$InstallRoot = "",
    [string]$BootstrapSetupPath = "",
    [string]$DesktopDirOverride = "",
    [switch]$SkipShortcut,
    [switch]$SkipDesktopShortcut,
    [switch]$SkipInstallRootPythonPrecheck,
    [switch]$NonInteractive,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$sharedScript = Join-Path $PSScriptRoot 'install_shared.ps1'
$skipDesktop = $SkipShortcut -or $SkipDesktopShortcut
& $sharedScript `
    -InstallerBuildId '2026-04-03_free_user_scope_installer_wrapper_skip_precheck_v1' `
    -ConfigPath (Join-Path $PSScriptRoot 'install_config_free.json') `
    -ManifestPath (Join-Path $PSScriptRoot 'package_manifest_free.json') `
    -PackageCommonPath (Join-Path $PSScriptRoot 'package_common.ps1') `
    -InstallRoot $InstallRoot `
    -BootstrapSetupPath $BootstrapSetupPath `
    -DesktopDirectoryOverride $DesktopDirOverride `
    -Force:$Force `
    -SkipInstallRootPythonPrecheck:$SkipInstallRootPythonPrecheck `
    -SkipDesktopShortcut:$skipDesktop `
    -NonInteractive:$NonInteractive
exit $LASTEXITCODE
