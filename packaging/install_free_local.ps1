# BUILD_ID: 2026-04-02_free_user_scope_installer_wrapper_v1
[CmdletBinding()]
param(
    [string]$InstallRoot = "",
    [string]$DesktopDirOverride = "",
    [switch]$SkipShortcut,
    [switch]$SkipDesktopShortcut,
    [switch]$NonInteractive,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$sharedScript = Join-Path $PSScriptRoot 'install_shared.ps1'
$skipDesktop = $SkipShortcut -or $SkipDesktopShortcut
& $sharedScript `
    -InstallerBuildId '2026-04-02_free_user_scope_installer_wrapper_v1' `
    -ConfigPath (Join-Path $PSScriptRoot 'install_config_free.json') `
    -ManifestPath (Join-Path $PSScriptRoot 'package_manifest_free.json') `
    -PackageCommonPath (Join-Path $PSScriptRoot 'package_common.ps1') `
    -InstallRoot $InstallRoot `
    -DesktopDirectoryOverride $DesktopDirOverride `
    -Force:$Force `
    -SkipDesktopShortcut:$skipDesktop `
    -NonInteractive:$NonInteractive
exit $LASTEXITCODE
