# BUILD_ID: 2026-03-25_free_package_build_v1
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string]$StagingRoot = "",
    [string]$DistRoot = "",
    [string]$ReleaseId = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-03-25_free_package_build_v1'
$RepoRoot = Split-Path -Parent $PSScriptRoot

. (Join-Path $PSScriptRoot 'package_common.ps1')

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $PSScriptRoot 'package_manifest_free.json'
}
if ([string]::IsNullOrWhiteSpace($StagingRoot)) {
    $StagingRoot = Join-Path $PSScriptRoot 'staging'
}
if ([string]::IsNullOrWhiteSpace($DistRoot)) {
    $DistRoot = Join-Path $PSScriptRoot 'dist'
}
if (-not (Test-Path -LiteralPath $StagingRoot)) {
    $null = New-Item -ItemType Directory -Path $StagingRoot -Force
}
if (-not (Test-Path -LiteralPath $DistRoot)) {
    $null = New-Item -ItemType Directory -Path $DistRoot -Force
}
if ([string]::IsNullOrWhiteSpace($ReleaseId)) {
    $ReleaseId = Get-Date -Format 'yyyyMMdd_HHmmss'
}

$plan = Get-PackagePlan -RepoRoot $RepoRoot -ManifestPath $ManifestPath
if (@($plan.missing_required).Count -gt 0) {
    throw ('Cannot build free package because required manifest entries are missing: {0}' -f (@($plan.missing_required) -join ', '))
}

$stageName = "build_$ReleaseId"
$stage = New-PackageStaging -Plan $plan -StagingRoot $StagingRoot -StageName $stageName

$boundary = Get-PackageBoundaryChecks -PackageRootPath $stage.package_root_path
$boundaryResult = Test-PackageBoundary -Checks $boundary.checks
if (-not $boundaryResult.is_ok) {
    throw ('Cannot build free package because boundary checks failed: {0}' -f (@($boundaryResult.failures) -join '; '))
}

$zipName = ('{0}_{1}.zip' -f $plan.package_root_name, $ReleaseId)
$zipPath = Join-Path $DistRoot $zipName
$resolvedZipPath = New-PackageZip -PackageRootPath $stage.package_root_path -ZipPath $zipPath

Write-Host '== Free Package Build =='
Write-Host ('BUILD_ID            : {0}' -f $BUILD_ID)
Write-Host ('Manifest            : {0}' -f $plan.manifest_path)
Write-Host ('Repo Root           : {0}' -f $plan.repo_root)
Write-Host ('Staging Path        : {0}' -f $stage.stage_path)
Write-Host ('Package Root        : {0}' -f $stage.package_root_path)
Write-Host ('Zip Path            : {0}' -f $resolvedZipPath)
Write-Host ('Total Files         : {0}' -f $plan.total_count)
