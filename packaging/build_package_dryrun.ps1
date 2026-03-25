# BUILD_ID: 2026-03-25_free_package_dryrun_v1
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string]$StagingRoot = "",
    [switch]$ListOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-03-25_free_package_dryrun_v1'
$RepoRoot = Split-Path -Parent $PSScriptRoot

. (Join-Path $PSScriptRoot 'package_common.ps1')

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $PSScriptRoot 'package_manifest_free.json'
}
if ([string]::IsNullOrWhiteSpace($StagingRoot)) {
    $StagingRoot = Join-Path $PSScriptRoot 'staging'
}

$plan = Get-PackagePlan -RepoRoot $RepoRoot -ManifestPath $ManifestPath
$stage = $null
$boundary = $null
$boundaryResult = $null

if (-not $ListOnly) {
    $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $stageName = "dryrun_$timestamp"
    $stage = New-PackageStaging -Plan $plan -StagingRoot $StagingRoot -StageName $stageName
    $boundary = Get-PackageBoundaryChecks -PackageRootPath $stage.package_root_path
    $boundaryResult = Test-PackageBoundary -Checks $boundary.checks
}

Write-Host '== Free Package Dry Run =='
Write-Host ('BUILD_ID            : {0}' -f $BUILD_ID)
Write-Host ('Manifest            : {0}' -f $plan.manifest_path)
Write-Host ('Repo Root           : {0}' -f $plan.repo_root)
Write-Host ('Staging Mode        : {0}' -f ($(if ($ListOnly) { 'list-only' } else { 'copy-to-staging' })))
if ($stage) {
    Write-Host ('Staging Path        : {0}' -f $stage.stage_path)
    Write-Host ('Package Root        : {0}' -f $stage.package_root_path)
}
Write-Host ''
Write-Host 'Included File Counts'
Write-Host ('  core              : {0}' -f $plan.core_count)
Write-Host ('  configs           : {0}' -f $plan.config_count)
Write-Host ('  launchers         : {0}' -f $plan.launcher_count)
Write-Host ('  docs              : {0}' -f $plan.docs_count)
Write-Host ('  total files       : {0}' -f $plan.total_count)
Write-Host ('  skeleton dirs     : {0}' -f @($plan.skeleton_dirs).Count)
Write-Host ''
Write-Host 'Runtime Skeleton'
foreach ($relativeDir in @($plan.skeleton_dirs)) {
    Write-Host ('  {0}' -f $relativeDir)
}
Write-Host ''
Write-Host 'Required Missing Entries'
if (@($plan.missing_required).Count -eq 0) {
    Write-Host '  (none)'
}
else {
    foreach ($item in @($plan.missing_required)) {
        Write-Host ('  {0}' -f $item)
    }
}
Write-Host ''
Write-Host 'Optional Missing Entries'
if (@($plan.missing_optional).Count -eq 0) {
    Write-Host '  (none)'
}
else {
    foreach ($item in @($plan.missing_optional)) {
        Write-Host ('  {0}' -f $item)
    }
}
Write-Host ''
Write-Host 'Excluded Candidate Samples'
if (@($plan.excluded_samples).Count -eq 0) {
    Write-Host '  (none)'
}
else {
    foreach ($item in (@($plan.excluded_samples) | Select-Object -First 12)) {
        Write-Host ('  {0}' -f $item)
    }
}
Write-Host ''

if ($boundary) {
    Write-Host 'Boundary Checks'
    foreach ($item in $boundary.checks.GetEnumerator()) {
        Write-Host ('  {0,-28} : {1}' -f $item.Key, $item.Value)
    }
    Write-Host ''
}

if (@($plan.missing_required).Count -gt 0) {
    Write-Warning ('Required manifest entries are missing from the current working tree: {0}' -f (@($plan.missing_required) -join ', '))
}
if ($boundaryResult -and (-not $boundaryResult.is_ok)) {
    Write-Warning ('Boundary check mismatches detected: {0}' -f (@($boundaryResult.failures) -join '; '))
}
