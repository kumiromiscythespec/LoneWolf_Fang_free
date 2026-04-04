# BUILD_ID: 2026-03-30_free_package_dryrun_native_artifacts_v1
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string]$StagingRoot = "",
    [switch]$ListOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-03-30_free_package_dryrun_native_artifacts_v1'
$RepoRoot = Split-Path -Parent $PSScriptRoot

. (Join-Path $PSScriptRoot 'package_common.ps1')

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $ManifestPath = Join-Path $PSScriptRoot 'package_manifest_free.json'
}
if ([string]::IsNullOrWhiteSpace($StagingRoot)) {
    $StagingRoot = Join-Path $PSScriptRoot 'staging'
}

$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
$nativeArtifactSync = Sync-PackageNativeArtifacts -RepoRoot $RepoRoot -Manifest $manifest -MaterializeToRepoRoot:$false

$plan = Get-PackagePlan -RepoRoot $RepoRoot -ManifestPath $ManifestPath
$stage = $null
$boundary = $null
$boundaryResult = $null
$nativeArtifactStatus = $null

if (-not $ListOnly) {
    $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $stageName = "dryrun_$timestamp"
    $stage = New-PackageStaging -Plan $plan -StagingRoot $StagingRoot -StageName $stageName
    $boundary = Get-PackageBoundaryChecks -PackageRootPath $stage.package_root_path
    $boundaryResult = Test-PackageBoundary -Checks $boundary.checks
    $nativeArtifactStatus = Get-NativeArtifactValidation `
        -NativeArtifacts $plan.native_artifacts `
        -PackageRootPath $stage.package_root_path `
        -PackageRootName $plan.package_root_name
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
Write-Host ('  native artifacts  : {0}' -f $plan.native_artifact_count)
Write-Host ('  docs              : {0}' -f $plan.docs_count)
Write-Host ('  total files       : {0}' -f $plan.total_count)
Write-Host ('  materialize build : {0}' -f (@($nativeArtifactSync.artifacts | Where-Object { $_.would_materialize_to_repo_root })).Count)
Write-Host ('  skeleton dirs     : {0}' -f @($plan.skeleton_dirs).Count)
Write-Host ''
Write-Host 'Native Artifacts'
if (@($nativeArtifactSync.artifacts).Count -eq 0) {
    Write-Host '  (none)'
}
else {
    foreach ($artifact in @($nativeArtifactSync.artifacts)) {
        $sourceLabel = 'missing'
        $resolutionLabel = 'missing'
        $repoRootLabel = if ($artifact.exists_at_target) { 'present' } else { 'missing' }
        $materializedLabel = 'no'

        if (-not [string]::IsNullOrWhiteSpace([string]$artifact.source_relative_path)) {
            $sourceLabel = [string]$artifact.source_relative_path
        }

        switch ([string]$artifact.source_kind) {
            'repo_root' { $resolutionLabel = 'resolved from repo root' }
            'publish_output' { $resolutionLabel = 'resolved from publish output' }
            'source_candidate' { $resolutionLabel = 'resolved from source candidate' }
            default { $resolutionLabel = 'missing' }
        }

        if ($artifact.was_materialized_to_repo_root) {
            $materializedLabel = 'yes'
        }
        elseif ($artifact.would_materialize_to_repo_root) {
            $materializedLabel = 'on build'
        }

        $packageLabel = 'not staged'
        if ($nativeArtifactStatus) {
            $stageRow = @($nativeArtifactStatus.artifacts | Where-Object { $_.package_path -eq $artifact.package_path } | Select-Object -First 1)
            if ($stageRow) {
                $packageLabel = if ($stageRow.exists_in_package_root) { 'present' } else { 'missing' }
            }
        }

        Write-Host ('  {0}' -f $artifact.package_path)
        Write-Host ('    resolved            : {0}' -f $resolutionLabel)
        Write-Host ('    source candidate    : {0}' -f $sourceLabel)
        Write-Host ('    repo_root           : {0}' -f $repoRootLabel)
        Write-Host ('    materialized root   : {0}' -f $materializedLabel)
        Write-Host ('    package_root        : {0}' -f $packageLabel)
    }
}
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
if (@($nativeArtifactSync.artifacts | Where-Object { [string]::IsNullOrWhiteSpace([string]$_.source_kind) }).Count -gt 0) {
    Write-Warning ('Native artifacts are missing from every supported source candidate: {0}' -f ((@($nativeArtifactSync.artifacts | Where-Object { [string]::IsNullOrWhiteSpace([string]$_.source_kind) } | ForEach-Object { $_.package_path })) -join ', '))
}
if ($boundaryResult -and (-not $boundaryResult.is_ok)) {
    Write-Warning ('Boundary check mismatches detected: {0}' -f (@($boundaryResult.failures) -join '; '))
}
if ($nativeArtifactStatus -and (@($nativeArtifactStatus.missing_in_package_root).Count -gt 0)) {
    Write-Warning ('Resolved native artifacts were not copied into the staged package root: {0}' -f (@($nativeArtifactStatus.missing_in_package_root) -join ', '))
}
