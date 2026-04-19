# BUILD_ID: 2026-04-03_free_setup_bootstrap_gui_package_integration_v1
[CmdletBinding()]
param(
    [string]$ManifestPath = "",
    [string]$StagingRoot = "",
    [string]$DistRoot = "",
    [string]$ReleaseId = "",
    [ValidateSet('warn', 'error', 'ignore')]
    [string]$NativeArtifactsPolicy = "warn"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$BUILD_ID = '2026-04-20_free_package_build_official_launcher_summary_v1'
$ReleaseAssetName = 'LoneWolf_Fang_Free_Package.zip'
$OfficialLauncherFileName = 'LoneWolfFangFreeLauncher.exe'
$OfficialSetupFileName = 'LoneWolf_Fang_Free_Setup.exe'
$OfficialSetupPublishRelativePath = 'setup_bootstrap/bin/Release/net8.0-windows/publish/win-x64-single-file/LoneWolf_Fang_Free_Setup.exe'
$PublishCommandHint = 'dotnet publish .\\setup_bootstrap\\LoneWolfFangFreeSetup.csproj -c Release -p:PublishProfile=Properties\\PublishProfiles\\win-x64-single-file.pubxml'
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

$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
$nativeArtifactSync = Sync-PackageNativeArtifacts -RepoRoot $RepoRoot -Manifest $manifest -MaterializeToRepoRoot:$true

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
$releaseAssetPath = Join-Path $DistRoot $ReleaseAssetName
$nativeArtifactStatus = Get-NativeArtifactValidation `
    -NativeArtifacts $plan.native_artifacts `
    -PackageRootPath $stage.package_root_path `
    -PackageRootName $plan.package_root_name `
    -ZipPath $resolvedZipPath

if (@($nativeArtifactStatus.missing_in_package_root).Count -gt 0) {
    throw ('Cannot build free package because resolved native artifacts are missing from the staged package root: {0}' -f (@($nativeArtifactStatus.missing_in_package_root) -join ', '))
}
if (@($nativeArtifactStatus.missing_in_zip).Count -gt 0) {
    throw ('Cannot build free package because resolved native artifacts are missing from the final zip: {0}' -f (@($nativeArtifactStatus.missing_in_zip) -join ', '))
}

$missingNativeArtifactText = @($nativeArtifactStatus.missing_source) -join ', '
if (($NativeArtifactsPolicy -eq 'error') -and (@($nativeArtifactStatus.missing_source).Count -gt 0)) {
    throw ('Cannot build free package because native artifacts are missing from every supported source candidate: {0}' -f $missingNativeArtifactText)
}
if (($NativeArtifactsPolicy -eq 'warn') -and (@($nativeArtifactStatus.missing_source).Count -gt 0)) {
    Write-Warning ('Native artifacts were not resolved from any supported source candidate and were not bundled: {0}' -f $missingNativeArtifactText)
}

$officialSetupArtifact = @(
    $nativeArtifactStatus.artifacts |
        Where-Object { $_.package_path -eq $OfficialSetupFileName } |
        Select-Object -First 1
)
if (-not $officialSetupArtifact) {
    throw ('Cannot build free package because the official setup bootstrap entry was not found in native artifact validation. Expected {0}.' -f $OfficialSetupFileName)
}
if (-not $officialSetupArtifact.exists_in_package_root) {
    throw ('Cannot build free package because the official setup bootstrap is missing from the staged package root: {0}. Expected repo-root artifact `{1}` or publish output `{2}`. Publish with: {3}' -f `
        $OfficialSetupFileName, `
        $OfficialSetupFileName, `
        $OfficialSetupPublishRelativePath, `
        $PublishCommandHint)
}
if (-not $officialSetupArtifact.exists_in_zip) {
    throw ('Cannot build free package because the official setup bootstrap is missing from the final zip: {0}' -f $OfficialSetupFileName)
}

Copy-Item -LiteralPath $resolvedZipPath -Destination $releaseAssetPath -Force

Write-Host '== Free Package Build =='
Write-Host ('BUILD_ID              : {0}' -f $BUILD_ID)
Write-Host ('Manifest              : {0}' -f $plan.manifest_path)
Write-Host ('Repo Root             : {0}' -f $plan.repo_root)
Write-Host ('Official Launcher     : {0}' -f (Join-Path $RepoRoot $OfficialLauncherFileName))
Write-Host ('Official Setup        : {0}' -f (Join-Path $RepoRoot $OfficialSetupFileName))
Write-Host ('Staging Path          : {0}' -f $stage.stage_path)
Write-Host ('Package Root          : {0}' -f $stage.package_root_path)
Write-Host ('Zip Path              : {0}' -f $resolvedZipPath)
Write-Host ('Release Asset         : {0}' -f $releaseAssetPath)
Write-Host ('Native Policy         : {0}' -f $NativeArtifactsPolicy)
Write-Host ('Native Artifacts      : {0}/{1} resolved' -f $nativeArtifactStatus.resolved_count, $nativeArtifactStatus.total_count)
Write-Host ('Materialized Root     : {0}' -f $nativeArtifactSync.materialized_count)
foreach ($artifact in @($nativeArtifactStatus.artifacts)) {
    $syncArtifact = @($nativeArtifactSync.artifacts | Where-Object { $_.package_path -eq $artifact.package_path } | Select-Object -First 1)
    $sourceLabel = 'missing'
    $resolutionLabel = 'missing'
    $repoRootLabel = 'missing'
    $materializedLabel = 'no'
    if ($syncArtifact) {
        if (-not [string]::IsNullOrWhiteSpace([string]$syncArtifact.source_relative_path)) {
            $sourceLabel = [string]$syncArtifact.source_relative_path
        }

        switch ([string]$syncArtifact.source_kind) {
            'repo_root' { $resolutionLabel = 'resolved from repo root' }
            'publish_output' { $resolutionLabel = 'resolved from publish output' }
            'source_candidate' { $resolutionLabel = 'resolved from source candidate' }
            default { $resolutionLabel = 'missing' }
        }

        $repoRootLabel = if ($syncArtifact.exists_at_target) { 'present' } else { 'missing' }
        $materializedLabel = if ($syncArtifact.was_materialized_to_repo_root) { 'yes' } else { 'no' }
    }

    $packageLabel = if ($artifact.exists_in_package_root) { 'present' } else { 'missing' }
    $zipLabel = if ($artifact.exists_in_zip) { 'present' } else { 'missing' }
    Write-Host ('  {0}' -f $artifact.package_path)
    Write-Host ('    resolved            : {0}' -f $resolutionLabel)
    Write-Host ('    source candidate    : {0}' -f $sourceLabel)
    Write-Host ('    repo_root           : {0}' -f $repoRootLabel)
    Write-Host ('    materialized root   : {0}' -f $materializedLabel)
    Write-Host ('    package_root        : {0}' -f $packageLabel)
    Write-Host ('    zip                 : {0}' -f $zipLabel)
}
Write-Host ('Total Files           : {0}' -f $plan.total_count)
