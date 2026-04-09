# BUILD_ID: 2026-03-30_free_package_common_native_artifacts_v1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Normalize-RelativePath {
    param([string]$PathText)

    if ([string]::IsNullOrWhiteSpace($PathText)) {
        return ''
    }

    $normalized = [string]$PathText
    $normalized = $normalized -replace '\\', '/'
    $normalized = $normalized.Trim()
    while ($normalized.StartsWith('./')) {
        $normalized = $normalized.Substring(2)
    }
    while ($normalized.StartsWith('/')) {
        $normalized = $normalized.Substring(1)
    }
    return $normalized
}

function Join-NormalizedPath {
    param(
        [string]$BasePath,
        [string]$RelativePath
    )

    $normalized = Normalize-RelativePath $RelativePath
    if ([string]::IsNullOrWhiteSpace($normalized)) {
        return [System.IO.Path]::GetFullPath($BasePath)
    }

    $parts = $normalized -split '/'
    $path = $BasePath
    foreach ($part in $parts) {
        if ([string]::IsNullOrWhiteSpace($part)) {
            continue
        }
        $path = Join-Path $path $part
    }
    return [System.IO.Path]::GetFullPath($path)
}

function Get-RepoRelativePath {
    param(
        [string]$FullPath,
        [string]$BasePath
    )

    $full = [System.IO.Path]::GetFullPath($FullPath)
    $base = [System.IO.Path]::GetFullPath($BasePath)

    if (-not $base.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $base = $base + [System.IO.Path]::DirectorySeparatorChar
    }

    $baseUri = [System.Uri]::new($base)
    $fullUri = [System.Uri]::new($full)
    $relative = $baseUri.MakeRelativeUri($fullUri).ToString()
    $relative = [System.Uri]::UnescapeDataString($relative)
    return Normalize-RelativePath $relative
}

function Add-UniqueString {
    param(
        [object]$List,
        [object]$Set,
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return
    }

    if ($Set.Add($Value)) {
        $null = $List.Add($Value)
    }
}

function Get-ExcludePattern {
    param([object]$Entry)

    if ($null -eq $Entry) {
        return ''
    }
    if ($Entry -is [string]) {
        return Normalize-RelativePath ([string]$Entry)
    }
    if ($null -ne $Entry.PSObject.Properties['pattern']) {
        return Normalize-RelativePath ([string]$Entry.pattern)
    }
    return ''
}

function Test-IsExcluded {
    param(
        [string]$RelativePath,
        [object[]]$ExcludeEntries
    )

    $normalized = Normalize-RelativePath $RelativePath
    foreach ($entry in @($ExcludeEntries)) {
        $pattern = Get-ExcludePattern -Entry $entry
        if ([string]::IsNullOrWhiteSpace($pattern)) {
            continue
        }

        $candidate = $normalized
        if ($candidate -like $pattern) {
            return $true
        }

        if ($pattern.EndsWith('/**')) {
            $prefix = $pattern.Substring(0, $pattern.Length - 3)
            if ($candidate -eq $prefix -or $candidate.StartsWith("$prefix/")) {
                return $true
            }
        }
        elseif ($pattern.EndsWith('/*')) {
            $prefix = $pattern.Substring(0, $pattern.Length - 2)
            if ($candidate.StartsWith("$prefix/")) {
                return $true
            }
        }
    }
    return $false
}

function Get-ManifestEntryPackagePath {
    param([object]$Entry)

    $packagePath = ''
    if ($null -ne $Entry.PSObject.Properties['package_path']) {
        $packagePath = Normalize-RelativePath ([string]$Entry.package_path)
    }
    if ([string]::IsNullOrWhiteSpace($packagePath)) {
        $packagePath = Normalize-RelativePath ([string]$Entry.path)
    }
    return $packagePath
}

function Get-ManifestEntrySourceCandidates {
    param([object]$Entry)

    $candidates = New-Object 'System.Collections.Generic.List[string]'
    $seenCandidates = New-Object 'System.Collections.Generic.HashSet[string]'

    if ($null -ne $Entry.PSObject.Properties['source_candidates']) {
        foreach ($candidate in @($Entry.source_candidates)) {
            Add-UniqueString -List $candidates -Set $seenCandidates -Value (Normalize-RelativePath ([string]$candidate))
        }
    }

    if ($candidates.Count -eq 0) {
        Add-UniqueString -List $candidates -Set $seenCandidates -Value (Normalize-RelativePath ([string]$Entry.path))
    }

    return @($candidates)
}

function Resolve-ManifestEntrySource {
    param(
        [string]$RepoRoot,
        [string[]]$SourceCandidates
    )

    foreach ($candidate in @($SourceCandidates)) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }

        $sourcePath = Join-NormalizedPath -BasePath $RepoRoot -RelativePath $candidate
        if (Test-Path -LiteralPath $sourcePath) {
            return [pscustomobject]@{
                source_relative_path = $candidate
                source_path          = (Get-Item -LiteralPath $sourcePath).FullName
            }
        }
    }

    return $null
}

function Get-NativeArtifactSourceKind {
    param(
        [string]$TargetRelativePath,
        [string]$SourceRelativePath
    )

    $target = Normalize-RelativePath $TargetRelativePath
    $source = Normalize-RelativePath $SourceRelativePath

    if ([string]::IsNullOrWhiteSpace($source)) {
        return ''
    }

    if ([string]::Equals($target, $source, [System.StringComparison]::OrdinalIgnoreCase)) {
        return 'repo_root'
    }

    if (($source -like '*/publish/*') -or ($source -like 'launcher_native/*') -or ($source -like 'setup_bootstrap/*')) {
        return 'publish_output'
    }

    return 'source_candidate'
}

function Resolve-ManifestEntryRefreshSource {
    param(
        [string]$RepoRoot,
        [string]$TargetRelativePath,
        [string[]]$SourceCandidates
    )

    $target = Normalize-RelativePath $TargetRelativePath
    $refreshCandidates = New-Object 'System.Collections.Generic.List[string]'
    $seenRefreshCandidates = New-Object 'System.Collections.Generic.HashSet[string]'

    foreach ($candidate in @($SourceCandidates)) {
        $normalizedCandidate = Normalize-RelativePath ([string]$candidate)
        if ([string]::IsNullOrWhiteSpace($normalizedCandidate)) {
            continue
        }
        if ([string]::Equals($normalizedCandidate, $target, [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }

        Add-UniqueString -List $refreshCandidates -Set $seenRefreshCandidates -Value $normalizedCandidate
    }

    if ($refreshCandidates.Count -eq 0) {
        return $null
    }

    return Resolve-ManifestEntrySource -RepoRoot $RepoRoot -SourceCandidates @($refreshCandidates)
}

function Test-NativeArtifactRefreshNeeded {
    param(
        [string]$TargetPath,
        [string]$CandidatePath
    )

    if ((-not (Test-Path -LiteralPath $TargetPath)) -or (-not (Test-Path -LiteralPath $CandidatePath))) {
        return $false
    }

    $targetItem = Get-Item -LiteralPath $TargetPath
    $candidateItem = Get-Item -LiteralPath $CandidatePath

    if ($candidateItem.LastWriteTimeUtc -gt $targetItem.LastWriteTimeUtc) {
        return $true
    }

    if ($candidateItem.LastWriteTimeUtc -lt $targetItem.LastWriteTimeUtc) {
        return $false
    }

    if ($candidateItem.Length -ne $targetItem.Length) {
        return $true
    }

    $targetHash = (Get-FileHash -LiteralPath $targetItem.FullName -Algorithm SHA256).Hash
    $candidateHash = (Get-FileHash -LiteralPath $candidateItem.FullName -Algorithm SHA256).Hash
    return (-not [string]::Equals($targetHash, $candidateHash, [System.StringComparison]::OrdinalIgnoreCase))
}

function Sync-PackageNativeArtifacts {
    param(
        [string]$RepoRoot,
        [object]$Manifest,
        [bool]$MaterializeToRepoRoot = $true
    )

    $artifactResults = New-Object 'System.Collections.Generic.List[object]'

    foreach ($entry in @($Manifest.include_native_artifacts)) {
        $packagePath = Get-ManifestEntryPackagePath -Entry $entry
        $targetRelativePath = $packagePath
        $targetPath = Join-NormalizedPath -BasePath $RepoRoot -RelativePath $targetRelativePath
        $sourceCandidates = @(Get-ManifestEntrySourceCandidates -Entry $entry)
        $resolvedSource = Resolve-ManifestEntrySource -RepoRoot $RepoRoot -SourceCandidates $sourceCandidates
        $refreshSource = Resolve-ManifestEntryRefreshSource -RepoRoot $RepoRoot -TargetRelativePath $targetRelativePath -SourceCandidates $sourceCandidates

        $sourceRelativePath = ''
        $sourcePath = ''
        $sourceKind = ''
        $status = 'missing'
        $wasMaterializedToRepoRoot = $false
        $wouldMaterializeToRepoRoot = $false

        $existsAtTarget = Test-Path -LiteralPath $targetPath
        if ($existsAtTarget -and $refreshSource -and (Test-NativeArtifactRefreshNeeded -TargetPath $targetPath -CandidatePath ([string]$refreshSource.source_path))) {
            $sourceRelativePath = [string]$refreshSource.source_relative_path
            $sourcePath = [string]$refreshSource.source_path
            $sourceKind = Get-NativeArtifactSourceKind -TargetRelativePath $targetRelativePath -SourceRelativePath $sourceRelativePath
            $wouldMaterializeToRepoRoot = ($sourceKind -ne 'repo_root')
            $status = $sourceKind

            if ($MaterializeToRepoRoot -and $wouldMaterializeToRepoRoot) {
                $parentDir = Split-Path -Parent $targetPath
                if ($parentDir) {
                    $null = New-Item -ItemType Directory -Path $parentDir -Force
                }
                Copy-Item -LiteralPath $sourcePath -Destination $targetPath -Force
                $wasMaterializedToRepoRoot = $true
                $wouldMaterializeToRepoRoot = $false
                $existsAtTarget = $true
                $status = 'materialized_to_repo_root'
            }
        }
        elseif ($existsAtTarget) {
            $sourceRelativePath = $targetRelativePath
            $sourcePath = (Resolve-Path -LiteralPath $targetPath).Path
            $sourceKind = 'repo_root'
            $status = 'repo_root'
        }
        elseif ($resolvedSource) {
            $sourceRelativePath = [string]$resolvedSource.source_relative_path
            $sourcePath = [string]$resolvedSource.source_path
            $sourceKind = Get-NativeArtifactSourceKind -TargetRelativePath $targetRelativePath -SourceRelativePath $sourceRelativePath
            $wouldMaterializeToRepoRoot = ($sourceKind -ne 'repo_root')
            $status = $sourceKind

            if ($MaterializeToRepoRoot -and $wouldMaterializeToRepoRoot) {
                $parentDir = Split-Path -Parent $targetPath
                if ($parentDir) {
                    $null = New-Item -ItemType Directory -Path $parentDir -Force
                }
                Copy-Item -LiteralPath $sourcePath -Destination $targetPath -Force
                $wasMaterializedToRepoRoot = $true
                $wouldMaterializeToRepoRoot = $false
                $existsAtTarget = $true
                $status = 'materialized_to_repo_root'
            }
        }

        $targetResolvedPath = if ($existsAtTarget) { (Resolve-Path -LiteralPath $targetPath).Path } else { $targetPath }

        $null = $artifactResults.Add([pscustomobject]@{
            package_path                   = $packagePath
            target_relative_path           = $targetRelativePath
            target_path                    = $targetResolvedPath
            source_candidates              = @($sourceCandidates)
            source_relative_path           = $sourceRelativePath
            source_path                    = $sourcePath
            source_kind                    = $sourceKind
            status                         = $status
            was_materialized_to_repo_root  = $wasMaterializedToRepoRoot
            would_materialize_to_repo_root = $wouldMaterializeToRepoRoot
            exists_at_target               = $existsAtTarget
            required                       = [bool]$entry.required
            group                          = [string]$entry.group
            role                           = $(if ($null -ne $entry.PSObject.Properties['role']) { [string]$entry.role } else { '' })
            signing_scope                  = $(if ($null -ne $entry.PSObject.Properties['signing_scope']) { [string]$entry.signing_scope } else { '' })
        })
    }

    $artifactArray = @($artifactResults | Sort-Object package_path)
    $resolvedArtifacts = @($artifactArray | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_.source_kind) })
    $materializedArtifacts = @($artifactArray | Where-Object { $_.was_materialized_to_repo_root })
    $missingArtifacts = @($artifactArray | Where-Object { [string]::IsNullOrWhiteSpace([string]$_.source_kind) })

    return [pscustomobject]@{
        artifacts          = $artifactArray
        total_count        = @($artifactArray).Count
        resolved_count     = @($resolvedArtifacts).Count
        materialized_count = @($materializedArtifacts).Count
        missing_count      = @($missingArtifacts).Count
    }
}

function Get-PackageNativeArtifactEntries {
    param(
        [string]$RepoRoot,
        [object]$Manifest
    )

    $nativeArtifacts = New-Object 'System.Collections.Generic.List[object]'

    foreach ($entry in @($Manifest.include_native_artifacts)) {
        $packagePath = Get-ManifestEntryPackagePath -Entry $entry
        $sourceCandidates = @(Get-ManifestEntrySourceCandidates -Entry $entry)
        $resolvedSource = Resolve-ManifestEntrySource -RepoRoot $RepoRoot -SourceCandidates $sourceCandidates

        $null = $nativeArtifacts.Add([pscustomobject]@{
            package_path         = $packagePath
            source_candidates    = @($sourceCandidates)
            source_relative_path = $(if ($resolvedSource) { [string]$resolvedSource.source_relative_path } else { '' })
            source_path          = $(if ($resolvedSource) { [string]$resolvedSource.source_path } else { '' })
            required             = [bool]$entry.required
            group                = [string]$entry.group
            role                 = $(if ($null -ne $entry.PSObject.Properties['role']) { [string]$entry.role } else { '' })
            signing_scope        = $(if ($null -ne $entry.PSObject.Properties['signing_scope']) { [string]$entry.signing_scope } else { '' })
            is_resolved          = ($null -ne $resolvedSource)
        })
    }

    return @($nativeArtifacts | Sort-Object package_path)
}

function Get-PackagePlan {
    param(
        [string]$RepoRoot,
        [string]$ManifestPath
    )

    if (-not (Test-Path -LiteralPath $ManifestPath)) {
        throw "Manifest not found: $ManifestPath"
    }

    $manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    $excludeEntries = @($manifest.exclude)

    $sectionEntries = [ordered]@{}
    foreach ($property in $manifest.PSObject.Properties) {
        if ($property.Name -like 'include_*') {
            $sectionEntries[$property.Name] = @($property.Value)
        }
    }

    $candidateFiles = New-Object 'System.Collections.Generic.List[object]'
    $seenCandidateFiles = New-Object 'System.Collections.Generic.HashSet[string]'
    $excludedSamples = New-Object 'System.Collections.Generic.List[string]'
    $seenExcludedSamples = New-Object 'System.Collections.Generic.HashSet[string]'
    $missingRequired = New-Object 'System.Collections.Generic.List[string]'
    $seenMissingRequired = New-Object 'System.Collections.Generic.HashSet[string]'
    $missingOptional = New-Object 'System.Collections.Generic.List[string]'
    $seenMissingOptional = New-Object 'System.Collections.Generic.HashSet[string]'
    $skeletonDirs = New-Object 'System.Collections.Generic.List[string]'
    $seenSkeletonDirs = New-Object 'System.Collections.Generic.HashSet[string]'
    $reincludeExact = New-Object 'System.Collections.Generic.List[string]'
    $seenReincludeExact = New-Object 'System.Collections.Generic.HashSet[string]'

    foreach ($entry in @($manifest.runtime_skeleton)) {
        $pathText = if ($entry -is [string]) { [string]$entry } else { [string]$entry.path }
        Add-UniqueString -List $skeletonDirs -Set $seenSkeletonDirs -Value (Normalize-RelativePath $pathText)
    }

    foreach ($entry in @($manifest.reinclude_after_exclude)) {
        Add-UniqueString -List $reincludeExact -Set $seenReincludeExact -Value (Normalize-RelativePath ([string]$entry))
    }

    foreach ($sectionName in $sectionEntries.Keys) {
        foreach ($entry in @($sectionEntries[$sectionName])) {
            $entryPath = Normalize-RelativePath ([string]$entry.path)
            $entryPackagePath = Get-ManifestEntryPackagePath -Entry $entry
            $entrySourceCandidates = @(Get-ManifestEntrySourceCandidates -Entry $entry)
            $entryType = [string]$entry.type
            $entryRequired = [bool]$entry.required
            $entryGroup = [string]$entry.group

            $sourcePath = Join-NormalizedPath -BasePath $RepoRoot -RelativePath $entryPath

            if ($entryType -eq 'directory') {
                if (-not (Test-Path -LiteralPath $sourcePath)) {
                    $label = "${sectionName}:$entryPath"
                    if ($entryRequired) {
                        Add-UniqueString -List $missingRequired -Set $seenMissingRequired -Value $label
                    }
                    else {
                        Add-UniqueString -List $missingOptional -Set $seenMissingOptional -Value $label
                    }
                    continue
                }

                foreach ($file in Get-ChildItem -LiteralPath $sourcePath -Recurse -File -Force) {
                    $relativePath = Get-RepoRelativePath -FullPath $file.FullName -BasePath $RepoRoot
                    if (Test-IsExcluded -RelativePath $relativePath -ExcludeEntries $excludeEntries) {
                        Add-UniqueString -List $excludedSamples -Set $seenExcludedSamples -Value $relativePath
                        continue
                    }

                    if ($seenCandidateFiles.Add($relativePath)) {
                        $null = $candidateFiles.Add([pscustomobject]@{
                            relative_path = $relativePath
                            source_path   = $file.FullName
                            section       = [string]$sectionName
                            group         = $entryGroup
                        })
                    }
                }
                continue
            }

            if (($entryType -eq 'file') -or ($entryType -eq 'file_if_present')) {
                $resolvedSource = Resolve-ManifestEntrySource -RepoRoot $RepoRoot -SourceCandidates $entrySourceCandidates
                if ($null -eq $resolvedSource) {
                    $label = if ($entrySourceCandidates.Count -gt 1) {
                        '{0}:{1} (sources: {2})' -f $sectionName, $entryPackagePath, ($entrySourceCandidates -join ' | ')
                    }
                    else {
                        "${sectionName}:$entryPackagePath"
                    }

                    if ($entryRequired) {
                        Add-UniqueString -List $missingRequired -Set $seenMissingRequired -Value $label
                    }
                    else {
                        Add-UniqueString -List $missingOptional -Set $seenMissingOptional -Value $label
                    }
                    continue
                }

                if (Test-IsExcluded -RelativePath $entryPackagePath -ExcludeEntries $excludeEntries) {
                    Add-UniqueString -List $excludedSamples -Set $seenExcludedSamples -Value $entryPackagePath
                    continue
                }

                if ($seenCandidateFiles.Add($entryPackagePath)) {
                    $null = $candidateFiles.Add([pscustomobject]@{
                        relative_path        = $entryPackagePath
                        source_path          = [string]$resolvedSource.source_path
                        source_relative_path = [string]$resolvedSource.source_relative_path
                        section              = [string]$sectionName
                        group                = $entryGroup
                    })
                }
                continue
            }

            throw "Unsupported manifest entry type '$entryType' for path '$entryPath'."
        }
    }

    $sortedCandidateFiles = @($candidateFiles | Sort-Object relative_path)

    $packageRootName = [string]$manifest.package_root_name
    if ([string]::IsNullOrWhiteSpace($packageRootName) -and ([string]$manifest.package_kind -ne 'app_payload')) {
        $packageRootName = [string]$manifest.product
    }
    if ([string]::IsNullOrWhiteSpace($packageRootName) -and ([string]$manifest.package_kind -ne 'app_payload')) {
        $packageRootName = 'package'
    }

    return [pscustomobject]@{
        build_id                = [string]$manifest.build_id
        manifest                = $manifest
        manifest_path           = (Resolve-Path -LiteralPath $ManifestPath).Path
        repo_root               = (Resolve-Path -LiteralPath $RepoRoot).Path
        package_root_name       = $packageRootName
        candidate_files         = $sortedCandidateFiles
        missing_required        = @($missingRequired)
        missing_optional        = @($missingOptional)
        excluded_samples        = @($excludedSamples)
        skeleton_dirs           = @($skeletonDirs)
        reinclude_after_exclude = @($reincludeExact)
        core_count              = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_core' }).Count
        advanced_count          = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_advanced_tools' }).Count
        docs_count              = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_docs' }).Count
        config_count            = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_configs' }).Count
        launcher_count          = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_launchers' }).Count
        native_artifact_count   = @($sortedCandidateFiles | Where-Object { $_.section -eq 'include_native_artifacts' }).Count
        native_artifacts        = @(Get-PackageNativeArtifactEntries -RepoRoot $RepoRoot -Manifest $manifest)
        total_count             = @($sortedCandidateFiles).Count
    }
}

function New-PackageStaging {
    param(
        [object]$Plan,
        [string]$StagingRoot,
        [string]$StageName
    )

    $stagePath = Join-Path $StagingRoot $StageName
    if (Test-Path -LiteralPath $stagePath) {
        Remove-Item -LiteralPath $stagePath -Recurse -Force
    }
    $null = New-Item -ItemType Directory -Path $stagePath -Force

    $packageRootPath = Join-Path $stagePath $Plan.package_root_name
    $null = New-Item -ItemType Directory -Path $packageRootPath -Force

    foreach ($relativeDir in @($Plan.skeleton_dirs)) {
        $destDir = Join-NormalizedPath -BasePath $packageRootPath -RelativePath $relativeDir
        $null = New-Item -ItemType Directory -Path $destDir -Force
    }

    foreach ($candidate in @($Plan.candidate_files)) {
        $destPath = Join-NormalizedPath -BasePath $packageRootPath -RelativePath $candidate.relative_path
        $parentDir = Split-Path -Parent $destPath
        if ($parentDir) {
            $null = New-Item -ItemType Directory -Path $parentDir -Force
        }
        Copy-Item -LiteralPath $candidate.source_path -Destination $destPath -Force
    }

    foreach ($relativePath in @($Plan.reinclude_after_exclude)) {
        $sourcePath = Join-NormalizedPath -BasePath $Plan.repo_root -RelativePath $relativePath
        if (-not (Test-Path -LiteralPath $sourcePath)) {
            continue
        }

        $destPath = Join-NormalizedPath -BasePath $packageRootPath -RelativePath $relativePath
        $parentDir = Split-Path -Parent $destPath
        if ($parentDir) {
            $null = New-Item -ItemType Directory -Path $parentDir -Force
        }
        Copy-Item -LiteralPath $sourcePath -Destination $destPath -Force
    }

    return [pscustomobject]@{
        stage_path        = (Resolve-Path -LiteralPath $stagePath).Path
        package_root_path = (Resolve-Path -LiteralPath $packageRootPath).Path
    }
}

function Get-PackageBoundaryChecks {
    param([string]$PackageRootPath)

    $checks = [ordered]@{}
    $checks['package-root-exists'] = Test-Path -LiteralPath $PackageRootPath
    $checks['readme-present'] = Test-Path -LiteralPath (Join-NormalizedPath -BasePath $PackageRootPath -RelativePath 'README.md')

    return [pscustomobject]@{
        package_root_path = $PackageRootPath
        checks = $checks
    }
}

function Test-PackageBoundary {
    param([hashtable]$Checks)

    $failures = New-Object 'System.Collections.Generic.List[string]'
    foreach ($item in $Checks.GetEnumerator()) {
        if (-not [bool]$item.Value) {
            $null = $failures.Add([string]$item.Key)
        }
    }

    return [pscustomobject]@{
        failures = @($failures)
        is_ok    = ($failures.Count -eq 0)
    }
}

function New-PackageZip {
    param(
        [string]$PackageRootPath,
        [string]$ZipPath
    )

    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    $packageRootResolved = (Resolve-Path -LiteralPath $PackageRootPath).Path
    $packageParent = Split-Path -Parent $packageRootResolved
    $zipParent = Split-Path -Parent $ZipPath

    if ($zipParent) {
        $null = New-Item -ItemType Directory -Path $zipParent -Force
    }
    if (Test-Path -LiteralPath $ZipPath) {
        Remove-Item -LiteralPath $ZipPath -Force
    }

    $zipStream = [System.IO.File]::Open($ZipPath, [System.IO.FileMode]::CreateNew)
    try {
        $archive = New-Object System.IO.Compression.ZipArchive($zipStream, [System.IO.Compression.ZipArchiveMode]::Create, $false)
        try {
            $files = Get-ChildItem -LiteralPath $packageRootResolved -Recurse -File -Force | Sort-Object FullName
            foreach ($file in $files) {
                $entryName = Get-RepoRelativePath -FullPath $file.FullName -BasePath $packageParent
                $null = [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $archive,
                    $file.FullName,
                    $entryName,
                    [System.IO.Compression.CompressionLevel]::Optimal
                )
            }
        }
        finally {
            $archive.Dispose()
        }
    }
    finally {
        $zipStream.Dispose()
    }

    return (Resolve-Path -LiteralPath $ZipPath).Path
}

function Get-ZipEntryNames {
    param([string]$ZipPath)

    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    $resolvedZipPath = (Resolve-Path -LiteralPath $ZipPath).Path
    $archive = [System.IO.Compression.ZipFile]::OpenRead($resolvedZipPath)
    try {
        $entryNames = New-Object 'System.Collections.Generic.List[string]'
        foreach ($entry in $archive.Entries) {
            $entryName = Normalize-RelativePath ($entry.FullName.TrimEnd('/'))
            if (-not [string]::IsNullOrWhiteSpace($entryName)) {
                $null = $entryNames.Add($entryName)
            }
        }

        return @($entryNames | Sort-Object -Unique)
    }
    finally {
        $archive.Dispose()
    }
}

function Get-NativeArtifactValidation {
    param(
        [object[]]$NativeArtifacts,
        [string]$PackageRootPath,
        [string]$PackageRootName,
        [string]$ZipPath = ""
    )

    $zipEntrySet = New-Object 'System.Collections.Generic.HashSet[string]'
    if (-not [string]::IsNullOrWhiteSpace($ZipPath) -and (Test-Path -LiteralPath $ZipPath)) {
        foreach ($entryName in @(Get-ZipEntryNames -ZipPath $ZipPath)) {
            $null = $zipEntrySet.Add($entryName)
        }
    }

    $artifactRows = New-Object 'System.Collections.Generic.List[object]'
    foreach ($artifact in @($NativeArtifacts)) {
        $packagePath = Normalize-RelativePath ([string]$artifact.package_path)
        $packageRootExists = $false
        if (-not [string]::IsNullOrWhiteSpace($PackageRootPath)) {
            $packageRootExists = Test-Path -LiteralPath (Join-NormalizedPath -BasePath $PackageRootPath -RelativePath $packagePath)
        }

        $zipExists = $false
        $zipChecked = ($zipEntrySet.Count -gt 0)
        if ($zipChecked) {
            $zipRelativePath = Normalize-RelativePath ("$PackageRootName/$packagePath")
            $zipExists = $zipEntrySet.Contains($zipRelativePath)
        }

        $null = $artifactRows.Add([pscustomobject]@{
            package_path           = $packagePath
            source_candidates      = @($artifact.source_candidates)
            source_relative_path   = [string]$artifact.source_relative_path
            required               = [bool]$artifact.required
            group                  = [string]$artifact.group
            role                   = [string]$artifact.role
            signing_scope          = [string]$artifact.signing_scope
            is_resolved_source     = [bool]$artifact.is_resolved
            exists_in_package_root = $packageRootExists
            zip_checked            = $zipChecked
            exists_in_zip          = $zipExists
        })
    }

    $resolvedArtifacts = @($artifactRows | Where-Object { $_.is_resolved_source })
    $missingSource = @($artifactRows | Where-Object { -not $_.is_resolved_source } | ForEach-Object { $_.package_path })
    $missingInPackageRoot = @($resolvedArtifacts | Where-Object { -not $_.exists_in_package_root } | ForEach-Object { $_.package_path })
    $missingInZip = @()
    if ($zipEntrySet.Count -gt 0) {
        $missingInZip = @($resolvedArtifacts | Where-Object { -not $_.exists_in_zip } | ForEach-Object { $_.package_path })
    }

    $artifactArray = @($artifactRows | Sort-Object package_path)
    $totalCount = @($artifactArray).Count
    $resolvedCount = @($resolvedArtifacts).Count

    return [pscustomobject]@{
        artifacts               = $artifactArray
        total_count             = $totalCount
        resolved_count          = $resolvedCount
        missing_source          = @($missingSource)
        missing_in_package_root = @($missingInPackageRoot)
        missing_in_zip          = @($missingInZip)
    }
}

function New-FlatPayloadStaging {
    param(
        [object]$Plan,
        [string]$StagingRoot,
        [string]$StageName
    )

    $stagePath = Join-Path $StagingRoot $StageName
    if (Test-Path -LiteralPath $stagePath) {
        Remove-Item -LiteralPath $stagePath -Recurse -Force
    }
    $null = New-Item -ItemType Directory -Path $stagePath -Force
    $payloadRootPath = $stagePath

    foreach ($relativeDir in @($Plan.skeleton_dirs)) {
        $destDir = Join-NormalizedPath -BasePath $payloadRootPath -RelativePath $relativeDir
        $null = New-Item -ItemType Directory -Path $destDir -Force
    }

    foreach ($candidate in @($Plan.candidate_files)) {
        $destPath = Join-NormalizedPath -BasePath $payloadRootPath -RelativePath $candidate.relative_path
        $parentDir = Split-Path -Parent $destPath
        if ($parentDir) {
            $null = New-Item -ItemType Directory -Path $parentDir -Force
        }
        Copy-Item -LiteralPath $candidate.source_path -Destination $destPath -Force
    }

    foreach ($relativePath in @($Plan.reinclude_after_exclude)) {
        $sourcePath = Join-NormalizedPath -BasePath $Plan.repo_root -RelativePath $relativePath
        if (-not (Test-Path -LiteralPath $sourcePath)) {
            continue
        }

        $destPath = Join-NormalizedPath -BasePath $payloadRootPath -RelativePath $relativePath
        $parentDir = Split-Path -Parent $destPath
        if ($parentDir) {
            $null = New-Item -ItemType Directory -Path $parentDir -Force
        }
        Copy-Item -LiteralPath $sourcePath -Destination $destPath -Force
    }

    return [pscustomobject]@{
        stage_path        = (Resolve-Path -LiteralPath $stagePath).Path
        payload_root_path = (Resolve-Path -LiteralPath $payloadRootPath).Path
    }
}

function Get-AppPayloadBoundaryChecks {
    param(
        [string]$PayloadRootPath,
        [string[]]$MustExist,
        [string[]]$MustAbsent
    )

    $checks = [ordered]@{}

    foreach ($relativePath in @($MustExist)) {
        $normalized = Normalize-RelativePath $relativePath
        $targetPath = Join-NormalizedPath -BasePath $PayloadRootPath -RelativePath $normalized
        $checks["exists:$normalized"] = Test-Path -LiteralPath $targetPath
    }

    foreach ($relativePath in @($MustAbsent)) {
        $normalized = Normalize-RelativePath $relativePath
        $targetPath = Join-NormalizedPath -BasePath $PayloadRootPath -RelativePath $normalized
        $checks["absent:$normalized"] = -not (Test-Path -LiteralPath $targetPath)
    }

    $runtimeSkeletonRules = @(
        @{
            Dir = 'runtime/exports'
            AllowedChildDirs = @('runs')
        },
        @{
            Dir = 'runtime/logs'
            AllowedChildDirs = @()
        },
        @{
            Dir = 'runtime/state'
            AllowedChildDirs = @()
        }
    )

    foreach ($rule in $runtimeSkeletonRules) {
        $relativeDir = [string]$rule.Dir
        $allowedChildDirs = @($rule.AllowedChildDirs)

        $dirPath = Join-NormalizedPath -BasePath $PayloadRootPath -RelativePath $relativeDir
        if (-not (Test-Path -LiteralPath $dirPath)) {
            $checks["skeleton:$relativeDir"] = $false
            continue
        }

        $entries = @(Get-ChildItem -LiteralPath $dirPath -Force -ErrorAction SilentlyContinue)

        $invalidEntries = @(
            $entries | Where-Object {
                if ($_.Name -eq '.gitkeep') {
                    return $false
                }

                if ($_.PSIsContainer) {
                    return -not ($allowedChildDirs -contains $_.Name)
                }

                return $true
            }
        )

        $checks["skeleton:$relativeDir"] = ($invalidEntries.Count -eq 0)
    }

    $checks['flat-root:no wrapper directory'] =
        -not (Test-Path -LiteralPath (Join-NormalizedPath -BasePath $PayloadRootPath -RelativePath 'LoneWolf_Fang_standard'))

    return [pscustomobject]@{
        payload_root_path = $PayloadRootPath
        checks = $checks
    }
}

function Test-AppPayloadBoundary {
    param([hashtable]$Checks)

    $failures = New-Object 'System.Collections.Generic.List[string]'
    foreach ($item in $Checks.GetEnumerator()) {
        if (-not [bool]$item.Value) {
            $null = $failures.Add([string]$item.Key)
        }
    }

    return [pscustomobject]@{
        failures = @($failures)
        is_ok    = ($failures.Count -eq 0)
    }
}

function New-FlatPayloadZip {
    param(
        [string]$PayloadRootPath,
        [string]$ZipPath
    )

    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    $payloadRootResolved = (Resolve-Path -LiteralPath $PayloadRootPath).Path
    $zipParent = Split-Path -Parent $ZipPath
    if ($zipParent) {
        $null = New-Item -ItemType Directory -Path $zipParent -Force
    }
    if (Test-Path -LiteralPath $ZipPath) {
        Remove-Item -LiteralPath $ZipPath -Force
    }

    $zipStream = [System.IO.File]::Open($ZipPath, [System.IO.FileMode]::CreateNew)
    try {
        $archive = New-Object System.IO.Compression.ZipArchive($zipStream, [System.IO.Compression.ZipArchiveMode]::Create, $false)
        try {
            $files = Get-ChildItem -LiteralPath $payloadRootResolved -Recurse -File -Force | Sort-Object FullName
            foreach ($file in $files) {
                $entryName = Get-RepoRelativePath -FullPath $file.FullName -BasePath $payloadRootResolved
                $null = [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $archive,
                    $file.FullName,
                    $entryName,
                    [System.IO.Compression.CompressionLevel]::Optimal
                )
            }
        }
        finally {
            $archive.Dispose()
        }
    }
    finally {
        $zipStream.Dispose()
    }

    return (Resolve-Path -LiteralPath $ZipPath).Path
}
