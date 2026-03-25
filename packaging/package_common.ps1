# BUILD_ID: 2026-03-24_standard_package_common_app_payload_impl_v1
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
            $entryType = [string]$entry.type
            $entryRequired = [bool]$entry.required
            $entryGroup = [string]$entry.group
            $sourcePath = Join-NormalizedPath -BasePath $RepoRoot -RelativePath $entryPath

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

            if ($entryType -eq 'directory') {
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
                if (Test-IsExcluded -RelativePath $entryPath -ExcludeEntries $excludeEntries) {
                    Add-UniqueString -List $excludedSamples -Set $seenExcludedSamples -Value $entryPath
                    continue
                }

                if ($seenCandidateFiles.Add($entryPath)) {
                    $null = $candidateFiles.Add([pscustomobject]@{
                        relative_path = $entryPath
                        source_path   = (Get-Item -LiteralPath $sourcePath).FullName
                        section       = [string]$sectionName
                        group         = $entryGroup
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
