using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Management;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace LoneWolfFangSetupBootstrapper;

internal static class BootstrapRuntime
{
    private const string CommonPackageUrlEnvName = "LWF_SETUP_PACKAGE_URL";

    public static string ResolveExecutablePath()
    {
        var processPath = Environment.ProcessPath;
        if (!string.IsNullOrWhiteSpace(processPath))
        {
            return Path.GetFullPath(processPath);
        }

        using var process = Process.GetCurrentProcess();
        var fallbackPath = process.MainModule?.FileName;
        if (!string.IsNullOrWhiteSpace(fallbackPath))
        {
            return Path.GetFullPath(fallbackPath);
        }

        throw BootstrapException.RootUnresolved(
            "Setup could not resolve its executable path.",
            "Unable to resolve the setup executable path.");
    }

    public static string ResolveBaseDirectory(string executablePath)
    {
        var exeDirectory = Path.GetDirectoryName(executablePath);
        if (!string.IsNullOrWhiteSpace(exeDirectory))
        {
            return Path.GetFullPath(exeDirectory);
        }

        return Directory.GetCurrentDirectory();
    }

    public static SetupPreparation PrepareInitial(string exePath, string baseDirectory)
    {
        var detected = DetectProduct(baseDirectory, exePath);
        var localPackageRoot = TryResolvePackageRoot(baseDirectory, detected);
        var config = TryLoadInstallerConfig(localPackageRoot, detected) ?? InstallerConfig.FromProduct(detected);
        var suggestedInstallRoot = ResolveInstallRoot(config, localPackageRoot ?? baseDirectory, null);
        var logFolderPath = ResolveLogFolderPath();

        return new SetupPreparation(
            detected,
            exePath,
            baseDirectory,
            localPackageRoot,
            config,
            suggestedInstallRoot,
            Path.Combine(logFolderPath, detected.SetupLogFileName),
            Path.Combine(logFolderPath, detected.BackendLogFileName),
            logFolderPath);
    }

    public static CultureInfo GetDefaultUiCulture()
    {
        var uiCulture = CultureInfo.CurrentUICulture;
        if (string.Equals(uiCulture.TwoLetterISOLanguageName, "ja", StringComparison.OrdinalIgnoreCase))
        {
            return new CultureInfo("ja");
        }

        return new CultureInfo("en");
    }

    public static async Task<InstallExecutionResult> RunInstallAsync(
        SetupPreparation preparation,
        InstallExecutionOptions options,
        FileLogger logger,
        ISetupObserver observer,
        CancellationToken cancellationToken)
    {
        observer.SetPhase(SetupPhase.ResolvingPackage);
        logger.Info($"package_resolution_start product={preparation.Product.Code}");

        var packageAcquisition = await ResolvePackageAcquisitionAsync(preparation, options.PackageUrl, logger, observer, cancellationToken).ConfigureAwait(false);
        var packageRoot = packageAcquisition.PackageRoot;
        var config = TryLoadInstallerConfig(packageRoot, preparation.Product) ?? preparation.InstallerConfig;
        var installRoot = ResolveInstallRoot(config, packageRoot, options.InstallRoot);
        var installerScriptPath = Path.Combine(packageRoot, preparation.Product.InstallerScriptRelativePath);
        if (!File.Exists(installerScriptPath))
        {
            throw BootstrapException.InstallerScriptMissing(
                $"Installer files were not found. Check {preparation.SetupLogPath} for details.",
                $"Missing installer script: {installerScriptPath}");
        }

        var powerShellPath = TryResolvePowerShellPath();
        if (string.IsNullOrWhiteSpace(powerShellPath))
        {
            throw BootstrapException.PowerShellMissing(
                "Windows PowerShell was not found. Please repair PowerShell and run Setup again.",
                "powershell.exe could not be resolved.");
        }

        observer.AppendDetail($"BUILD_ID={Program.BuildId}");
        observer.AppendDetail($"package_source={packageAcquisition.Source}");
        observer.AppendDetail($"app_root={packageRoot}");
        observer.AppendDetail($"installer_script_path={installerScriptPath}");
        observer.AppendDetail($"powershell_path={powerShellPath}");
        observer.AppendDetail($"install_root={installRoot}");
        observer.AppendDetail($"setup_log={preparation.SetupLogPath}");
        observer.AppendDetail($"backend_log={preparation.BackendLogPath}");

        logger.Info($"package_source={packageAcquisition.Source}");
        logger.Info($"package_url_source={packageAcquisition.PackageUrlSource}");
        logger.Info($"requested_download_url={packageAcquisition.RequestedDownloadUrl ?? "<none>"}");
        logger.Info($"resolved_download_url={packageAcquisition.ResolvedDownloadUrl ?? "<none>"}");
        logger.Info($"downloaded_asset_path={packageAcquisition.DownloadedAssetPath ?? "<none>"}");
        logger.Info($"extract_dir={packageAcquisition.ExtractDirectory ?? "<none>"}");
        logger.Info($"app_root={packageRoot}");
        logger.Info($"installer_script_path={installerScriptPath}");
        logger.Info($"powershell_path={powerShellPath}");
        logger.Info($"install_root={installRoot}");

        observer.SetPhase(SetupPhase.RunningInstaller);

        var bridgedArgs = BuildInstallerArguments(options, installRoot);
        logger.Info($"bridged_args={FormatArguments(bridgedArgs)}");

        var processResult = await RunInstallerProcessAsync(
            powerShellPath,
            installerScriptPath,
            packageRoot,
            bridgedArgs,
            observer,
            logger,
            cancellationToken).ConfigureAwait(false);

        logger.Info($"exit_code={processResult.ExitCode}");
        if (processResult.ExitCode != 0)
        {
            var failureSummary = ExtractFailureSummary(processResult.OutputLines);
            throw BootstrapException.InstallFailed(
                $"Installation did not complete. Check {preparation.LogFolderPath} for details.",
                processResult.ExitCode,
                failureSummary);
        }

        string? warningMessage = null;
        if (options.StartAfterInstall)
        {
            observer.SetPhase(SetupPhase.StartingApplication);
            var launchResult = TryStartInstalledApplication(preparation.Product, installRoot, logger);
            if (!launchResult.IsSuccess)
            {
                warningMessage = launchResult.ErrorMessage;
                observer.AppendDetail($"start_after_install_warning={launchResult.ErrorMessage}");
                logger.Error($"start_after_install_warning={SanitizeForLog(launchResult.ErrorMessage ?? string.Empty)}");
            }
        }

        observer.SetPhase(SetupPhase.Completed);
        return new InstallExecutionResult(
            true,
            0,
            installRoot,
            preparation.SetupLogPath,
            preparation.BackendLogPath,
            preparation.LogFolderPath,
            null,
            warningMessage);
    }

    public static int RunCommandMode(SetupPreparation preparation, BootstrapCliOptions options, FileLogger logger)
    {
        var observer = new ConsoleObserver();

        try
        {
            var packageAcquisition = ResolvePackageAcquisitionAsync(preparation, options.PackageUrl, logger, observer, CancellationToken.None)
                .GetAwaiter()
                .GetResult();

            var config = TryLoadInstallerConfig(packageAcquisition.PackageRoot, preparation.Product) ?? preparation.InstallerConfig;
            var installRoot = ResolveInstallRoot(config, packageAcquisition.PackageRoot, options.ExplicitInstallDir);
            var installerScriptPath = Path.Combine(packageAcquisition.PackageRoot, preparation.Product.InstallerScriptRelativePath);
            if (!File.Exists(installerScriptPath))
            {
                throw BootstrapException.InstallerScriptMissing(
                    $"Installer files were not found. Check {preparation.SetupLogPath} for details.",
                    $"Missing installer script: {installerScriptPath}");
            }

            var powerShellPath = TryResolvePowerShellPath();
            if (string.IsNullOrWhiteSpace(powerShellPath))
            {
                throw BootstrapException.PowerShellMissing(
                    "Windows PowerShell was not found. Please repair PowerShell and run Setup again.",
                    "powershell.exe could not be resolved.");
            }

            var dryRunOptions = new InstallExecutionOptions(
                installRoot,
                options.DesktopShortcutMode != DesktopShortcutMode.Skip,
                options.StartAfterInstall,
                options.DesktopShortcutMode,
                options.PackageUrl,
                options.Force);

            logger.Info($"command_mode check_only={options.CheckOnly} dry_run={options.DryRun}");
            Console.WriteLine($"BUILD_ID={Program.BuildId}");
            Console.WriteLine($"product_code={preparation.Product.Code}");
            Console.WriteLine($"package_source={packageAcquisition.Source}");
            Console.WriteLine($"app_root={packageAcquisition.PackageRoot}");
            Console.WriteLine($"installer_script_path={installerScriptPath}");
            Console.WriteLine($"powershell_path={powerShellPath}");
            Console.WriteLine($"install_root={installRoot}");
            Console.WriteLine($"setup_log={preparation.SetupLogPath}");
            Console.WriteLine($"backend_log={preparation.BackendLogPath}");

            var pythonProcesses = QueryPythonProcessesAsync(installRoot, logger, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
            Console.WriteLine($"python_process_query_count={pythonProcesses.Count}");
            foreach (var process in pythonProcesses)
            {
                Console.WriteLine(
                    $"python_process_detected pid={process.ProcessId} " +
                    $"ppid={process.ParentProcessId} " +
                    $"name={process.Name} " +
                    $"path={FormatFieldForDisplay(process.ExecutablePath)} " +
                    $"cmd={FormatFieldForDisplay(process.CommandLine)}");
            }

            if (options.CheckOnly)
            {
                Console.WriteLine("check_only=ok");
                return 0;
            }

            if (options.DryRun)
            {
                if (pythonProcesses.Count > 0)
                {
                    var dryRunStopResult = EnsurePythonProcessesStoppedAsync(
                            installRoot,
                            pythonProcesses,
                            logger,
                            CancellationToken.None,
                            dryRun: true)
                        .GetAwaiter()
                        .GetResult();

                    foreach (var attempt in dryRunStopResult.Attempts)
                    {
                        Console.WriteLine(
                            $"would_stop_python_process pid={attempt.ProcessId} " +
                            $"path={FormatFieldForDisplay(attempt.ExecutablePath)} " +
                            $"cmd={FormatFieldForDisplay(attempt.CommandLine)}");
                    }

                    Console.WriteLine($"python_process_remaining_count={dryRunStopResult.RemainingProcesses.Count}");
                }

                Console.WriteLine($"bridged_args={FormatArguments(BuildInstallerArguments(dryRunOptions, installRoot))}");
                return 0;
            }

            return 0;
        }
        catch (BootstrapException ex)
        {
            logger.Error($"failure_category={ex.Category}");
            logger.Error($"failure_details={SanitizeForLog(ex.LogDetails)}");
            logger.Error($"exit_code={ex.ExitCode}");
            Console.WriteLine($"failure_category={ex.Category}");
            Console.WriteLine($"failure_details={ex.LogDetails}");
            return ex.ExitCode;
        }
    }

    public static bool DirectoryHasExistingFiles(string targetRoot, string? packageRoot)
    {
        if (string.IsNullOrWhiteSpace(targetRoot) || !Directory.Exists(targetRoot))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(packageRoot) &&
            string.Equals(
                Path.GetFullPath(targetRoot),
                Path.GetFullPath(packageRoot),
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return Directory.EnumerateFileSystemEntries(targetRoot).Any();
    }

    public static async Task<IReadOnlyList<PythonProcessInfo>> QueryPythonProcessesAsync(
        string targetRoot,
        FileLogger logger,
        CancellationToken cancellationToken)
    {
        var normalizedTargetRoot = TryNormalizePath(targetRoot);
        logger.Info($"python_process_query_started target_root={FormatFieldForDisplay(normalizedTargetRoot)}");

        if (string.IsNullOrWhiteSpace(normalizedTargetRoot) || !Directory.Exists(normalizedTargetRoot))
        {
            logger.Info("python_process_query_count=0");
            return Array.Empty<PythonProcessInfo>();
        }

        return await Task.Run(
            () => QueryPythonProcessesCore(normalizedTargetRoot, logger, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }

    public static async Task<PythonProcessStopResult> StopPythonProcessesAsync(
        string targetRoot,
        IReadOnlyList<PythonProcessInfo> processes,
        FileLogger logger,
        CancellationToken cancellationToken,
        bool dryRun = false,
        int attemptNumber = 1)
    {
        var requestedProcesses = DistinctPythonProcesses(processes);
        logger.Info(
            $"python_process_stop_started attempt={attemptNumber} count={requestedProcesses.Count} dry_run={dryRun}");

        if (requestedProcesses.Count == 0)
        {
            logger.Info("python_process_remaining_count=0");
            return new PythonProcessStopResult(
                Array.Empty<PythonProcessInfo>(),
                Array.Empty<PythonProcessStopAttempt>(),
                Array.Empty<PythonProcessInfo>(),
                attemptNumber,
                dryRun);
        }

        if (dryRun)
        {
            var dryRunAttempts = requestedProcesses
                .Select(processInfo => new PythonProcessStopAttempt(
                    processInfo.ProcessId,
                    processInfo.Name,
                    processInfo.ExecutablePath,
                    processInfo.CommandLine,
                    false,
                    true,
                    "dry_run"))
                .ToArray();

            foreach (var attempt in dryRunAttempts)
            {
                logger.Info(
                    $"python_process_stop_would_stop pid={attempt.ProcessId} " +
                    $"path={FormatFieldForDisplay(attempt.ExecutablePath)}");
            }

            var dryRunRemaining = await TryQueryPythonProcessesOrFallbackAsync(
                targetRoot,
                requestedProcesses,
                logger,
                cancellationToken).ConfigureAwait(false);
            logger.Info($"python_process_remaining_count={dryRunRemaining.Count}");

            return new PythonProcessStopResult(
                requestedProcesses,
                dryRunAttempts,
                dryRunRemaining,
                attemptNumber,
                true);
        }

        var attempts = await Task.Run(
            () =>
            {
                var results = new List<PythonProcessStopAttempt>(requestedProcesses.Count);
                foreach (var processInfo in requestedProcesses)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    results.Add(StopPythonProcess(processInfo));
                }

                return results.ToArray();
            },
            cancellationToken).ConfigureAwait(false);

        foreach (var attempt in attempts)
        {
            if (attempt.Succeeded)
            {
                logger.Info($"python_process_stop_succeeded pid={attempt.ProcessId}");
            }
            else
            {
                logger.Error(
                    $"python_process_stop_failed pid={attempt.ProcessId} reason={SanitizeForLog(attempt.Reason)}");
            }
        }

        await Task.Delay(350, cancellationToken).ConfigureAwait(false);
        var remainingProcesses = await TryQueryPythonProcessesOrFallbackAsync(
            targetRoot,
            requestedProcesses,
            logger,
            cancellationToken).ConfigureAwait(false);
        logger.Info($"python_process_remaining_count={remainingProcesses.Count}");

        return new PythonProcessStopResult(
            requestedProcesses,
            attempts,
            remainingProcesses,
            attemptNumber,
            false);
    }

    public static async Task<PythonProcessStopResult> EnsurePythonProcessesStoppedAsync(
        string targetRoot,
        IReadOnlyList<PythonProcessInfo> processes,
        FileLogger logger,
        CancellationToken cancellationToken,
        bool dryRun = false,
        int maxAttempts = 3)
    {
        var requestedProcesses = DistinctPythonProcesses(processes);
        if (requestedProcesses.Count == 0)
        {
            return new PythonProcessStopResult(
                Array.Empty<PythonProcessInfo>(),
                Array.Empty<PythonProcessStopAttempt>(),
                Array.Empty<PythonProcessInfo>(),
                0,
                dryRun);
        }

        if (dryRun)
        {
            return await StopPythonProcessesAsync(
                targetRoot,
                requestedProcesses,
                logger,
                cancellationToken,
                dryRun: true,
                attemptNumber: 1).ConfigureAwait(false);
        }

        var allAttempts = new List<PythonProcessStopAttempt>();
        var remainingProcesses = requestedProcesses;
        var totalAttempts = 0;

        while (remainingProcesses.Count > 0 && totalAttempts < Math.Max(1, maxAttempts))
        {
            totalAttempts++;
            var stopResult = await StopPythonProcessesAsync(
                targetRoot,
                remainingProcesses,
                logger,
                cancellationToken,
                dryRun: false,
                attemptNumber: totalAttempts).ConfigureAwait(false);

            allAttempts.AddRange(stopResult.Attempts);
            remainingProcesses = DistinctPythonProcesses(stopResult.RemainingProcesses);
        }

        return new PythonProcessStopResult(
            requestedProcesses,
            allAttempts,
            remainingProcesses,
            totalAttempts,
            false);
    }

    public static bool TryOpenFolder(string folderPath)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(folderPath))
            {
                return false;
            }

            Directory.CreateDirectory(folderPath);
            Process.Start(new ProcessStartInfo
            {
                FileName = folderPath,
                UseShellExecute = true,
            });
            return true;
        }
        catch
        {
            return false;
        }
    }

    public static string FormatArguments(IEnumerable<string> args)
    {
        return string.Join(
            " ",
            args.Select(arg => arg.Contains(' ') || arg.Contains('\t') ? $"\"{arg}\"" : arg));
    }

    public static string SanitizeForLog(string value)
    {
        return value
            .Replace("\r", " ", StringComparison.Ordinal)
            .Replace("\n", " ", StringComparison.Ordinal)
            .Trim();
    }

    private static ProductDefinition DetectProduct(string baseDirectory, string exePath)
    {
        foreach (var product in ProductDefinition.All)
        {
            if (!string.IsNullOrWhiteSpace(TryResolvePackageRoot(baseDirectory, product)))
            {
                return product;
            }
        }

        var exeName = Path.GetFileNameWithoutExtension(exePath);
        if (exeName.Contains("free", StringComparison.OrdinalIgnoreCase))
        {
            return ProductDefinition.Free;
        }

        return ProductDefinition.Standard;
    }

    private static string? TryResolvePackageRoot(string startDirectory, ProductDefinition product)
    {
        var current = new DirectoryInfo(startDirectory);
        while (current is not null)
        {
            if (IsPackageRootCandidate(current.FullName, product))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }

    private static bool IsPackageRootCandidate(string candidateRoot, ProductDefinition product)
    {
        return File.Exists(Path.Combine(candidateRoot, product.InstallerScriptRelativePath))
            && File.Exists(Path.Combine(candidateRoot, product.InstallConfigRelativePath));
    }

    private static InstallerConfig? TryLoadInstallerConfig(string? packageRoot, ProductDefinition product)
    {
        if (string.IsNullOrWhiteSpace(packageRoot))
        {
            return null;
        }

        var configPath = Path.Combine(packageRoot, product.InstallConfigRelativePath);
        if (!File.Exists(configPath))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(configPath, Encoding.UTF8));
            var root = document.RootElement;
            return new InstallerConfig(
                GetJsonString(root, "product_display_name") ?? product.ProductDisplayName,
                GetJsonString(root, "install_root_mode") ?? product.DefaultInstallRootMode,
                GetJsonString(root, "install_root_name") ?? product.DefaultInstallRootName,
                GetJsonString(root, "shortcut_name") ?? Path.GetFileNameWithoutExtension(product.DesktopShortcutFileName));
        }
        catch
        {
            return null;
        }
    }

    private static string ResolveInstallRoot(InstallerConfig config, string packageRoot, string? explicitInstallRoot)
    {
        if (!string.IsNullOrWhiteSpace(explicitInstallRoot))
        {
            return Path.GetFullPath(explicitInstallRoot);
        }

        var installRootMode = config.InstallRootMode.Trim().ToLowerInvariant();
        return installRootMode switch
        {
            "repo_root" => Path.GetFullPath(packageRoot),
            "user_profile" => Path.GetFullPath(Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                config.InstallRootName)),
            "localappdata" => Path.GetFullPath(Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                config.InstallRootName)),
            _ => Path.GetFullPath(Path.Combine(GetProgramFilesRoot(), config.InstallRootName)),
        };
    }

    private static async Task<PackageAcquisition> ResolvePackageAcquisitionAsync(
        SetupPreparation preparation,
        string? packageUrlOverride,
        FileLogger logger,
        ISetupObserver observer,
        CancellationToken cancellationToken)
    {
        var packageUrlSelection = ResolvePackageUrlSelection(preparation.Product, packageUrlOverride);
        if (!packageUrlSelection.IsOverride && !string.IsNullOrWhiteSpace(preparation.LocalPackageRoot))
        {
            return new PackageAcquisition(
                preparation.LocalPackageRoot,
                "local_package_root",
                "local",
                null,
                null,
                null,
                null);
        }

        return await DownloadAndExtractPackageAsync(preparation.Product, packageUrlSelection, logger, observer, cancellationToken).ConfigureAwait(false);
    }

    private static PackageUrlSelection ResolvePackageUrlSelection(ProductDefinition product, string? packageUrlOverride)
    {
        if (!string.IsNullOrWhiteSpace(packageUrlOverride))
        {
            return new PackageUrlSelection(packageUrlOverride, "argument", true);
        }

        var productOverride = Environment.GetEnvironmentVariable(product.ProductPackageUrlEnvName);
        if (!string.IsNullOrWhiteSpace(productOverride))
        {
            return new PackageUrlSelection(productOverride, "env_product", true);
        }

        var commonOverride = Environment.GetEnvironmentVariable(CommonPackageUrlEnvName);
        if (!string.IsNullOrWhiteSpace(commonOverride))
        {
            return new PackageUrlSelection(commonOverride, "env_common", true);
        }

        return new PackageUrlSelection(product.LatestPackageUrl, "default_latest_release", false);
    }

    private static async Task<PackageAcquisition> DownloadAndExtractPackageAsync(
        ProductDefinition product,
        PackageUrlSelection packageUrlSelection,
        FileLogger logger,
        ISetupObserver observer,
        CancellationToken cancellationToken)
    {
        var bootstrapRoot = CreateBootstrapTempRoot(product.Code);
        var downloadedAssetPath = Path.Combine(
            bootstrapRoot,
            "download",
            ResolvePackageArchiveFileName(product, packageUrlSelection.Url));
        var extractDirectory = Path.Combine(bootstrapRoot, "extract");
        var resolvedDownloadUrl = packageUrlSelection.Url;

        observer.SetPhase(SetupPhase.DownloadingPackage);
        observer.AppendDetail($"requested_download_url={packageUrlSelection.Url}");
        logger.Info($"requested_download_url={packageUrlSelection.Url}");

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(downloadedAssetPath) ?? bootstrapRoot);

            using var handler = new HttpClientHandler
            {
                AllowAutoRedirect = true,
            };
            using var client = new HttpClient(handler)
            {
                Timeout = TimeSpan.FromMinutes(15),
            };
            client.DefaultRequestHeaders.UserAgent.ParseAdd("LoneWolfFangSetupBootstrapper/" + Program.BuildId);

            using var response = await client.GetAsync(
                packageUrlSelection.Url,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken).ConfigureAwait(false);

            resolvedDownloadUrl = response.RequestMessage?.RequestUri?.AbsoluteUri ?? packageUrlSelection.Url;
            response.EnsureSuccessStatusCode();

            await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            await using var outputStream = new FileStream(
                downloadedAssetPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None);
            await responseStream.CopyToAsync(outputStream, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or InvalidOperationException or TaskCanceledException)
        {
            throw BootstrapException.PackageDownloadFailed(
                $"Setup could not download the package. Check {logger.LogPath} for details.",
                $"requested_url={packageUrlSelection.Url} resolved_url={resolvedDownloadUrl} error={ex.Message}");
        }

        observer.SetPhase(SetupPhase.ExtractingPackage);
        observer.AppendDetail($"downloaded_asset_path={downloadedAssetPath}");
        observer.AppendDetail($"extract_dir={extractDirectory}");

        try
        {
            Directory.CreateDirectory(extractDirectory);
            await Task.Run(() => ZipFile.ExtractToDirectory(downloadedAssetPath, extractDirectory, true), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or NotSupportedException)
        {
            throw BootstrapException.PackageExtractFailed(
                $"Setup could not extract the downloaded package. Check {logger.LogPath} for details.",
                $"downloaded_asset_path={downloadedAssetPath} extract_dir={extractDirectory} error={ex.Message}");
        }

        var extractedPackageRoot = TryResolveExtractedPackageRoot(extractDirectory, product);
        if (string.IsNullOrWhiteSpace(extractedPackageRoot))
        {
            throw BootstrapException.DownloadedPackageInvalid(
                $"Setup could not locate installer files inside the downloaded package. Check {logger.LogPath} for details.",
                $"requested_url={packageUrlSelection.Url} downloaded_asset_path={downloadedAssetPath} extract_dir={extractDirectory}");
        }

        return new PackageAcquisition(
            extractedPackageRoot,
            "downloaded_package",
            packageUrlSelection.Source,
            packageUrlSelection.Url,
            resolvedDownloadUrl,
            downloadedAssetPath,
            extractDirectory);
    }

    private static string CreateBootstrapTempRoot(string productCode)
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var parent = string.IsNullOrWhiteSpace(localAppData)
            ? Path.GetTempPath()
            : Path.Combine(localAppData, "LoneWolfFang", "temp", "setup", productCode);
        var runId = $"{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid():N}";
        var root = Path.Combine(parent, runId);
        Directory.CreateDirectory(root);
        return root;
    }

    private static string ResolvePackageArchiveFileName(ProductDefinition product, string packageUrl)
    {
        if (Uri.TryCreate(packageUrl, UriKind.Absolute, out var uri))
        {
            var fileName = Path.GetFileName(uri.LocalPath);
            if (!string.IsNullOrWhiteSpace(fileName))
            {
                return fileName;
            }
        }

        return product.PackageAssetFileName;
    }

    private static string? TryResolveExtractedPackageRoot(string extractDirectory, ProductDefinition product)
    {
        var pending = new Queue<(string Path, int Depth)>();
        pending.Enqueue((extractDirectory, 0));

        while (pending.Count > 0)
        {
            var current = pending.Dequeue();
            if (IsPackageRootCandidate(current.Path, product))
            {
                return Path.GetFullPath(current.Path);
            }

            if (current.Depth >= 2)
            {
                continue;
            }

            foreach (var child in Directory.EnumerateDirectories(current.Path))
            {
                pending.Enqueue((child, current.Depth + 1));
            }
        }

        return null;
    }

    private static string? TryResolvePowerShellPath()
    {
        var candidates = new List<string>();

        var systemDirectory = Environment.SystemDirectory;
        if (!string.IsNullOrWhiteSpace(systemDirectory))
        {
            candidates.Add(Path.Combine(systemDirectory, @"WindowsPowerShell\v1.0\powershell.exe"));
        }

        var windowsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
        if (!string.IsNullOrWhiteSpace(windowsDirectory))
        {
            candidates.Add(Path.Combine(windowsDirectory, @"System32\WindowsPowerShell\v1.0\powershell.exe"));
        }

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        var pathVariable = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathVariable))
        {
            return null;
        }

        foreach (var segment in pathVariable.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            try
            {
                var candidate = Path.Combine(segment.Trim(), "powershell.exe");
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
            catch
            {
                // Ignore malformed PATH entries.
            }
        }

        return null;
    }

    private static IReadOnlyList<string> BuildInstallerArguments(InstallExecutionOptions options, string installRoot)
    {
        var arguments = new List<string>
        {
            "-InstallRoot",
            installRoot,
            "-NonInteractive",
            "-Force",
            "-SkipInstallRootPythonPrecheck",
        };

        if (!options.CreateDesktopShortcut || options.DesktopShortcutMode == DesktopShortcutMode.Skip)
        {
            arguments.Add("-SkipDesktopShortcut");
        }

        return arguments;
    }

    private static async Task<ProcessCaptureResult> RunInstallerProcessAsync(
        string powerShellPath,
        string installerScriptPath,
        string packageRoot,
        IReadOnlyList<string> bridgedArgs,
        ISetupObserver observer,
        FileLogger logger,
        CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = powerShellPath,
            WorkingDirectory = packageRoot,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };

        startInfo.ArgumentList.Add("-NoLogo");
        startInfo.ArgumentList.Add("-NoProfile");
        startInfo.ArgumentList.Add("-ExecutionPolicy");
        startInfo.ArgumentList.Add("Bypass");
        startInfo.ArgumentList.Add("-File");
        startInfo.ArgumentList.Add(installerScriptPath);

        foreach (var argument in bridgedArgs)
        {
            startInfo.ArgumentList.Add(argument);
        }

        try
        {
            using var process = new Process { StartInfo = startInfo };
            if (!process.Start())
            {
                throw BootstrapException.InstallFailed(
                    "Installation did not complete. Check the setup logs for details.",
                    25,
                    "Process.Start returned false for powershell.exe.");
            }

            var outputLines = new List<string>();
            var stdoutTask = PumpReaderAsync(process.StandardOutput, false, outputLines, observer, logger, cancellationToken);
            var stderrTask = PumpReaderAsync(process.StandardError, true, outputLines, observer, logger, cancellationToken);

            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(stdoutTask, stderrTask).ConfigureAwait(false);

            return new ProcessCaptureResult(process.ExitCode, outputLines);
        }
        catch (BootstrapException)
        {
            throw;
        }
        catch (Exception ex) when (ex is InvalidOperationException or Win32Exception or IOException)
        {
            throw BootstrapException.InstallFailed(
                "Installation did not complete. Check the setup logs for details.",
                25,
                ex.Message);
        }
    }

    private static async Task PumpReaderAsync(
        StreamReader reader,
        bool isError,
        List<string> outputLines,
        ISetupObserver observer,
        FileLogger logger,
        CancellationToken cancellationToken)
    {
        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                continue;
            }

            lock (outputLines)
            {
                outputLines.Add(line);
            }

            observer.AppendDetail(isError ? $"[stderr] {line}" : line);
            if (isError)
            {
                logger.Error($"backend_stderr={SanitizeForLog(line)}");
            }
            else
            {
                logger.Info($"backend_stdout={SanitizeForLog(line)}");
            }
        }
    }

    private static LaunchResult TryStartInstalledApplication(ProductDefinition product, string installRoot, FileLogger logger)
    {
        foreach (var relativeTarget in product.StartTargets)
        {
            var candidate = Path.Combine(installRoot, relativeTarget);
            if (!File.Exists(candidate))
            {
                continue;
            }

            try
            {
                using var process = Process.Start(new ProcessStartInfo
                {
                    FileName = candidate,
                    WorkingDirectory = installRoot,
                    UseShellExecute = true,
                });

                if (process is null)
                {
                    continue;
                }

                logger.Info($"start_after_install_target={candidate}");
                return new LaunchResult(true, null);
            }
            catch (Exception ex)
            {
                logger.Error($"start_after_install_error={SanitizeForLog(ex.Message)}");
                return new LaunchResult(false, ex.Message);
            }
        }

        return new LaunchResult(false, $"No launch target was found under {installRoot}.");
    }

    private static async Task<IReadOnlyList<PythonProcessInfo>> TryQueryPythonProcessesOrFallbackAsync(
        string targetRoot,
        IReadOnlyList<PythonProcessInfo> fallbackProcesses,
        FileLogger logger,
        CancellationToken cancellationToken)
    {
        try
        {
            return DistinctPythonProcesses(
                await QueryPythonProcessesAsync(targetRoot, logger, cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            var details = ex is BootstrapException bootstrapException
                ? bootstrapException.LogDetails
                : ex.Message;
            logger.Error($"python_process_query_after_stop_failed details={SanitizeForLog(details)}");
            return DistinctPythonProcesses(fallbackProcesses);
        }
    }

    private static IReadOnlyList<PythonProcessInfo> DistinctPythonProcesses(
        IEnumerable<PythonProcessInfo> processes)
    {
        return processes
            .Where(processInfo => processInfo is not null && processInfo.ProcessId > 0)
            .GroupBy(processInfo => processInfo.ProcessId)
            .Select(group => group.First())
            .ToArray();
    }

    private static IReadOnlyList<PythonProcessInfo> QueryPythonProcessesCore(
        string normalizedTargetRoot,
        FileLogger logger,
        CancellationToken cancellationToken)
    {
        var matches = new List<PythonProcessInfo>();

        try
        {
            using var searcher = new ManagementObjectSearcher(
                "SELECT ProcessId, ParentProcessId, Name, ExecutablePath, CommandLine " +
                "FROM Win32_Process WHERE Name='python.exe' OR Name='pythonw.exe'");
            using var results = searcher.Get();

            foreach (ManagementObject processObject in results)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (processObject)
                {
                    try
                    {
                        var processInfo = TryCreatePythonProcessInfo(processObject, normalizedTargetRoot);
                        if (processInfo is null)
                        {
                            continue;
                        }

                        matches.Add(processInfo);
                    }
                    catch (Exception ex) when (ex is ManagementException or COMException or UnauthorizedAccessException or InvalidOperationException)
                    {
                        logger.Error($"python_process_query_entry_failed error={SanitizeForLog(ex.Message)}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is ManagementException or COMException or UnauthorizedAccessException or InvalidOperationException)
        {
            logger.Error($"python_process_query_failed error={SanitizeForLog(ex.Message)}");
            logger.Info("python_process_query_count=0");
            return Array.Empty<PythonProcessInfo>();
        }

        var processes = DistinctPythonProcesses(matches);
        logger.Info($"python_process_query_count={processes.Count}");
        foreach (var processInfo in processes)
        {
            logger.Info(
                $"python_process_detected pid={processInfo.ProcessId} " +
                $"ppid={processInfo.ParentProcessId} " +
                $"name={processInfo.Name} " +
                $"path={FormatFieldForDisplay(processInfo.ExecutablePath)} " +
                $"cmd={FormatFieldForDisplay(processInfo.CommandLine)}");
        }

        return processes;
    }

    private static PythonProcessInfo? TryCreatePythonProcessInfo(
        ManagementBaseObject processObject,
        string normalizedTargetRoot)
    {
        var processId = GetManagementInt32(processObject, "ProcessId");
        if (processId <= 0)
        {
            return null;
        }

        var parentProcessId = GetManagementInt32(processObject, "ParentProcessId");
        var name = GetManagementString(processObject, "Name");
        if (string.IsNullOrWhiteSpace(name))
        {
            name = "python.exe";
        }

        var executablePath = GetManagementString(processObject, "ExecutablePath");
        var normalizedExecutablePath = TryNormalizePath(executablePath);
        if (!string.IsNullOrWhiteSpace(normalizedExecutablePath))
        {
            executablePath = normalizedExecutablePath;
        }

        var commandLine = GetManagementString(processObject, "CommandLine");
        var executableMatch = IsPathUnderRoot(normalizedExecutablePath, normalizedTargetRoot);
        var commandLineMatch =
            string.IsNullOrWhiteSpace(normalizedExecutablePath) &&
            !string.IsNullOrWhiteSpace(commandLine) &&
            commandLine.IndexOf(normalizedTargetRoot, StringComparison.OrdinalIgnoreCase) >= 0;

        if (!executableMatch && !commandLineMatch)
        {
            return null;
        }

        return new PythonProcessInfo(
            processId,
            parentProcessId,
            name,
            executablePath,
            commandLine);
    }

    private static PythonProcessStopAttempt StopPythonProcess(PythonProcessInfo processInfo)
    {
        try
        {
            using var process = Process.GetProcessById(processInfo.ProcessId);

            try
            {
                KillProcess(process);
            }
            catch (InvalidOperationException)
            {
                return CreateStopAttempt(processInfo, true, "already_exited");
            }

            if (!WaitForExit(process, 2500))
            {
                return CreateStopAttempt(processInfo, false, "Timed out waiting for the process to exit.");
            }

            return CreateStopAttempt(processInfo, true, "stopped");
        }
        catch (ArgumentException)
        {
            return CreateStopAttempt(processInfo, true, "already_exited");
        }
        catch (InvalidOperationException)
        {
            return CreateStopAttempt(processInfo, true, "already_exited");
        }
        catch (Win32Exception ex) when (ex.NativeErrorCode == 5)
        {
            return CreateStopAttempt(processInfo, false, "Access denied.");
        }
        catch (Win32Exception ex)
        {
            return CreateStopAttempt(processInfo, false, ex.Message);
        }
    }

    private static PythonProcessStopAttempt CreateStopAttempt(
        PythonProcessInfo processInfo,
        bool succeeded,
        string reason)
    {
        return new PythonProcessStopAttempt(
            processInfo.ProcessId,
            processInfo.Name,
            processInfo.ExecutablePath,
            processInfo.CommandLine,
            succeeded,
            false,
            reason);
    }

    private static void KillProcess(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: false);
        }
        catch (PlatformNotSupportedException)
        {
            process.Kill();
        }
        catch (NotSupportedException)
        {
            process.Kill();
        }
    }

    private static bool WaitForExit(Process process, int timeoutMilliseconds)
    {
        try
        {
            return process.WaitForExit(timeoutMilliseconds);
        }
        catch (InvalidOperationException)
        {
            return true;
        }
    }

    private static int GetManagementInt32(ManagementBaseObject processObject, string propertyName)
    {
        try
        {
            var value = processObject[propertyName];
            return value switch
            {
                null => 0,
                int intValue => intValue,
                uint uintValue when uintValue <= int.MaxValue => (int)uintValue,
                short shortValue => shortValue,
                ushort ushortValue => ushortValue,
                long longValue when longValue is >= int.MinValue and <= int.MaxValue => (int)longValue,
                ulong ulongValue when ulongValue <= int.MaxValue => (int)ulongValue,
                string stringValue when int.TryParse(stringValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
                _ => Convert.ToInt32(value, CultureInfo.InvariantCulture),
            };
        }
        catch (Exception ex) when (ex is ManagementException or COMException or InvalidCastException or FormatException or OverflowException)
        {
            return 0;
        }
    }

    private static string GetManagementString(ManagementBaseObject processObject, string propertyName)
    {
        try
        {
            return Convert.ToString(processObject[propertyName], CultureInfo.InvariantCulture)?.Trim() ?? string.Empty;
        }
        catch (Exception ex) when (ex is ManagementException or COMException or InvalidCastException or FormatException)
        {
            return string.Empty;
        }
    }

    private static bool IsPathUnderRoot(string? candidatePath, string normalizedTargetRoot)
    {
        if (string.IsNullOrWhiteSpace(candidatePath) || string.IsNullOrWhiteSpace(normalizedTargetRoot))
        {
            return false;
        }

        return candidatePath.StartsWith(AppendDirectorySeparator(normalizedTargetRoot), StringComparison.OrdinalIgnoreCase);
    }

    private static string AppendDirectorySeparator(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return string.Empty;
        }

        return path.EndsWith(Path.DirectorySeparatorChar) || path.EndsWith(Path.AltDirectorySeparatorChar)
            ? path
            : path + Path.DirectorySeparatorChar;
    }

    private static string ResolveLogFolderPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (!string.IsNullOrWhiteSpace(localAppData))
        {
            return Path.Combine(localAppData, "LoneWolfFang", "logs");
        }

        return Path.GetTempPath();
    }

    private static string GetProgramFilesRoot()
    {
        foreach (var candidate in new[]
                 {
                     Environment.GetEnvironmentVariable("ProgramW6432"),
                     Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles),
                 })
        {
            if (!string.IsNullOrWhiteSpace(candidate))
            {
                return Path.GetFullPath(candidate);
            }
        }

        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (!string.IsNullOrWhiteSpace(localAppData))
        {
            return Path.GetFullPath(Path.Combine(localAppData, "Programs"));
        }

        return Path.GetFullPath(AppContext.BaseDirectory);
    }

    private static string? GetJsonString(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var property) && property.ValueKind == JsonValueKind.String)
        {
            return property.GetString();
        }

        return null;
    }

    private static string? TryNormalizePath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        try
        {
            return Path.GetFullPath(path);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return null;
        }
    }

    private static string FormatFieldForDisplay(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "<none>" : value.Trim();
    }

    private static string ExtractFailureSummary(IReadOnlyList<string> outputLines)
    {
        foreach (var line in outputLines.Reverse())
        {
            if (line.StartsWith("Reason", StringComparison.OrdinalIgnoreCase))
            {
                return line.Trim();
            }

            if (line.Contains("[FAILED]", StringComparison.OrdinalIgnoreCase))
            {
                return line.Trim();
            }
        }

        return "Installer exited with a failure code.";
    }

    private sealed record PackageUrlSelection(
        string Url,
        string Source,
        bool IsOverride);

    private sealed record ProcessCaptureResult(
        int ExitCode,
        IReadOnlyList<string> OutputLines);

    private sealed record LaunchResult(
        bool IsSuccess,
        string? ErrorMessage);

    private sealed class ConsoleObserver : ISetupObserver
    {
        public void SetPhase(SetupPhase phase)
        {
            Console.WriteLine($"phase={phase}");
        }

        public void AppendDetail(string message)
        {
            Console.WriteLine(message);
        }
    }
}
