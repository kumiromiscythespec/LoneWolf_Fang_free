using System.Globalization;
using System.Resources;
using System.Text;

namespace LoneWolfFangSetupBootstrapper;

internal enum UiLanguage
{
    Auto,
    Japanese,
    English,
}

internal enum DesktopShortcutMode
{
    Auto,
    Skip,
}

internal enum SetupPhase
{
    Ready,
    CheckingRunningProcesses,
    StoppingRunningProcesses,
    ResolvingPackage,
    DownloadingPackage,
    ExtractingPackage,
    RunningInstaller,
    StartingApplication,
    Completed,
    Failed,
}

internal interface ISetupObserver
{
    void SetPhase(SetupPhase phase);

    void AppendDetail(string message);
}

internal sealed record ProductDefinition(
    string Code,
    string SetupDisplayName,
    string ProductDisplayName,
    string InstallerScriptRelativePath,
    string InstallConfigRelativePath,
    string PackageAssetFileName,
    string LatestPackageUrl,
    string ProductPackageUrlEnvName,
    string SetupLogFileName,
    string BackendLogFileName,
    string DefaultInstallRootMode,
    string DefaultInstallRootName,
    bool RequiresLicenseActivation,
    string DesktopShortcutFileName,
    IReadOnlyList<string> StartTargets)
{
    public static ProductDefinition Standard { get; } = new(
        "standard",
        "LoneWolf Fang Standard",
        "LoneWolf Fang standard",
        @"packaging\install_runtime.ps1",
        @"packaging\install_config_standard.json",
        "LoneWolf_Fang_Standard_Package.zip",
        "https://github.com/kumiromiscythespec/LoneWolf_Fang_standard_releases/releases/latest/download/LoneWolf_Fang_Standard_Package.zip",
        "LWF_STANDARD_SETUP_PACKAGE_URL",
        "setup_bootstrap_standard.log",
        "installer_standard.log",
        "programfiles",
        "LoneWolf Fang standard",
        true,
        "LoneWolf Fang standard.lnk",
        new[]
        {
            "LoneWolfFangStandardLauncher.exe",
            "Launch_LoneWolf_Fang_GUI.vbs",
            "Launch_LoneWolf_Fang_GUI.cmd",
        });

    public static ProductDefinition Free { get; } = new(
        "free",
        "LoneWolf Fang Free",
        "LoneWolf Fang free",
        @"packaging\install_free_local.ps1",
        @"packaging\install_config_free.json",
        "LoneWolf_Fang_Free_Package.zip",
        "https://github.com/kumiromiscythespec/LoneWolf_Fang_free/releases/latest/download/LoneWolf_Fang_Free_Package.zip",
        "LWF_FREE_SETUP_PACKAGE_URL",
        "setup_bootstrap_free.log",
        "installer_free.log",
        "programfiles",
        "LoneWolf Fang free",
        false,
        "LoneWolf Fang free.lnk",
        new[]
        {
            "Launch_LoneWolf_Fang_Free_GUI.vbs",
            "Launch_LoneWolf_Fang_Free_GUI.cmd",
            "LoneWolfFangFreeLauncher.exe",
        });

    public static IReadOnlyList<ProductDefinition> All { get; } = new[]
    {
        Standard,
        Free,
    };
}

internal sealed record InstallerConfig(
    string ProductDisplayName,
    string InstallRootMode,
    string InstallRootName,
    string ShortcutName)
{
    public static InstallerConfig FromProduct(ProductDefinition product)
    {
        return new InstallerConfig(
            product.ProductDisplayName,
            product.DefaultInstallRootMode,
            product.DefaultInstallRootName,
            Path.GetFileNameWithoutExtension(product.DesktopShortcutFileName));
    }
}

internal sealed record SetupPreparation(
    ProductDefinition Product,
    string ExePath,
    string BaseDirectory,
    string? LocalPackageRoot,
    InstallerConfig InstallerConfig,
    string SuggestedInstallRoot,
    string SetupLogPath,
    string BackendLogPath,
    string LogFolderPath);

internal sealed record PackageAcquisition(
    string PackageRoot,
    string Source,
    string PackageUrlSource,
    string? RequestedDownloadUrl,
    string? ResolvedDownloadUrl,
    string? DownloadedAssetPath,
    string? ExtractDirectory);

internal sealed record InstallExecutionOptions(
    string InstallRoot,
    bool CreateDesktopShortcut,
    bool StartAfterInstall,
    DesktopShortcutMode DesktopShortcutMode,
    string? PackageUrl,
    bool Force);

internal sealed record InstallExecutionResult(
    bool Success,
    int ExitCode,
    string InstallRoot,
    string SetupLogPath,
    string BackendLogPath,
    string LogFolderPath,
    string? FailureSummary,
    string? WarningMessage);

internal sealed record PythonProcessInfo(
    int ProcessId,
    int ParentProcessId,
    string Name,
    string ExecutablePath,
    string CommandLine);

internal sealed record PythonProcessStopAttempt(
    int ProcessId,
    string Name,
    string ExecutablePath,
    string CommandLine,
    bool Succeeded,
    bool WouldStop,
    string Reason);

internal sealed record PythonProcessStopResult(
    IReadOnlyList<PythonProcessInfo> RequestedProcesses,
    IReadOnlyList<PythonProcessStopAttempt> Attempts,
    IReadOnlyList<PythonProcessInfo> RemainingProcesses,
    int AttemptCount,
    bool IsDryRun)
{
    public bool AllStopped => RemainingProcesses.Count == 0;
}

internal sealed class UiLocalizer
{
    private static readonly ResourceManager ResourceManager = new(
        "LoneWolfFangSetupBootstrapper.Resources.Strings",
        typeof(UiLocalizer).Assembly);

    public UiLocalizer(CultureInfo culture)
    {
        Culture = culture;
    }

    public CultureInfo Culture { get; }

    public string Get(string key)
    {
        return ResourceManager.GetString(key, Culture) ?? key;
    }

    public string Format(string key, params object[] args)
    {
        return string.Format(Culture, Get(key), args);
    }

    public string GetPhaseText(SetupPhase phase)
    {
        return phase switch
        {
            SetupPhase.CheckingRunningProcesses => Get("StatusCheckingRunningProcesses"),
            SetupPhase.StoppingRunningProcesses => Get("StatusStoppingRunningProcesses"),
            SetupPhase.ResolvingPackage => Get("StatusResolvingPackage"),
            SetupPhase.DownloadingPackage => Get("StatusDownloadingPackage"),
            SetupPhase.ExtractingPackage => Get("StatusExtractingPackage"),
            SetupPhase.RunningInstaller => Get("StatusRunningInstaller"),
            SetupPhase.StartingApplication => Get("StatusStartingApplication"),
            SetupPhase.Completed => Get("StatusCompleted"),
            SetupPhase.Failed => Get("StatusFailed"),
            _ => Get("StatusReady"),
        };
    }
}

internal sealed class FileLogger
{
    private readonly object _sync = new();

    public FileLogger(string logPath)
    {
        LogPath = EnsureLogPath(logPath);
    }

    public string LogPath { get; }

    public void Info(string message)
    {
        Write("INFO", message);
    }

    public void Error(string message)
    {
        Write("ERROR", message);
    }

    private void Write(string level, string message)
    {
        var line = $"{DateTimeOffset.Now:yyyy-MM-ddTHH:mm:sszzz} [{level}] {BootstrapRuntime.SanitizeForLog(message)}";

        try
        {
            lock (_sync)
            {
                File.AppendAllText(LogPath, line + Environment.NewLine, Encoding.UTF8);
            }
        }
        catch
        {
            // Logging must never block setup execution.
        }
    }

    private static string EnsureLogPath(string preferredPath)
    {
        try
        {
            var directory = Path.GetDirectoryName(preferredPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }
        }
        catch
        {
            return preferredPath;
        }

        return preferredPath;
    }
}

internal sealed class BootstrapException : Exception
{
    private BootstrapException(int exitCode, string category, string userMessage, string logDetails)
        : base(logDetails)
    {
        ExitCode = exitCode;
        Category = category;
        UserMessage = userMessage;
        LogDetails = logDetails;
    }

    public int ExitCode { get; }

    public string Category { get; }

    public string UserMessage { get; }

    public string LogDetails { get; }

    public static BootstrapException RootUnresolved(string userMessage, string logDetails)
    {
        return new BootstrapException(20, "root_unresolved", userMessage, logDetails);
    }

    public static BootstrapException InstallerScriptMissing(string userMessage, string logDetails)
    {
        return new BootstrapException(21, "installer_script_missing", userMessage, logDetails);
    }

    public static BootstrapException PowerShellMissing(string userMessage, string logDetails)
    {
        return new BootstrapException(22, "powershell_missing", userMessage, logDetails);
    }

    public static BootstrapException InvalidArguments(string userMessage, string logDetails)
    {
        return new BootstrapException(23, "invalid_arguments", userMessage, logDetails);
    }

    public static BootstrapException PythonProcessesBlocked(string userMessage, string logDetails)
    {
        return new BootstrapException(24, "python_processes_blocked", userMessage, logDetails);
    }

    public static BootstrapException PackageDownloadFailed(string userMessage, string logDetails)
    {
        return new BootstrapException(26, "package_download_failed", userMessage, logDetails);
    }

    public static BootstrapException PackageExtractFailed(string userMessage, string logDetails)
    {
        return new BootstrapException(27, "package_extract_failed", userMessage, logDetails);
    }

    public static BootstrapException DownloadedPackageInvalid(string userMessage, string logDetails)
    {
        return new BootstrapException(28, "downloaded_package_invalid", userMessage, logDetails);
    }

    public static BootstrapException InstallFailed(string userMessage, int exitCode, string logDetails)
    {
        return new BootstrapException(exitCode, "install_failed", userMessage, logDetails);
    }
}
