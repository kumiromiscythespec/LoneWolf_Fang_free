// BUILD_ID: 2026-03-30_free_native_setup_bootstrap_dotnet8_v1
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace LoneWolfFangFreeSetup;

internal static class Program
{
    private const string BuildId = "2026-03-30_free_native_setup_bootstrap_dotnet8_v1";
    private const string InstallerScriptRelativePath = @"packaging\install_free_local.ps1";
    private const string InstallConfigRelativePath = @"packaging\install_config_free.json";
    private const string InstallerCompatCmdRelativePath = @"Install_LoneWolf_Fang_Free.cmd";
    private const string DefaultStartLauncherRelativePath = @"Launch_LoneWolf_Fang_Free_GUI.vbs";
    private const string SecondaryStartLauncherRelativePath = @"Launch_LoneWolf_Fang_Free_GUI.cmd";
    private const string TertiaryStartLauncherRelativePath = @"LoneWolfFangFreeLauncher.exe";
    private const string DesktopShortcutFileName = "LoneWolf Fang Free GUI.lnk";
    private const string LogFileName = "setup_bootstrap_free.log";
    private const string MessageBoxTitle = "LoneWolf Fang Free Setup";
    private const uint MbOk = 0x00000000;
    private const uint MbIconError = 0x00000010;

    [STAThread]
    private static int Main(string[] args)
    {
        var logger = new FileLogger(ResolveLogPath(LogFileName));
        var options = SetupOptions.Parse(args);
        var exitCode = (int)SetupExitCode.Success;
        string? packageRoot = null;
        string? installerScriptPath = null;

        logger.Info($"setup_bootstrap start BUILD_ID={BuildId}");
        logger.Info($"raw_args={FormatArguments(args)}");
        logger.Info($"log_path={logger.LogPath}");

        try
        {
            var exePath = ResolveExecutablePath();
            logger.Info($"exe_path={exePath}");

            if (!options.IsValid)
            {
                throw SetupException.UnsupportedArguments(
                    "Setup received an unsupported argument.",
                    options.InvalidReason);
            }

            var exeDirectory = Path.GetDirectoryName(exePath)
                ?? throw SetupException.RootUnresolved(
                    "Setup could not resolve its executable directory. Please extract the full package and try again.",
                    $"Unable to resolve executable directory from: {exePath}");

            packageRoot = TryResolvePackageRoot(exeDirectory)
                ?? throw SetupException.RootUnresolved(
                    "Setup could not find the installer package. Please extract the full package and try again.",
                    $"Unable to resolve package root from: {exeDirectory}");

            installerScriptPath = Path.Combine(packageRoot, InstallerScriptRelativePath);
            if (!File.Exists(installerScriptPath))
            {
                throw SetupException.InstallerScriptMissing(
                    "Setup could not find the installer script. Please extract the full package and try again.",
                    $"Missing installer script: {installerScriptPath}");
            }

            var compatCmdPath = Path.Combine(packageRoot, InstallerCompatCmdRelativePath);
            logger.Info($"app_root={packageRoot}");
            logger.Info($"installer_script_path={installerScriptPath}");
            logger.Info($"compat_cmd_path={compatCmdPath}");

            var powerShellPath = TryResolvePowerShellPath()
                ?? throw SetupException.PowershellMissing(
                    "Windows PowerShell was not found. Please enable PowerShell and try again.",
                    "powershell.exe was not found in the expected Windows locations or PATH.");

            var installRoot = ResolveInstallRoot(packageRoot, options.InstallDirectory);
            var bridgedArgs = BuildInstallerArguments(installerScriptPath, installRoot, options);
            var fullCommand = $"{QuoteArgument(powerShellPath)} {BuildArgumentString(bridgedArgs)}".Trim();

            logger.Info($"powershell_path={powerShellPath}");
            logger.Info($"install_root={installRoot}");
            logger.Info($"desktop_shortcut_mode={options.DesktopShortcutMode}");
            logger.Info($"start_after_install={options.StartAfterInstall}");
            logger.Info($"check_only={options.CheckOnly}");
            logger.Info($"dry_run={options.DryRun}");
            logger.Info($"bridged_args={FormatArguments(bridgedArgs)}");
            logger.Info($"full_command={fullCommand}");

            if (options.CheckOnly)
            {
                logger.Info("setup_result=check_only_ok");
                exitCode = (int)SetupExitCode.Success;
                return exitCode;
            }

            if (options.DryRun)
            {
                logger.Info("setup_result=dry_run_ok");
                exitCode = (int)SetupExitCode.Success;
                return exitCode;
            }

            var installerExitCode = RunInstaller(packageRoot, powerShellPath, bridgedArgs);
            logger.Info($"installer_exit_code={installerExitCode}");
            if (installerExitCode != 0)
            {
                throw SetupException.InstallFailed(
                    "Setup failed. Please review the installer window and setup_bootstrap.log.",
                    $"Installer exited with code {installerExitCode}.",
                    installerExitCode);
            }

            if (options.DesktopShortcutMode == DesktopShortcutMode.Skip)
            {
                CleanupDesktopShortcut(logger);
            }

            if (options.StartAfterInstall)
            {
                StartInstalledApp(installRoot, logger);
            }

            logger.Info("setup_result=success");
            exitCode = (int)SetupExitCode.Success;
            return exitCode;
        }
        catch (SetupException ex)
        {
            logger.Info($"app_root={(string.IsNullOrWhiteSpace(packageRoot) ? "<unresolved>" : packageRoot)}");
            logger.Info($"installer_script_path={(string.IsNullOrWhiteSpace(installerScriptPath) ? "<unresolved>" : installerScriptPath)}");
            logger.Error($"setup_result=failed category={ToLogCategory(ex.Kind)} reason={SanitizeForLog(ex.LogDetails)}");
            if (ex.InstallerExitCode.HasValue)
            {
                logger.Error($"installer_exit_code={ex.InstallerExitCode.Value}");
            }

            exitCode = (int)ex.Kind;
            if (!options.CheckOnly && !options.DryRun)
            {
                ShowFailure(ex.UserMessage);
            }

            return exitCode;
        }
        catch (Exception ex)
        {
            logger.Info($"app_root={(string.IsNullOrWhiteSpace(packageRoot) ? "<unresolved>" : packageRoot)}");
            logger.Info($"installer_script_path={(string.IsNullOrWhiteSpace(installerScriptPath) ? "<unresolved>" : installerScriptPath)}");
            logger.Error($"setup_result=failed category=unexpected_failure reason={SanitizeForLog(ex.Message)}");

            exitCode = (int)SetupExitCode.UnexpectedFailure;
            if (!options.CheckOnly && !options.DryRun)
            {
                ShowFailure("Setup failed unexpectedly. Please review setup_bootstrap.log.");
            }

            return exitCode;
        }
        finally
        {
            logger.Info($"exit_code={exitCode}");
        }
    }

    private static string ResolveInstallRoot(string packageRoot, string? installDirectory)
    {
        if (!string.IsNullOrWhiteSpace(installDirectory))
        {
            return Path.GetFullPath(installDirectory, packageRoot);
        }

        var configPath = Path.Combine(packageRoot, InstallConfigRelativePath);
        if (!File.Exists(configPath))
        {
            return packageRoot;
        }

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            var installRootMode = GetJsonString(root, "install_root_mode")?.Trim().ToLowerInvariant() ?? "programfiles";
            var installRootName = GetJsonString(root, "install_root_name");
            if (string.IsNullOrWhiteSpace(installRootName))
            {
                installRootName = "LoneWolf Fang free";
            }

            return installRootMode switch
            {
                "user_profile" => Path.GetFullPath(Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                    installRootName)),
                "localappdata" => Path.GetFullPath(Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    installRootName)),
                "programfiles" => Path.GetFullPath(Path.Combine(
                    GetProgramFilesRoot(),
                    installRootName)),
                _ => packageRoot,
            };
        }
        catch
        {
            return packageRoot;
        }
    }

    private static string[] BuildInstallerArguments(string installerScriptPath, string installRoot, SetupOptions options)
    {
        var arguments = new List<string>
        {
            "-NoLogo",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            installerScriptPath,
        };

        if (!string.IsNullOrWhiteSpace(options.InstallDirectory))
        {
            arguments.Add("-InstallRoot");
            arguments.Add(installRoot);
        }

        if (options.DesktopShortcutMode == DesktopShortcutMode.Skip)
        {
            arguments.Add("-SkipShortcut");
        }

        if (options.Force)
        {
            arguments.Add("-Force");
        }

        return arguments.ToArray();
    }

    private static int RunInstaller(string packageRoot, string powerShellPath, IReadOnlyList<string> bridgedArgs)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = powerShellPath,
            Arguments = BuildArgumentString(bridgedArgs),
            WorkingDirectory = packageRoot,
            UseShellExecute = true,
        };

        try
        {
            using var process = Process.Start(startInfo);
            if (process is null)
            {
                throw SetupException.InstallFailed(
                    "Setup failed. Please review the installer window and setup_bootstrap.log.",
                    "Process.Start returned null for the installer process.");
            }

            process.WaitForExit();
            return process.ExitCode;
        }
        catch (SetupException)
        {
            throw;
        }
        catch (Win32Exception ex)
        {
            throw SetupException.InstallFailed(
                "Setup failed. Please review the installer window and setup_bootstrap.log.",
                ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            throw SetupException.InstallFailed(
                "Setup failed. Please review the installer window and setup_bootstrap.log.",
                ex.Message);
        }
    }

    private static void StartInstalledApp(string installRoot, FileLogger logger)
    {
        var candidatePaths = new[]
        {
            Path.Combine(installRoot, DefaultStartLauncherRelativePath),
            Path.Combine(installRoot, SecondaryStartLauncherRelativePath),
            Path.Combine(installRoot, TertiaryStartLauncherRelativePath),
        };

        var launcherPath = candidatePaths.FirstOrDefault(File.Exists);
        if (string.IsNullOrWhiteSpace(launcherPath))
        {
            throw SetupException.StartAfterInstallFailed(
                "Setup finished, but the app could not be started automatically. Start it from the package folder.",
                $"No start-after-install launcher was found under: {installRoot}");
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = launcherPath,
            WorkingDirectory = installRoot,
            UseShellExecute = true,
        };

        try
        {
            using var process = Process.Start(startInfo);
            if (process is null)
            {
                throw SetupException.StartAfterInstallFailed(
                    "Setup finished, but the app could not be started automatically. Start it from the package folder.",
                    $"Process.Start returned null for launcher: {launcherPath}");
            }

            logger.Info($"start_after_install_path={launcherPath}");
            logger.Info($"start_after_install_pid={process.Id}");
        }
        catch (SetupException)
        {
            throw;
        }
        catch (Win32Exception ex)
        {
            throw SetupException.StartAfterInstallFailed(
                "Setup finished, but the app could not be started automatically. Start it from the package folder.",
                $"{launcherPath}: {ex.Message}");
        }
        catch (InvalidOperationException ex)
        {
            throw SetupException.StartAfterInstallFailed(
                "Setup finished, but the app could not be started automatically. Start it from the package folder.",
                $"{launcherPath}: {ex.Message}");
        }
    }

    private static void CleanupDesktopShortcut(FileLogger logger)
    {
        try
        {
            var desktopDirectory = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);
            if (string.IsNullOrWhiteSpace(desktopDirectory))
            {
                logger.Info("desktop_shortcut_cleanup=skipped reason=desktop_directory_unavailable");
                return;
            }

            var shortcutPath = Path.Combine(desktopDirectory, DesktopShortcutFileName);
            if (!File.Exists(shortcutPath))
            {
                logger.Info($"desktop_shortcut_cleanup=not_found path={shortcutPath}");
                return;
            }

            File.Delete(shortcutPath);
            logger.Info($"desktop_shortcut_cleanup=removed path={shortcutPath}");
        }
        catch (Exception ex)
        {
            logger.Error($"desktop_shortcut_cleanup=failed reason={SanitizeForLog(ex.Message)}");
        }
    }

    private static string? TryResolvePackageRoot(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);
        while (current is not null)
        {
            var installerScriptPath = Path.Combine(current.FullName, InstallerScriptRelativePath);
            var compatCmdPath = Path.Combine(current.FullName, InstallerCompatCmdRelativePath);
            if (File.Exists(installerScriptPath) && File.Exists(compatCmdPath))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        return null;
    }

    private static string? TryResolvePowerShellPath()
    {
        var windowsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
        if (!string.IsNullOrWhiteSpace(windowsDirectory))
        {
            var systemCandidate = Path.Combine(
                windowsDirectory,
                "System32",
                "WindowsPowerShell",
                "v1.0",
                "powershell.exe");

            if (File.Exists(systemCandidate))
            {
                return Path.GetFullPath(systemCandidate);
            }
        }

        return TryFindInPath("powershell.exe");
    }

    private static string? TryFindInPath(string fileName)
    {
        var pathValue = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathValue))
        {
            return null;
        }

        foreach (var entry in pathValue.Split(Path.PathSeparator))
        {
            if (string.IsNullOrWhiteSpace(entry))
            {
                continue;
            }

            try
            {
                var candidatePath = Path.Combine(entry.Trim(), fileName);
                if (File.Exists(candidatePath))
                {
                    return Path.GetFullPath(candidatePath);
                }
            }
            catch
            {
                // Ignore malformed PATH entries and continue.
            }
        }

        return null;
    }

    private static string ResolveLogPath(string fileName)
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (!string.IsNullOrWhiteSpace(localAppData))
        {
            return Path.Combine(localAppData, "LoneWolfFang", "logs", fileName);
        }

        return Path.Combine(Path.GetTempPath(), fileName);
    }

    private static string GetProgramFilesRoot()
    {
        var programW6432 = Environment.GetEnvironmentVariable("ProgramW6432");
        if (!string.IsNullOrWhiteSpace(programW6432))
        {
            return Path.GetFullPath(programW6432);
        }

        var programFiles = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
        if (!string.IsNullOrWhiteSpace(programFiles))
        {
            return Path.GetFullPath(programFiles);
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

    private static string ResolveExecutablePath()
    {
        var processPath = Environment.ProcessPath;
        if (!string.IsNullOrWhiteSpace(processPath))
        {
            return Path.GetFullPath(processPath);
        }

        using var currentProcess = Process.GetCurrentProcess();
        var fallbackPath = currentProcess.MainModule?.FileName;
        if (!string.IsNullOrWhiteSpace(fallbackPath))
        {
            return Path.GetFullPath(fallbackPath);
        }

        throw SetupException.RootUnresolved(
            "Setup could not resolve its executable path. Please extract the full package and try again.",
            "Unable to resolve the setup executable path.");
    }

    private static string BuildArgumentString(IEnumerable<string> arguments)
    {
        return string.Join(" ", arguments.Select(QuoteArgument));
    }

    private static string QuoteArgument(string argument)
    {
        if (string.IsNullOrEmpty(argument))
        {
            return "\"\"";
        }

        if (argument.IndexOfAny(new[] { ' ', '\t', '\n', '\v', '"' }) < 0)
        {
            return argument;
        }

        var builder = new StringBuilder();
        builder.Append('"');

        var backslashCount = 0;
        foreach (var character in argument)
        {
            if (character == '\\')
            {
                backslashCount++;
                continue;
            }

            if (character == '"')
            {
                builder.Append('\\', backslashCount * 2 + 1);
                builder.Append(character);
                backslashCount = 0;
                continue;
            }

            if (backslashCount > 0)
            {
                builder.Append('\\', backslashCount);
                backslashCount = 0;
            }

            builder.Append(character);
        }

        if (backslashCount > 0)
        {
            builder.Append('\\', backslashCount * 2);
        }

        builder.Append('"');
        return builder.ToString();
    }

    private static string FormatArguments(IEnumerable<string> arguments)
    {
        var items = arguments.ToArray();
        if (items.Length == 0)
        {
            return "<none>";
        }

        return string.Join(" ", items.Select(QuoteArgument));
    }

    private static string SanitizeForLog(string value)
    {
        return value
            .Replace("\r", " ", StringComparison.Ordinal)
            .Replace("\n", " ", StringComparison.Ordinal)
            .Trim();
    }

    private static void ShowFailure(string message)
    {
        MessageBoxW(IntPtr.Zero, message, MessageBoxTitle, MbOk | MbIconError);
    }

    [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern int MessageBoxW(IntPtr hWnd, string text, string caption, uint type);

    private sealed record SetupOptions(
        bool CheckOnly,
        bool DryRun,
        bool StartAfterInstall,
        bool Force,
        DesktopShortcutMode DesktopShortcutMode,
        string? InstallDirectory,
        bool IsValid,
        string InvalidReason)
    {
        public static SetupOptions Parse(string[] args)
        {
            var checkOnly = false;
            var dryRun = false;
            var startAfterInstall = false;
            var force = false;
            var desktopShortcutMode = DesktopShortcutMode.Auto;
            string? installDirectory = null;

            for (var index = 0; index < args.Length; index++)
            {
                var argument = args[index];
                if (string.Equals(argument, "--check-only", StringComparison.OrdinalIgnoreCase))
                {
                    checkOnly = true;
                    continue;
                }

                if (string.Equals(argument, "--dry-run", StringComparison.OrdinalIgnoreCase))
                {
                    dryRun = true;
                    continue;
                }

                if (string.Equals(argument, "--start-after-install", StringComparison.OrdinalIgnoreCase))
                {
                    startAfterInstall = true;
                    continue;
                }

                if (string.Equals(argument, "--force", StringComparison.OrdinalIgnoreCase))
                {
                    force = true;
                    continue;
                }

                if (TryReadOptionValue(args, ref index, argument, "--install-dir", out var installDirValue))
                {
                    if (string.IsNullOrWhiteSpace(installDirValue))
                    {
                        return Invalid(
                            checkOnly,
                            dryRun,
                            startAfterInstall,
                            force,
                            desktopShortcutMode,
                            installDirectory,
                            "Missing value for --install-dir.");
                    }

                    installDirectory = installDirValue;
                    continue;
                }

                if (TryReadOptionValue(args, ref index, argument, "--desktop-shortcut", out var shortcutModeValue))
                {
                    if (!TryParseDesktopShortcutMode(shortcutModeValue, out desktopShortcutMode))
                    {
                        return Invalid(
                            checkOnly,
                            dryRun,
                            startAfterInstall,
                            force,
                            desktopShortcutMode,
                            installDirectory,
                            $"Unsupported --desktop-shortcut value: {shortcutModeValue}");
                    }

                    continue;
                }

                return Invalid(
                    checkOnly,
                    dryRun,
                    startAfterInstall,
                    force,
                    desktopShortcutMode,
                    installDirectory,
                    $"Unsupported argument: {argument}");
            }

            return new SetupOptions(
                checkOnly,
                dryRun,
                startAfterInstall,
                force,
                desktopShortcutMode,
                installDirectory,
                true,
                string.Empty);
        }

        private static SetupOptions Invalid(
            bool checkOnly,
            bool dryRun,
            bool startAfterInstall,
            bool force,
            DesktopShortcutMode desktopShortcutMode,
            string? installDirectory,
            string reason)
        {
            return new SetupOptions(
                checkOnly,
                dryRun,
                startAfterInstall,
                force,
                desktopShortcutMode,
                installDirectory,
                false,
                reason);
        }

        private static bool TryReadOptionValue(
            string[] args,
            ref int index,
            string argument,
            string optionName,
            out string value)
        {
            if (argument.StartsWith(optionName + "=", StringComparison.OrdinalIgnoreCase))
            {
                value = argument.Substring(optionName.Length + 1);
                return true;
            }

            if (string.Equals(argument, optionName, StringComparison.OrdinalIgnoreCase))
            {
                var nextIndex = index + 1;
                if (nextIndex >= args.Length)
                {
                    value = string.Empty;
                    return true;
                }

                index = nextIndex;
                value = args[nextIndex];
                return true;
            }

            value = string.Empty;
            return false;
        }

        private static bool TryParseDesktopShortcutMode(string value, out DesktopShortcutMode mode)
        {
            if (string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
            {
                mode = DesktopShortcutMode.Auto;
                return true;
            }

            if (string.Equals(value, "skip", StringComparison.OrdinalIgnoreCase))
            {
                mode = DesktopShortcutMode.Skip;
                return true;
            }

            mode = DesktopShortcutMode.Auto;
            return false;
        }
    }

    private sealed class FileLogger
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
            var line = $"{DateTimeOffset.Now:yyyy-MM-ddTHH:mm:sszzz} [{level}] {SanitizeForLog(message)}";

            try
            {
                lock (_sync)
                {
                    File.AppendAllText(LogPath, line + Environment.NewLine);
                }
            }
            catch
            {
                // Logging must never block setup execution.
            }
        }

        private static string EnsureLogPath(string logPath)
        {
            try
            {
                var directory = Path.GetDirectoryName(logPath);
                if (string.IsNullOrWhiteSpace(directory))
                {
                    return logPath;
                }

                Directory.CreateDirectory(directory);
                return Path.GetFullPath(logPath);
            }
            catch
            {
                return logPath;
            }
        }
    }

    private sealed class SetupException : Exception
    {
        private SetupException(
            SetupExitCode kind,
            string userMessage,
            string logDetails,
            int? installerExitCode = null)
            : base(logDetails)
        {
            Kind = kind;
            UserMessage = userMessage;
            LogDetails = logDetails;
            InstallerExitCode = installerExitCode;
        }

        public SetupExitCode Kind { get; }

        public string UserMessage { get; }

        public string LogDetails { get; }

        public int? InstallerExitCode { get; }

        public static SetupException RootUnresolved(string userMessage, string logDetails)
        {
            return new SetupException(SetupExitCode.RootUnresolved, userMessage, logDetails);
        }

        public static SetupException InstallerScriptMissing(string userMessage, string logDetails)
        {
            return new SetupException(SetupExitCode.InstallerScriptMissing, userMessage, logDetails);
        }

        public static SetupException PowershellMissing(string userMessage, string logDetails)
        {
            return new SetupException(SetupExitCode.PowershellMissing, userMessage, logDetails);
        }

        public static SetupException UnsupportedArguments(string userMessage, string logDetails)
        {
            return new SetupException(SetupExitCode.UnsupportedArguments, userMessage, logDetails);
        }

        public static SetupException InstallFailed(string userMessage, string logDetails, int? installerExitCode = null)
        {
            return new SetupException(SetupExitCode.InstallFailed, userMessage, logDetails, installerExitCode);
        }

        public static SetupException StartAfterInstallFailed(string userMessage, string logDetails)
        {
            return new SetupException(SetupExitCode.StartAfterInstallFailed, userMessage, logDetails);
        }
    }

    private enum DesktopShortcutMode
    {
        Auto,
        Skip,
    }

    private enum SetupExitCode
    {
        Success = 0,
        RootUnresolved = 2,
        InstallerScriptMissing = 3,
        PowershellMissing = 4,
        UnsupportedArguments = 5,
        InstallFailed = 6,
        StartAfterInstallFailed = 7,
        UnexpectedFailure = 8,
    }

    private static string ToLogCategory(SetupExitCode code)
    {
        return code switch
        {
            SetupExitCode.RootUnresolved => "root_unresolved",
            SetupExitCode.InstallerScriptMissing => "installer_script_missing",
            SetupExitCode.PowershellMissing => "powershell_missing",
            SetupExitCode.UnsupportedArguments => "unsupported_arguments",
            SetupExitCode.InstallFailed => "install_failed",
            SetupExitCode.StartAfterInstallFailed => "start_after_install_failed",
            SetupExitCode.UnexpectedFailure => "unexpected_failure",
            _ => "success",
        };
    }
}



