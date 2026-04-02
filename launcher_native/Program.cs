// BUILD_ID: 2026-03-31_free_native_launcher_delegate_ps1_v1
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace LoneWolfFangFreeLauncher;

internal static class Program
{
    private const string BuildId = "2026-04-02_free_native_launcher_argument_bridge_v1";
    private const string LauncherScriptName = "Launch_LoneWolf_Fang_Free_GUI.ps1";
    private const string AppMarkerRelativePath = @"app\app\cli\app_main.py";
    private const string MessageBoxTitle = "LoneWolf Fang free";
    private const uint MbOk = 0x00000000;
    private const uint MbIconError = 0x00000010;

    [STAThread]
    private static int Main(string[] args)
    {
        try
        {
            var scriptPath = ResolveLauncherScript();
            var powerShellPath = ResolvePowerShellPath();
            if (string.IsNullOrWhiteSpace(powerShellPath))
            {
                ShowFailure("Windows PowerShell was not found.");
                return 2;
            }

            var startInfo = new ProcessStartInfo
            {
                FileName = powerShellPath,
                WorkingDirectory = Path.GetDirectoryName(scriptPath) ?? AppContext.BaseDirectory,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden,
            };
            startInfo.ArgumentList.Add("-NoLogo");
            startInfo.ArgumentList.Add("-NoProfile");
            startInfo.ArgumentList.Add("-ExecutionPolicy");
            startInfo.ArgumentList.Add("Bypass");
            startInfo.ArgumentList.Add("-File");
            startInfo.ArgumentList.Add(scriptPath);
            foreach (var arg in args)
            {
                if (string.Equals(arg, "--check-only", StringComparison.OrdinalIgnoreCase))
                {
                    startInfo.ArgumentList.Add("-CheckOnly");
                    continue;
                }

                if (string.Equals(arg, "--foreground", StringComparison.OrdinalIgnoreCase))
                {
                    startInfo.ArgumentList.Add("-Foreground");
                    continue;
                }

                startInfo.ArgumentList.Add(arg);
            }

            using var process = Process.Start(startInfo);
            if (process is null)
            {
                ShowFailure("Launcher bootstrap could not be started.");
                return 3;
            }

            process.WaitForExit();
            return process.ExitCode;
        }
        catch (Exception ex) when (ex is Win32Exception or InvalidOperationException or FileNotFoundException)
        {
            ShowFailure($"Launcher failed to start. {ex.Message}");
            return 5;
        }
    }

    private static string ResolveLauncherScript()
    {
        var exeDirectory = AppContext.BaseDirectory;
        var current = new DirectoryInfo(exeDirectory);
        while (current is not null)
        {
            var scriptPath = Path.Combine(current.FullName, LauncherScriptName);
            var appMarkerPath = Path.Combine(current.FullName, AppMarkerRelativePath);
            if (File.Exists(scriptPath) && File.Exists(appMarkerPath))
            {
                return scriptPath;
            }

            current = current.Parent;
        }

        throw new FileNotFoundException($"Unable to resolve {LauncherScriptName} from {exeDirectory}.");
    }

    private static string? ResolvePowerShellPath()
    {
        var windowsDirectory = Environment.GetFolderPath(Environment.SpecialFolder.Windows);
        if (!string.IsNullOrWhiteSpace(windowsDirectory))
        {
            var candidate = Path.Combine(windowsDirectory, @"System32\WindowsPowerShell\v1.0\powershell.exe");
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        var pathValue = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathValue))
        {
            return null;
        }

        foreach (var entry in pathValue.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            try
            {
                var candidate = Path.Combine(entry.Trim(), "powershell.exe");
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
            catch
            {
            }
        }

        return null;
    }

    private static void ShowFailure(string message)
    {
        MessageBoxW(IntPtr.Zero, $"{message}\nBUILD_ID={BuildId}", MessageBoxTitle, MbOk | MbIconError);
    }

    [DllImport("user32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern int MessageBoxW(IntPtr hWnd, string text, string caption, uint type);
}
