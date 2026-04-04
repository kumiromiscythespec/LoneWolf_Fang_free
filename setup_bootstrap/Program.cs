// BUILD_ID: 2026-04-03_setup_bootstrap_runtime_layout_free_v2
using System.Globalization;
using System.Windows.Forms;

namespace LoneWolfFangSetupBootstrapper;

internal static class Program
{
    internal const string BuildId = "2026-04-03_setup_bootstrap_runtime_layout_free_v2";

    [STAThread]
    private static int Main(string[] args)
    {
        var options = BootstrapCliOptions.Parse(args);
        var exePath = BootstrapRuntime.ResolveExecutablePath();
        var baseDirectory = BootstrapRuntime.ResolveBaseDirectory(exePath);
        var preparation = BootstrapRuntime.PrepareInitial(exePath, baseDirectory);
        var logger = new FileLogger(preparation.SetupLogPath);

        logger.Info($"setup_bootstrap start BUILD_ID={BuildId}");
        logger.Info($"exe_path={exePath}");
        logger.Info($"base_directory={baseDirectory}");
        logger.Info($"product_code={preparation.Product.Code}");
        logger.Info($"raw_args={BootstrapRuntime.FormatArguments(args)}");
        logger.Info($"setup_log_path={preparation.SetupLogPath}");

        try
        {
            if (!options.IsValid)
            {
                throw BootstrapException.InvalidArguments(
                    "Unsupported setup arguments were supplied.",
                    options.ValidationError ?? "Unknown argument parsing error.");
            }

            if (options.CheckOnly || options.DryRun)
            {
                return BootstrapRuntime.RunCommandMode(preparation, options, logger);
            }

            ApplicationConfiguration.Initialize();
            using var form = new SetupWizardForm(preparation, options, logger);
            Application.Run(form);
            return form.ExitCode;
        }
        catch (BootstrapException ex)
        {
            logger.Error($"failure_category={ex.Category}");
            logger.Error($"failure_details={BootstrapRuntime.SanitizeForLog(ex.LogDetails)}");
            logger.Error($"exit_code={ex.ExitCode}");

            if (!options.CheckOnly && !options.DryRun)
            {
                var culture = ResolveCulture(options.InitialLanguage);
                var localizer = new UiLocalizer(culture);
                MessageBox.Show(
                    localizer.Get("SetupFailureUnexpected"),
                    localizer.Format("SetupWindowTitle", preparation.Product.SetupDisplayName),
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
            }

            return ex.ExitCode;
        }
        catch (Exception ex)
        {
            logger.Error("failure_category=unexpected_error");
            logger.Error($"failure_details={BootstrapRuntime.SanitizeForLog(ex.Message)}");
            logger.Error("exit_code=29");

            if (!options.CheckOnly && !options.DryRun)
            {
                var culture = ResolveCulture(options.InitialLanguage);
                var localizer = new UiLocalizer(culture);
                MessageBox.Show(
                    localizer.Get("SetupFailureUnexpected"),
                    localizer.Format("SetupWindowTitle", preparation.Product.SetupDisplayName),
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
            }

            return 29;
        }
    }

    private static CultureInfo ResolveCulture(UiLanguage language)
    {
        return language switch
        {
            UiLanguage.Japanese => new CultureInfo("ja"),
            UiLanguage.English => new CultureInfo("en"),
            _ => BootstrapRuntime.GetDefaultUiCulture(),
        };
    }
}
