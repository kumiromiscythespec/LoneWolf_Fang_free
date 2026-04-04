namespace LoneWolfFangSetupBootstrapper;

internal sealed record BootstrapCliOptions(
    bool CheckOnly,
    bool DryRun,
    bool StartAfterInstall,
    bool Force,
    DesktopShortcutMode DesktopShortcutMode,
    string? ExplicitInstallDir,
    string? PackageUrl,
    UiLanguage InitialLanguage,
    bool IsValid,
    string? ValidationError)
{
    public static BootstrapCliOptions Parse(string[] args)
    {
        var checkOnly = false;
        var dryRun = false;
        var startAfterInstall = false;
        var force = false;
        var desktopShortcutMode = DesktopShortcutMode.Auto;
        string? explicitInstallDir = null;
        string? packageUrl = null;
        var initialLanguage = UiLanguage.Auto;

        for (var index = 0; index < args.Length; index++)
        {
            var arg = args[index];

            if (string.Equals(arg, "--check-only", StringComparison.OrdinalIgnoreCase))
            {
                checkOnly = true;
                continue;
            }

            if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
            {
                dryRun = true;
                continue;
            }

            if (string.Equals(arg, "--start-after-install", StringComparison.OrdinalIgnoreCase))
            {
                startAfterInstall = true;
                continue;
            }

            if (string.Equals(arg, "--force", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-Force", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-Force:$true", StringComparison.OrdinalIgnoreCase))
            {
                force = true;
                continue;
            }

            if (TryReadOptionValue(args, ref index, arg, "--install-dir", out var installDirValue, out var installDirError) ||
                TryReadOptionValue(args, ref index, arg, "-InstallRoot", out installDirValue, out installDirError))
            {
                if (!string.IsNullOrWhiteSpace(installDirError))
                {
                    return Invalid(installDirError);
                }

                explicitInstallDir = installDirValue;
                continue;
            }

            if (TryReadOptionValue(args, ref index, arg, "--desktop-shortcut", out var desktopShortcutValue, out var desktopShortcutError))
            {
                if (!string.IsNullOrWhiteSpace(desktopShortcutError))
                {
                    return Invalid(desktopShortcutError);
                }

                if (!TryParseDesktopShortcutMode(desktopShortcutValue, out desktopShortcutMode))
                {
                    return Invalid($"Unsupported --desktop-shortcut value: {desktopShortcutValue}");
                }

                continue;
            }

            if (string.Equals(arg, "-SkipDesktopShortcut", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-SkipDesktopShortcut:$true", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-SkipShortcut", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-SkipShortcut:$true", StringComparison.OrdinalIgnoreCase))
            {
                desktopShortcutMode = DesktopShortcutMode.Skip;
                continue;
            }

            if (string.Equals(arg, "-SkipDesktopShortcut:$false", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(arg, "-SkipShortcut:$false", StringComparison.OrdinalIgnoreCase))
            {
                desktopShortcutMode = DesktopShortcutMode.Auto;
                continue;
            }

            if (TryReadOptionValue(args, ref index, arg, "--package-url", out var packageUrlValue, out var packageUrlError) ||
                TryReadOptionValue(args, ref index, arg, "-PackageUrl", out packageUrlValue, out packageUrlError))
            {
                if (!string.IsNullOrWhiteSpace(packageUrlError))
                {
                    return Invalid(packageUrlError);
                }

                packageUrl = packageUrlValue;
                continue;
            }

            if (TryReadOptionValue(args, ref index, arg, "--language", out var languageValue, out var languageError))
            {
                if (!string.IsNullOrWhiteSpace(languageError))
                {
                    return Invalid(languageError);
                }

                if (!TryParseLanguage(languageValue, out initialLanguage))
                {
                    return Invalid($"Unsupported --language value: {languageValue}");
                }

                continue;
            }

            return Invalid($"Unsupported argument: {arg}");
        }

        return new BootstrapCliOptions(
            checkOnly,
            dryRun,
            startAfterInstall,
            force,
            desktopShortcutMode,
            explicitInstallDir,
            packageUrl,
            initialLanguage,
            true,
            null);
    }

    private static BootstrapCliOptions Invalid(string validationError)
    {
        return new BootstrapCliOptions(
            false,
            false,
            false,
            false,
            DesktopShortcutMode.Auto,
            null,
            null,
            UiLanguage.Auto,
            false,
            validationError);
    }

    private static bool TryReadOptionValue(
        string[] args,
        ref int index,
        string currentArgument,
        string optionName,
        out string? value,
        out string? validationError)
    {
        value = null;
        validationError = null;

        if (!currentArgument.StartsWith(optionName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.Equals(currentArgument, optionName, StringComparison.OrdinalIgnoreCase))
        {
            if (index + 1 >= args.Length)
            {
                validationError = $"Missing value for {optionName}.";
                return true;
            }

            value = args[index + 1];
            index++;
            return true;
        }

        var equalsPrefix = optionName + "=";
        if (currentArgument.StartsWith(equalsPrefix, StringComparison.OrdinalIgnoreCase))
        {
            value = currentArgument[equalsPrefix.Length..];
            return true;
        }

        var colonPrefix = optionName + ":";
        if (currentArgument.StartsWith(colonPrefix, StringComparison.OrdinalIgnoreCase))
        {
            value = currentArgument[colonPrefix.Length..];
            return true;
        }

        return false;
    }

    private static bool TryParseDesktopShortcutMode(string? value, out DesktopShortcutMode mode)
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

    private static bool TryParseLanguage(string? value, out UiLanguage language)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            string.Equals(value, "auto", StringComparison.OrdinalIgnoreCase))
        {
            language = UiLanguage.Auto;
            return true;
        }

        if (string.Equals(value, "ja", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(value, "jp", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(value, "japanese", StringComparison.OrdinalIgnoreCase))
        {
            language = UiLanguage.Japanese;
            return true;
        }

        if (string.Equals(value, "en", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(value, "english", StringComparison.OrdinalIgnoreCase))
        {
            language = UiLanguage.English;
            return true;
        }

        language = UiLanguage.Auto;
        return false;
    }
}
