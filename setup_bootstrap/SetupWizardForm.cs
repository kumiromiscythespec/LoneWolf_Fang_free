using System.Drawing;
using System.Globalization;
using System.Windows.Forms;

namespace LoneWolfFangSetupBootstrapper;

internal sealed class SetupWizardForm : Form, ISetupObserver
{
    private const int DefaultClientWidth = 900;
    private const int ScreenEdgeMargin = 32;
    private const int CollapsedHeight = 560;
    private const int ExpandedHeight = 780;
    private const int FooterHorizontalPadding = 30;
    private const int FooterMinimumGap = 24;

    private readonly SetupPreparation _preparation;
    private readonly BootstrapCliOptions _options;
    private readonly FileLogger _logger;

    private UiLanguage _selectedLanguage;
    private UiLocalizer _localizer;
    private bool _detailsExpanded;
    private bool _installRunning;
    private SetupPhase _currentPhase = SetupPhase.Ready;
    private string? _summaryResourceKey;
    private object[] _summaryResourceArgs = Array.Empty<object>();
    private string? _detailSummary;

    private readonly Label _bannerProductLabel;
    private readonly Label _bannerVariantLabel;
    private readonly Label _welcomeLabel;
    private readonly Label _welcomeBodyLabel;
    private readonly Label _languageLabel;
    private readonly ComboBox _languageComboBox;
    private readonly Label _installLocationLabel;
    private readonly TextBox _installPathTextBox;
    private readonly Button _browseButton;
    private readonly CheckBox _desktopShortcutCheckBox;
    private readonly CheckBox _startAfterInstallCheckBox;
    private readonly Label _licenseNoteLabel;
    private readonly ProgressBar _progressBar;
    private readonly Label _statusLabel;
    private readonly Label _summaryLabel;
    private readonly LinkLabel _detailsLinkLabel;
    private readonly Panel _detailsPanel;
    private readonly Label _detailsHeaderLabel;
    private readonly TextBox _detailsTextBox;
    private readonly FlowLayoutPanel _footerActionsPanel;
    private readonly Button _openLogFolderButton;
    private readonly Button _installButton;
    private readonly Button _cancelButton;

    public SetupWizardForm(
        SetupPreparation preparation,
        BootstrapCliOptions options,
        FileLogger logger)
    {
        _preparation = preparation;
        _options = options;
        _logger = logger;
        _selectedLanguage = options.InitialLanguage;
        _localizer = new UiLocalizer(ResolveCulture(options.InitialLanguage));

        Font = new Font("Segoe UI", 9F);
        FormBorderStyle = FormBorderStyle.FixedDialog;
        MaximizeBox = false;
        MinimizeBox = true;
        StartPosition = FormStartPosition.CenterScreen;
        ClientSize = new Size(DefaultClientWidth, CollapsedHeight);

        var bannerPanel = new Panel
        {
            Dock = DockStyle.Left,
            Width = 220,
            BackColor = Color.FromArgb(20, 76, 120),
        };

        _bannerProductLabel = new Label
        {
            AutoSize = false,
            ForeColor = Color.White,
            Font = new Font("Segoe UI Semibold", 20F, FontStyle.Bold),
            Location = new Point(24, 42),
            Size = new Size(172, 118),
        };

        _bannerVariantLabel = new Label
        {
            AutoSize = false,
            ForeColor = Color.FromArgb(220, 235, 248),
            Font = new Font("Segoe UI", 10F, FontStyle.Regular),
            Location = new Point(24, 168),
            Size = new Size(172, 80),
        };

        bannerPanel.Controls.Add(_bannerProductLabel);
        bannerPanel.Controls.Add(_bannerVariantLabel);

        var footerPanel = new Panel
        {
            Dock = DockStyle.Bottom,
            Height = 84,
            BackColor = Color.FromArgb(244, 246, 248),
            Padding = new Padding(FooterHorizontalPadding, 18, FooterHorizontalPadding, 18),
        };

        var footerLayout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 3,
            RowCount = 1,
            BackColor = footerPanel.BackColor,
        };
        footerLayout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        footerLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
        footerLayout.ColumnStyles.Add(new ColumnStyle(SizeType.AutoSize));
        footerLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));

        _footerActionsPanel = new FlowLayoutPanel
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            FlowDirection = FlowDirection.LeftToRight,
            WrapContents = false,
            Margin = new Padding(0),
            Padding = new Padding(0),
            Anchor = AnchorStyles.Right,
        };

        _openLogFolderButton = new Button
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(0),
            MinimumSize = new Size(140, 34),
            Visible = false,
        };
        _openLogFolderButton.Click += OpenLogFolderButton_Click;

        _cancelButton = new Button
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(12, 0, 0, 0),
            MinimumSize = new Size(120, 34),
        };
        _cancelButton.Click += CancelButton_Click;

        _installButton = new Button
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(0),
            MinimumSize = new Size(120, 34),
        };
        _installButton.Click += InstallButton_Click;

        _footerActionsPanel.Controls.Add(_installButton);
        _footerActionsPanel.Controls.Add(_cancelButton);
        footerLayout.Controls.Add(_openLogFolderButton, 0, 0);
        footerLayout.Controls.Add(_footerActionsPanel, 2, 0);
        footerPanel.Controls.Add(footerLayout);

        _detailsPanel = new Panel
        {
            Dock = DockStyle.Bottom,
            Height = 210,
            Visible = false,
            Padding = new Padding(24, 8, 24, 16),
        };

        _detailsHeaderLabel = new Label
        {
            AutoSize = true,
            Dock = DockStyle.Top,
            Font = new Font("Segoe UI", 9.5F, FontStyle.Bold),
        };

        _detailsTextBox = new TextBox
        {
            Dock = DockStyle.Fill,
            Multiline = true,
            ScrollBars = ScrollBars.Vertical,
            ReadOnly = true,
            Font = new Font("Consolas", 9F),
        };

        _detailsPanel.Controls.Add(_detailsTextBox);
        _detailsPanel.Controls.Add(_detailsHeaderLabel);

        var contentPanel = new Panel
        {
            Dock = DockStyle.Fill,
            Padding = new Padding(28, 28, 28, 20),
            BackColor = Color.White,
            AutoScroll = true,
        };

        _welcomeLabel = new Label
        {
            AutoSize = false,
            Font = new Font("Segoe UI Semibold", 18F, FontStyle.Bold),
            Location = new Point(0, 0),
            Size = new Size(620, 42),
        };

        _welcomeBodyLabel = new Label
        {
            AutoSize = false,
            Location = new Point(0, 48),
            Size = new Size(620, 54),
        };

        _languageLabel = new Label
        {
            AutoSize = true,
            Location = new Point(0, 118),
        };

        _languageComboBox = new ComboBox
        {
            DropDownStyle = ComboBoxStyle.DropDownList,
            Location = new Point(0, 142),
            Width = 220,
        };
        _languageComboBox.Items.AddRange(new object[]
        {
            new LanguageChoice(UiLanguage.Auto, "Auto"),
            new LanguageChoice(UiLanguage.Japanese, "日本語"),
            new LanguageChoice(UiLanguage.English, "English"),
        });
        _languageComboBox.SelectedIndexChanged += LanguageComboBox_SelectedIndexChanged;

        _installLocationLabel = new Label
        {
            AutoSize = true,
            Location = new Point(0, 188),
        };

        _installPathTextBox = new TextBox
        {
            Location = new Point(0, 212),
            Width = 520,
        };

        _browseButton = new Button
        {
            Location = new Point(530, 210),
            Size = new Size(90, 28),
        };
        _browseButton.Click += BrowseButton_Click;

        _desktopShortcutCheckBox = new CheckBox
        {
            AutoSize = true,
            Location = new Point(0, 254),
        };

        _startAfterInstallCheckBox = new CheckBox
        {
            AutoSize = true,
            Location = new Point(0, 284),
        };

        _licenseNoteLabel = new Label
        {
            AutoSize = false,
            ForeColor = Color.FromArgb(125, 76, 0),
            Location = new Point(0, 322),
            Size = new Size(620, 34),
        };

        _progressBar = new ProgressBar
        {
            Location = new Point(0, 374),
            Size = new Size(620, 18),
        };

        _statusLabel = new Label
        {
            AutoSize = false,
            Font = new Font("Segoe UI", 10F, FontStyle.Bold),
            Location = new Point(0, 406),
            Size = new Size(620, 24),
        };

        _summaryLabel = new Label
        {
            AutoSize = false,
            Location = new Point(0, 436),
            Size = new Size(620, 72),
        };

        _detailsLinkLabel = new LinkLabel
        {
            AutoSize = true,
            Location = new Point(0, 512),
        };
        _detailsLinkLabel.LinkClicked += DetailsLinkLabel_LinkClicked;

        contentPanel.Controls.Add(_welcomeLabel);
        contentPanel.Controls.Add(_welcomeBodyLabel);
        contentPanel.Controls.Add(_languageLabel);
        contentPanel.Controls.Add(_languageComboBox);
        contentPanel.Controls.Add(_installLocationLabel);
        contentPanel.Controls.Add(_installPathTextBox);
        contentPanel.Controls.Add(_browseButton);
        contentPanel.Controls.Add(_desktopShortcutCheckBox);
        contentPanel.Controls.Add(_startAfterInstallCheckBox);
        contentPanel.Controls.Add(_licenseNoteLabel);
        contentPanel.Controls.Add(_progressBar);
        contentPanel.Controls.Add(_statusLabel);
        contentPanel.Controls.Add(_summaryLabel);
        contentPanel.Controls.Add(_detailsLinkLabel);

        Controls.Add(contentPanel);
        Controls.Add(_detailsPanel);
        Controls.Add(footerPanel);
        Controls.Add(bannerPanel);

        _installPathTextBox.Text = string.IsNullOrWhiteSpace(options.ExplicitInstallDir)
            ? preparation.SuggestedInstallRoot
            : options.ExplicitInstallDir;
        _desktopShortcutCheckBox.Checked = options.DesktopShortcutMode != DesktopShortcutMode.Skip;
        _startAfterInstallCheckBox.Checked = options.StartAfterInstall;

        var initialChoice = _languageComboBox.Items
            .Cast<LanguageChoice>()
            .FirstOrDefault(choice => choice.Language == options.InitialLanguage)
            ?? _languageComboBox.Items.Cast<LanguageChoice>().First(choice => choice.Language == UiLanguage.Auto);
        _languageComboBox.SelectedItem = initialChoice;

        ApplyLocalization();
        ApplyPreferredClientSize(CollapsedHeight);
        AppendDetail($"BUILD_ID={Program.BuildId}");
        AppendDetail($"product_code={preparation.Product.Code}");
        AppendDetail($"setup_log={preparation.SetupLogPath}");
        AppendDetail($"backend_log={preparation.BackendLogPath}");

        FormClosing += SetupWizardForm_FormClosing;
        Shown += SetupWizardForm_Shown;
    }

    public int ExitCode { get; private set; }

    public void SetPhase(SetupPhase phase)
    {
        if (InvokeRequired)
        {
            BeginInvoke(() => SetPhase(phase));
            return;
        }

        _currentPhase = phase;
        _statusLabel.Text = _localizer.GetPhaseText(phase);
    }

    public void AppendDetail(string message)
    {
        if (InvokeRequired)
        {
            BeginInvoke(() => AppendDetail(message));
            return;
        }

        if (string.IsNullOrWhiteSpace(message))
        {
            return;
        }

        foreach (var line in message.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None))
        {
            _detailsTextBox.AppendText(line + Environment.NewLine);
        }
    }

    private void ApplyLocalization()
    {
        Text = _localizer.Format("SetupWindowTitle", _preparation.Product.SetupDisplayName);
        _bannerProductLabel.Text = "LoneWolf\nFang";
        _bannerVariantLabel.Text = _preparation.Product.SetupDisplayName;
        _welcomeLabel.Text = _localizer.Get("Welcome");
        _welcomeBodyLabel.Text = _localizer.Format("WelcomeBody", _preparation.Product.SetupDisplayName);
        _languageLabel.Text = _localizer.Get("Language");
        _installLocationLabel.Text = _localizer.Get("InstallLocation");
        _browseButton.Text = _localizer.Get("Browse");
        _desktopShortcutCheckBox.Text = _localizer.Get("CreateDesktopShortcut");
        _startAfterInstallCheckBox.Text = _localizer.Get("StartAfterInstallation");
        _licenseNoteLabel.Text = _preparation.Product.RequiresLicenseActivation
            ? _localizer.Get("LicenseActivationRequired")
            : string.Empty;
        _detailsHeaderLabel.Text = _localizer.Get("DetailsHeader");
        _detailsLinkLabel.Text = _detailsExpanded ? _localizer.Get("HideDetails") : _localizer.Get("ShowDetails");
        _openLogFolderButton.Text = _localizer.Get("OpenLogFolder");
        _installButton.Text = _installRunning ? _localizer.Get("Installing") : _localizer.Get("Install");
        _cancelButton.Text = _summaryResourceKey is null ? _localizer.Get("Cancel") : _localizer.Get("Close");
        _statusLabel.Text = _localizer.GetPhaseText(_currentPhase);

        RefreshSummaryLabel();

        if (IsHandleCreated)
        {
            ApplyPreferredClientSize(_detailsExpanded ? ExpandedHeight : CollapsedHeight);
        }
    }

    private void RefreshSummaryLabel()
    {
        var lines = new List<string>();
        if (!string.IsNullOrWhiteSpace(_summaryResourceKey))
        {
            lines.Add(_localizer.Format(_summaryResourceKey, _summaryResourceArgs));
        }

        if (!string.IsNullOrWhiteSpace(_detailSummary))
        {
            lines.Add(_detailSummary);
        }

        if ((_summaryResourceKey == "SetupSuccessSummary") && _preparation.Product.RequiresLicenseActivation)
        {
            lines.Add(_localizer.Get("SetupSuccessLicenseNote"));
        }

        _summaryLabel.Text = string.Join(Environment.NewLine, lines.Where(line => !string.IsNullOrWhiteSpace(line)));
    }

    private void ApplyPreferredClientSize(int targetClientHeight)
    {
        var desiredClientSize = new Size(GetDesiredClientWidth(), targetClientHeight);
        if (!IsHandleCreated)
        {
            ClientSize = desiredClientSize;
            return;
        }

        var workingArea = Screen.FromHandle(Handle).WorkingArea;
        var maximumWindowWidth = workingArea.Width - ScreenEdgeMargin;
        var maximumWindowHeight = workingArea.Height - ScreenEdgeMargin;

        if (maximumWindowWidth <= 0 || maximumWindowHeight <= 0)
        {
            ClientSize = desiredClientSize;
            return;
        }

        var desiredWindowSize = SizeFromClientSize(desiredClientSize);
        Size = new Size(
            Math.Min(desiredWindowSize.Width, maximumWindowWidth),
            Math.Min(desiredWindowSize.Height, maximumWindowHeight));
        PerformLayout();
    }

    private async void InstallButton_Click(object? sender, EventArgs e)
    {
        if (_installRunning)
        {
            return;
        }

        if (!TryNormalizeInstallRoot(_installPathTextBox.Text, out var installRoot))
        {
            MessageBox.Show(
                _localizer.Get("InvalidInstallDirectory"),
                Text,
                MessageBoxButtons.OK,
                MessageBoxIcon.Warning);
            return;
        }

        _installPathTextBox.Text = installRoot;

        if (!_options.Force &&
            BootstrapRuntime.DirectoryHasExistingFiles(installRoot, _preparation.LocalPackageRoot))
        {
            var overwritePrompt = string.Join(
                Environment.NewLine + Environment.NewLine,
                _localizer.Get("InstallFolderPromptBody"),
                _localizer.Format("InstallFolderPromptTarget", installRoot),
                _localizer.Get("ContinueAndRefreshFiles"));

            if (MessageBox.Show(
                    overwritePrompt,
                    _localizer.Get("InstallFolderPromptTitle"),
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Warning) != DialogResult.Yes)
            {
                SetPhase(SetupPhase.Ready);
                return;
            }
        }

        try
        {
            SetPhase(SetupPhase.CheckingRunningProcesses);
            var pythonProcesses = await BootstrapRuntime.QueryPythonProcessesAsync(
                installRoot,
                _logger,
                CancellationToken.None);

            if (pythonProcesses.Count > 0)
            {
                EnsureDetailsExpanded();
                AppendDetail(_localizer.Get("RunningPythonProcessesDetected"));
                AppendPythonProcesses(pythonProcesses);

                if (!_options.Force)
                {
                    var processPrompt = string.Join(
                        Environment.NewLine + Environment.NewLine,
                        _localizer.Get("RunningPythonProcessesDetected"),
                        _localizer.Format("PythonPromptTarget", installRoot),
                        _localizer.Format("PythonPromptCount", pythonProcesses.Count),
                        _localizer.Get("StopThemAndContinue"));

                    if (MessageBox.Show(
                            processPrompt,
                            _localizer.Get("PythonPromptTitle"),
                            MessageBoxButtons.YesNo,
                            MessageBoxIcon.Warning) != DialogResult.Yes)
                    {
                        SetPhase(SetupPhase.Ready);
                        return;
                    }
                }
            }

            BeginInstall();

            if (pythonProcesses.Count > 0)
            {
                SetPhase(SetupPhase.StoppingRunningProcesses);
                AppendDetail(_localizer.Get("StoppingRunningPythonProcesses"));

                var stopResult = await BootstrapRuntime.EnsurePythonProcessesStoppedAsync(
                    installRoot,
                    pythonProcesses,
                    _logger,
                    CancellationToken.None);
                AppendPythonProcessStopResult(stopResult);

                if (!stopResult.AllStopped)
                {
                    _logger.Error("python_process_preinstall_gate_blocked");
                    AppendDetail(_localizer.Get("SomePythonProcessesCouldNotBeStopped"));
                    AppendDetail(_localizer.Get("InstallationCannotContinueUntilThoseProcessesAreClosed"));
                    HandlePreinstallBlocked(stopResult);
                    return;
                }

                AppendDetail(_localizer.Get("PythonProcessesWereStoppedSuccessfully"));
            }

            _logger.Info("python_process_preinstall_gate_passed");
            SetPhase(SetupPhase.Ready);

            var result = await BootstrapRuntime.RunInstallAsync(
                _preparation,
                new InstallExecutionOptions(
                    installRoot,
                    _desktopShortcutCheckBox.Checked,
                    _startAfterInstallCheckBox.Checked,
                    _desktopShortcutCheckBox.Checked ? DesktopShortcutMode.Auto : DesktopShortcutMode.Skip,
                    _options.PackageUrl,
                    true),
                _logger,
                this,
                CancellationToken.None);

            ExitCode = result.ExitCode;
            _summaryResourceKey = "SetupSuccessSummary";
            _summaryResourceArgs = new object[] { _preparation.Product.SetupDisplayName };
            _detailSummary = result.WarningMessage;
            _openLogFolderButton.Visible = true;
            _progressBar.Style = ProgressBarStyle.Continuous;
            _progressBar.Value = 100;
            _installRunning = false;
            _installButton.Visible = false;
            _cancelButton.Enabled = true;
            ApplyLocalization();
        }
        catch (BootstrapException ex)
        {
            HandleBootstrapFailure(ex);
        }
        catch (Exception ex)
        {
            HandleUnexpectedFailure(ex);
        }
    }

    private void HandleBootstrapFailure(BootstrapException ex)
    {
        ExitCode = ex.ExitCode;
        _currentPhase = SetupPhase.Failed;
        _summaryResourceKey = "SetupFailureSummary";
        _summaryResourceArgs = new object[] { _preparation.Product.SetupDisplayName };
        _detailSummary = ex.LogDetails;
        _openLogFolderButton.Visible = true;
        _progressBar.Style = ProgressBarStyle.Blocks;
        _progressBar.Value = 0;
        _installRunning = false;
        _installButton.Visible = false;
        _cancelButton.Enabled = true;
        ApplyLocalization();

        using var dialog = new FailureDialog(
            _localizer,
            _preparation.Product.SetupDisplayName,
            ex.ExitCode,
            _preparation.LogFolderPath,
            ex.LogDetails);
        dialog.ShowDialog(this);
    }

    private void HandleUnexpectedFailure(Exception ex)
    {
        ExitCode = 29;
        _currentPhase = SetupPhase.Failed;
        _summaryResourceKey = "SetupFailureSummary";
        _summaryResourceArgs = new object[] { _preparation.Product.SetupDisplayName };
        _detailSummary = ex.Message;
        _openLogFolderButton.Visible = true;
        _progressBar.Style = ProgressBarStyle.Blocks;
        _progressBar.Value = 0;
        _installRunning = false;
        _installButton.Visible = false;
        _cancelButton.Enabled = true;
        ApplyLocalization();

        using var dialog = new FailureDialog(
            _localizer,
            _preparation.Product.SetupDisplayName,
            ExitCode,
            _preparation.LogFolderPath,
            ex.Message);
        dialog.ShowDialog(this);
    }

    private static bool TryNormalizeInstallRoot(string rawPath, out string normalizedInstallRoot)
    {
        normalizedInstallRoot = string.Empty;
        if (string.IsNullOrWhiteSpace(rawPath))
        {
            return false;
        }

        try
        {
            normalizedInstallRoot = Path.GetFullPath(rawPath.Trim());
            return true;
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return false;
        }
    }

    private void AppendPythonProcesses(IEnumerable<PythonProcessInfo> processes)
    {
        foreach (var process in processes)
        {
            AppendDetail(FormatPythonProcessDetail(process));
        }
    }

    private void AppendPythonProcessStopResult(PythonProcessStopResult stopResult)
    {
        foreach (var attempt in stopResult.Attempts)
        {
            if (attempt.WouldStop)
            {
                AppendDetail(
                    $"would_stop_python_process pid={attempt.ProcessId} " +
                    $"path={FormatDetailField(attempt.ExecutablePath)} " +
                    $"cmd={FormatDetailField(attempt.CommandLine)}");
                continue;
            }

            if (attempt.Succeeded)
            {
                AppendDetail($"python_process_stop_succeeded pid={attempt.ProcessId}");
            }
            else
            {
                AppendDetail(
                    $"python_process_stop_failed pid={attempt.ProcessId} " +
                    $"reason={FormatDetailField(attempt.Reason)}");
            }
        }

        AppendDetail($"python_process_remaining_count={stopResult.RemainingProcesses.Count}");
        if (stopResult.RemainingProcesses.Count > 0)
        {
            AppendPythonProcesses(stopResult.RemainingProcesses);
        }
    }

    private void HandlePreinstallBlocked(PythonProcessStopResult stopResult)
    {
        ExitCode = 24;
        _currentPhase = SetupPhase.Failed;
        _summaryResourceKey = "SomePythonProcessesCouldNotBeStopped";
        _summaryResourceArgs = Array.Empty<object>();
        _detailSummary = _localizer.Get("InstallationCannotContinueUntilThoseProcessesAreClosed");
        _openLogFolderButton.Visible = true;
        _progressBar.Style = ProgressBarStyle.Blocks;
        _progressBar.Value = 0;
        _installRunning = false;
        _installButton.Visible = true;
        _installButton.Enabled = true;
        _cancelButton.Enabled = true;
        _browseButton.Enabled = true;
        _installPathTextBox.Enabled = true;
        _desktopShortcutCheckBox.Enabled = true;
        _startAfterInstallCheckBox.Enabled = true;
        _languageComboBox.Enabled = true;
        ApplyLocalization();

        var failureDetails = string.Join(
            Environment.NewLine,
            _localizer.Get("SomePythonProcessesCouldNotBeStopped"),
            _localizer.Get("InstallationCannotContinueUntilThoseProcessesAreClosed"),
            string.Empty,
            $"python_process_remaining_count={stopResult.RemainingProcesses.Count}",
            string.Join(Environment.NewLine, stopResult.RemainingProcesses.Select(FormatPythonProcessDetail)));

        using var dialog = new FailureDialog(
            _localizer,
            _preparation.Product.SetupDisplayName,
            ExitCode,
            _preparation.LogFolderPath,
            failureDetails);
        dialog.ShowDialog(this);
    }

    private void BeginInstall()
    {
        _installRunning = true;
        _summaryResourceKey = null;
        _summaryResourceArgs = Array.Empty<object>();
        _detailSummary = null;
        _progressBar.Style = ProgressBarStyle.Marquee;
        _openLogFolderButton.Visible = false;
        _installButton.Enabled = false;
        _cancelButton.Enabled = false;
        _browseButton.Enabled = false;
        _installPathTextBox.Enabled = false;
        _desktopShortcutCheckBox.Enabled = false;
        _startAfterInstallCheckBox.Enabled = false;
        _languageComboBox.Enabled = false;
        ApplyLocalization();
    }

    private void EnsureDetailsExpanded()
    {
        if (_detailsExpanded)
        {
            return;
        }

        _detailsExpanded = true;
        _detailsPanel.Visible = true;
        ApplyLocalization();
    }

    private void BrowseButton_Click(object? sender, EventArgs e)
    {
        using var dialog = new FolderBrowserDialog
        {
            Description = _localizer.Get("BrowseDialogDescription"),
            UseDescriptionForTitle = true,
            ShowNewFolderButton = true,
            InitialDirectory = _installPathTextBox.Text,
        };

        if (dialog.ShowDialog(this) == DialogResult.OK)
        {
            _installPathTextBox.Text = dialog.SelectedPath;
        }
    }

    private void LanguageComboBox_SelectedIndexChanged(object? sender, EventArgs e)
    {
        if (_languageComboBox.SelectedItem is not LanguageChoice choice)
        {
            return;
        }

        _selectedLanguage = choice.Language;
        _localizer = new UiLocalizer(ResolveCulture(choice.Language));
        ApplyLocalization();
    }

    private void DetailsLinkLabel_LinkClicked(object? sender, LinkLabelLinkClickedEventArgs e)
    {
        _detailsExpanded = !_detailsExpanded;
        _detailsPanel.Visible = _detailsExpanded;
        ApplyLocalization();
    }

    private int GetDesiredClientWidth()
    {
        return Math.Max(DefaultClientWidth, GetMinimumFooterClientWidth());
    }

    private int GetMinimumFooterClientWidth()
    {
        var actionsWidth = _footerActionsPanel.GetPreferredSize(Size.Empty).Width;
        var openLogWidth = _openLogFolderButton.Visible
            ? GetPreferredWidth(_openLogFolderButton)
            : 0;
        var gapWidth = _openLogFolderButton.Visible ? FooterMinimumGap : 0;
        return (FooterHorizontalPadding * 2) + openLogWidth + gapWidth + actionsWidth;
    }

    private void SetupWizardForm_Shown(object? sender, EventArgs e)
    {
        ApplyPreferredClientSize(_detailsExpanded ? ExpandedHeight : CollapsedHeight);
    }

    private void OpenLogFolderButton_Click(object? sender, EventArgs e)
    {
        if (!BootstrapRuntime.TryOpenFolder(_preparation.LogFolderPath))
        {
            MessageBox.Show(
                _localizer.Get("OpenLogFolderFailed"),
                Text,
                MessageBoxButtons.OK,
                MessageBoxIcon.Warning);
        }
    }

    private void CancelButton_Click(object? sender, EventArgs e)
    {
        if (_installRunning)
        {
            return;
        }

        Close();
    }

    private void SetupWizardForm_FormClosing(object? sender, FormClosingEventArgs e)
    {
        if (_installRunning)
        {
            e.Cancel = true;
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

    private sealed record LanguageChoice(UiLanguage Language, string Label)
    {
        public override string ToString()
        {
            return Label;
        }
    }

    private static string FormatPythonProcessDetail(PythonProcessInfo process)
    {
        return
            $"python_process pid={process.ProcessId} " +
            $"ppid={process.ParentProcessId} " +
            $"name={process.Name} " +
            $"path={FormatDetailField(process.ExecutablePath)} " +
            $"cmd={FormatDetailField(process.CommandLine)}";
    }

    private static string FormatDetailField(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? "<none>" : value.Trim();
    }

    private static int GetPreferredWidth(Control control)
    {
        return Math.Max(control.MinimumSize.Width, control.GetPreferredSize(Size.Empty).Width);
    }
}
