using System.Drawing;
using System.Windows.Forms;

namespace LoneWolfFangSetupBootstrapper;

internal sealed class FailureDialog : Form
{
    private const int DefaultClientWidth = 620;
    private const int DefaultClientHeight = 280;
    private const int ScreenEdgeMargin = 32;
    private const int FooterHorizontalPadding = 20;
    private const int FooterMinimumGap = 24;

    private readonly Button _openLogButton;
    private readonly Button _closeButton;

    public FailureDialog(
        UiLocalizer localizer,
        string productDisplayName,
        int exitCode,
        string logFolderPath,
        string failureSummary)
    {
        Text = localizer.Get("SetupFailureTitle");
        FormBorderStyle = FormBorderStyle.FixedDialog;
        StartPosition = FormStartPosition.CenterParent;
        MaximizeBox = false;
        MinimizeBox = false;
        ShowInTaskbar = false;
        ClientSize = new Size(DefaultClientWidth, DefaultClientHeight);

        var footerPanel = new Panel
        {
            Dock = DockStyle.Bottom,
            Height = 72,
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

        var summaryLabel = new Label
        {
            AutoSize = false,
            Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right,
            Location = new Point(20, 18),
            Size = new Size(580, 52),
            Font = new Font("Segoe UI", 11F, FontStyle.Bold),
            Text = localizer.Format("SetupFailureSummary", productDisplayName),
        };

        var detailsBox = new TextBox
        {
            Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right,
            Location = new Point(20, 78),
            Size = new Size(580, 122),
            Multiline = true,
            ReadOnly = true,
            ScrollBars = ScrollBars.Vertical,
            Text =
                $"{failureSummary}{Environment.NewLine}{Environment.NewLine}" +
                $"{localizer.Format("SetupFailureExitCode", exitCode)}{Environment.NewLine}" +
                $"{localizer.Format("SetupFailureLogPath", logFolderPath)}",
        };

        _openLogButton = new Button
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(0),
            MinimumSize = new Size(140, 34),
            Text = localizer.Get("OpenLogFolder"),
        };
        _openLogButton.Click += (_, _) =>
        {
            if (!BootstrapRuntime.TryOpenFolder(logFolderPath))
            {
                MessageBox.Show(
                    localizer.Get("OpenLogFolderFailed"),
                    localizer.Get("SetupFailureTitle"),
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Warning);
            }
        };

        _closeButton = new Button
        {
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            Margin = new Padding(0),
            MinimumSize = new Size(120, 34),
            Text = localizer.Get("Close"),
            DialogResult = DialogResult.OK,
        };

        footerLayout.Controls.Add(_openLogButton, 0, 0);
        footerLayout.Controls.Add(_closeButton, 2, 0);
        footerPanel.Controls.Add(footerLayout);

        Controls.Add(summaryLabel);
        Controls.Add(detailsBox);
        Controls.Add(footerPanel);

        AcceptButton = _closeButton;
        Shown += FailureDialog_Shown;
    }

    private void ApplyPreferredClientSize()
    {
        var desiredClientSize = new Size(GetDesiredClientWidth(), DefaultClientHeight);
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

    private int GetDesiredClientWidth()
    {
        return Math.Max(
            DefaultClientWidth,
            (FooterHorizontalPadding * 2) +
            GetPreferredWidth(_openLogButton) +
            FooterMinimumGap +
            GetPreferredWidth(_closeButton));
    }

    private void FailureDialog_Shown(object? sender, EventArgs e)
    {
        ApplyPreferredClientSize();
    }

    private static int GetPreferredWidth(Control control)
    {
        return Math.Max(control.MinimumSize.Width, control.GetPreferredSize(Size.Empty).Width);
    }
}
