# BUILD_ID: 2026-05-23_free_trial_layer2_ui_cta_impl_v1

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCT_CATALOG = REPO_ROOT / "lwf-site" / "src" / "data" / "product-catalog.mjs"
SITE_NAVIGATION = REPO_ROOT / "lwf-site" / "src" / "data" / "site-navigation.mjs"

REQUIRED_SAFE_LABELS = (
    "Preview Standard Trial",
    "Preview Aggressive Trial",
    "Open Starter Pack",
    "Read Trial Safety Notes",
    "View Sample Artifact",
    "Compare Standard and Aggressive",
    "View Hybrid Trial Placeholder",
    "View AI Trading Trial Placeholder",
)

FORBIDDEN_IMPLEMENTATION_TOKENS = (
    "FREE_AGGRESSIVE_TRIAL_COMMAND",
    "runner.py",
    "--free-trial",
    "command:",
    "launchesTrialRunner: true",
    "walletConnection",
    "Connect Exchange",
    "Start LIVE",
    "Start PAPER",
    "Place Order",
    "Enable Runtime",
    "Deploy Now",
    "Pay Now",
    "Public Radar",
    "trading signal",
    "Aggressive coming soon",
    "Aggressive planned",
    "Aggressive unreleased",
    "Aggressive unavailable",
)


def _read_site_text() -> str:
    return "\n".join(
        [
            PRODUCT_CATALOG.read_text(encoding="utf-8"),
            SITE_NAVIGATION.read_text(encoding="utf-8"),
        ]
    )


def test_free_trial_layer2_ctas_are_static_previews() -> None:
    text = _read_site_text()

    for label in REQUIRED_SAFE_LABELS:
        assert label in text

    assert "Aggressive is available" in text
    assert "Execution features require separate explicit approval" in text
    assert "Exchange connection is not part of this preview" in text
    assert "PAPER and LIVE are not enabled" in text
    assert "Runtime is not connected" in text
    assert "Billing and deploy are not part of the preview" in text
    assert "launchesTrialRunner: false" in text
    assert "runtimeConnected: false" in text
    assert "orderExecutionAllowed: false" in text
    assert "privateApiAllowed: false" in text
    assert "billingOperationAllowed: false" in text
    assert "productionDeployAllowed: false" in text


def test_free_trial_layer2_ctas_do_not_link_to_execution() -> None:
    text = _read_site_text()

    for token in FORBIDDEN_IMPLEMENTATION_TOKENS:
        assert token not in text
