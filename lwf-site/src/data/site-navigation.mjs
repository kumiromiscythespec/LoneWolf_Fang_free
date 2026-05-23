// BUILD_ID: 2026-05-23_free_trial_layer2_ui_cta_impl_v1

export const primaryNavigation = [
  {
    id: "free",
    label: "Free",
    href: "/free",
    children: [
      {
        id: "free-trial-strategies",
        label: "Trial Previews",
        href: "/free#trial-strategies",
      },
      {
        id: "free-standard-trial-preview",
        label: "Preview Standard Trial",
        href: "docs/free_aggressive_trial_ui_copy_cta_design.md#standard-trial-card-copy",
      },
      {
        id: "free-aggressive-trial",
        label: "Preview Aggressive Trial",
        href: "docs/free_aggressive_trial_ui_copy_cta_design.md#aggressive-trial-card-copy",
      },
      {
        id: "free-starter-pack",
        label: "Open Starter Pack",
        href: "docs/free_aggressive_trial_starter_pack_alignment.md",
      },
      {
        id: "free-trial-safety-notes",
        label: "Read Trial Safety Notes",
        href: "docs/free_aggressive_trial_ui_copy_safety_boundary.md",
      },
    ],
  },
];

export const freeTrialNavigation = {
  id: "free-trial-preview",
  planId: "free",
  strategyId: "aggressive",
  label: "Preview Aggressive Trial",
  href: "docs/free_aggressive_trial_ui_copy_cta_design.md#aggressive-trial-card-copy",
  requiresLogin: false,
  exchangeConnection: "not_part_of_preview",
  previewOnly: true,
  runtimeConnected: false,
  paperEnabled: false,
  liveEnabled: false,
  orderExecutionAllowed: false,
  privateApiAllowed: false,
  fetchBalanceAllowed: false,
  cancelAllowed: false,
  billingOperationAllowed: false,
  productionDeployAllowed: false,
  liveOrdersEnabled: false,
  launchesTrialRunner: false,
  helperText:
    "Aggressive is available, but this Free preview does not enable execution or order placement.",
  secondaryLinks: [
    {
      id: "standard-trial-preview",
      label: "Preview Standard Trial",
      href: "docs/free_aggressive_trial_ui_copy_cta_design.md#standard-trial-card-copy",
      targetType: "static_preview",
    },
    {
      id: "starter-pack",
      label: "Open Starter Pack",
      href: "docs/free_aggressive_trial_starter_pack_alignment.md",
      targetType: "starter_pack",
    },
    {
      id: "trial-safety-notes",
      label: "Read Trial Safety Notes",
      href: "docs/free_aggressive_trial_ui_copy_safety_boundary.md",
      targetType: "safety_notes",
    },
    {
      id: "sample-artifact",
      label: "View Sample Artifact",
      href: "tests/fixtures/free-aggressive-trial-ui-copy-cta/valid_ui_copy_cta_payload.json",
      targetType: "sample_artifact",
    },
  ],
};

export default {
  primaryNavigation,
  freeTrialNavigation,
};
