// BUILD_ID: 2026-05-23_free_trial_layer2_ui_cta_impl_v1

const FREE_TRIAL_PREVIEW_TARGETS = Object.freeze({
  standard: "docs/free_aggressive_trial_ui_copy_cta_design.md#standard-trial-card-copy",
  aggressive: "docs/free_aggressive_trial_ui_copy_cta_design.md#aggressive-trial-card-copy",
  starterPack: "docs/free_aggressive_trial_starter_pack_alignment.md",
  safetyNotes: "docs/free_aggressive_trial_ui_copy_safety_boundary.md",
  sampleArtifact: "tests/fixtures/free-aggressive-trial-ui-copy-cta/valid_ui_copy_cta_payload.json",
  comparison: "docs/free_aggressive_trial_cta_flow_map.md",
  upgradeGuidance: "docs/free_aggressive_trial_owner_choice_gate.md",
  hybridPlaceholder: "docs/free_aggressive_trial_ui_copy_cta_design.md#future-hybrid-trial-placeholder-copy",
  aiTradingPlaceholder: "docs/free_aggressive_trial_ui_copy_cta_design.md#future-ai-trading-trial-placeholder-copy",
});

const FREE_TRIAL_PREVIEW_FLAGS = Object.freeze({
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
  launchesTrialRunner: false,
  ownerReviewRequired: true,
});

export const freeTrialCtas = [
  {
    id: "standard-trial-preview",
    label: "Preview Standard Trial",
    href: FREE_TRIAL_PREVIEW_TARGETS.standard,
    targetType: "static_preview",
    helperText: "Preview only. Execution and account operations remain disabled.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
  {
    id: "aggressive-trial-preview",
    label: "Preview Aggressive Trial",
    href: FREE_TRIAL_PREVIEW_TARGETS.aggressive,
    targetType: "static_preview",
    helperText: "Aggressive is available; this preview does not enable execution features.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
  {
    id: "starter-pack",
    label: "Open Starter Pack",
    href: FREE_TRIAL_PREVIEW_TARGETS.starterPack,
    targetType: "starter_pack",
    helperText: "Start with safety notes, comparison context, and sample artifact orientation.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
  {
    id: "trial-safety-notes",
    label: "Read Trial Safety Notes",
    href: FREE_TRIAL_PREVIEW_TARGETS.safetyNotes,
    targetType: "safety_notes",
    helperText: "Execution features require separate explicit approval.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
  {
    id: "sample-artifact",
    label: "View Sample Artifact",
    href: FREE_TRIAL_PREVIEW_TARGETS.sampleArtifact,
    targetType: "sample_artifact",
    helperText: "This sample artifact is static and safe to inspect.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
  {
    id: "compare-standard-aggressive",
    label: "Compare Standard and Aggressive",
    href: FREE_TRIAL_PREVIEW_TARGETS.comparison,
    targetType: "comparison",
    helperText: "Comparison is informational and is not trading advice.",
    ...FREE_TRIAL_PREVIEW_FLAGS,
  },
];

export const trialStrategies = [
  {
    id: "aggressive",
    planId: "free",
    name: "Aggressive Trial Preview",
    status: "available",
    availabilityLabel: "Aggressive is available",
    ...FREE_TRIAL_PREVIEW_FLAGS,
    executionMode: "none",
    requiresLogin: false,
    exchangeConnection: "not_part_of_preview",
    timeframes: {
      entry: "5m",
      filter: "1h",
    },
    dataSources: {
      ohlcv: ["5m CSV", "1h CSV", "5m->1h resample fallback"],
      indicators: ["EMA9", "EMA21", "RSI14", "ATR14"],
      precomputed: true,
    },
    riskControls: {
      maxOpenPositions: 1,
      notionalCeiling: "enabled",
      liveOrders: "not_enabled",
      paperMode: "not_enabled",
      breakEven: "disabled",
      takeProfitStopLoss: "enforced",
      trailingStop: "disabled",
    },
    diagnostics: {
      rangeEntryDiagLimit: 20,
      pullbackFunnel: true,
      buyRejectLogs: true,
      signalHoldLogs: true,
    },
    cta: {
      label: "Preview Aggressive Trial",
      href: FREE_TRIAL_PREVIEW_TARGETS.aggressive,
      targetType: "static_preview",
      variant: "primary",
      ...FREE_TRIAL_PREVIEW_FLAGS,
      helperText:
        "Aggressive is available, but this Free preview does not enable execution or order placement.",
    },
  },
  {
    id: "standard",
    planId: "free",
    name: "Standard Trial Preview",
    status: "available",
    availabilityLabel: "Preview available",
    ...FREE_TRIAL_PREVIEW_FLAGS,
    executionMode: "none",
    requiresLogin: false,
    exchangeConnection: "not_part_of_preview",
    cta: {
      label: "Preview Standard Trial",
      href: FREE_TRIAL_PREVIEW_TARGETS.standard,
      targetType: "static_preview",
      variant: "secondary",
      ...FREE_TRIAL_PREVIEW_FLAGS,
      helperText: "Preview only. Execution and account operations remain disabled.",
    },
  },
  {
    id: "hybrid",
    planId: "free",
    name: "Hybrid Trial Placeholder",
    status: "future_placeholder",
    availabilityLabel: "Future placeholder",
    ...FREE_TRIAL_PREVIEW_FLAGS,
    executionMode: "none",
    requiresLogin: false,
    exchangeConnection: "not_part_of_preview",
    cta: {
      label: "View Hybrid Trial Placeholder",
      href: FREE_TRIAL_PREVIEW_TARGETS.hybridPlaceholder,
      targetType: "docs",
      variant: "secondary",
      ...FREE_TRIAL_PREVIEW_FLAGS,
      helperText: "Placeholder only. Owner review is required before expansion.",
    },
  },
  {
    id: "ai-trading",
    planId: "free",
    name: "AI Trading Trial Placeholder",
    status: "future_placeholder",
    availabilityLabel: "Future placeholder",
    ...FREE_TRIAL_PREVIEW_FLAGS,
    executionMode: "none",
    requiresLogin: false,
    exchangeConnection: "not_part_of_preview",
    cta: {
      label: "View AI Trading Trial Placeholder",
      href: FREE_TRIAL_PREVIEW_TARGETS.aiTradingPlaceholder,
      targetType: "docs",
      variant: "secondary",
      ...FREE_TRIAL_PREVIEW_FLAGS,
      helperText: "Placeholder only. Owner review is required before expansion.",
    },
  },
];

export const productPlans = [
  {
    id: "free",
    name: "Free",
    billing: "free",
    audience: "documentation, comparison, and sample artifact preview",
    loginRequired: false,
    primaryCta: {
      label: "Preview Aggressive Trial",
      href: FREE_TRIAL_PREVIEW_TARGETS.aggressive,
      targetType: "static_preview",
      ...FREE_TRIAL_PREVIEW_FLAGS,
      helperText: "Preview only. No execution features are enabled.",
    },
    constraints: [
      "This preview does not place orders.",
      "Execution features require separate explicit approval.",
      "Exchange connection is not part of this preview.",
      "PAPER and LIVE are not enabled.",
      "Runtime is not connected.",
      "Billing and deploy are not part of the preview.",
      "Artifact previews are read-only and do not extract ZIP files.",
    ],
    ctas: freeTrialCtas,
    trialStrategies: trialStrategies.filter((strategy) => strategy.planId === "free"),
  },
];

export const freePlanPage = {
  planId: "free",
  title: "Explore LoneWolf Fang trial paths from Free",
  description:
    "Preview Standard and Aggressive trial workflows with docs, safety notes, and sample artifacts. Execution features require a separate owner approval.",
  helperText:
    "Free previews are educational and non-executable. They do not create exchange sessions, orders, billing changes, deployments, or runtime jobs.",
  primaryCta: productPlans[0].primaryCta,
  ctas: freeTrialCtas,
  trialStrategiesSection: {
    id: "trial-strategies",
    title: "Trial previews",
    description:
      "Review Standard and Aggressive paths as static previews. Aggressive is available, but this Free preview does not enable execution or order placement.",
    strategies: productPlans[0].trialStrategies,
  },
  starterPack: {
    title: "Starter Pack",
    href: FREE_TRIAL_PREVIEW_TARGETS.starterPack,
    ctaLabel: "Open Starter Pack",
    helperText:
      "Compare Standard and Aggressive paths, inspect sample artifacts, and review the safety boundary before choosing a next phase.",
  },
  sampleArtifactPreview: {
    title: "Sample Artifact Preview",
    href: FREE_TRIAL_PREVIEW_TARGETS.sampleArtifact,
    ctaLabel: "View Sample Artifact",
    helperText:
      "View a static sample artifact to understand packet structure, safety flags, and review notes.",
  },
  safetyNotice: {
    title: "Trial safety notes",
    items: productPlans[0].constraints,
  },
};

export default {
  freeTrialCtas,
  productPlans,
  trialStrategies,
  freePlanPage,
};
