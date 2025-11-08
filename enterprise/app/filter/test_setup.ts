// Set up window mock before any modules that use it are loaded
declare const global: any;

const windowMock = {
  buildbuddyConfig: {
    version: "",
    configuredIssuers: [],
    githubEnabled: false,
    anonymousUsageEnabled: false,
    testDashboardEnabled: false,
    ssoEnabled: false,
    workflowsEnabled: false,
    remoteExecutionEnabled: false,
    userOwnedExecutorsEnabled: false,
    executorKeyCreationEnabled: false,
    codeEditorEnabled: false,
    usageEnabled: false,
    readOnlyGithubAppEnabled: false,
  },
};

// Set on global (Node.js) and globalThis (works in all environments)
if (typeof global !== "undefined") {
  (global as any).window = windowMock;
}
if (typeof globalThis !== "undefined") {
  (globalThis as any).window = windowMock;
}
