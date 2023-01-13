import { config } from "../../proto/config_ts_proto";

declare const window: Window & {
  buildbuddyConfig: config.IFrontendConfig;
  gtag?: (method: string, ...args: any[]) => void;
};

export class Capabilities {
  name = "";
  paths = new Set<string>();
  enterprise = false;

  config: config.IFrontendConfig = {};

  version = "";
  github = false;
  auth = "";
  anonymous = false;
  test = false;
  createOrg = false;
  workflows = false;
  executors = false;
  action = false;
  userOwnedExecutors = false;
  executorKeyCreation = false;
  code = false;
  sso = false;
  globalFilter = false;
  usage = false;
  userManagement = false;

  // TODO: remove these
  invocationSharing = true;
  compareInvocations = true;
  deleteInvocation = true;
  manageApiKeys = true;

  register(name: string, enterprise: boolean, paths: Array<string>) {
    this.name = name;
    this.paths = new Set(paths);
    this.enterprise = enterprise;

    this.createOrg = this.enterprise;

    this.config = window.buildbuddyConfig;

    // Note: Please don't add any new config fields below;
    // get them from the config directly.
    this.version = this.config.version || "";
    this.auth = this.config.configuredIssuers?.[0] || "";
    this.github = this.config.githubEnabled || false;
    this.anonymous = this.config.anonymousUsageEnabled || false;
    this.test = this.config.testDashboardEnabled || false;
    this.sso = this.config.ssoEnabled || false;
    this.workflows = this.config.workflowsEnabled || false;
    this.executors = this.config.remoteExecutionEnabled || false;
    this.userOwnedExecutors = this.config.userOwnedExecutorsEnabled || false;
    this.executorKeyCreation = this.config.executorKeyCreationEnabled || false;
    this.code = this.config.codeEditorEnabled || false;
    this.globalFilter = this.config.globalFilterEnabled || false;
    this.usage = this.config.usageEnabled || false;
    this.userManagement = this.config.userManagementEnabled || false;

    if (window.gtag) {
      window.gtag("set", {
        app_name: this.name,
        app_version: this.version,
        app_installer_id: window.location.host,
      });
      window.gtag("config", "UA-156160991-2");
    }

    this.didNavigateToPath();
  }

  canNavigateToPath(path: string) {
    return this.paths.has(path);
  }

  didNavigateToPath() {
    if (window.gtag) {
      window.gtag("event", "path", {
        event_category: "navigate",
        event_label: window.location.pathname + window.location.search + window.location.hash,
      });
    }
  }
}

export default new Capabilities();
