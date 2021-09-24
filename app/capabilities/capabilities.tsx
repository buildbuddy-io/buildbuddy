import { config } from "../../proto/config_ts_proto";

declare const window: Window & {
  buildbuddyConfig: config.IFrontendConfig;
  gtag?: (method: string, ...args: any[]) => void;
};

export class Capabilities {
  name: string;
  paths: Set<string>;
  enterprise: boolean;

  config: config.IFrontendConfig;

  version: string;
  github: boolean;
  auth: string;
  anonymous: boolean;
  test: boolean;
  createOrg: boolean;
  invocationSharing: boolean;
  compareInvocations: boolean;
  deleteInvocation: boolean;
  manageApiKeys: boolean;
  workflows: boolean;
  executors: boolean;
  action: boolean;
  userOwnedExecutors: boolean;
  executorKeyCreation: boolean;
  code: boolean;
  sso: boolean;
  globalFilter: boolean;
  usage: boolean;
  userManagement: boolean;

  register(name: string, enterprise: boolean, paths: Array<string>) {
    this.name = name;
    this.paths = new Set(paths);
    this.enterprise = enterprise;

    this.createOrg = this.enterprise;

    this.invocationSharing = true;
    this.compareInvocations = true;
    this.deleteInvocation = true;
    this.manageApiKeys = true;

    this.config = window.buildbuddyConfig;

    // Note: Please don't add any new config fields below;
    // get them from the config directly.
    this.version = this.config.version || "";
    this.auth = this.config.configuredIssuers?.[0] || "";
    this.github = this.config.githubEnabled;
    this.anonymous = this.config.anonymousUsageEnabled;
    this.test = this.config.testDashboardEnabled;
    this.sso = this.config.ssoEnabled;
    this.workflows = this.config.workflowsEnabled;
    this.executors = this.config.remoteExecutionEnabled;
    this.userOwnedExecutors = this.config.userOwnedExecutorsEnabled;
    this.executorKeyCreation = this.config.executorKeyCreationEnabled;
    this.code = this.config.codeEditorEnabled;
    this.globalFilter = this.config.globalFilterEnabled;
    this.usage = this.config.usageEnabled;
    this.userManagement = this.config.userManagementEnabled;

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
