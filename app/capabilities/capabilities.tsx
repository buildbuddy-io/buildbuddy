import { config } from "../../proto/config_ts_proto";

declare const window: Window & {
  buildbuddyConfig: config.FrontendConfig;
  gtag?: (method: string, ...args: any[]) => void;
};

export class Capabilities {
  name: string = "";
  paths: Set<string> = new Set();
  enterprise: boolean = false;

  config: config.FrontendConfig = new config.FrontendConfig({});

  version: string = "";
  github: boolean = false;
  auth: string = "";
  anonymous: boolean = false;
  test: boolean = false;
  createOrg: boolean = false;
  invocationSharing: boolean = false;
  compareInvocations: boolean = false;
  deleteInvocation: boolean = false;
  manageApiKeys: boolean = false;
  workflows: boolean = false;
  executors: boolean = false;
  action: boolean = false;
  userOwnedExecutors: boolean = false;
  executorKeyCreation: boolean = false;
  code: boolean = false;
  sso: boolean = false;
  usage: boolean = false;
  userManagement: boolean = false;

  constructor() {
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
    this.usage = this.config.usageEnabled;
    this.userManagement = this.config.userManagementEnabled;
  }

  register(name: string, enterprise: boolean, paths: Array<string>) {
    this.name = name;
    this.paths = new Set(paths);
    this.enterprise = enterprise;

    this.createOrg = this.enterprise;

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
