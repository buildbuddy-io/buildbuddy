declare var window: any;

export class Capabilities {
  name: string;
  version: string;
  paths: Set<string>;
  enterprise: boolean;
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
  userOwnedExecutors: boolean;
  executorKeyCreation: boolean;

  register(name: string, enterprise: boolean, paths: Array<string>) {
    this.name = name;
    this.version = window.buildbuddyConfig && window.buildbuddyConfig.version;
    this.auth =
      window.buildbuddyConfig &&
      window.buildbuddyConfig.configured_issuers &&
      window.buildbuddyConfig.configured_issuers.length &&
      window.buildbuddyConfig.configured_issuers[0];
    this.github = window.buildbuddyConfig && window.buildbuddyConfig.github_enabled;
    this.anonymous = window.buildbuddyConfig && window.buildbuddyConfig.anonymous_usage_enabled;
    this.test = window.buildbuddyConfig && window.buildbuddyConfig.test_dashboard_enabled;
    this.enterprise = enterprise;
    this.createOrg = this.enterprise;
    this.invocationSharing = true;
    this.compareInvocations = true;
    this.deleteInvocation = true;
    this.manageApiKeys = true;
    this.workflows = localStorage["workflows_enabled"] === "true";
    this.executors = localStorage["executors_enabled"] === "true";
    this.userOwnedExecutors = window.buildbuddyConfig && window.buildbuddyConfig.user_owned_executors_enabled;
    this.executorKeyCreation = window.buildbuddyConfig && window.buildbuddyConfig.executor_key_creation_enabled;
    this.paths = new Set(paths);
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
