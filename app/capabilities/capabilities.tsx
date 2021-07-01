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
  action: boolean;
  userOwnedExecutors: boolean;
  executorKeyCreation: boolean;

  register(name: string, enterprise: boolean, paths: Array<string>) {
    this.name = name;

    const config = (window.buildbuddyConfig || {}) as Record<string, any>;

    this.version = config.version || "";
    this.auth = config.configured_issuers?.[0] || "";
    this.github = Boolean(config.github_enabled);
    this.anonymous = Boolean(config.anonymous_usage_enabled);
    this.test = Boolean(config.test_dashboard_enabled);
    this.enterprise = enterprise;
    this.createOrg = this.enterprise;
    this.invocationSharing = true;
    this.compareInvocations = true;
    this.deleteInvocation = true;
    this.manageApiKeys = true;
    this.workflows = Boolean(config.workflows_enabled);
    this.executors = Boolean(config.user_owned_executors_enabled);
    this.userOwnedExecutors = Boolean(config.user_owned_executors_enabled);
    this.executorKeyCreation = Boolean(config.executor_key_creation_enabled);
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
