import { User } from "../auth/user";
import capabilities from "../capabilities/capabilities";
import shortcuts, { KeyCombo } from "../shortcuts/shortcuts";
import format from "../format/format";
import rpc_service from "../service/rpc_service";

import { GLOBAL_FILTER_PARAM_NAMES, PERSISTENT_URL_PARAMS } from "./router_params";

class Router {
  register(pathChangeHandler: VoidFunction) {
    const oldPushState = history.pushState;
    history.pushState = (data: any, unused: string, url?: string | URL): void => {
      oldPushState.apply(history, [data, unused, url]);
      pathChangeHandler();
      return undefined;
    };

    const oldReplaceState = history.replaceState;
    history.replaceState = (data: any, unused: string, url?: string | URL): void => {
      oldReplaceState.apply(history, [data, unused, url]);
      pathChangeHandler();
      return undefined;
    };

    window.addEventListener("popstate", () => {
      pathChangeHandler();
    });

    // The router is only created once, so no need to keep the handles and
    // deregister these shortcuts.
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.a], () => {
      this.navigateHome();
    });
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.r], () => {
      this.navigateToTrends();
    });
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.t], () => {
      this.navigateToTap();
    });
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.x], () => {
      this.navigateToExecutors();
    });
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.q], () => {
      this.navigateToSetup();
    });
    shortcuts.registerSequence([KeyCombo.g, KeyCombo.g], () => {
      this.navigateToSettings();
    });
  }

  /**
   * Updates the URL relative to the origin. The new URL is formed relative to
   * the current href.
   *
   * - Creates a new browser history entry.
   * - Preserves global filter params.
   */
  navigateTo(url: string) {
    const oldUrl = new URL(window.location.href);
    const newUrl = new URL(url, window.location.href);

    const matchedPath = getMatchedPath(newUrl.pathname);
    if (matchedPath === null) {
      alert("Requested path not found.");
      return;
    }
    const unavailableMsg = getUnavailableMessage(matchedPath);
    if (unavailableMsg && !capabilities.canNavigateToPath(matchedPath)) {
      alert(unavailableMsg);
      return;
    }

    // Preserve persistent URL params.
    for (const key of PERSISTENT_URL_PARAMS) {
      const oldParam = oldUrl.searchParams.get(key);
      if (!newUrl.searchParams.get(key) && oldParam) {
        newUrl.searchParams.set(key, oldParam);
      }
    }

    if (oldUrl.href === newUrl.href) {
      rpc_service.events.next("refresh");
      return;
    }

    // Initiate a full page load for server-side redirects.
    if (newUrl.pathname.startsWith("/auth/")) {
      window.location.href = newUrl.href;
      return;
    }

    window.history.pushState({}, "", newUrl.href);
  }

  /**
   * Sets the given query param.
   *
   * - Creates a new browser history entry.
   * - Preserves global filter params.
   * - Preserves the current `path`, but not the `hash`.
   */
  navigateToQueryParam(key: string, value: string) {
    const url = new URL(window.location.href);
    url.searchParams.set(key, value);
    window.history.pushState({}, "", url.href);
  }

  /**
   * Replaces the current URL query.
   *
   * - Does not create a new browser history entry.
   * - Preserves global filter params.
   */
  setQuery(query: Record<string, string>) {
    window.history.replaceState({}, "", getModifiedUrl({ query }));
  }
  /**
   * Replaces a single query param, preserving all other params.
   */
  setQueryParam(key: string, value: any) {
    const url = new URL(window.location.href);
    if (value === undefined || value === null) {
      url.searchParams.delete(key);
    } else {
      url.searchParams.set(key, String(value));
    }
    window.history.replaceState({}, "", url.href);
  }

  navigateHome(hash?: string) {
    this.navigateTo("/" + (hash || ""));
  }

  navigateToSetup() {
    this.navigateTo(Path.setupPath);
  }

  navigateToWorkflows() {
    this.navigateTo(Path.workflowsPath);
  }

  navigateToCode() {
    this.navigateTo(Path.codePath);
  }

  navigateToSettings() {
    this.navigateTo(Path.settingsPath);
  }

  navigateToTrends() {
    this.navigateTo(Path.trendsPath);
  }

  navigateToUsage() {
    this.navigateTo(Path.usagePath);
  }

  navigateToExecutors() {
    this.navigateTo(Path.executorsPath);
  }

  navigateToTap() {
    this.navigateTo(Path.tapPath);
  }

  navigateToInvocation(invocationId: string) {
    this.navigateTo(Path.invocationPath + invocationId);
  }

  getInvocationUrl(invocationId: string) {
    return Path.invocationPath + invocationId;
  }

  navigateToUserHistory(user: string) {
    this.navigateTo(Path.userHistoryPath + user);
  }

  navigateToHostHistory(host: string) {
    this.navigateTo(Path.hostHistoryPath + host);
  }

  getWorkflowHistoryUrl(repo: string) {
    return `${Path.repoHistoryPath}${getRepoUrlPathParam(repo)}?role=CI_RUNNER`;
  }

  getWorkflowActionHistoryUrl(repo: string, actionName: string) {
    return `${Path.repoHistoryPath}${getRepoUrlPathParam(repo)}?role=CI_RUNNER&pattern=${actionName}`;
  }

  navigateToWorkflowHistory(repo: string) {
    this.navigateTo(this.getWorkflowHistoryUrl(repo));
  }

  navigateToRepoHistory(repo: string) {
    this.navigateTo(`${Path.repoHistoryPath}${getRepoUrlPathParam(repo)}`);
  }

  navigateToBranchHistory(branch: string) {
    this.navigateTo(Path.branchHistoryPath + branch);
  }

  navigateToCommitHistory(commit: string) {
    this.navigateTo(Path.commitHistoryPath + commit);
  }

  navigateToTagHistory(tag: string) {
    this.navigateTo(Path.home + "?tag=" + tag);
  }

  navigateToCreateOrg() {
    if (!capabilities.createOrg) {
      window.open("https://buildbuddy.typeform.com/to/PFjD5A", "_blank");
      return;
    }
    this.navigateTo(Path.createOrgPath);
  }

  updateParams(params: Record<string, string>) {
    const newUrl = getModifiedUrl({ query: params });
    window.history.pushState({ path: newUrl }, "", newUrl);
  }

  replaceParams(params: Record<string, string>) {
    const newUrl = getModifiedUrl({ query: params });
    this.replaceURL(newUrl);
  }

  replaceURL(newUrl: string) {
    window.history.replaceState({ path: newUrl }, "", newUrl);
  }

  getLastPathComponent(path: string, pathPrefix: string) {
    if (!path.startsWith(pathPrefix)) {
      return null;
    }
    return decodeURIComponent(path.replace(pathPrefix, ""));
  }

  getInvocationId(path: string) {
    return this.getLastPathComponent(path, Path.invocationPath);
  }

  getInvocationIdsForCompare(path: string) {
    const idsComponent = this.getLastPathComponent(path, Path.comparePath);
    if (!idsComponent) {
      return null;
    }
    const [a, b] = idsComponent.split("...");
    if (!a || !b) {
      return null;
    }
    return { a, b };
  }

  getHistoryUser(path: string) {
    return this.getLastPathComponent(path, Path.userHistoryPath);
  }

  getHistoryHost(path: string) {
    return this.getLastPathComponent(path, Path.hostHistoryPath);
  }

  getHistoryRepo(path: string) {
    let repoComponent = this.getLastPathComponent(path, Path.repoHistoryPath);
    if (repoComponent?.includes("/")) {
      return `https://github.com/${repoComponent}`;
    }
    return repoComponent ? atob(repoComponent) : "";
  }

  getHistoryBranch(path: string) {
    return this.getLastPathComponent(path, Path.branchHistoryPath);
  }

  getHistoryCommit(path: string) {
    return this.getLastPathComponent(path, Path.commitHistoryPath);
  }

  isFiltering() {
    const url = new URL(window.location.href);
    for (const param of GLOBAL_FILTER_PARAM_NAMES) {
      if (url.searchParams.has(param)) return true;
    }
    return false;
  }

  clearFilters() {
    const url = new URL(window.location.href);
    for (const param of GLOBAL_FILTER_PARAM_NAMES) {
      url.searchParams.delete(param);
    }
    this.replaceParams(Object.fromEntries(url.searchParams.entries()));
  }

  canAccessExecutorsPage(user?: User) {
    return capabilities.executors && Boolean(user?.canCall("getExecutionNodes"));
  }

  canAccessUsagePage(user?: User) {
    return capabilities.usage && Boolean(user?.canCall("getUsage"));
  }

  canAccessWorkflowsPage(user?: User) {
    const workflowsAdmin = capabilities.workflows && Boolean(user?.canCall("createWorkflow"));
    const workflowsUser =
      capabilities.workflows &&
      capabilities.config.workflowHistoryEnabled &&
      Boolean(user?.canCall("getWorkflowHistory"));
    return workflowsAdmin || workflowsUser;
  }

  canAccessOrgDetailsPage(user?: User) {
    return Boolean(user?.canCall("updateGroup"));
  }

  canAccessOrgMembersPage(user?: User) {
    return Boolean(user?.canCall("updateGroupUsers"));
  }

  canCreateOrg(user?: User) {
    return Boolean(user?.canCall("createGroup"));
  }

  canAccessOrgGitHubLinkPage(user?: User) {
    // GitHub linking does not call updateGroup, but the required permissions
    // are equivalent.
    return Boolean(user?.canCall("updateGroup"));
  }

  canAccessOrgSecretsPage(user?: User) {
    return Boolean(user?.canCall("listSecrets"));
  }

  getTab() {
    let tab = window.location.hash.split("@")[0];
    return tab == "#" ? "" : tab;
  }

  getLineNumber() {
    let hashParts = location.hash.split("@");
    if (hashParts.length > 1) {
      return parseInt(hashParts[1]);
    }
    return undefined;
  }

  canAccessEncryptionPage(user?: User) {
    return Boolean(user?.canCall("getEncryptionConfig"));
  }

  /**
   * Routes the user to a new page if they don't have the ability to access the
   * current page.
   */
  rerouteIfNecessary(user?: User) {
    const fallbackPath = this.getFallbackPath(user);
    if (fallbackPath === null) return;

    const newUrl = getModifiedUrl({ path: fallbackPath });
    window.history.replaceState({}, "", newUrl);
  }

  private getFallbackPath(user?: User): string | null {
    // Require the user to create an org if they are logged in but not part of
    // an org.
    if (user && !user.groups?.length) {
      return Path.createOrgPath;
    }

    const path = window.location.pathname;

    if (path === Path.executorsPath && !this.canAccessExecutorsPage(user)) {
      return Path.home;
    }
    if (path === Path.workflowsPath && !this.canAccessWorkflowsPage(user)) {
      return Path.home;
    }
    if (path === Path.usagePath && !this.canAccessUsagePage(user)) {
      return Path.home;
    }

    if (path === Path.settingsOrgDetailsPath && !this.canAccessOrgDetailsPage(user)) {
      return Path.settingsPath;
    }
    if (path === Path.settingsOrgMembersPath && !this.canAccessOrgMembersPage(user)) {
      return Path.settingsPath;
    }
    if (path === Path.settingsOrgGitHubLinkPath && !this.canAccessOrgGitHubLinkPage(user)) {
      return Path.settingsPath;
    }

    return null;
  }
}

// If a repo matches https://github.com/{owner}/{repo} or https://github.com/{owner}/{repo}.git
// then we'll show it directly in the URL like `{owner}/{repo}`. Otherwise we encode it
// using `window.btoa`.
const GITHUB_URL_PREFIX = "^https://github.com";
const PATH_SEGMENT_PATTERN = "[^/]+";
const OPTIONAL_DOTGIT_SUFFIX = "(\\.git)?$";
const GITHUB_REPO_URL_PATTERN = new RegExp(
  `${GITHUB_URL_PREFIX}/${PATH_SEGMENT_PATTERN}/${PATH_SEGMENT_PATTERN}${OPTIONAL_DOTGIT_SUFFIX}`
);

function getRepoUrlPathParam(repo: string): string {
  if (repo.match(GITHUB_REPO_URL_PATTERN)) {
    return format.formatGitUrl(repo);
  }
  return window.btoa(repo);
}

function getQueryString(params: Record<string, string>) {
  return new URLSearchParams(
    Object.fromEntries(Object.entries(params).filter(([_, value]) => Boolean(value)))
  ).toString();
}

function getModifiedUrl({ query, path }: { query?: Record<string, string>; path?: string }) {
  const queryString = query ? getQueryString(query) : window.location.search;
  return (
    window.location.protocol +
    "//" +
    window.location.host +
    (path === undefined ? window.location.pathname : path) +
    (queryString ? "?" : "") +
    queryString +
    window.location.hash
  );
}

export class Path {
  static home = "/";
  static comparePath = "/compare/";
  static invocationPath = "/invocation/";
  static userHistoryPath = "/history/user/";
  static hostHistoryPath = "/history/host/";
  static repoHistoryPath = "/history/repo/";
  static branchHistoryPath = "/history/branch/";
  static commitHistoryPath = "/history/commit/";
  static setupPath = "/docs/setup/";
  static settingsPath = "/settings/";
  static settingsOrgDetailsPath = "/settings/org/details";
  static settingsOrgMembersPath = "/settings/org/members";
  static settingsOrgGitHubLinkPath = "/settings/org/github";
  static createOrgPath = "/org/create";
  static editOrgPath = "/org/edit";
  static trendsPath = "/trends/";
  static usagePath = "/usage/";
  static executorsPath = "/executors/";
  static tapPath = "/tests/";
  static workflowsPath = "/workflows/";
  static codePath = "/code/";
}

/** Returns the longest path value in `Path` matching the given URL path. */
function getMatchedPath(urlPath: string): string | null {
  if (!urlPath.endsWith("/")) {
    urlPath += "/";
  }
  let curMatch: string | null = null;
  for (const path of Object.values(Path)) {
    let prefix = path;
    if (!prefix.endsWith("/")) {
      prefix += "/";
    }
    if (urlPath.startsWith(prefix) && (curMatch === null || curMatch.length < path)) {
      curMatch = path;
    }
  }
  return curMatch;
}

function getUnavailableMessage(matchedPath: string) {
  switch (matchedPath) {
    case Path.workflowsPath:
    case Path.codePath:
    case Path.settingsPath:
    case Path.trendsPath:
    case Path.executorsPath:
    case Path.tapPath:
    case Path.userHistoryPath:
    case Path.hostHistoryPath:
    case Path.repoHistoryPath:
    case Path.branchHistoryPath:
    case Path.commitHistoryPath:
    case Path.invocationPath:
      return `This feature is not available in ${capabilities.name}.\n\nClick 'Upgrade to Enterprise' in the menu to enable user build history, organization build history, SSO, and more!`;
    default:
      return "";
  }
}

export default new Router();
