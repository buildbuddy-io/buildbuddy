import { User } from "../auth/user";
import capabilities from "../capabilities/capabilities";
import format from "../format/format";
import rpc_service from "../service/rpc_service";

// Query params for the global filter.
// These should be preserved when navigating between pages in the app.

export const ROLE_PARAM_NAME = "role";
export const STATUS_PARAM_NAME = "status";
export const START_DATE_PARAM_NAME = "start";
export const END_DATE_PARAM_NAME = "end";
export const LAST_N_DAYS_PARAM_NAME = "days";

export const USER_PARAM_NAME = "user";
export const REPO_PARAM_NAME = "repo";
export const BRANCH_PARAM_NAME = "branch";
export const COMMIT_PARAM_NAME = "commit";
export const HOST_PARAM_NAME = "host";
export const COMMAND_PARAM_NAME = "command";
export const MINIMUM_DURATION_PARAM_NAME = "min-dur";
export const MAXIMUM_DURATION_PARAM_NAME = "max-dur";

// Sort params for the global filter.

export const SORT_BY_PARAM_NAME = "sort-by";
export const SORT_ORDER_PARAM_NAME = "sort-order";

const GLOBAL_FILTER_PARAM_NAMES = [
  ROLE_PARAM_NAME,
  STATUS_PARAM_NAME,
  START_DATE_PARAM_NAME,
  END_DATE_PARAM_NAME,
  LAST_N_DAYS_PARAM_NAME,

  USER_PARAM_NAME,
  REPO_PARAM_NAME,
  BRANCH_PARAM_NAME,
  COMMIT_PARAM_NAME,
  HOST_PARAM_NAME,
  COMMAND_PARAM_NAME,
  MINIMUM_DURATION_PARAM_NAME,
  MAXIMUM_DURATION_PARAM_NAME,
];

const GLOBAL_SORT_PARAM_NAMES = [SORT_BY_PARAM_NAME, SORT_ORDER_PARAM_NAME];

const PERSISTENT_URL_PARAMS = [...GLOBAL_FILTER_PARAM_NAMES, ...GLOBAL_SORT_PARAM_NAMES];

class Router {
  register(pathChangeHandler: VoidFunction) {
    history.pushState = ((f) =>
      function pushState() {
        var ret = f.apply(this, arguments);
        pathChangeHandler();
        return ret;
      })(history.pushState);

    history.replaceState = ((f) =>
      function replaceState() {
        var ret = f.apply(this, arguments);
        pathChangeHandler();
        return ret;
      })(history.replaceState);

    window.addEventListener("popstate", () => {
      pathChangeHandler();
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
    const unavailableMsg = getUnavailableMessage(matchedPath);
    if (unavailableMsg && !capabilities.canNavigateToPath(matchedPath)) {
      alert(unavailableMsg);
      return;
    }

    // Preserve persistent URL params.
    for (const key of PERSISTENT_URL_PARAMS) {
      if (!newUrl.searchParams.get(key) && oldUrl.searchParams.get(key)) {
        newUrl.searchParams.set(key, oldUrl.searchParams.get(key));
      }
    }

    if (oldUrl.href === newUrl.href) {
      rpc_service.events.next("refresh");
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
    url.searchParams.set(key, String(value));
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

  navigateToUserHistory(user: string) {
    this.navigateTo(Path.userHistoryPath + user);
  }

  navigateToHostHistory(host: string) {
    this.navigateTo(Path.hostHistoryPath + host);
  }

  getWorkflowHistoryUrl(repo: string) {
    return `${Path.repoHistoryPath}${getRepoUrlPathParam(repo)}?role=CI_RUNNER`;
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

  canAccessExecutorsPage(user: User | null) {
    return capabilities.executors && Boolean(user?.canCall("getExecutionNodes"));
  }

  canAccessUsagePage(user: User | null) {
    return capabilities.usage && Boolean(user?.canCall("getUsage"));
  }

  canAccessWorkflowsPage(user: User | null) {
    return capabilities.workflows && Boolean(user?.canCall("getWorkflows"));
  }

  canAccessOrgDetailsPage(user: User | null) {
    return Boolean(user?.canCall("updateGroup"));
  }

  canAccessOrgMembersPage(user: User | null) {
    return Boolean(user?.canCall("updateGroupUsers"));
  }

  canCreateOrg(user: User | null) {
    return Boolean(user?.canCall("createGroup"));
  }

  canAccessOrgGitHubLinkPage(user: User | null) {
    // GitHub linking does not call updateGroup, but the required permissions
    // are equivalent.
    return Boolean(user?.canCall("updateGroup"));
  }

  canAccessOrgSecretsPage(user: User | null) {
    return Boolean(user?.canCall("listSecrets"));
  }

  /**
   * Routes the user to a new page if they don't have the ability to access the
   * current page.
   */
  rerouteIfNecessary(user: User | null) {
    const fallbackPath = this.getFallbackPath(user);
    if (fallbackPath === null) return;

    const newUrl = getModifiedUrl({ path: fallbackPath });
    window.history.replaceState({}, "", newUrl);
  }

  private getFallbackPath(user: User | null): string | null {
    // Require the user to create an org if they are logged in but not part of
    // an org.
    if (user !== null && !user.groups?.length) {
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
