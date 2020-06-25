import capabilities from '../capabilities/capabilities';

class Router {

  register(pathChangeHandler: VoidFunction) {
    history.pushState = (f => function pushState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.pushState);

    history.replaceState = (f => function replaceState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.replaceState);

    window.addEventListener('popstate', () => {
      pathChangeHandler();
    });
  }

  navigateTo(path: string) {
    var newUrl = window.location.protocol + "//" + window.location.host + path;
    window.history.pushState({ path: newUrl }, '', newUrl);
  }

  navigateToQueryParam(key: string, value: string) {
    let targetUrl = `?${key}=${value}`;
    window.history.pushState({ path: targetUrl }, '', targetUrl);
  }

  navigateHome(hash?: string) {
    this.navigateTo('/' + (hash || ""));
  }

  navigateToInvocation(invocationId: string) {
    if (!capabilities.canNavigateToPath(Path.invocationPath)) {
      alert(`Invocations are not available in ${capabilities.name}`);
      return;
    }
    this.navigateTo(Path.invocationPath + invocationId);
  }

  navigateToUserHistory(user: string) {
    if (!capabilities.canNavigateToPath(Path.userHistoryPath)) {
      alert(`User history is not available in ${capabilities.name}.\n\nClick 'Upgrade to Enterprise' in the menu to enable user build history, organization build history, SSO, and more!`);
      return;
    }
    this.navigateTo(Path.userHistoryPath + user);
  }

  navigateToHostHistory(host: string) {
    if (!capabilities.canNavigateToPath(Path.hostHistoryPath)) {
      alert(`Host history is not available in ${capabilities.name}.\n\nClick 'Upgrade to Enterprise' in the menu to enable user build history, organization build history, SSO, and more!`);
      return;
    }
    this.navigateTo(Path.hostHistoryPath + host);
  }

  navigateToRepoHistory(repo: string) {
    if (!capabilities.canNavigateToPath(Path.repoHistoryPath)) {
      alert(`Repo history is not available in ${capabilities.name}.\n\nClick 'Upgrade to Enterprise' in the menu to enable user build history, organization build history, SSO, and more!`);
      return;
    }
    this.navigateTo(Path.repoHistoryPath + btoa(repo));
  }

  navigateToCommitHistory(commit: string) {
    if (!capabilities.canNavigateToPath(Path.commitHistoryPath)) {
      alert(`Commit history is not available in ${capabilities.name}.\n\nClick 'Upgrade to Enterprise' in the menu to enable user build history, organization build history, SSO, and more!`);
      return;
    }
    this.navigateTo(Path.commitHistoryPath + commit);
  }

  updateParams(params: any) {
    let keys = Object.keys(params);
    let queryParam = keys.map(key => `${key}=${params[key]}`).join('&');
    var newUrl = window.location.protocol + "//" + window.location.host + window.location.pathname + "?" + queryParam + window.location.hash;
    window.history.pushState({ path: newUrl }, '', newUrl);
  }

  getLastPathComponent(path: string, pathPrefix: string) {
    if (!path.startsWith(pathPrefix)) {
      return null;
    }
    return path.replace(pathPrefix, "");
  }

  getInvocationId(path: string) {
    return this.getLastPathComponent(path, Path.invocationPath);
  }

  getHistoryUser(path: string) {
    return this.getLastPathComponent(path, Path.userHistoryPath);
  }

  getHistoryHost(path: string) {
    return this.getLastPathComponent(path, Path.hostHistoryPath);
  }

  getHistoryRepo(path: string) {
    let repoBase64 = this.getLastPathComponent(path, Path.repoHistoryPath);
    return repoBase64 ? atob(repoBase64) : "";
  }

  getHistoryCommit(path: string) {
    return this.getLastPathComponent(path, Path.commitHistoryPath);
  }
}
export class Path {
  static invocationPath = "/invocation/";
  static userHistoryPath = "/history/user/";
  static hostHistoryPath = "/history/host/";
  static repoHistoryPath = "/history/repo/";
  static commitHistoryPath = "/history/commit/";
}

export default new Router();