import React from "react";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import faviconService from "../../../app/favicon/favicon";
import router, { Path } from "../../../app/router/router";
import InvocationLog from "../../../app/log/invocation_log";

const denseModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";

interface State {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
  denseMode: boolean;
  loading: boolean;
}

export default class EnterpriseRootComponent extends React.Component {
  state: State = {
    loading: true,
    user: undefined,
    hash: window.location.hash,
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    denseMode: window.localStorage.getItem(denseModeKey) == denseModeValue || false,
  };

  componentWillMount() {
    capabilities.register("BuildBuddy Enterprise", true, [
      Path.invocationPath,
      Path.userHistoryPath,
      Path.hostHistoryPath,
      Path.repoHistoryPath,
      Path.commitHistoryPath,
      Path.workflowsPath,
      Path.settingsPath,
      Path.trendsPath,
      Path.executorsPath,
      Path.tapPath,
      Path.codePath,
    ]);
    authService.userStream.subscribe({
      next: (user: User) => this.setState({ ...this.state, user, loading: false }),
    });
    authService.register();
    router.register(this.handlePathChange.bind(this));
    faviconService.setDefaultFavicon();
  }

  handlePathChange() {
    if (this.state.path != window.location.pathname) {
      faviconService.setDefaultFavicon();
    }
    this.setState({
      hash: window.location.hash,
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search),
    });
    capabilities.didNavigateToPath();
  }

  handleToggleDenseClicked() {
    let newDenseMode = !this.state.denseMode;
    this.setState({ ...this.state, denseMode: newDenseMode });
    window.localStorage.setItem(denseModeKey, newDenseMode ? denseModeValue : "");
  }

  handleOrganizationClicked() {
    router.navigateHome();
  }

  render() {
    let invocationId = router.getInvocationId(this.state.path);
    let compareInvocationIds = router.getInvocationIdsForCompare(this.state.path);
    let historyUser = this.state.user && router.getHistoryUser(this.state.path);
    let historyHost = this.state.user && router.getHistoryHost(this.state.path);
    let historyRepo = this.state.user && router.getHistoryRepo(this.state.path);
    let historyCommit = this.state.user && router.getHistoryCommit(this.state.path);
    let settings = this.state.path.startsWith("/settings");
    let org = this.state.path.startsWith("/org/");
    let orgCreate = this.state.path === Path.createOrgPath;
    let orgJoinAuthenticated = this.state.path.startsWith("/join") && this.state.user;
    let trends = this.state.user && this.state.path.startsWith("/trends");
    let executors = this.state.user && this.state.path.startsWith("/executors");
    let tests = this.state.user && this.state.path.startsWith("/tests");
    let workflows = this.state.user && this.state.path.startsWith("/workflows");
    let code = this.state.user && this.state.path.startsWith("/code");
    let fallback =
      !code &&
      !workflows &&
      !settings &&
      !org &&
      !orgJoinAuthenticated &&
      !trends &&
      !executors &&
      !tests &&
      !invocationId &&
      !compareInvocationIds &&
      !historyHost &&
      !historyUser &&
      !historyRepo &&
      !historyCommit;

    let setup =
      (this.state.path.startsWith("/docs/setup") && (this.state.user || capabilities.anonymous)) ||
      (fallback && !capabilities.auth);
    let login = fallback && !setup && !this.state.loading && !this.state.user;
    let home = fallback && !setup && !this.state.loading && this.state.user;
    let sidebar = !!this.state.user && !code;
    let menu = !sidebar && !code && !this.state.loading;

    return (
      <div
        className={`root ${this.state.denseMode ? "dense" : ""} ${sidebar || code ? "left" : ""} ${
          login ? "dark" : ""
        }`}>
        <div style={{ height: "100%", backgroundColor: "white" }}>
          <InvocationLog invocationId={invocationId} />
        </div>
      </div>
    );
  }
}
