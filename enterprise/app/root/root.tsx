import React from "react";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import CompareInvocationsComponent from "../../../app/compare/compare_invocations";
import SetupComponent from "../../../app/docs/setup";
import AlertComponent from "../../../app/alert/alert";
import faviconService from "../../../app/favicon/favicon";
import FooterComponent from "../../../app/footer/footer";
import WorkflowsComponent from "../workflows/workflows";
import InvocationComponent from "../../../app/invocation/invocation";
import MenuComponent from "../../../app/menu/menu";
import router, { Path } from "../../../app/router/router";
import HistoryComponent from "../history/history";
import LoginComponent from "../login/login";
import CreateOrgComponent from "../org/create_org";
import JoinOrgComponent from "../org/join_org";
import SettingsComponent from "../settings/settings";
import SidebarComponent from "../sidebar/sidebar";
import TapComponent from "../tap/tap";
import TrendsComponent from "../trends/trends";
import ExecutorsComponent from "../executors/executors";

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
    ]);
    if (!capabilities.auth) {
      this.setState({ ...this.state, user: null, loading: false });
    }
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
    let historyUser = router.getHistoryUser(this.state.path);
    let historyHost = router.getHistoryHost(this.state.path);
    let historyRepo = router.getHistoryRepo(this.state.path);
    let historyCommit = router.getHistoryCommit(this.state.path);
    let settings = this.state.path.startsWith("/settings");
    let org = this.state.path.startsWith("/org/");
    let orgCreate = this.state.path === Path.createOrgPath;
    let orgJoinAuthenticated = this.state.path.startsWith("/join") && this.state.user;
    let trends = this.state.user && this.state.path.startsWith("/trends");
    let executors = this.state.user && this.state.path.startsWith("/executors");
    let tests = this.state.user && this.state.path.startsWith("/tests");
    let workflows = this.state.user && this.state.path.startsWith("/workflows");
    let fallback =
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
    let sidebar = !!this.state.user;

    return (
      <div className={`root ${this.state.denseMode ? "dense" : ""} ${sidebar ? "left" : ""} ${login ? "dark" : ""}`}>
        {!sidebar && !this.state.loading && (
          <MenuComponent
            user={this.state.user}
            showHamburger={!this.state.user && !!invocationId}
            denseModeEnabled={this.state.denseMode}
            handleDenseModeToggled={this.handleToggleDenseClicked.bind(this)}>
            <div onClick={this.handleOrganizationClicked.bind(this)}>{this.state.user?.selectedGroupName()}</div>
          </MenuComponent>
        )}
        <div className="page">
          {sidebar && (
            <SidebarComponent
              path={this.state.path}
              hash={this.state.hash}
              user={this.state.user}
              search={this.state.search}></SidebarComponent>
          )}
          <div className="main">
            {!this.state.loading && (
              <div className={`content ${login ? "content-flex" : ""}`}>
                {invocationId && (
                  <InvocationComponent
                    user={this.state.user}
                    invocationId={invocationId}
                    hash={this.state.hash}
                    search={this.state.search}
                    denseMode={this.state.denseMode}
                  />
                )}
                {compareInvocationIds && (
                  <CompareInvocationsComponent
                    invocationAId={compareInvocationIds.a}
                    invocationBId={compareInvocationIds.b}
                    search={this.state.search}
                    user={this.state.user}
                  />
                )}
                {historyUser && (
                  <HistoryComponent user={this.state.user} username={historyUser} hash={this.state.hash} />
                )}
                {historyHost && (
                  <HistoryComponent user={this.state.user} hostname={historyHost} hash={this.state.hash} />
                )}
                {historyRepo && <HistoryComponent user={this.state.user} repo={historyRepo} hash={this.state.hash} />}
                {historyCommit && (
                  <HistoryComponent user={this.state.user} commit={historyCommit} hash={this.state.hash} />
                )}
                {settings && (
                  <SettingsComponent
                    user={this.state.user}
                    denseModeEnabled={this.state.denseMode}
                    handleDenseModeToggled={this.handleToggleDenseClicked.bind(this)}
                  />
                )}
                {orgCreate && <CreateOrgComponent user={this.state.user} />}
                {orgJoinAuthenticated && <JoinOrgComponent user={this.state.user} />}
                {tests && <TapComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />}
                {trends && <TrendsComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />}
                {executors && (
                  <ExecutorsComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />
                )}
                {home && <HistoryComponent user={this.state.user} hash={this.state.hash} />}
                {workflows && <WorkflowsComponent path={this.state.path} user={this.state.user} />}
                {setup && (
                  <SetupComponent>
                    {!capabilities.auth && (
                      <p className="callout">
                        <b>Note:</b> To enable enterprise features, configure an auth provider using the{" "}
                        <a target="_blank" href="https://www.buildbuddy.io/docs/config">
                          config.yaml file
                        </a>
                        .
                      </p>
                    )}
                  </SetupComponent>
                )}
                {login && <LoginComponent />}
              </div>
            )}
            {!this.state.loading && <FooterComponent />}
            {this.state.loading && <div className="loading loading-dark"></div>}
          </div>
        </div>
        <AlertComponent />
      </div>
    );
  }
}
