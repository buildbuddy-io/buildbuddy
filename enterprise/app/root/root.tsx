import React, { Suspense } from "react";
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
const CodeComponent = React.lazy(() => import("../code/code"));
// TODO(siggisim): lazy load all components that make sense more gracefully.

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
      Path.codePath,
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
        {menu && (
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
          <div className="root-main">
            {!this.state.loading && (
              <div className={`content ${login ? "content-flex" : ""}`}>
                {invocationId && (
                  <Suspense fallback={<div className="loading" />}>
                    <InvocationComponent
                      user={this.state.user}
                      invocationId={invocationId}
                      key={invocationId}
                      hash={this.state.hash}
                      search={this.state.search}
                      denseMode={this.state.denseMode}
                    />
                  </Suspense>
                )}
                {compareInvocationIds && (
                  <Suspense fallback={<div className="loading" />}>
                    <CompareInvocationsComponent
                      invocationAId={compareInvocationIds.a}
                      invocationBId={compareInvocationIds.b}
                      search={this.state.search}
                      user={this.state.user}
                    />
                  </Suspense>
                )}
                {historyUser && (
                  <HistoryComponent user={this.state.user} username={historyUser} hash={this.state.hash} />
                )}
                {historyHost && (
                  <HistoryComponent user={this.state.user} hostname={historyHost} hash={this.state.hash} />
                )}
                {historyRepo && (
                  <HistoryComponent
                    user={this.state.user}
                    repo={historyRepo}
                    hash={this.state.hash}
                    search={this.state.search}
                  />
                )}
                {historyCommit && (
                  <HistoryComponent user={this.state.user} commit={historyCommit} hash={this.state.hash} />
                )}
                {settings && (
                  <Suspense fallback={<div className="loading" />}>
                    <SettingsComponent
                      user={this.state.user}
                      denseModeEnabled={this.state.denseMode}
                      handleDenseModeToggled={this.handleToggleDenseClicked.bind(this)}
                      path={this.state.path}
                    />
                  </Suspense>
                )}
                {orgCreate && <CreateOrgComponent user={this.state.user} />}
                {orgJoinAuthenticated && <JoinOrgComponent user={this.state.user} />}
                {tests && (
                  <Suspense fallback={<div className="loading" />}>
                    <TapComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />
                  </Suspense>
                )}
                {trends && (
                  <Suspense fallback={<div className="loading" />}>
                    <TrendsComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />
                  </Suspense>
                )}
                {executors && (
                  <ExecutorsComponent user={this.state.user} search={this.state.search} hash={this.state.hash} />
                )}
                {home && <HistoryComponent user={this.state.user} hash={this.state.hash} />}
                {workflows && <WorkflowsComponent path={this.state.path} user={this.state.user} />}
                {code && (
                  <Suspense fallback={<div className="loading" />}>
                    <CodeComponent
                      path={this.state.path}
                      user={this.state.user}
                      search={this.state.search}
                      hash={this.state.hash}
                    />
                  </Suspense>
                )}
                {setup && (
                  <Suspense fallback={<div className="loading" />}>
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
                  </Suspense>
                )}
                {login && <LoginComponent />}
              </div>
            )}
            {!this.state.loading && !code && <FooterComponent />}
            {this.state.loading && <div className="loading loading-dark"></div>}
          </div>
        </div>
        <AlertComponent />
      </div>
    );
  }
}
