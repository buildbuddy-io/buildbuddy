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
import errorService from "../../../app/errors/error_service";
import HistoryComponent from "../history/history";
import LoginComponent from "../login/login";
import CreateOrgComponent from "../org/create_org";
import JoinOrgComponent from "../org/join_org";
import RepoComponent from "../repo/repo";
import SettingsComponent from "../settings/settings";
import SidebarComponent from "../sidebar/sidebar";
import ShortcutsComponent from "../shortcuts/shortcuts";
import TapComponent from "../tap/tap";
import TrendsComponent from "../trends/trends";
import Shortcuts from "../../../app/shortcuts/shortcuts";
import UsageComponent from "../usage/usage";
import GroupSearchComponent from "../group_search/group_search";
import AuditLogsComponent from "../auditlogs/auditlogs";
import { AlertCircle, Check, Copy, LogOut } from "lucide-react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
const CodeComponent = React.lazy(() => import("../code/code"));
// TODO(siggisim): lazy load all components that make sense more gracefully.

import ExecutorsComponent from "../executors/executors";
import UserPreferences from "../../../app/preferences/preferences";
import Modal from "../../../app/components/modal/modal";
import NewTrendsComponent from "../trends/new_trends";
import OrgAccessDeniedComponent from "../org/org_access_denied";
import rpc_service from "../../../app/service/rpc_service";
import { api_key } from "../../../proto/api_key_ts_proto";
import { copyToClipboard } from "../../../app/util/clipboard";
import alert_service from "../../../app/alert/alert_service";
import InvocationModel from "../../../app/invocation/invocation_model";
import PickerComponent from "../../../app/picker/picker";
import CodeReviewComponent from "../review/review";
import CodeSearchComponent from "../codesearch/codesearch";

interface State {
  user?: User;
  tab: string;
  path: string;
  search: URLSearchParams;
  preferences: UserPreferences;
  loading: boolean;
  keyboardShortcutHelpShowing: boolean;
}

capabilities.register("BuildBuddy Enterprise", true, [
  Path.invocationPath,
  Path.userHistoryPath,
  Path.hostHistoryPath,
  Path.repoHistoryPath,
  Path.branchHistoryPath,
  Path.commitHistoryPath,
  Path.workflowsPath,
  Path.settingsPath,
  Path.trendsPath,
  Path.executorsPath,
  Path.tapPath,
  Path.codePath,
  Path.codesearchPath,
]);

interface ImpersonationProps {
  user: User;
}

interface ImpersonationState {
  isCopied: boolean;
  apiKey?: api_key.ApiKey;
}

class ImpersonationComponent extends React.Component<ImpersonationProps, ImpersonationState> {
  state: ImpersonationState = {
    isCopied: false,
  };

  private copyTimeout?: number;

  private async generateApiKey() {
    if (this.state.apiKey) {
      return this.state.apiKey.value;
    }
    const response = await rpc_service.service.createImpersonationApiKey(
      api_key.CreateImpersonationApiKeyRequest.create()
    );
    this.setState({
      apiKey: response.apiKey!,
    });
    // Store the generated key for some time to avoid generating a new key if
    // the user needs the key again.
    window.setTimeout(() => {
      this.setState({ apiKey: undefined });
    }, 45 * 60 * 1000);
    return response.apiKey!.value;
  }

  handleGenerateImpersonationAPIKeyClicked() {
    this.generateApiKey().then((key) => {
      copyToClipboard(key);
      this.setState({ isCopied: true }, () => {
        alert_service.success("Copied API key to clipboard");
      });
      clearTimeout(this.copyTimeout);
      this.copyTimeout = window.setTimeout(() => {
        this.setState({ isCopied: false });
      }, 4000);
    });
  }

  handleExitImpersonationModeClicked() {
    authService.exitImpersonationMode();
  }

  render() {
    return (
      <div className="impersonation-toolbar">
        <div className="impersonation-caution">
          <AlertCircle className="icon black" />
          <span>
            <span className="hide-on-mobile">Caution: authenticated as a member of </span>
            <b>{this.props.user.selectedGroupName()}</b> ({this.props.user.selectedGroup?.id})
          </span>
        </div>
        <div className="spacer" />
        <OutlinedButton
          onClick={this.handleGenerateImpersonationAPIKeyClicked.bind(this)}
          className="generate-api-key-button hide-on-mobile">
          <span>{this.state.apiKey ? "Copy" : "Get"} temporary API key</span>
          {this.state.isCopied ? (
            <Check style={{ stroke: "green" }} className="icon black" />
          ) : (
            <Copy className="icon black" />
          )}
        </OutlinedButton>
        <OutlinedButton onClick={this.handleExitImpersonationModeClicked.bind(this)} className="exit-button">
          <span>Exit</span>
          <LogOut className="icon black" />
        </OutlinedButton>
      </div>
    );
  }
}

export default class EnterpriseRootComponent extends React.Component {
  state: State = {
    loading: true,
    tab: router.getTab(),
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    preferences: new UserPreferences(this.handlePreferencesChanged.bind(this)),
    keyboardShortcutHelpShowing: false,
  };

  componentWillMount() {
    if (!capabilities.auth) {
      this.setState({ user: undefined, loading: false });
    }
    authService.userStream.subscribe({
      next: (user?: User) => this.setState({ user, loading: false }),
    });
    authService.register();
    router.register(this.handlePathChange.bind(this));
    faviconService.setDefaultFavicon();
    (window as any)._preferences = this.state.preferences;
    Shortcuts.setPreferences(this.state.preferences);
  }

  componentDidMount() {
    errorService.register();
  }

  handlePathChange() {
    if (this.state.path != window.location.pathname) {
      faviconService.setDefaultFavicon();
    }
    this.setState({
      tab: router.getTab(),
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search),
    });
    capabilities.didNavigateToPath();
  }

  handlePreferencesChanged() {
    this.forceUpdate();
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
    let historyBranch = this.state.user && router.getHistoryBranch(this.state.path);
    let historyCommit = this.state.user && router.getHistoryCommit(this.state.path);
    let settings = this.state.user && this.state.path.startsWith("/settings");
    let org = this.state.user && this.state.path.startsWith("/org/");
    let orgCreate = this.state.user && this.state.path === Path.createOrgPath;
    let orgJoinAuthenticated = this.state.path.startsWith(Path.joinOrgPath) && this.state.user;
    let orgAccessDenied = this.state.user && this.state.path === Path.orgAccessDeniedPath;
    let trends = this.state.user && this.state.path.startsWith("/trends");
    let usage = this.state.user && this.state.path.startsWith("/usage/");
    let auditLogs = this.state.user && this.state.path.startsWith("/audit-logs/");
    let executors = this.state.user && this.state.path.startsWith("/executors");
    let tests = this.state.user && this.state.path.startsWith("/tests");
    let workflows = this.state.user && this.state.path.startsWith("/workflows");
    let code = this.state.user && this.state.path.startsWith("/code");
    let repo = this.state.path.startsWith("/repo");
    let review = this.state.user && this.state.path.startsWith("/reviews");
    let codesearch = this.state.user && this.state.path.startsWith("/search");
    let fallback =
      !code &&
      !workflows &&
      !settings &&
      !org &&
      !orgJoinAuthenticated &&
      !orgAccessDenied &&
      !trends &&
      !usage &&
      !executors &&
      !tests &&
      !invocationId &&
      !compareInvocationIds &&
      !historyHost &&
      !historyUser &&
      !historyRepo &&
      !historyBranch &&
      !historyCommit &&
      !auditLogs &&
      !repo &&
      !codesearch &&
      !review;

    let setup =
      (this.state.path.startsWith("/docs/setup") && (this.state.user || capabilities.anonymous)) ||
      (fallback && !capabilities.auth);
    let login = fallback && !setup && !repo && !this.state.loading && !this.state.user;
    let home = fallback && !setup && !this.state.loading && this.state.user;
    let sidebar = Boolean(this.state.user) && Boolean(this.state.user?.groups?.length) && !code && !repo;
    let menu = !sidebar && !repo && !code && !this.state.loading;

    return (
      <>
        {this.state.user?.isImpersonating && <ImpersonationComponent user={this.state.user} />}
        <div
          className={`root ${this.state.preferences.denseModeEnabled ? "dense" : ""} ${sidebar || code ? "left" : ""}`}>
          <div className={`page ${menu ? "has-menu" : ""}`}>
            {menu && (
              <MenuComponent
                light={login}
                user={this.state.user}
                showHamburger={!this.state.user && !!invocationId}
                preferences={this.state.preferences}>
                <div onClick={this.handleOrganizationClicked.bind(this)}>{this.state.user?.selectedGroupName()}</div>
              </MenuComponent>
            )}
            {sidebar && (
              <SidebarComponent
                path={this.state.path}
                tab={this.state.tab}
                user={this.state.user}
                dense={this.state.preferences.denseModeEnabled}
                search={this.state.search}></SidebarComponent>
            )}
            <div
              className={`root-main ${code ? "root-code" : ""} ${login ? "root-login" : ""} ${
                tests ? "root-tests" : ""
              }`}>
              {!this.state.loading && (
                <div className={`content ${login || repo ? "content-flex" : ""}`}>
                  {invocationId && (
                    <Suspense fallback={<div className="loading" />}>
                      <InvocationComponent
                        user={this.state.user}
                        invocationId={invocationId}
                        key={invocationId}
                        tab={this.state.tab}
                        search={this.state.search}
                        preferences={this.state.preferences}
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
                    <HistoryComponent
                      user={this.state.user}
                      username={historyUser}
                      tab={this.state.tab}
                      search={this.state.search}
                    />
                  )}
                  {historyHost && (
                    <HistoryComponent
                      user={this.state.user}
                      hostname={historyHost}
                      tab={this.state.tab}
                      search={this.state.search}
                    />
                  )}
                  {historyRepo && (
                    <HistoryComponent
                      user={this.state.user}
                      repo={historyRepo}
                      tab={this.state.tab}
                      search={this.state.search}
                    />
                  )}
                  {historyBranch && (
                    <HistoryComponent
                      user={this.state.user}
                      branch={historyBranch}
                      tab={this.state.tab}
                      search={this.state.search}
                    />
                  )}
                  {historyCommit && (
                    <HistoryComponent
                      user={this.state.user}
                      commit={historyCommit}
                      tab={this.state.tab}
                      search={this.state.search}
                    />
                  )}
                  {settings && this.state.user && (
                    <Suspense fallback={<div className="loading" />}>
                      <SettingsComponent
                        user={this.state.user}
                        search={this.state.search}
                        preferences={this.state.preferences}
                        path={this.state.path}
                      />
                    </Suspense>
                  )}
                  {orgCreate && this.state.user && <CreateOrgComponent user={this.state.user} />}
                  {orgJoinAuthenticated && this.state.user && <JoinOrgComponent user={this.state.user} />}
                  {orgAccessDenied && this.state.user && <OrgAccessDeniedComponent user={this.state.user} />}
                  {tests && this.state.user && (
                    <Suspense fallback={<div className="loading" />}>
                      <TapComponent user={this.state.user} search={this.state.search} tab={this.state.tab} />
                    </Suspense>
                  )}
                  {trends && this.state.user && (
                    <Suspense fallback={<div className="loading" />}>
                      {capabilities.config.newTrendsUiEnabled && (
                        <NewTrendsComponent user={this.state.user} search={this.state.search} tab={this.state.tab} />
                      )}
                      {!capabilities.config.newTrendsUiEnabled && (
                        <TrendsComponent user={this.state.user} search={this.state.search} tab={this.state.tab} />
                      )}
                    </Suspense>
                  )}
                  {usage && this.state.user && <UsageComponent user={this.state.user} />}
                  {auditLogs && this.state.user && <AuditLogsComponent user={this.state.user} />}
                  {executors && this.state.user && <ExecutorsComponent path={this.state.path} user={this.state.user} />}
                  {home && <HistoryComponent user={this.state.user} tab={this.state.tab} search={this.state.search} />}
                  {workflows && this.state.user && <WorkflowsComponent path={this.state.path} user={this.state.user} />}
                  {repo && <RepoComponent path={this.state.path} search={this.state.search} user={this.state.user} />}
                  {codesearch && <CodeSearchComponent path={this.state.path} />}
                  {review && <CodeReviewComponent path={this.state.path} />}
                  {code && this.state.user && (
                    <Suspense fallback={<div className="loading" />}>
                      <CodeComponent
                        path={this.state.path}
                        user={this.state.user}
                        search={this.state.search}
                        tab={this.state.tab}
                      />
                    </Suspense>
                  )}
                  {setup && (
                    <Suspense fallback={<div className="loading" />}>
                      <SetupComponent user={this.state.user}>
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
                  {login && <LoginComponent search={this.state.search} />}
                </div>
              )}
              {!this.state.loading && !code && <FooterComponent />}
              {this.state.loading && <div className="loading loading"></div>}
            </div>
          </div>
          <GroupSearchComponent />
          <AlertComponent />
          <PickerComponent />
          <ShortcutsComponent preferences={this.state.preferences} />
        </div>
      </>
    );
  }
}
