import {
  BookOpen,
  Gauge,
  Settings,
  Cloud,
  PlayCircle,
  Code,
  HardDrive,
  Users,
  GitBranch,
  Github,
  BarChart2,
  LayoutGrid,
  CheckCircle,
  Circle,
  GitCommit,
  ChevronUp,
  ChevronDown,
  List,
  LogOut,
  PlusCircle,
  Sliders,
} from "lucide-react";
import React from "react";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";

interface Props {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
}
interface State {
  profileExpanded: boolean;
}

export default class SidebarComponent extends React.Component {
  props: Props;

  state: State = {
    profileExpanded: false,
  };

  handleProfileClicked() {
    this.setState({ profileExpanded: !this.state.profileExpanded });
  }

  handleCreateOrgClicked(e: React.MouseEvent) {
    router.navigateToCreateOrg();
  }

  async handleOrgClicked(groupId: string) {
    if (this.props.user?.selectedGroup?.id === groupId) return;

    await authService.setSelectedGroupId(groupId, { reload: true });
  }

  isHomeSelected() {
    return this.props.path == "/" && (!this.props.hash || this.props.hash == "#");
  }

  isTrendsSelected() {
    return this.props.path.startsWith("/trends/");
  }

  isExecutorsSelected() {
    return this.props.path.startsWith("/executors/");
  }

  isTapSelected() {
    return this.props.path.startsWith("/tests/");
  }

  isUsersSelected() {
    return this.props.path.startsWith("/history/user/") || this.props.hash == "#users";
  }

  isReposSelected() {
    return this.props.path.startsWith("/history/repo/") || this.props.hash == "#repos";
  }

  isBranchesSelected() {
    return this.props.path.startsWith("/history/branch/") || this.props.hash == "#branches";
  }

  isCommitsSelected() {
    return this.props.path.startsWith("/history/commit/") || this.props.hash == "#commits";
  }

  isHostsSelected() {
    return this.props.path.startsWith("/history/host/") || this.props.hash == "#hosts";
  }

  isWorkflowsSelected() {
    return this.props.path === "/workflows/";
  }

  isCodeSelected() {
    return this.props.path === "/code/";
  }

  isSettingsSelected() {
    return this.props.path.startsWith("/docs/setup/");
  }

  isUsageSelected() {
    return this.props.path.startsWith("/usage/");
  }

  refreshCurrentPage() {
    rpcService.events.next("refresh");
  }

  navigateToAllBuilds() {
    this.isHomeSelected() ? this.refreshCurrentPage() : router.navigateHome();
  }

  navigateToTrends() {
    this.isTrendsSelected() && this.props.search.toString().length == 0
      ? this.refreshCurrentPage()
      : router.navigateToTrends();
  }

  navigateToUsage() {
    this.isUsageSelected() ? this.refreshCurrentPage() : router.navigateToUsage();
  }

  navigateToExecutors() {
    this.isExecutorsSelected() && this.props.search.toString().length == 0
      ? this.refreshCurrentPage()
      : router.navigateToExecutors();
  }

  navigateToTap() {
    this.isTapSelected() ? this.refreshCurrentPage() : router.navigateToTap();
  }

  navigateToUsers() {
    this.isUsersSelected() ? this.refreshCurrentPage() : router.navigateHome("#users");
  }

  navigateToRepos() {
    this.isReposSelected() ? this.refreshCurrentPage() : router.navigateHome("#repos");
  }

  navigateToBranches() {
    this.isBranchesSelected() ? this.refreshCurrentPage() : router.navigateHome("#branches");
  }

  navigateToCommits() {
    this.isCommitsSelected() ? this.refreshCurrentPage() : router.navigateHome("#commits");
  }

  navigateToHosts() {
    this.isHostsSelected() ? this.refreshCurrentPage() : router.navigateHome("#hosts");
  }

  navigateToWorkflows() {
    this.isWorkflowsSelected() ? this.refreshCurrentPage() : router.navigateToWorkflows();
  }

  navigateToCode() {
    this.isCodeSelected() ? this.refreshCurrentPage() : router.navigateToCode();
  }

  render() {
    return (
      <div className="sidebar">
        <div className="sidebar-header">
          <a href="/">
            <img src="/image/logo_white.svg" className="logo" />
          </a>
        </div>
        <div className="sidebar-body">
          <div
            className={`sidebar-item ${this.isHomeSelected() ? "selected" : ""}`}
            onClick={this.navigateToAllBuilds.bind(this)}>
            <List /> All builds
          </div>
          <div
            className={`sidebar-item ${this.isTrendsSelected() ? "selected" : ""}`}
            onClick={this.navigateToTrends.bind(this)}>
            <BarChart2 /> Trends
          </div>
          {capabilities.test && (
            <div
              className={`sidebar-item ${this.isTapSelected() ? "selected" : ""}`}
              onClick={this.navigateToTap.bind(this)}>
              <LayoutGrid /> Tests
            </div>
          )}
          <div
            className={`sidebar-item ${this.isUsersSelected() ? "selected" : ""}`}
            onClick={this.navigateToUsers.bind(this)}>
            <Users /> Users
          </div>
          <div
            className={`sidebar-item ${this.isReposSelected() ? "selected" : ""}`}
            onClick={this.navigateToRepos.bind(this)}>
            <Github /> Repos
          </div>
          <div
            className={`sidebar-item ${this.isBranchesSelected() ? "selected" : ""}`}
            onClick={this.navigateToBranches.bind(this)}>
            <GitBranch /> Branches
          </div>
          <div
            className={`sidebar-item ${this.isCommitsSelected() ? "selected" : ""}`}
            onClick={this.navigateToCommits.bind(this)}>
            <GitCommit /> Commits
          </div>
          <div
            className={`sidebar-item ${this.isHostsSelected() ? "selected" : ""}`}
            onClick={this.navigateToHosts.bind(this)}>
            <HardDrive /> Hosts
          </div>
          {router.canAccessExecutorsPage(this.props.user) && (
            <div
              className={`sidebar-item ${this.isExecutorsSelected() ? "selected" : ""}`}
              onClick={this.navigateToExecutors.bind(this)}>
              <Cloud /> Executors
            </div>
          )}
          {router.canAccessWorkflowsPage(this.props.user) && (
            <div
              className={`sidebar-item ${this.isWorkflowsSelected() ? "selected" : ""}`}
              onClick={this.navigateToWorkflows.bind(this)}>
              <PlayCircle /> Workflows
            </div>
          )}
          {capabilities.code && (
            <div
              className={`sidebar-item ${this.isCodeSelected() ? "selected" : ""}`}
              onClick={this.navigateToCode.bind(this)}>
              <Code /> Code
            </div>
          )}
          <div
            className={`sidebar-item ${this.isSettingsSelected() ? "selected" : ""}`}
            onClick={() => router.navigateToSetup()}>
            <Settings /> Setup
          </div>
          {router.canAccessUsagePage(this.props.user) && (
            <div
              className={`sidebar-item ${this.isUsageSelected() ? "selected" : ""}`}
              onClick={this.navigateToUsage.bind(this)}>
              <Gauge /> Usage
            </div>
          )}
          <a className="sidebar-item" href="https://www.buildbuddy.io/docs/" target="_blank">
            <BookOpen /> Docs
          </a>
        </div>
        <div className={`sidebar-footer ${this.state.profileExpanded ? "expanded" : ""}`}>
          {this.state.profileExpanded && (
            <div className="sidebar-expanded-profile">
              <div className="org-picker">
                <div className="org-picker-header">Organization</div>
                <div className="org-list" role="menu">
                  {this.props.user.groups.map((group) => (
                    <div
                      key={group.id}
                      role="menuitem"
                      className={`sidebar-item org-picker-item ${
                        group.id === this.props.user.selectedGroup.id ? "selected" : ""
                      }`}
                      onClick={this.handleOrgClicked.bind(this, group.id)}>
                      {group.id === this.props.user.selectedGroup.id ? <CheckCircle /> : <Circle />}
                      <div className="org-picker-item-label">{group.name}</div>
                    </div>
                  ))}
                </div>
                {this.props.user && !this.props.user?.isInDefaultGroup() && (
                  <div className="sidebar-item create-organization" onClick={this.handleCreateOrgClicked.bind(this)}>
                    <PlusCircle />
                    Create org
                  </div>
                )}
              </div>
              <hr />
              <div className="sidebar-item sidebar-logout-item" onClick={() => authService.logout()}>
                <LogOut /> Logout
              </div>
              <div className="sidebar-item" onClick={() => router.navigateToSettings()}>
                <Sliders />
                Settings
              </div>
            </div>
          )}
          {capabilities.auth && this.props.user && (
            <div onClick={this.handleProfileClicked.bind(this)} className="sidebar-profile">
              <img
                className={`sidebar-profile-photo ${
                  this.props.user?.displayUser?.profileImageUrl ? "" : "default-photo"
                }`}
                src={this.props.user?.displayUser?.profileImageUrl || "/image/user-regular.svg"}
              />
              <div className="sidebar-profile-name">
                <div className="sidebar-profile-user">
                  {this.props.user?.displayUser?.name?.full || this.props.user?.displayUser?.email}
                </div>
                <div className="sidebar-profile-org">{this.props.user?.selectedGroupName()}</div>
              </div>
              {this.state.profileExpanded ? (
                <ChevronDown className="sidebar-profile-arrow" />
              ) : (
                <ChevronUp className="sidebar-profile-arrow" />
              )}
            </div>
          )}
        </div>
      </div>
    );
  }
}
