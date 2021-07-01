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

  isCommitsSelected() {
    return this.props.path.startsWith("/history/commit/") || this.props.hash == "#commits";
  }

  isHostsSelected() {
    return this.props.path.startsWith("/history/host/") || this.props.hash == "#hosts";
  }

  isWorkflowsSelected() {
    return this.props.path === "/workflows/";
  }

  isSettingsSelected() {
    return this.props.path.startsWith("/docs/setup/");
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

  navigateToCommits() {
    this.isCommitsSelected() ? this.refreshCurrentPage() : router.navigateHome("#commits");
  }

  navigateToHosts() {
    this.isHostsSelected() ? this.refreshCurrentPage() : router.navigateHome("#hosts");
  }

  navigateToWorkflows() {
    this.isWorkflowsSelected() ? this.refreshCurrentPage() : router.navigateToWorkflows();
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
            <img src="/image/list-white.svg" /> All builds
          </div>
          <div
            className={`sidebar-item ${this.isTrendsSelected() ? "selected" : ""}`}
            onClick={this.navigateToTrends.bind(this)}>
            <img src="/image/bar-chart-white.svg" /> Trends
          </div>
          {capabilities.test && (
            <div
              className={`sidebar-item ${this.isTapSelected() ? "selected" : ""}`}
              onClick={this.navigateToTap.bind(this)}>
              <img src="/image/grid-white.svg" /> Tests
            </div>
          )}
          <div
            className={`sidebar-item ${this.isUsersSelected() ? "selected" : ""}`}
            onClick={this.navigateToUsers.bind(this)}>
            <img src="/image/users-white.svg" /> Users
          </div>
          <div
            className={`sidebar-item ${this.isReposSelected() ? "selected" : ""}`}
            onClick={this.navigateToRepos.bind(this)}>
            <img src="/image/github-white.svg" /> Repos
          </div>
          <div
            className={`sidebar-item ${this.isCommitsSelected() ? "selected" : ""}`}
            onClick={this.navigateToCommits.bind(this)}>
            <img src="/image/git-commit-white.svg" /> Commits
          </div>
          <div
            className={`sidebar-item ${this.isHostsSelected() ? "selected" : ""}`}
            onClick={this.navigateToHosts.bind(this)}>
            <img src="/image/hard-drive-white.svg" /> Hosts
          </div>
          {capabilities.executors && (
            <div
              className={`sidebar-item ${this.isExecutorsSelected() ? "selected" : ""}`}
              onClick={this.navigateToExecutors.bind(this)}>
              <img src="/image/cloud-white.svg" /> Executors
            </div>
          )}
          {capabilities.workflows && (
            <div
              className={`sidebar-item ${this.isWorkflowsSelected() ? "selected" : ""}`}
              onClick={this.navigateToWorkflows.bind(this)}>
              <img src="/image/play-circle-white.svg" /> Workflows
            </div>
          )}
          <div
            className={`sidebar-item ${this.isSettingsSelected() ? "selected" : ""}`}
            onClick={() => router.navigateToSetup()}>
            <img src="/image/settings-white.svg" /> Setup
          </div>
          <a className="sidebar-item" href="https://www.buildbuddy.io/docs/" target="_blank">
            <img src="/image/book-open-white.svg" /> Docs
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
                      <img
                        src={
                          group.id === this.props.user.selectedGroup.id
                            ? "/image/check-circle-white.svg"
                            : "/image/circle-white.svg"
                        }
                      />{" "}
                      <div className="org-picker-item-label">{group.name}</div>
                    </div>
                  ))}
                </div>
                {this.props.user && !this.props.user?.isInDefaultGroup() && (
                  <div className="sidebar-item create-organization" onClick={this.handleCreateOrgClicked.bind(this)}>
                    <img src="/image/plus-circle-white.svg" />
                    Create org
                  </div>
                )}
              </div>
              <div className="sidebar-item" onClick={() => authService.logout()}>
                <img src="/image/log-out-white.svg" /> Logout
              </div>
              <div className="sidebar-item" onClick={() => router.navigateToSettings()}>
                <img src="/image/sliders-white.svg" />
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
                <div className="sidebar-profile-user">{this.props.user?.displayUser?.name?.full}</div>
                <div className="sidebar-profile-org">{this.props.user?.selectedGroupName()}</div>
              </div>
              <img
                className="sidebar-profile-arrow"
                src={this.state.profileExpanded ? "/image/chevron-down-white.svg" : "/image/chevron-up-white.svg"}
              />
            </div>
          )}
        </div>
      </div>
    );
  }
}
