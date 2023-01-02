import {
  BookOpen,
  Gauge,
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
  ArrowRightCircle,
  Sliders,
  Terminal,
} from "lucide-react";
import React from "react";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import Link, { LinkProps } from "../../../app/components/link/link";
import router, { Path } from "../../../app/router/router";
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

export default class SidebarComponent extends React.Component<Props, State> {
  state: State = {
    profileExpanded: false,
  };

  handleProfileClicked() {
    this.setState({ profileExpanded: !this.state.profileExpanded });
  }

  handleCreateOrgClicked(e: React.MouseEvent) {
    router.navigateToCreateOrg();
  }

  handleSearchGroupsClicked() {
    window.dispatchEvent(new CustomEvent("groupSearchClick"));
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

  isSetupSelected() {
    return this.props.path.startsWith("/docs/");
  }

  isSettingsSelected() {
    return this.props.path.startsWith("/settings/");
  }

  isUsageSelected() {
    return this.props.path.startsWith("/usage/");
  }

  refreshCurrentPage() {
    rpcService.events.next("refresh");
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
          <SidebarLink selected={this.isHomeSelected()} href={Path.home}>
            <List className="icon" /> All builds
          </SidebarLink>
          <SidebarLink selected={this.isTrendsSelected()} href={Path.trendsPath}>
            <BarChart2 className="icon" /> Trends
          </SidebarLink>
          {capabilities.test && (
            <SidebarLink selected={this.isTapSelected()} href={Path.tapPath}>
              <LayoutGrid className="icon" /> Tests
            </SidebarLink>
          )}
          <SidebarLink selected={this.isUsersSelected()} href="/#users">
            <Users className="icon" /> Users
          </SidebarLink>
          <SidebarLink selected={this.isReposSelected()} href="/#repos">
            <Github className="icon" /> Repos
          </SidebarLink>
          <SidebarLink selected={this.isBranchesSelected()} href="/#branches">
            <GitBranch className="icon" /> Branches
          </SidebarLink>
          <SidebarLink selected={this.isCommitsSelected()} href="/#commits">
            <GitCommit className="icon" /> Commits
          </SidebarLink>
          <SidebarLink selected={this.isHostsSelected()} href="/#hosts">
            <HardDrive className="icon" /> Hosts
          </SidebarLink>
          {router.canAccessExecutorsPage(this.props.user) && (
            <SidebarLink selected={this.isExecutorsSelected()} href={Path.executorsPath}>
              <Cloud className="icon" /> Executors
            </SidebarLink>
          )}
          {router.canAccessWorkflowsPage(this.props.user) && (
            <SidebarLink selected={this.isWorkflowsSelected()} href={Path.workflowsPath}>
              <PlayCircle className="icon" /> Workflows
            </SidebarLink>
          )}
          {capabilities.code && (
            <SidebarLink selected={this.isCodeSelected()} href={Path.codePath}>
              <Code className="icon" /> Code
            </SidebarLink>
          )}
          <SidebarLink selected={this.isSetupSelected()} href={Path.setupPath}>
            <Terminal className="icon" /> Quickstart
          </SidebarLink>

          <SidebarLink selected={this.isSettingsSelected()} href={Path.settingsPath}>
            <Sliders className="icon" /> Settings
          </SidebarLink>

          {router.canAccessUsagePage(this.props.user) && (
            <SidebarLink selected={this.isUsageSelected()} href={Path.usagePath}>
              <Gauge className="icon" /> Usage
            </SidebarLink>
          )}
          <a className="sidebar-item" href="https://www.buildbuddy.io/docs/" target="_blank">
            <BookOpen className="icon" /> Docs
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
                      {group.id === this.props.user.selectedGroup.id ? (
                        <CheckCircle className="icon" />
                      ) : (
                        <Circle className="icon" />
                      )}
                      <div className="org-picker-item-label">{group.name}</div>
                    </div>
                  ))}
                </div>
                {this.props.user && router.canCreateOrg(this.props.user) && (
                  <div className="sidebar-item create-organization" onClick={this.handleCreateOrgClicked.bind(this)}>
                    <PlusCircle className="icon" />
                    Create org
                  </div>
                )}
                {Boolean(this.props.user?.canImpersonate()) && (
                  <div className="sidebar-item admin-only" onClick={this.handleSearchGroupsClicked.bind(this)}>
                    <ArrowRightCircle className="icon" />
                    Go to org
                  </div>
                )}
              </div>
              <hr />
              <div className="sidebar-item sidebar-logout-item" onClick={() => authService.logout()}>
                <LogOut className="icon" /> Logout
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
                <ChevronDown className="icon sidebar-profile-arrow" />
              ) : (
                <ChevronUp className="icon sidebar-profile-arrow" />
              )}
            </div>
          )}
        </div>
      </div>
    );
  }
}

type SidebarLinkProps = LinkProps & {
  selected: boolean;
};

class SidebarLink extends React.Component<SidebarLinkProps> {
  render() {
    const { selected, ...rest } = this.props;

    return <Link className={`sidebar-item ${selected ? "selected" : ""}`} {...rest} />;
  }
}
