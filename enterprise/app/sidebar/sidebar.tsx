import {
  BookOpen,
  Gauge,
  Cloud,
  PlayCircle,
  Code,
  SearchCode,
  HardDrive,
  Users,
  GitBranch,
  Github,
  BarChart2,
  LayoutGrid,
  GitCommit,
  List,
  Sliders,
  Terminal,
  PanelLeftClose,
  PanelLeftOpen,
  Fingerprint,
  MessageCircle,
} from "lucide-react";
import React from "react";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import Link, { LinkProps } from "../../../app/components/link/link";
import router, { Path } from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import OrgPicker from "../org_picker/org_picker";

interface Props {
  user?: User;
  tab: string;
  path: string;
  search: URLSearchParams;
  dense: boolean;
}
interface State {
  sidebarExpanded: boolean;
  profileExpanded: boolean;
}

const sidebarExpandedKey = "sidebar-expanded";

export default class SidebarComponent extends React.Component<Props, State> {
  state: State = {
    sidebarExpanded: localStorage[sidebarExpandedKey] != "false",
    profileExpanded: false,
  };

  handleSidebarToggled(newState: boolean) {
    localStorage[sidebarExpandedKey] = newState ? "true" : "false";
    this.setState({ sidebarExpanded: newState });
  }

  isHomeSelected() {
    return this.props.path == "/" && (!this.props.tab || this.props.tab == "#");
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

  isCodeSearchSelected() {
    return this.props.path.startsWith("/search/");
  }

  isUsersSelected() {
    return this.props.path.startsWith("/history/user/") || this.props.tab == "#users";
  }

  isReposSelected() {
    return this.props.path.startsWith("/history/repo/") || this.props.tab == "#repos";
  }

  isBranchesSelected() {
    return this.props.path.startsWith("/history/branch/") || this.props.tab == "#branches";
  }

  isCommitsSelected() {
    return this.props.path.startsWith("/history/commit/") || this.props.tab == "#commits";
  }

  isHostsSelected() {
    return this.props.path.startsWith("/history/host/") || this.props.tab == "#hosts";
  }

  isWorkflowsSelected() {
    return this.props.path === "/workflows/";
  }

  isCodeSelected() {
    return this.props.path === "/code/";
  }

  isReviewsSelected() {
    return this.props.path === "/reviews/";
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

  isAuditLogsSelected() {
    return this.props.path.startsWith("/audit-logs/");
  }

  refreshCurrentPage() {
    rpcService.events.next("refresh");
  }

  render() {
    let expanded =
      (!localStorage[sidebarExpandedKey] && !this.props.dense) ||
      this.state.profileExpanded ||
      (localStorage[sidebarExpandedKey] && this.state.sidebarExpanded);
    return (
      <div className={`sidebar ${expanded ? "expanded" : "collapsed"}`}>
        <div className="sidebar-header">
          <a href="/">
            <img src="/image/logo_white.svg" className="logo" />
          </a>
        </div>
        <div className="sidebar-body">
          <SidebarLink selected={this.isHomeSelected()} href={Path.home} title="All builds">
            <List className="icon" />
            <span className="sidebar-item-text">All builds</span>
          </SidebarLink>
          <SidebarLink selected={this.isTrendsSelected()} href={Path.trendsPath} title="Trends">
            <BarChart2 className="icon" />
            <span className="sidebar-item-text">Trends</span>
          </SidebarLink>
          {capabilities.test && (
            <SidebarLink selected={this.isTapSelected()} href={Path.tapPath} title="Tests">
              <LayoutGrid className="icon" />
              <span className="sidebar-item-text">Tests</span>
            </SidebarLink>
          )}
          <SidebarLink selected={this.isUsersSelected()} href="/#users" title="Users">
            <Users className="icon" />
            <span className="sidebar-item-text">Users</span>
          </SidebarLink>
          <SidebarLink selected={this.isReposSelected()} href="/#repos" title="Repos">
            <Github className="icon" />
            <span className="sidebar-item-text">Repos</span>
          </SidebarLink>
          <SidebarLink selected={this.isBranchesSelected()} href="/#branches" title="Branches">
            <GitBranch className="icon" />
            <span className="sidebar-item-text">Branches</span>
          </SidebarLink>
          <SidebarLink selected={this.isCommitsSelected()} href="/#commits" title="Commits">
            <GitCommit className="icon" />
            <span className="sidebar-item-text">Commits</span>
          </SidebarLink>
          <SidebarLink selected={this.isHostsSelected()} href="/#hosts" title="Hosts">
            <HardDrive className="icon" />
            <span className="sidebar-item-text">Hosts</span>
          </SidebarLink>
          {router.canAccessExecutorsPage(this.props.user) && (
            <SidebarLink selected={this.isExecutorsSelected()} href={Path.executorsPath} title="Executors">
              <Cloud className="icon" />
              <span className="sidebar-item-text">Executors</span>
            </SidebarLink>
          )}
          {router.canAccessWorkflowsPage(this.props.user) && (
            <SidebarLink selected={this.isWorkflowsSelected()} href={Path.workflowsPath} title="Workflows">
              <PlayCircle className="icon" />
              <span className="sidebar-item-text">Workflows</span>
            </SidebarLink>
          )}
          {capabilities.code && (
            <SidebarLink selected={this.isCodeSelected()} href={Path.codePath} title="Code">
              <Code className="icon" />
              <span className="sidebar-item-text">Code</span>
            </SidebarLink>
          )}
          {capabilities.config.codeReviewEnabled && (
            <SidebarLink selected={this.isReviewsSelected()} href={Path.reviewsPath} title="Review">
              <MessageCircle className="icon" />
              <span className="sidebar-item-text">Reviews</span>
            </SidebarLink>
          )}
          {capabilities.config.codesearchEnabled && (
            <SidebarLink selected={this.isCodeSearchSelected()} href={Path.codesearchPath} title="Search">
              <SearchCode className="icon" />
              <span className="sidebar-item-text">Search</span>
            </SidebarLink>
          )}
          <SidebarLink selected={this.isSetupSelected()} href={Path.setupPath} title="Quickstart">
            <Terminal className="icon" />
            <span className="sidebar-item-text">Quickstart</span>
          </SidebarLink>

          <SidebarLink selected={this.isSettingsSelected()} href={Path.settingsPath} title="Settings">
            <Sliders className="icon" />
            <span className="sidebar-item-text">Settings</span>
          </SidebarLink>

          {router.canAccessUsagePage(this.props.user) && (
            <SidebarLink selected={this.isUsageSelected()} href={Path.usagePath} title="Usage">
              <Gauge className="icon" />
              <span className="sidebar-item-text">Usage</span>
            </SidebarLink>
          )}
          {router.canAccessAuditLogsPage(this.props.user) && capabilities.config.auditLogsUiEnabled && (
            <SidebarLink selected={this.isAuditLogsSelected()} href={Path.auditLogsPath} title="Audit logs">
              <Fingerprint className="icon" />
              <span className="sidebar-item-text">Audit logs</span>
            </SidebarLink>
          )}
          <a className="sidebar-item" href="https://www.buildbuddy.io/docs/" target="_blank" title="Docs">
            <BookOpen className="icon" />
            <span className="sidebar-item-text">Docs</span>
          </a>
          <div
            className="sidebar-item sidebar-toggle"
            onClick={() => this.handleSidebarToggled(!expanded)}
            title={expanded ? "Collapse" : "Expand"}>
            {expanded ? (
              <>
                <PanelLeftClose />
                <span className="sidebar-item-text">Collapse</span>
              </>
            ) : (
              <>
                <PanelLeftOpen />
                <span className="sidebar-item-text">Expand</span>
              </>
            )}
          </div>
        </div>
        <OrgPicker
          user={this.props.user}
          forceCollapse={!expanded}
          onClick={(profileExpanded) => {
            this.setState({ profileExpanded: profileExpanded });
          }}
        />
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
