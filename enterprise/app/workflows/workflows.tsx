import React from "react";
import { User } from "../../../app/auth/auth_service";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup from "../../../app/components/popup/popup";
import router from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { copyToClipboard } from "../../../app/util/clipboard";
import { workflow } from "../../../proto/workflow_ts_proto";
import CreateWorkflowComponent from "./create_workflow";
import GitHubImport from "./github_import";
import GitHubAppImport from "./github_app_import";
import WorkflowsZeroStateAnimation from "./zero_state";
import { AlertCircle, GitMerge, MoreVertical } from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import { github } from "../../../proto/github_ts_proto";
import error_service from "../../../app/errors/error_service";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import { normalizeRepoURL } from "../../../app/util/git";
import Link from "../../../app/components/link/link";
import alert_service from "../../../app/alert/alert_service";

type Workflow = workflow.GetWorkflowsResponse.Workflow;

export type WorkflowsProps = {
  path: string;
  user: User;
};

export default class WorkflowsComponent extends React.Component<WorkflowsProps> {
  render() {
    const { path, user } = this.props;

    if (capabilities.config.githubAppEnabled && (path === "/workflows/new" || path.startsWith("/workflows/new/"))) {
      return <GitHubAppImport user={user} />;
    }

    if (path === "/workflows/new") {
      if (user.selectedGroup.githubLinked) {
        return <GitHubImport />;
      } else {
        return <CreateWorkflowComponent user={user} />;
      }
    }
    if (path === "/workflows/new/github") {
      return <GitHubImport />;
    }
    if (path === "/workflows/new/custom") {
      return <CreateWorkflowComponent user={user} />;
    }

    return <ListWorkflowsComponent user={user} />;
  }
}

type State = {
  workflowsLoading: boolean;
  workflowsResponse: workflow.GetWorkflowsResponse | null;

  reposLoading: boolean;
  reposResponse: github.GetLinkedReposResponse | null;

  repoToDelete: string | null;

  workflowToDelete: Workflow | null;
  isDeletingWorkflow: boolean;

  repoToUnlink: string | null;
  isUnlinkingRepo: boolean;
};

export type ListWorkflowsProps = {
  user: User;
};

class ListWorkflowsComponent extends React.Component<ListWorkflowsProps, State> {
  state: State = {
    workflowsLoading: false,
    workflowsResponse: null,

    reposLoading: false,
    reposResponse: null,

    repoToDelete: null,

    workflowToDelete: null,
    isDeletingWorkflow: false,

    repoToUnlink: null,
    isUnlinkingRepo: false,
  };

  private fetchWorkflowsRPC?: CancelablePromise;
  private fetchReposRPC?: CancelablePromise;

  componentDidMount() {
    document.title = "Workflows | BuildBuddy";
    this.fetch();
  }

  componentDidUpdate(prevProps: WorkflowsProps) {
    if (this.props.user !== prevProps.user) {
      this.fetch();
    }
  }

  private fetch() {
    this.fetchWorkflows();
    if (capabilities.config.githubAppEnabled) {
      this.fetchRepos();
    }
  }

  private fetchWorkflows() {
    this.fetchWorkflowsRPC?.cancel();
    if (!this.props.user) return;

    this.setState({ workflowsLoading: true });
    this.fetchWorkflowsRPC = rpcService.service
      .getWorkflows(new workflow.GetWorkflowsRequest())
      .then((response) => this.setState({ workflowsResponse: response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ workflowsLoading: false }));
  }

  private fetchRepos() {
    this.fetchReposRPC?.cancel();
    if (!this.props.user) return;

    this.setState({ reposLoading: true });
    this.fetchReposRPC = rpcService.service
      .getLinkedGitHubRepos(new github.GetLinkedReposRequest())
      .then((response) => this.setState({ reposResponse: response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ reposLoading: false }));
  }

  private onClickCreate() {
    router.navigateTo("/workflows/new");
  }

  private async onClickUnlinkWorkflow() {
    this.setState({ isDeletingWorkflow: true });
    try {
      await rpcService.service.deleteWorkflow(
        new workflow.DeleteWorkflowRequest({ id: this.state.workflowToDelete?.id })
      );
      this.setState({ workflowToDelete: null });
      this.fetch();
    } catch (e) {
      error_service.handleError(e);
    } finally {
      this.setState({ isDeletingWorkflow: false });
    }
  }

  private onClickUnlinkRepo() {
    this.setState({ isUnlinkingRepo: true });
    const repoUrl = this.state.repoToUnlink!;
    rpcService.service
      .unlinkGitHubRepo(new github.UnlinkRepoRequest({ repoUrl }))
      .then(() => {
        alert_service.success(`Successfully unlinked ${repoUrl}`);
        this.setState({ repoToUnlink: null });
        this.fetch();
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ isUnlinkingRepo: false }));
  }

  render() {
    if (this.state.workflowsLoading || this.state.reposLoading) {
      return <div className="loading" />;
    }
    return (
      <div className="workflows-page">
        <div className="shelf">
          <div className="container">
            <div>
              <div className="breadcrumbs">
                {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
                <span>Workflows</span>
              </div>
              <div className="title">Workflows</div>
            </div>
            {Boolean(this.state.workflowsResponse?.workflow?.length || this.state.reposResponse?.repoUrls?.length) && (
              <div className="buttons create-new-container">
                <Button onClick={this.onClickCreate.bind(this)}>Link a repository</Button>
                <OutlinedLinkButton href="https://docs.buildbuddy.io/docs/workflows-setup" target="_blank">
                  Learn more
                </OutlinedLinkButton>
              </div>
            )}
          </div>
        </div>
        <div className="content">
          {!(this.state.workflowsResponse?.workflow?.length || this.state.reposResponse?.repoUrls?.length) && (
            <div className="no-workflows-container">
              <div className="no-workflows-card">
                <WorkflowsZeroStateAnimation />
                <div className="details">
                  <div>
                    Workflows automatically build and test your code with BuildBuddy when commits are pushed to your
                    repo.
                  </div>
                  <div className="buttons">
                    <Button onClick={this.onClickCreate.bind(this)}>Link a repository</Button>
                    <OutlinedLinkButton href="https://docs.buildbuddy.io/docs/workflows-setup" target="_blank">
                      Learn more
                    </OutlinedLinkButton>
                  </div>
                </div>
              </div>
            </div>
          )}
          {Boolean(this.state.workflowsResponse?.workflow?.length || this.state.reposResponse?.repoUrls?.length) && (
            <div className="workflows-list">
              {/* Render linked repositories */}
              {this.state.reposResponse?.repoUrls.map((repoUrl) => (
                <RepoItem repoUrl={repoUrl} onClickUnlinkItem={() => this.setState({ repoToUnlink: repoUrl })} />
              ))}
              {/* Render legacy workflows */}
              {this.state.workflowsResponse?.workflow.map((workflow) => (
                <RepoItem
                  repoUrl={workflow.repoUrl}
                  webhookUrl={workflow.webhookUrl}
                  onClickUnlinkItem={() => this.setState({ workflowToDelete: workflow })}
                />
              ))}
            </div>
          )}
          <SimpleModalDialog
            title="Unlink repository"
            isOpen={Boolean(this.state.repoToUnlink)}
            onRequestClose={() => this.setState({ repoToUnlink: null })}
            submitLabel="Unlink"
            destructive
            onSubmit={() => this.onClickUnlinkRepo()}
            loading={this.state.isUnlinkingRepo}>
            <p>
              Are you sure you want to unlink <strong>{formatURL(this.state.repoToUnlink || "")}</strong>? This will
              prevent BuildBuddy workflows from being run.
            </p>
          </SimpleModalDialog>
          <SimpleModalDialog
            title="Unlink repository"
            isOpen={Boolean(this.state.workflowToDelete)}
            onRequestClose={() => this.setState({ workflowToDelete: null })}
            submitLabel="Unlink"
            destructive
            onSubmit={() => this.onClickUnlinkWorkflow()}
            loading={this.state.isDeletingWorkflow}>
            <p>
              Are you sure you want to unlink <strong>{formatURL(this.state.workflowToDelete?.repoUrl || "")}</strong>?
              This will prevent BuildBuddy workflows from being run.
            </p>
          </SimpleModalDialog>
        </div>
      </div>
    );
  }
}

type RepoItemProps = {
  repoUrl: string;
  webhookUrl?: string;
  onClickUnlinkItem: (url: string) => void;
};

type RepoItemState = {
  isMenuOpen: boolean;
};

class RepoItem extends React.Component<RepoItemProps, RepoItemState> {
  state: RepoItemState = { isMenuOpen: false };

  private onClickMenuButton() {
    this.setState({ isMenuOpen: !this.state.isMenuOpen });
  }

  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private onClickCopyWebhookUrl() {
    copyToClipboard(this.props.webhookUrl || "");
    alert_service.success("Copied webhook URL to clipboard!");
  }

  private onClickUnlinkMenuItem() {
    this.setState({ isMenuOpen: false });
    this.props.onClickUnlinkItem(this.props.repoUrl);
  }

  render() {
    return (
      <div className="workflow-item container">
        <div className="workflow-item-column">
          <div className="workflow-item-row">
            <GitMerge />
            <div>
              <Link href={router.getWorkflowHistoryUrl(normalizeRepoURL(this.props.repoUrl))} className="repo-url">
                {formatURL(this.props.repoUrl)}
              </Link>
              {capabilities.config.githubAppEnabled && this.props.webhookUrl && (
                <div className="upgrade-notice">
                  <AlertCircle className="icon orange" /> This repository uses the legacy GitHub OAuth integration.
                  Unlink and re-link to use the new GitHub App integration.
                </div>
              )}
            </div>
          </div>
        </div>
        <div className="workflow-item-column workflow-dropdown-container">
          <div>
            <OutlinedButton
              title="Workflow options"
              className="icon-button"
              onClick={this.onClickMenuButton.bind(this)}>
              <MoreVertical />
            </OutlinedButton>
            <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)}>
              <Menu className="workflow-dropdown-menu">
                {this.props.webhookUrl && (
                  <MenuItem onClick={this.onClickCopyWebhookUrl.bind(this)}>Copy webhook URL</MenuItem>
                )}
                <MenuItem onClick={this.onClickUnlinkMenuItem.bind(this)}>Unlink repository</MenuItem>
              </Menu>
            </Popup>
          </div>
        </div>
      </div>
    );
  }
}

function formatURL(url: string) {
  return normalizeRepoURL(url).replace(/^https:\/\//, "");
}
