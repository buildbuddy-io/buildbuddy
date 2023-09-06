import React from "react";
import { User } from "../../../app/auth/auth_service";
import Button, { OutlinedButton, FilledButton } from "../../../app/components/button/button";
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
import Banner from "../../../app/components/banner/banner";
import { Link, TextLink } from "../../../app/components/link/link";
import { Tooltip } from "../../../app/components/tooltip/tooltip";
import TextInput from "../../../app/components/input/input";
import alert_service from "../../../app/alert/alert_service";
import errorService from "../../../app/errors/error_service";
import Spinner from "../../../app/components/spinner/spinner";
import Checkbox from "../../../app/components/checkbox/checkbox";
import ActionListComponent from "./action_list";

type Workflow = workflow.GetWorkflowsResponse.Workflow;

export type WorkflowsProps = {
  path: string;
  user: User;
};

export default class WorkflowsComponent extends React.Component<WorkflowsProps> {
  render() {
    const { path, user } = this.props;

    if (user.isGroupAdmin()) {
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
    }

    return <ListWorkflowsComponent user={user} />;
  }
}

type State = {
  workflowsLoading: boolean;
  workflowsResponse: workflow.GetWorkflowsResponse | null;

  reposLoading: boolean;
  reposResponse: github.GetLinkedReposResponse | null;

  workflowHistoryLoading: boolean;
  workflowHistoryResponse: workflow.GetWorkflowHistoryResponse | null;

  repoToDelete: string | null;

  workflowToDelete: Workflow | null;
  isDeletingWorkflow: boolean;

  repoToUnlink: string | null;
  isUnlinkingRepo: boolean;

  showCleanWorkflowWarning: boolean;
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

    workflowHistoryLoading: false,
    workflowHistoryResponse: null,

    repoToDelete: null,

    workflowToDelete: null,
    isDeletingWorkflow: false,

    repoToUnlink: null,
    isUnlinkingRepo: false,

    showCleanWorkflowWarning: false,
  };

  private fetchWorkflowsRPC?: CancelablePromise;
  private fetchReposRPC?: CancelablePromise;
  private fetchWorkflowHistoryRPC?: CancelablePromise;

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
    this.fetchWorkflowHistory();
  }

  private fetchWorkflowHistory() {
    this.fetchWorkflowHistoryRPC?.cancel();
    this.fetchWorkflowHistoryRPC = undefined;
    this.setState({ workflowHistoryLoading: false, workflowHistoryResponse: null });

    if (!capabilities.config.workflowHistoryEnabled) {
      return;
    }
    this.setState({ workflowHistoryLoading: true });
    this.fetchWorkflowHistoryRPC = rpcService.service
      .getWorkflowHistory(new workflow.GetWorkflowHistoryRequest())
      .then((response) => this.setState({ workflowHistoryResponse: response }))
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ workflowHistoryLoading: false }));
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

  renderActionList(repoUrl: string): JSX.Element | null {
    const history = this.state.workflowHistoryResponse?.workflowHistory.find(
      (h: workflow.GetWorkflowHistoryResponse.WorkflowHistory) => h.repoUrl === repoUrl
    );

    if (history && history.actionHistory.length > 0) {
      return <ActionListComponent repoUrl={history.repoUrl} history={history.actionHistory}></ActionListComponent>;
    } else {
      return null;
    }
  }

  render() {
    if (this.state.workflowsLoading || this.state.reposLoading || this.state.workflowHistoryLoading) {
      return <div className="loading" />;
    }
    const isAdmin = this.props.user.isGroupAdmin();
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
                {isAdmin && <Button onClick={this.onClickCreate.bind(this)}>Link a repository</Button>}
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
                    repo.{" "}
                    {!isAdmin && "Contact your organization's administrator if you are interested in using workflows."}
                  </div>
                  <div className="buttons">
                    {isAdmin && <Button onClick={this.onClickCreate.bind(this)}>Link a repository</Button>}
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
                <>
                  <RepoItem
                    user={this.props.user}
                    repoUrl={repoUrl}
                    onClickUnlinkItem={() => this.setState({ repoToUnlink: repoUrl })}
                    showCleanWorkflowWarning={() => this.setState({ showCleanWorkflowWarning: true })}
                  />
                  {this.renderActionList(repoUrl)}
                </>
              ))}
              {/* Render legacy workflows */}
              {this.state.workflowsResponse?.workflow.map((workflow) => (
                <>
                  <RepoItem
                    user={this.props.user}
                    repoUrl={workflow.repoUrl}
                    webhookUrl={workflow.webhookUrl}
                    onClickUnlinkItem={() => this.setState({ workflowToDelete: workflow })}
                  />
                  {workflow.repoUrl && this.renderActionList(workflow.repoUrl)}
                </>
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
          <SimpleModalDialog
            title="Run workflow in clean container"
            isOpen={Boolean(this.state.showCleanWorkflowWarning)}
            onRequestClose={() => this.setState({ showCleanWorkflowWarning: false })}
            submitLabel="Okay"
            destructive
            onSubmit={() => this.setState({ showCleanWorkflowWarning: false })}>
            <p>Are you sure you want to run the workflow in a clean container?</p>
            <p>
              This will prevent all existing workflow containers from being reused by other workflow runs, making them
              slower, so this flag is not encouraged.
            </p>
          </SimpleModalDialog>
        </div>
      </div>
    );
  }
}

type RepoItemProps = {
  user?: User;
  repoUrl: string;
  webhookUrl?: string;
  onClickUnlinkItem: (url: string) => void;
  showCleanWorkflowWarning?: () => void;
};

type RepoItemState = {
  isMenuOpen: boolean;

  showRunWorkflowInput: boolean;
  runWorkflowBranch: string;
  runWorkflowVisibility: string;
  runClean: boolean;
  isWorkflowRunning: boolean;
  runWorkflowActionStatuses: workflow.ExecuteWorkflowResponse.ActionStatus[] | null;
};

class RepoItem extends React.Component<RepoItemProps, RepoItemState> {
  state: RepoItemState = {
    isMenuOpen: false,
    showRunWorkflowInput: false,
    runWorkflowBranch: "",
    runWorkflowVisibility: "",
    runClean: false,
    isWorkflowRunning: false,
    runWorkflowActionStatuses: null,
  };

  private onClickMenuButton() {
    this.setState({ isMenuOpen: !this.state.isMenuOpen });
  }

  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
    this.setState({ showRunWorkflowInput: false });
  }

  private onClickCopyWebhookUrl() {
    copyToClipboard(this.props.webhookUrl || "");
    alert_service.success("Copied webhook URL to clipboard!");
  }

  private onClickUnlinkMenuItem() {
    this.setState({ isMenuOpen: false });
    this.props.onClickUnlinkItem(this.props.repoUrl);
  }

  private showRunWorkflowInput() {
    this.setState({ showRunWorkflowInput: true });
  }

  onClickRunClean() {
    if (!this.state.runClean && this.props.showCleanWorkflowWarning) {
      this.props.showCleanWorkflowWarning();
    }
    this.setState({ runClean: !this.state.runClean });
  }

  private runWorkflow() {
    this.setState({ runWorkflowActionStatuses: null });
    this.setState({ isWorkflowRunning: true });
    this.onCloseMenu();

    rpcService.service
      .executeWorkflow(
        new workflow.ExecuteWorkflowRequest({
          pushedRepoUrl: this.props.repoUrl,
          pushedBranch: this.state.runWorkflowBranch,
          targetRepoUrl: this.props.repoUrl,
          targetBranch: this.state.runWorkflowBranch,
          clean: this.state.runClean,
          visibility: this.state.runWorkflowVisibility,
        })
      )
      .then((response) => {
        if (response.actionStatuses.length > 0) {
          this.setState({ runWorkflowActionStatuses: response.actionStatuses });
        } else {
          errorService.handleError(`No actions to execute for ${this.props.repoUrl}.`);
        }
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isWorkflowRunning: false }));
  }

  renderWorkflowResults() {
    if (this.state.runWorkflowActionStatuses) {
      return this.state.runWorkflowActionStatuses.map((actionStatus) => {
        if ((actionStatus.status?.code || 0) !== 0 /*OK*/) {
          return (
            <Tooltip renderContent={() => this.renderActionErrorCard(actionStatus)}>
              <TextLink>{actionStatus.actionName}</TextLink>
            </Tooltip>
          );
        }
        const invocationLink = `/invocation/${actionStatus.invocationId}`;
        return (
          <div>
            <TextLink href={invocationLink} target="_blank">
              {actionStatus.actionName}
            </TextLink>
          </div>
        );
      });
    }
  }

  renderActionErrorCard(actionResult: workflow.ExecuteWorkflowResponse.ActionStatus) {
    return <div className="action-error-hovercard">{actionResult.status?.message || "Error"}</div>;
  }

  render() {
    const showCleanRerun = Boolean(
      this.props.user?.isGroupAdmin() || !this.props.user?.selectedGroup?.restrictCleanWorkflowRunsToAdmins
    );

    return (
      <div className="workflow-item container">
        <div className="workflow-item-column">
          <div className="workflow-item-row">
            {this.state.runWorkflowActionStatuses && (
              <Banner type="success">
                Executed workflow actions:
                <div>{this.renderWorkflowResults()}</div>
              </Banner>
            )}
            {this.state.isWorkflowRunning && <Spinner />}
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
        {this.props.user?.isGroupAdmin() && (
          <div className="workflow-item-column workflow-buttons-container">
            <div className="workflow-item-row">
              {/* The Run Workflow button is only supported for workflows configured with the Github App, not legacy workflows */}
              {!this.props.webhookUrl && (
                <div className="workflow-button-container">
                  <OutlinedButton
                    onClick={this.showRunWorkflowInput.bind(this)}
                    disabled={this.state.isWorkflowRunning}>
                    Run workflow
                  </OutlinedButton>
                  <Popup
                    isOpen={this.state.showRunWorkflowInput}
                    onRequestClose={this.onCloseMenu.bind(this)}
                    className="run-workflow-input">
                    <div className="title">Run workflow from branch:</div>
                    <TextInput
                      placeholder={"e.g. main"}
                      onChange={(e) => this.setState({ runWorkflowBranch: e.target.value })}
                    />
                    <div className="title">Visibility metadata:</div>
                    <TextInput
                      placeholder={"e.g. PUBLIC (optional)"}
                      onChange={(e) => this.setState({ runWorkflowVisibility: e.target.value })}
                    />
                    {/*The Popup component has e.preventDefault in its onClick handler, which messes up the checkbox.
                  We need e.stopPropagation to prevent the parent Popup's onClick handler from triggering
                  */}
                    {showCleanRerun && (
                      <label className="run-clean-container" onClick={(e) => e.stopPropagation()}>
                        <Checkbox checked={this.state.runClean} onChange={this.onClickRunClean.bind(this)} />
                        <span>Run in a clean container</span>
                      </label>
                    )}
                    <FilledButton onClick={this.runWorkflow.bind(this)} disabled={this.state.runWorkflowBranch === ""}>
                      Run workflow
                    </FilledButton>
                  </Popup>
                </div>
              )}
              <div className="workflow-button-container">
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
        )}
      </div>
    );
  }
}

function formatURL(url: string) {
  return normalizeRepoURL(url).replace(/^https:\/\//, "");
}
