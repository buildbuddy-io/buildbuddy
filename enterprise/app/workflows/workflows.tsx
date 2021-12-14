import React from "react";
import { from, Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Modal from "../../../app/components/modal/modal";
import Popup from "../../../app/components/popup/popup";
import Spinner from "../../../app/components/spinner/spinner";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { copyToClipboard } from "../../../app/util/clipboard";
import { BuildBuddyError } from "../../../app/util/errors";
import { workflow } from "../../../proto/workflow_ts_proto";
import CreateWorkflowComponent from "./create_workflow";
import GitHubImport from "./github_import";
import WorkflowsZeroStateAnimation from "./zero_state";
import { GitMerge, MoreVertical } from "lucide-react";

type Workflow = workflow.GetWorkflowsResponse.IWorkflow;

export type WorkflowsProps = {
  path: string;
  user: User;
};

export default class WorkflowsComponent extends React.Component<WorkflowsProps> {
  render() {
    const { path, user } = this.props;

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
  error?: BuildBuddyError;
  response?: workflow.GetWorkflowsResponse;
  workflowToDelete?: Workflow | null;
  isDeleting?: boolean;
  deleteError?: BuildBuddyError;
};

export type ListWorkflowsProps = {
  user: User;
};

class ListWorkflowsComponent extends React.Component<ListWorkflowsProps, State> {
  private workflowsSubscription: Subscription = new Subscription();
  state: State = {};

  componentDidMount() {
    document.title = "Workflows | BuildBuddy";
    this.fetchWorkflows();
  }

  componentDidUpdate(prevProps: WorkflowsProps) {
    if (this.props.user !== prevProps.user) {
      this.workflowsSubscription.unsubscribe();
      this.fetchWorkflows();
    }
  }

  componentWillUnmount() {
    this.workflowsSubscription.unsubscribe();
  }

  private fetchWorkflows() {
    if (!this.props.user) return;

    this.state = {};
    this.workflowsSubscription = from<Promise<workflow.GetWorkflowsResponse>>(
      rpcService.service.getWorkflows(new workflow.GetWorkflowsRequest())
    ).subscribe(
      (response) => this.setState({ response }),
      (e) => this.setState({ error: BuildBuddyError.parse(e) })
    );
  }

  private onClickCreate() {
    router.navigateTo("/workflows/new");
  }

  private onClickUnlinkItem(workflowToDelete: Workflow) {
    this.setState({ workflowToDelete });
  }

  private async onClickUnlink() {
    this.setState({ isDeleting: true });
    try {
      await rpcService.service.deleteWorkflow(
        new workflow.DeleteWorkflowRequest({ id: this.state.workflowToDelete.id })
      );
      this.setState({ workflowToDelete: null });

      this.workflowsSubscription.unsubscribe();
      this.fetchWorkflows();
    } catch (e) {
      this.setState({ deleteError: BuildBuddyError.parse(e) });
    } finally {
      this.setState({ isDeleting: false });
    }
  }

  private onCloseDeleteDialog() {
    this.setState({ workflowToDelete: null, deleteError: null });
  }

  render() {
    const { error, response, workflowToDelete, isDeleting, deleteError } = this.state;
    const loading = !(error || response);
    if (loading) {
      return <div className="loading" />;
    }

    const workflowToDeleteUrl = workflowToDelete ? new URL(workflowToDelete.repoUrl) : null;

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
            {response && Boolean(response.workflow.length) && (
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
          {/* TODO: better styling of this error */}
          {error && <div className="error">{error.message}</div>}
          {response && !response.workflow.length && (
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
          {Boolean(response?.workflow?.length) && (
            <>
              <div className="workflows-list">
                {response.workflow.map((workflow) => (
                  <WorkflowItem workflow={workflow} onClickUnlinkItem={this.onClickUnlinkItem.bind(this)} />
                ))}
              </div>
              <Modal isOpen={Boolean(workflowToDelete)} onRequestClose={this.onCloseDeleteDialog.bind(this)}>
                <Dialog className="delete-workflow-dialog">
                  <DialogHeader>
                    <DialogTitle>Unlink repository</DialogTitle>
                  </DialogHeader>
                  <DialogBody className="dialog-body">
                    <div>
                      Are you sure you want to unlink{" "}
                      <strong>
                        {workflowToDeleteUrl?.host}
                        {workflowToDeleteUrl?.pathname}
                      </strong>
                      ? This will prevent BuildBuddy workflows from being run.
                    </div>
                    {deleteError && <div className="error">{deleteError.message}</div>}
                  </DialogBody>
                  <DialogFooter>
                    <DialogFooterButtons>
                      {this.state.isDeleting && <Spinner />}
                      <Button className="destructive" onClick={this.onClickUnlink.bind(this)} disabled={isDeleting}>
                        Unlink
                      </Button>
                    </DialogFooterButtons>
                  </DialogFooter>
                </Dialog>
              </Modal>
            </>
          )}
        </div>
      </div>
    );
  }
}

type WorkflowItemProps = {
  workflow: Workflow;
  onClickUnlinkItem: (workflow: Workflow) => void;
};

type WorkflowItemState = {
  isMenuOpen?: boolean;
  copiedToClipboard?: boolean;
};

class WorkflowItem extends React.Component<WorkflowItemProps, WorkflowItemState> {
  state: WorkflowItemState = {};

  private onClickMenuButton() {
    this.setState({ isMenuOpen: !this.state.isMenuOpen });
  }

  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private onClickCopyWebhookUrl() {
    copyToClipboard(this.props.workflow.webhookUrl);
    this.setState({ copiedToClipboard: true });
    setTimeout(() => {
      this.setState({ copiedToClipboard: false });
    }, 1000);
  }

  private onClickUnlinkMenuItem() {
    this.setState({ isMenuOpen: false });
    this.props.onClickUnlinkItem(this.props.workflow);
  }

  private onClickRepoUrl(e: React.MouseEvent) {
    e.preventDefault();
    const path = (e.target as HTMLAnchorElement).getAttribute("href");
    router.navigateTo(path);
  }

  render() {
    const { repoUrl } = this.props.workflow;
    const { isMenuOpen, copiedToClipboard } = this.state;

    const url = new URL(repoUrl);
    url.protocol = "https:";

    return (
      <div className="workflow-item container">
        <div className="workflow-item-column">
          <div className="workflow-item-row">
            <GitMerge />
            <a
              href={router.getWorkflowHistoryUrl(repoUrl)}
              onClick={this.onClickRepoUrl.bind(this)}
              className="repo-url">
              {url.host}
              {url.pathname}
            </a>
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
            <Popup isOpen={isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)}>
              <Menu className="workflow-dropdown-menu">
                <MenuItem
                  onClick={this.onClickCopyWebhookUrl.bind(this)}
                  className={copiedToClipboard ? "copied-to-clipboard" : ""}>
                  {copiedToClipboard ? <>Copied!</> : <>Copy webhook URL</>}
                </MenuItem>
                <MenuItem onClick={this.onClickUnlinkMenuItem.bind(this)}>Unlink repository</MenuItem>
              </Menu>
            </Popup>
          </div>
        </div>
      </div>
    );
  }
}
