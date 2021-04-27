import React from "react";
import { from, Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import Button from "../../../app/components/button/button";
import Input from "../../../app/components/input/input";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import { workflow } from "../../../proto/workflow_ts_proto";

export type CreateWorkflowComponentProps = {
  user: User;
};

type State = {
  request: workflow.CreateWorkflowRequest;
  submitting: boolean;
  submitted: boolean;
  response?: workflow.CreateWorkflowResponse;
  error?: BuildBuddyError | null;
};

export default class CreateWorkflowComponent extends React.Component<CreateWorkflowComponentProps, State> {
  state: State = {
    submitting: false,
    submitted: false,
    request: new workflow.CreateWorkflowRequest({ gitRepo: new workflow.CreateWorkflowRequest.GitRepo() }),
  };

  private createWorkflowSubscription?: Subscription;

  private onChange(object: Record<string, any>, key: string, e: React.ChangeEvent<HTMLInputElement>) {
    object[key] = e.target.value;
    this.setState({ request: new workflow.CreateWorkflowRequest({ ...this.state.request }), error: null });
  }

  private onSubmit(e: React.FormEvent) {
    e.preventDefault();
    this.setState({ submitting: true });
    this.createWorkflowSubscription = from<Promise<workflow.CreateWorkflowResponse>>(
      rpcService.service.createWorkflow(this.state.request)
    ).subscribe(
      (response) => this.setState({ submitted: true, submitting: false, response }),
      (error) => this.setState({ submitted: false, submitting: false, error: BuildBuddyError.parse(error) })
    );
  }

  private onDialogClosed() {
    router.navigateToWorkflows();
  }

  componentDidMount() {
    document.title = "New workflow | BuildBuddy";
  }

  componentDidUpdate(prevProps: CreateWorkflowComponentProps) {
    if (this.props.user !== prevProps.user) {
      this.createWorkflowSubscription?.unsubscribe();
    }
  }

  componentWillUnmount() {
    this.createWorkflowSubscription?.unsubscribe();
  }

  private onClickWorkflowBreadcrumb(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateToWorkflows();
  }

  render() {
    const { response, error, submitted, submitting } = this.state;

    return (
      <div className="create-workflow-page">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              <span>
                <a href="/workflows/" onClick={this.onClickWorkflowBreadcrumb.bind(this)}>
                  Workflows
                </a>
              </span>
              <span>Add repository</span>
            </div>
            <div className="title">Add repository</div>
          </div>
        </div>
        <div className="content">
          <div className="container">
            <form autoComplete="off" className="workflow-form" onSubmit={this.onSubmit.bind(this)}>
              <div className="form-row">
                <label htmlFor="gitRepo.repoUrl">Git repository URL</label>
                <Input
                  name="gitRepo.repoUrl"
                  value={this.state.request.gitRepo.repoUrl}
                  onChange={this.onChange.bind(this, this.state.request.gitRepo, "repoUrl")}
                  placeholder="https://github.com/acme-inc/app"
                />
                <div className="explanation">GitHub, Bitbucket, and GitLab are supported.</div>
              </div>
              <div className="form-row">
                <label htmlFor="gitRepo.accessToken">Repository access token</label>
                <Input
                  name="gitRepo.accessToken"
                  value={this.state.request.gitRepo.accessToken}
                  onChange={this.onChange.bind(this, this.state.request.gitRepo, "accessToken")}
                  type="password"
                  {...{ autocomplete: "false" }} // Silence devtools warning.
                />
                <div className="explanation">
                  The access token must have repository permissions. For improved security, generate a new token just
                  for this workflow, and don't use it anywhere else.
                </div>
              </div>
              <Button type="submit" disabled={submitting || submitted}>
                Create
              </Button>
              {error && <div className="error">{error.message}</div>}
            </form>
          </div>
        </div>
        <Modal isOpen={Boolean(response)} onRequestClose={this.onDialogClosed.bind(this)}>
          <Dialog className="workflow-created-dialog">
            <DialogHeader>
              <DialogTitle>Just one more step!</DialogTitle>
            </DialogHeader>
            <DialogBody className="dialog-body">
              <div>
                Copy the webhook URL below and add it to your repo. If you don't want to do this now, you can always see
                this URL in the Workflows page.
              </div>
              <Input readOnly value={response?.webhookUrl} />
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <Button onClick={this.onDialogClosed.bind(this)}>OK</Button>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </div>
    );
  }
}
