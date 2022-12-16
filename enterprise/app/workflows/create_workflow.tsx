import React from "react";
import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/auth_service";
import Button from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Input from "../../../app/components/input/input";
import Modal from "../../../app/components/modal/modal";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { workflow } from "../../../proto/workflow_ts_proto";

export type CreateWorkflowComponentProps = {
  user: User;
};

type State = {
  request: workflow.CreateWorkflowRequest;
  submitting: boolean;
  submitted: boolean;
  response?: workflow.CreateWorkflowResponse;
};

export default class CreateWorkflowComponent extends React.Component<CreateWorkflowComponentProps, State> {
  state: State = {
    submitting: false,
    submitted: false,
    request: new workflow.CreateWorkflowRequest({ gitRepo: new workflow.CreateWorkflowRequest.GitRepo() }),
  };

  private onChange(object: Record<string, any>, key: string, e: React.ChangeEvent<HTMLInputElement>) {
    object[key] = e.target.value;
    this.setState({ request: new workflow.CreateWorkflowRequest({ ...this.state.request }) });
  }

  private onSubmit(e: React.FormEvent) {
    e.preventDefault();
    this.setState({ submitting: true });
    rpcService.service
      .createWorkflow(this.state.request)
      .then((response) => {
        if (!response.webhookRegistered) {
          // Show the "copy webhook URL" modal if we didn't auto-register it.
          this.setState({ submitted: true, submitting: false, response });
          return;
        }
        alertService.success("Repo linked successfully");
        router.navigateToWorkflows();
      })
      .catch((error) => {
        errorService.handleError(error);
        this.setState({ submitted: false, submitting: false });
      });
  }

  private onDialogClosed() {
    router.navigateToWorkflows();
  }

  componentDidMount() {
    document.title = "Link repository | BuildBuddy";
  }

  private onClickWorkflowBreadcrumb(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateToWorkflows();
  }

  render() {
    const { response, submitted, submitting } = this.state;

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
              <span>Link repository</span>
            </div>
            <div className="title">Link repository</div>
          </div>
        </div>
        <div className="content">
          <div className="container">
            <form autoComplete="off" className="workflow-form" onSubmit={this.onSubmit.bind(this)}>
              <div className="form-row">
                <label htmlFor="gitRepo.repoUrl">GitHub repository URL</label>
                <Input
                  name="gitRepo.repoUrl"
                  value={this.state.request.gitRepo.repoUrl}
                  onChange={this.onChange.bind(this, this.state.request.gitRepo, "repoUrl")}
                  placeholder="https://github.com/acme-inc/app"
                />
                <div className="explanation">Currently only GitHub is supported.</div>
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
                  The access token must have <b>repo</b> and <b>admin:repo_hook</b> permissions. For improved security,
                  generate a new token just for this workflow, and don't use it anywhere else.
                </div>
              </div>
              <Button type="submit" disabled={submitting || submitted}>
                Create
              </Button>
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
