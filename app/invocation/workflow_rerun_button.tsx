import React from "react";
import { Subscription, from } from "rxjs";
import { workflow } from "../../proto/workflow_ts_proto";
import { OutlinedButton } from "../components/button/button";
import errorService from "../errors/error_service";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import InvocationModel from "./invocation_model";

export interface WorkflowRerunButtonProps {
  model: InvocationModel;
}

type State = {
  isLoading?: boolean;
};

export default class WorkflowRerunButton extends React.Component<WorkflowRerunButtonProps, State> {
  state: State = {};

  private subscription: Subscription;

  private onClick() {
    this.subscription?.unsubscribe();

    this.setState({ isLoading: true });
    this.subscription = from<Promise<workflow.ExecuteWorkflowResponse>>(
      rpcService.service.executeWorkflow(
        new workflow.ExecuteWorkflowRequest({
          workflowId: this.props.model.workflowConfigured.workflowId,
          actionName: this.props.model.workflowConfigured.actionName,
          // Workspace status event is controlled by the CI runner
          // (not the workspace status command), so commit and branch
          // should always be available.
          commitSha: this.props.model.getCommit(),
          branch: this.props.model.getBranch(),
          // mergeRef: this.props.model.workflowConfigured.mergeRef,
        })
      )
    ).subscribe(
      (response) => router.navigateTo(`/invocation/${response.invocationId}`),
      (e) => errorService.handleError(e),
      () => this.setState({ isLoading: false })
    );
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  render() {
    const isEnabled = this.props.model.workflowConfigured && !this.state.isLoading;

    return (
      <OutlinedButton disabled={!isEnabled} className="workflow-rerun-button" onClick={this.onClick.bind(this)}>
        {this.state.isLoading ? <div className="loading"></div> : <img alt="" src="/image/refresh-cw.svg" />}
        <span>Re-run</span>
      </OutlinedButton>
    );
  }
}
