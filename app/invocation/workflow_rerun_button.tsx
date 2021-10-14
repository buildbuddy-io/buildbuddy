import React from "react";
import { workflow } from "../../proto/workflow_ts_proto";
import { OutlinedButton } from "../components/button/button";
import { OutlinedButtonGroup } from "../components/button/button_group";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup, { PopupContainer } from "../components/popup/popup";
import errorService from "../errors/error_service";
import router from "../router/router";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import InvocationModel from "./invocation_model";

export interface WorkflowRerunButtonProps {
  model: InvocationModel;
}

type State = {
  isMenuOpen?: boolean;
  isLoading?: boolean;
};

export default class WorkflowRerunButton extends React.Component<WorkflowRerunButtonProps, State> {
  state: State = {};

  private inFlightRpc: CancelablePromise;

  private onOpenMenu() {
    this.setState({ isMenuOpen: true });
  }
  private onRequestClose() {
    this.setState({ isMenuOpen: false });
  }

  private onClickRerun(clean: boolean) {
    this.inFlightRpc?.cancel();

    this.setState({ isMenuOpen: false, isLoading: true });

    const configuredEvent = this.props.model.workflowConfigured;

    this.inFlightRpc = rpcService.service
      .executeWorkflow(
        new workflow.ExecuteWorkflowRequest({
          workflowId: configuredEvent.workflowId,
          actionName: configuredEvent.actionName,
          pushedRepoUrl: configuredEvent.pushedRepoUrl,
          pushedBranch: configuredEvent.pushedBranch,
          commitSha: configuredEvent.commitSha,
          targetRepoUrl: configuredEvent.targetRepoUrl,
          targetBranch: configuredEvent.targetBranch,
          clean,
        })
      )
      .then((response) => router.navigateTo(`/invocation/${response.invocationId}`))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isLoading: false }));
  }

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  render() {
    const isEnabled = this.props.model.workflowConfigured && !this.state.isLoading;

    return (
      <PopupContainer>
        <OutlinedButtonGroup>
          <OutlinedButton
            disabled={!isEnabled}
            className="workflow-rerun-button"
            onClick={this.onClickRerun.bind(this, /*clean=*/ false)}>
            {this.state.isLoading ? <div className="loading"></div> : <img alt="" src="/image/refresh-cw.svg" />}
            <span>Re-run</span>
          </OutlinedButton>
          <OutlinedButton disabled={!isEnabled} className="icon-button" onClick={this.onOpenMenu.bind(this)}>
            <img alt="" src="/image/chevron-down.svg" />
          </OutlinedButton>
        </OutlinedButtonGroup>
        <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onRequestClose.bind(this)} anchor="right">
          <Menu>
            <MenuItem onClick={this.onClickRerun.bind(this, /*clean=*/ true)}>Clean workspace and re-run</MenuItem>
          </Menu>
        </Popup>
      </PopupContainer>
    );
  }
}
