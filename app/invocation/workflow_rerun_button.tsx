import React from "react";
import { workflow } from "../../proto/workflow_ts_proto";
import Button, { OutlinedButton } from "../components/button/button";
import { OutlinedButtonGroup } from "../components/button/button_group";
import Modal from "../components/modal/modal";
import Dialog, {
  DialogHeader,
  DialogTitle,
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
} from "../components/dialog/dialog";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup, { PopupContainer } from "../components/popup/popup";
import errorService from "../errors/error_service";
import router from "../router/router";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import InvocationModel from "./invocation_model";
import Spinner from "../components/spinner/spinner";
import { ChevronDown, RefreshCw } from "lucide-react";
import Long from "long";

export interface WorkflowRerunButtonProps {
  model: InvocationModel;
}

type State = {
  isMenuOpen: boolean;
  isDialogOpen: boolean;
  isLoading: boolean;
};

export default class WorkflowRerunButton extends React.Component<WorkflowRerunButtonProps, State> {
  state: State = {
    isMenuOpen: false,
    isDialogOpen: false,
    isLoading: false,
  };

  private inFlightRpc?: CancelablePromise;

  private onOpenMenu() {
    this.setState({ isMenuOpen: true });
  }
  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private onOpenDialog() {
    this.setState({ isMenuOpen: false, isDialogOpen: true });
  }
  private onCloseDialog() {
    this.setState({ isDialogOpen: false });
  }

  private onClickRerun(clean: boolean) {
    // Buttons isn't clickable in this case; just making strict TS happy.
    if (!this.props.model.workflowConfigured) {
      return;
    }

    this.inFlightRpc?.cancel();

    this.setState({ isMenuOpen: false, isDialogOpen: false, isLoading: true });

    const configuredEvent = this.props.model.workflowConfigured;

    // TODO(Maggie): Deprecate ExecuteWorkflowResponse.InvocationID field
    this.inFlightRpc = rpcService.service
      .executeWorkflow(
        new workflow.ExecuteWorkflowRequest({
          workflowId: configuredEvent.workflowId,
          actionNames: [configuredEvent.actionName],
          pushedRepoUrl: configuredEvent.pushedRepoUrl,
          pushedBranch: configuredEvent.pushedBranch,
          commitSha: configuredEvent.commitSha,
          targetRepoUrl: configuredEvent.targetRepoUrl,
          targetBranch: configuredEvent.targetBranch,
          clean,
          visibility: this.props.model.buildMetadataMap.get("VISIBILITY") || "",
          pullRequestNumber: Long.fromString(this.props.model.buildMetadataMap.get("PULL_REQUEST_NUMBER") || "0"),
        })
      )
      .then((response) => {
        let invocationId = "";
        let errorMsg = `Failed to execute action ${configuredEvent.actionName}.`;

        response.actionStatuses.forEach(function (actionStatus, _) {
          if (actionStatus.actionName == configuredEvent.actionName) {
            if ((actionStatus.status?.code || 0) !== 0 /*OK*/) {
              errorMsg = actionStatus.status?.message || errorMsg;
            } else {
              invocationId = actionStatus.invocationId;
            }
            return;
          }
        });

        if (invocationId !== "") {
          router.navigateTo(`/invocation/${invocationId}`);
        } else {
          errorService.handleError(errorMsg);
        }
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isLoading: false }));
  }

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  render() {
    const isEnabled = this.props.model.workflowConfigured && !this.state.isLoading;

    return (
      <>
        <PopupContainer>
          <OutlinedButtonGroup>
            <OutlinedButton
              disabled={!isEnabled}
              className="workflow-rerun-button"
              onClick={this.onClickRerun.bind(this, /*clean=*/ false)}>
              {this.state.isLoading ? <Spinner /> : <RefreshCw />}
              <span>Re-run</span>
            </OutlinedButton>
            <OutlinedButton disabled={!isEnabled} className="icon-button" onClick={this.onOpenMenu.bind(this)}>
              <ChevronDown />
            </OutlinedButton>
          </OutlinedButtonGroup>
          <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="right">
            <Menu>
              <MenuItem onClick={this.onOpenDialog.bind(this)}>Re-run from clean workspace</MenuItem>
            </Menu>
          </Popup>
        </PopupContainer>
        <Modal isOpen={this.state.isDialogOpen} onRequestClose={this.onCloseDialog.bind(this)}>
          <Dialog>
            <DialogHeader>
              <DialogTitle>Confirm clean re-run</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>
                This will create a new runner for this workflow, re-clone the Git repo, and start from a new, empty
                Bazel cache.
              </p>
              <p>
                In some cases, this can recover workflows that are in a broken state, but may temporarily slow down all
                executions of this workflow, so it is intended to be used sparingly.
              </p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <OutlinedButton onClick={this.onCloseDialog.bind(this)}>Cancel</OutlinedButton>
                <Button onClick={this.onClickRerun.bind(this, /*clean=*/ true)}>OK</Button>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
