import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
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
import { User } from "../auth/user";
import { firecracker } from "../../proto/firecracker_ts_proto";

export interface WorkflowRerunButtonProps {
  model: InvocationModel;
  user?: User;
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

  private async onClickRerun(clean: boolean) {
    // Buttons isn't clickable in this case; just making strict TS happy.
    if (!this.props.model.workflowConfigured) {
      return;
    }

    this.inFlightRpc?.cancel();

    this.setState({ isMenuOpen: false, isDialogOpen: false, isLoading: true });

    const configuredEvent = this.props.model.workflowConfigured;

    if (clean) {
      try {
        await this.invalidateSnapshot();
      } catch (e) {
        errorService.handleError(`Failed to invalidate snapshot: ${e}`);
        this.setState({ isLoading: false });
        return;
      }
    }

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
          visibility: this.props.model.buildMetadataMap.get("VISIBILITY") || "",
          pullRequestNumber: Long.fromString(this.props.model.buildMetadataMap.get("PULL_REQUEST_NUMBER") || "0"),
          async: true,
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
          router.navigateTo(`/invocation/${invocationId}?queued=true`);
        } else {
          errorService.handleError(errorMsg);
        }
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isLoading: false }));
  }

  private async invalidateSnapshot() {
    // Get the execution for the workflow run (ci_runner).
    const executionRequest = new execution_stats.GetExecutionRequest();
    executionRequest.executionLookup = new execution_stats.ExecutionLookup();
    executionRequest.executionLookup.invocationId = this.props.model.getInvocationId();
    const executionResponse = await rpcService.service.getExecution(executionRequest);

    if (executionResponse.execution.length != 1) {
      throw new Error(`expected 1 workflow execution, got ${executionResponse.execution.length}`);
    }
    const workflowExecution = executionResponse!.execution[0];
    if (!workflowExecution.commandSnippet.startsWith("buildbuddy_ci_runner")) {
      throw new Error(`expected workflow execution, got ${workflowExecution.commandSnippet}`);
    }

    const executeResponseDigest = workflowExecution.executeResponseDigest;
    if (executeResponseDigest === null || executeResponseDigest === undefined) {
      throw new Error(`empty workflow execute response digest`);
    }

    // Get the execute response, which contains the snapshot key.
    const executeResponseUrl = this.props.model.getActionCacheURL(executeResponseDigest);
    const executeResponseBuffer = await rpcService
      .fetchBytestreamFile(executeResponseUrl, this.props.model.getInvocationId(), "arraybuffer")
      .catch((e) => {
        throw new Error(
          `workflow execute response does not exist in the cache. Try invalidating from a more recent workflow run`
        );
      });

    const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(executeResponseBuffer));
    // ExecuteResponse is encoded in ActionResult.stdout_raw field. See
    // proto field docs on `Execution.execute_response_digest`.
    const executeResponseBytes = actionResult.stdoutRaw;
    const executeResponse = build.bazel.remote.execution.v2.ExecuteResponse.decode(executeResponseBytes);

    // Vm metadata is stored in the auxiliary metadata field of the execution metadata.
    const auxiliaryMetadata = executeResponse.result?.executionMetadata?.auxiliaryMetadata;
    if (!auxiliaryMetadata || auxiliaryMetadata.length == 0) {
      throw new Error("empty snapshot key in execute response");
    }
    let snapshotKey: firecracker.SnapshotKey | null | undefined;
    for (const metadata of auxiliaryMetadata) {
      if (metadata.typeUrl === "type.googleapis.com/firecracker.VMMetadata") {
        const vmMetadata = firecracker.VMMetadata.decode(metadata.value);
        snapshotKey = vmMetadata.snapshotKey;
        break;
      }
    }
    if (snapshotKey === null || snapshotKey === undefined) {
      throw new Error("empty snapshot key in execute response");
    }

    rpcService.service.invalidateSnapshot(
      new workflow.InvalidateSnapshotRequest({
        snapshotKey: snapshotKey,
      })
    );
  }

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  render() {
    const isEnabled = this.props.model.workflowConfigured && !this.state.isLoading;
    const showCleanRerun = Boolean(
      this.props.user?.isGroupAdmin() || !this.props.user?.selectedGroup?.restrictCleanWorkflowRunsToAdmins
    );

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
            {showCleanRerun && (
              <OutlinedButton disabled={!isEnabled} className="icon-button" onClick={this.onOpenMenu.bind(this)}>
                <ChevronDown />
              </OutlinedButton>
            )}
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
