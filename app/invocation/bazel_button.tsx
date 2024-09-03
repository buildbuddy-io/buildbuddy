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
import { runner } from "../../proto/runner_ts_proto";
import { git } from "../../proto/git_ts_proto";

export interface BazelButtonProps {
  model: InvocationModel;
  user?: User;
}

type State = {
  isMenuOpen: boolean;
  isDialogOpen: boolean;
  isLoading: boolean;
};

export default class BazelButton extends React.Component<BazelButtonProps, State> {
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

  getRepoState() {
    let state = new git.RepoState();
    state.commitSha = this.props.model.getCommit();
    state.branch = this.props.model.getBranchName();
    // should we do some cool patch stuff?
    //
    // for (let path of this.state.changes.keys()) {
    //   state.patch.push(
    //     textEncoder.encode(
    //       diff.createTwoFilesPatch(
    //         `a/${path}`,
    //         `b/${path}`,
    //         this.state.originalFileContents.get(path) || "",
    //         this.state.changes.get(path) || ""
    //       )
    //     )
    //   );
    // }
    return state;
  }

  private async onClickRerun(command: string, compare = "", additionalFlags = "") {
    let request = new runner.RunRequest();
    request.gitRepo = new git.GitRepo();
    request.gitRepo.repoUrl = this.props.model.getRepo();
    request.bazelCommand = command.replace(/^(bazel )/, "");
    if (additionalFlags) {
      request.bazelCommand += " " + additionalFlags;
    }
    // request.
    // request.env
    request.repoState = this.getRepoState();
    request.async = true;
    request.runRemotely = true;
    request.execProperties = [
      // todo: if github isn't linked, suggest its linked
      new build.bazel.remote.execution.v2.Platform.Property({ name: "OSFamily", value: "Darwin" }),
      new build.bazel.remote.execution.v2.Platform.Property({ name: "Arch", value: "arm64" }),
      new build.bazel.remote.execution.v2.Platform.Property({ name: "workload-isolation-type", value: "" }),
      new build.bazel.remote.execution.v2.Platform.Property({ name: "container-image", value: "" }),
      // new build.bazel.remote.execution.v2.Platform.Property({ name: "include-secrets", value: "true" }),
    ];

    this.setState({ isLoading: true });
    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        if (compare) {
          window.open(`/compare/${compare}...${response.invocationId}`, "_blank");
          return;
        }
        window.open(`/invocation/${response.invocationId}?queued=true`, "_blank");
      })
      .catch((error) => {
        alert(error);
      })
      .finally(() => {
        this.setState({ isLoading: false });
      });
  }

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  render() {
    const isEnabled = !this.state.isLoading;

    let command = this.props.model.explicitCommandLine().replaceAll(/--[a-zA-Z_]+='\<REDACTED\>'/g, "");

    return (
      <>
        <PopupContainer>
          <OutlinedButtonGroup>
            <OutlinedButton
              disabled={!isEnabled}
              className="workflow-rerun-button"
              onClick={this.onClickRerun.bind(this, command, "", "")}>
              {this.state.isLoading ? <Spinner /> : <RefreshCw />}
              <span>Re-run</span>
            </OutlinedButton>
            <OutlinedButton disabled={!isEnabled} className="icon-button" onClick={this.onOpenMenu.bind(this)}>
              <ChevronDown />
            </OutlinedButton>
          </OutlinedButtonGroup>
          <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)} anchor="right">
            <Menu>
              <MenuItem onClick={this.onClickRerun.bind(this, command, this.props.model.getInvocationId(), "")}>
                Re-run and compare
              </MenuItem>
              <MenuItem
                onClick={this.onClickRerun.bind(
                  this,
                  command,
                  "",
                  "--remote_grpc_log=$BUILDBUDDY_ARTIFACTS_DIRECTORY/grpc.log"
                )}>
                Re-run with grpc log
              </MenuItem>
              <MenuItem
                onClick={this.onClickRerun.bind(
                  this,
                  command,
                  "",
                  "--experimental_execution_log_compact_file==$BUILDBUDDY_ARTIFACTS_DIRECTORY/exec.log"
                )}>
                Re-run with execution log
              </MenuItem>
              {/* <MenuItem onClick={this.onClickRerun.bind(this, command, "", prompt("additional bazel flags") || "")}>
                Re-run with additional flags
              </MenuItem> */}
              <MenuItem onClick={this.onClickRerun.bind(this, "mod graph --enable_bzlmod", "", "")}>
                View dependencies
              </MenuItem>
              <MenuItem
                onClick={this.onClickRerun.bind(
                  this,
                  `query deps(${this.props.model.getPattern()}) --output=graph --noimplicit_deps`,
                  "",
                  ""
                )}>
                View build graph
              </MenuItem>
              <MenuItem onClick={this.onClickRerun.bind(this, command, "", "--explain")}>Explain</MenuItem>
              <MenuItem onClick={this.onClickRerun.bind(this, command, "", "--verbose_failures")}>
                Re-run with verbose failures
              </MenuItem>
              <MenuItem
                onClick={this.onClickRerun.bind(
                  this,
                  command.replaceAll("test ", " coverage "),
                  "",
                  "--combined_report=lcov"
                )}>
                Re-run with coverage
              </MenuItem>
              <MenuItem onClick={this.onClickRerun.bind(this, command, "", "--noremote_accept_cached")}>
                Re-run uncached
              </MenuItem>
              <MenuItem onClick={this.onClickRerun.bind(this, command.replaceAll("build ", "print_action "), "", "")}>
                Print action
              </MenuItem>
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
                <Button onClick={this.onClickRerun.bind(this, /*clean=*/ "", "", "")}>OK</Button>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
