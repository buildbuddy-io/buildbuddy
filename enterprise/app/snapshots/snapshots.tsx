import React from "react";
import rpcService from "../../../app/service/rpc_service";
import error_service from "../../../app/errors/error_service";
import { firecracker } from "../../../proto/firecracker_ts_proto";
import { User } from "../../../app/auth/user";
import { formatDate } from "../../../app/format/format";
import * as proto from "../../../app/util/proto";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import { copyToClipboard } from "../../../app/util/clipboard";
import alert_service from "../../../app/alert/alert_service";
import Modal from "../../../app/components/modal/modal";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import { runner } from "../../../proto/runner_ts_proto";
import { git } from "../../../proto/git_ts_proto";
import { build } from "../../../proto/remote_execution_ts_proto";

export type SnapshotsProps = {
  user: User;
};

type State = {
  snapshots: firecracker.GetNamedSnapshotResponse[];
  selectedSnapshot: firecracker.GetNamedSnapshotResponse | null;
  showRunCommandModal: boolean;
  snapshotCmd: string;
};

export default class SnapshotsComponent extends React.Component<SnapshotsProps, State> {
  state: State = {
    snapshots: [],
    selectedSnapshot: null,
    showRunCommandModal: false,
    snapshotCmd: "",
  };

  componentDidMount() {
    document.title = "Snapshots | BuildBuddy";
    this.fetch();
  }

  fetch() {
    rpcService.service
      .getAllNamedSnapshots(new firecracker.GetAllNamedSnapshotsRequest())
      .then((response) => this.setState({ snapshots: response.snapshots }))
      .catch((e) => error_service.handleError(e));
  }

  onCopyRemoteBazelCommand(snapshotName: string) {
    var cmd = `local_bb remote --remote_runner="grpc://localhost:1985" --start_from=${snapshotName} --script=""`;
    copyToClipboard(cmd);
    alert_service.success("Copied command to clipboard");
  }

  onRunCommandInSnapshot(ss: firecracker.GetNamedSnapshotResponse | null) {
    if (!ss) {
      return;
    }
    let execProps: build.bazel.remote.execution.v2.Platform.Property[] = [];
    execProps.push(
      new build.bazel.remote.execution.v2.Platform.Property({
        name: "vm-config-override",
        value: JSON.stringify(ss.vmConfiguration),
      })
    );
    execProps.push(
      new build.bazel.remote.execution.v2.Platform.Property({
        name: "snapshot-name",
        value: ss.name,
      })
    );
    execProps.push(
      new build.bazel.remote.execution.v2.Platform.Property({
        name: "snapshot-key-override",
        value: JSON.stringify(ss.snapshotKey),
      })
    );
    const request = new runner.RunRequest({
      gitRepo: new git.GitRepo({
        // TODO: Save repo in NamedSnapshots table
        repoUrl: "https://github.com/buildbuddy-io/buildbuddy.git",
      }),
      repoState: new git.RepoState({
        branch: "master",
      }),
      steps: [
        new runner.Step({
          run: this.state.snapshotCmd,
        }),
      ],
      async: true,
      runRemotely: true,
      execProperties: execProps,
    });

    rpcService.service
      .run(request)
      .then((response: runner.RunResponse) => {
        let url = `/invocation/${response.invocationId}?queued=true&openChild=true`;
        window.open(url, "_blank");
      })
      .catch((error) => {
        error_service.handleError(error);
      });

    this.setState({ showRunCommandModal: false });
  }

  render() {
    return (
      <div className="snapshots-page">
        <div className="shelf">
          <div className="container">
            <div>
              <div className="breadcrumbs">
                {this.props.user && <span>{this.props.user?.selectedGroupName()}</span>}
                <span>Snapshots</span>
              </div>
              <div className="title">Snapshots</div>
            </div>
          </div>
        </div>
        <div className="content">
          {Boolean(this.state.snapshots.length) && (
            <div className="snapshot-table">
              <div className="snapshot-table-header">
                <div className="timestamp">Name</div>
                <div className="user">Cached</div>
                <div className="resource">Snapshot Key</div>
                <div className="buttons"></div>
              </div>
              {this.state.snapshots.map((snapshot) => (
                <div className={"snapshot-entry"}>
                  <div className="name">{snapshot.name}</div>
                  <div className="cached">{String(snapshot.isValid)}</div>
                  <div className="key">{JSON.stringify(snapshot.snapshotKey, null, 2)}</div>
                  <div className="buttons">
                    <div className={"button-row"}>
                      <OutlinedButton onClick={this.onCopyRemoteBazelCommand.bind(this, snapshot.name)}>
                        <span>Copy Remote Bazel command</span>
                      </OutlinedButton>
                    </div>
                    <div className={"button-row"}>
                      <OutlinedButton
                        onClick={() => this.setState({ showRunCommandModal: true, selectedSnapshot: snapshot })}>
                        <span>Run command in snapshot</span>
                      </OutlinedButton>
                    </div>
                    <div className={"button-row"}>
                      <OutlinedButton>
                        <span>Start remote terminal</span>
                      </OutlinedButton>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
          <Modal
            isOpen={this.state.showRunCommandModal}
            onRequestClose={() =>
              this.setState({
                showRunCommandModal: false,
                selectedSnapshot: null,
              })
            }>
            <Dialog classname={"show-run-command-modal"}>
              <DialogHeader>
                <DialogTitle>Run command in snapshot</DialogTitle>
              </DialogHeader>
              <DialogBody>
                <textarea
                  className={"run-command-input"}
                  rows={"15"}
                  onChange={(e) => this.setState({ snapshotCmd: e.target.value })}
                />
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  <Button onClick={this.onRunCommandInSnapshot.bind(this, this.state.selectedSnapshot)}>Run</Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>
        </div>
      </div>
    );
  }
}
