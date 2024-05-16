import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { ArrowRight, Download, FileSymlink, Info } from "lucide-react";
import { build } from "../../proto/remote_execution_ts_proto";
import { firecracker } from "../../proto/firecracker_ts_proto";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { google as google_grpc_code } from "../../proto/grpc_code_ts_proto";
import TreeNodeComponent, { TreeNode } from "./invocation_action_tree_node";
import rpcService, { Cancelable } from "../service/rpc_service";
import DigestComponent from "../components/digest/digest";
import { TextLink } from "../components/link/link";
import TerminalComponent from "../terminal/terminal";
import { parseActionDigest, digestToString } from "../util/cache";
import UserPreferences from "../preferences/preferences";
import alert_service from "../alert/alert_service";
import { workflow } from "../../proto/workflow_ts_proto";
import errorService from "../errors/error_service";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import Button, { OutlinedButton } from "../components/button/button";
import Modal from "../components/modal/modal";
import { ExecuteOperation, executionStatusLabel, waitExecution } from "./execution_status";
import capabilities from "../capabilities/capabilities";

type Timestamp = google_timestamp.protobuf.Timestamp;
type ITimestamp = google_timestamp.protobuf.ITimestamp;

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  preferences: UserPreferences;
}

interface State {
  action?: build.bazel.remote.execution.v2.Action;
  loadingAction: boolean;
  executeResponse?: build.bazel.remote.execution.v2.ExecuteResponse;
  actionResult?: build.bazel.remote.execution.v2.ActionResult;
  // The first entry in the tuple is the size, the second is the number of files.
  treeShaToTotalSizeMap: Map<string, [Number, Number]>;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  inputNodes: TreeNode[];
  isMenuOpen: boolean;
  showInvalidateSnapshotModal: boolean;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
  stderr?: string;
  stdout?: string;
  serverLogs?: ServerLog[];
  lastOperation?: ExecuteOperation;
}

interface ServerLog {
  name: string;
  text: string;
}

export default class InvocationActionCardComponent extends React.Component<Props, State> {
  state: State = {
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, TreeNode[]>(),
    treeShaToTotalSizeMap: new Map<string, [Number, Number]>(),
    serverLogs: [],
    inputNodes: [],
    loadingAction: true,
    isMenuOpen: false,
    showInvalidateSnapshotModal: false,
  };

  componentDidMount() {
    this.fetchAction();
    // Prefer executeResponseDigest since it is always specific to the current
    // invocation (later executions of the same action digest don't overwrite
    // it) and also has additional useful info such as gRPC status.
    if (this.props.search.has("executeResponseDigest")) {
      this.fetchExecuteResponse();
    } else {
      this.fetchActionResult();
    }
    if (this.props.search.has("executionId")) {
      this.streamExecution();
    }
  }

  componentDidUpdate(prevProps: Readonly<Props>): void {
    if (prevProps.search.get("actionDigest") != this.props.search.get("actionDigest")) {
      this.fetchAction();
      if (!this.props.search.has("executeResponseDigest")) {
        this.fetchActionResult();
      }
    }
    if (prevProps.search.get("executeResponseDigest") != this.props.search.get("executeResponseDigest")) {
      this.fetchExecuteResponse();
    }
    if (prevProps.search.get("executionId") !== this.props.search.get("executionId")) {
      this.streamExecution();
    }
  }

  fetchAction() {
    this.setState({ loadingAction: true });
    const digestParam = this.props.search.get("actionDigest");
    if (!digestParam) {
      alert_service.error("Missing action digest URL param");
      return;
    }
    const digest = parseActionDigest(digestParam);
    const actionUrl = this.props.model.getBytestreamURL(digest);
    rpcService
      .fetchBytestreamFile(actionUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        let action = build.bazel.remote.execution.v2.Action.decode(new Uint8Array(buffer));
        this.setState({
          action: action,
        });
        this.fetchCommand(action);
        this.fetchInputRoot(action.inputRootDigest ?? build.bazel.remote.execution.v2.Digest.create({}));
        this.fetchDirectorySizes(action.inputRootDigest ?? build.bazel.remote.execution.v2.Digest.create({}));
      })
      .catch((e) => console.error("Failed to fetch action:", e))
      .finally(() => this.setState({ loadingAction: false }));
  }

  private operationStream?: Cancelable;

  streamExecution() {
    if (!capabilities.config.streamingHttpEnabled) return;

    this.operationStream?.cancel();
    this.setState({ lastOperation: undefined });
    const executionId = this.props.search.get("executionId");
    if (!executionId) return;
    this.operationStream = waitExecution(executionId, {
      next: (operation) => {
        this.setState({ lastOperation: operation });
        if (operation.response) {
          this.setState({ executeResponse: operation.response });
          console.log(operation.response);
        }
        if (operation.response?.result) {
          this.setState({ actionResult: operation.response.result });
        }
      },
      error: (error) => {
        // TODO: better error handling
        console.log(error);
      },
      complete: () => {},
    });
  }

  fetchDirectorySizes(rootDigest: build.bazel.remote.execution.v2.Digest) {
    const remoteInstanceName = this.props.model.optionsMap.get("remote_instance_name") || undefined;
    rpcService.service
      .getTreeDirectorySizes({
        rootDigest: rootDigest,
        instanceName: remoteInstanceName,
        digestFunction: this.props.model.getDigestFunction(),
      })
      .then((r) => {
        const sizes = new Map<string, [Number, Number]>();
        r.sizes.forEach((v) => {
          sizes.set(v.digest, [+v.totalSize, +v.childCount]);
        });
        this.setState({ treeShaToTotalSizeMap: sizes });
      })
      .catch(() => {
        this.setState({ treeShaToTotalSizeMap: new Map<string, [Number, Number]>() });
      });
  }

  fetchInputRoot(rootDigest: build.bazel.remote.execution.v2.IDigest) {
    let inputRootURL = this.props.model.getBytestreamURL(rootDigest);
    rpcService
      .fetchBytestreamFile(inputRootURL, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        let inputRoot = build.bazel.remote.execution.v2.Directory.decode(new Uint8Array(buffer));
        let inputDirectories: TreeNode[] = inputRoot.directories.map((node) => ({
          obj: node,
          type: "dir",
        }));
        let inputSymlinks: TreeNode[] = inputRoot.symlinks.map((node) => ({
          obj: node,
          type: "symlink",
        }));
        const inputNodes = [...inputDirectories, ...inputSymlinks];
        this.setState({ inputRoot, inputNodes });
      })
      .catch((e) => console.error("Failed to fetch input root:", e));
  }

  fetchActionResult() {
    let digestParam = this.props.search.get("actionResultDigest") ?? this.props.search.get("actionDigest");
    if (!digestParam) {
      alert_service.error("Missing action digest in URL");
      return;
    }
    const digest = parseActionDigest(digestParam);
    const actionResultUrl = this.props.model.getActionCacheURL(digest);
    rpcService
      .fetchBytestreamFile(actionResultUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        this.setState({ actionResult });
        this.fetchStdout(actionResult);
        this.fetchStderr(actionResult);
      })
      .catch((e) => console.error("Failed to fetch action result:", e));
  }

  fetchExecuteResponse() {
    this.setState({ executeResponse: undefined });
    const digestParam = this.props.search.get("executeResponseDigest");
    if (!digestParam) {
      alert_service.error("Missing execute response digest in URL");
      return;
    }
    const digest = parseActionDigest(digestParam);
    rpcService
      .fetchBytestreamFile(
        this.props.model.getActionCacheURL(digest),
        this.props.model.getInvocationId(),
        "arraybuffer"
      )
      .then((buffer) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        // ExecuteResponse is encoded in ActionResult.stdout_raw field. See
        // proto field docs on `Execution.execute_response_digest`.
        const executeResponseBytes = actionResult.stdoutRaw;
        const executeResponse = build.bazel.remote.execution.v2.ExecuteResponse.decode(executeResponseBytes);
        this.setState({ executeResponse });
        if (executeResponse.result) {
          const actionResult = executeResponse.result;
          this.setState({ actionResult });
          this.fetchStdout(actionResult);
          this.fetchStderr(actionResult);
          this.fetchServerLogs(executeResponse);
        }
      })
      .catch((e) => console.error("Failed to fetch execute response:", e));
  }

  fetchStdout(actionResult: build.bazel.remote.execution.v2.ActionResult) {
    if (!actionResult.stdoutDigest) return;

    let stdoutUrl = this.props.model.getBytestreamURL(actionResult.stdoutDigest);
    rpcService
      .fetchBytestreamFile(stdoutUrl, this.props.model.getInvocationId())
      .then((stdout) => this.setState({ stdout }))
      .catch((e) => console.error("Failed to fetch stdout:", e));
  }

  fetchStderr(actionResult: build.bazel.remote.execution.v2.ActionResult) {
    if (!actionResult.stderrDigest) return;

    let stderrUrl = this.props.model.getBytestreamURL(actionResult.stderrDigest);
    rpcService
      .fetchBytestreamFile(stderrUrl, this.props.model.getInvocationId())
      .then((stderr) => this.setState({ stderr }))
      .catch((e) => console.error("Failed to fetch stderr:", e));
  }

  fetchServerLogs(executeResponse: build.bazel.remote.execution.v2.ExecuteResponse) {
    for (const [name, file] of Object.entries(executeResponse.serverLogs)) {
      if (!file.digest) continue;
      const logsURL = this.props.model.getBytestreamURL(file.digest);
      rpcService
        .fetchBytestreamFile(logsURL, this.props.model.getInvocationId())
        .then((text) => {
          const log: ServerLog = { name, text };
          const serverLogs = [...(this.state.serverLogs ?? []), log];
          serverLogs.sort((a, b) => a.name.localeCompare(b.name));
          this.setState({ serverLogs: serverLogs });
        })
        .catch((e) => console.error(`Failed to fetch server log ${name}: ${e}`));
    }
  }

  fetchCommand(action: build.bazel.remote.execution.v2.Action) {
    if (!action.commandDigest) return;

    let commandURL = this.props.model.getBytestreamURL(action.commandDigest);
    rpcService
      .fetchBytestreamFile(commandURL, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        this.setState({
          command: build.bazel.remote.execution.v2.Command.decode(new Uint8Array(buffer)),
        });
      })
      .catch((e) => console.error("Failed to fetch command:", e));
  }

  displayList(list: string[]) {
    if (list.length == 0) return <div>None found</div>;
    return (
      <div className="action-list">
        {list.map((argument) => (
          <div>{argument}</div>
        ))}
      </div>
    );
  }

  handleOutputFileClicked(file: build.bazel.remote.execution.v2.OutputFile) {
    if (!file.digest) return;

    rpcService.downloadBytestreamFile(
      file.path,
      this.props.model.getBytestreamURL(file.digest),
      this.props.model.getInvocationId()
    );
  }

  private renderTimelines() {
    const metadata = this.state.actionResult?.executionMetadata;

    type TimelineEvent = {
      name: string;
      color: string;
      timestamp?: Timestamp | null;
    };
    const events: TimelineEvent[] = [
      {
        name: "Queued",
        color: "#3F51B5",
        timestamp: metadata?.queuedTimestamp,
      },
      {
        name: "Initializing",
        color: "#673AB7",
        timestamp: metadata?.workerStartTimestamp,
      },
      {
        name: "Downloading inputs",
        color: "#FF6F00",
        timestamp: metadata?.inputFetchStartTimestamp,
      },
      {
        name: "Preparing runner",
        color: "#673AB7",
        timestamp: metadata?.inputFetchCompletedTimestamp,
      },
      {
        name: "Executing",
        color: "#1E88E5",
        timestamp: metadata?.executionStartTimestamp,
      },
      {
        name: "Preparing for upload",
        color: "#673AB7",
        timestamp: metadata?.executionCompletedTimestamp,
      },
      {
        name: "Uploading outputs",
        color: "#FF6F00",
        timestamp: metadata?.outputUploadStartTimestamp,
      },
      // End marker -- not actually rendered.
      {
        name: "Upload complete",
        color: "",
        timestamp: metadata?.outputUploadCompletedTimestamp,
      },
    ];

    const filteredEvents = events.filter((event) => event && event.timestamp);
    if (filteredEvents.length == 0) {
      return null;
    }
    const startTimestamp = filteredEvents[0].timestamp;

    const totalDuration = durationSeconds(
      filteredEvents[0].timestamp!,
      filteredEvents[filteredEvents.length - 1].timestamp!
    );

    let offset = 0;
    return (
      <div>
        <div className="metadata-detail">
          <span className="label">
            Total
            {startTimestamp && <> @ {format.formatTimestamp(startTimestamp)}</>}
          </span>
          <span className="bar-description">{format.durationSec(totalDuration)} (100%)</span>
        </div>
        <div className="action-timeline">
          <div
            className="timeline-event"
            title={`Total: (${format.durationSec(totalDuration)}, 100%)`}
            style={{ flex: `1 0 0`, backgroundColor: "green" }}></div>
        </div>
        {filteredEvents.map((event, i) => {
          // Don't render the end marker.
          if (i == filteredEvents.length - 1) return null;

          const next = filteredEvents[i + 1];
          const duration = durationSeconds(event.timestamp!, next.timestamp!);
          const weight = duration / totalDuration;
          offset += weight;
          return (
            <div>
              <div className="metadata-detail">
                <span className="label">
                  {event.name}
                  {event.timestamp && <> @ {format.formatTimestamp(event.timestamp)}</>}
                </span>
                <span className="bar-description">
                  {format.compactDurationSec(duration)} ({(weight * 100).toFixed(1)}%)
                </span>
              </div>
              <div className="action-timeline">
                <div
                  className="timeline-event-gray"
                  title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
                  style={{ flex: `${offset - weight} 0 0`, backgroundColor: `rgba(0, 0, 0, .1)` }}></div>
                <div
                  className="timeline-event"
                  title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
                  style={{ flex: `${weight} 0 0`, backgroundColor: event.color }}></div>
                <div
                  className="timeline-event-gray"
                  title={`${event.name} (${format.durationSec(duration)}, ${(weight * 100).toFixed(2)}%)`}
                  style={{ flex: `${1 - offset} 0 0`, backgroundColor: `rgba(0, 0, 0, .1)` }}></div>
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  handleFileClicked(node: TreeNode) {
    if (!("digest" in node.obj)) return;
    if (!node.obj?.digest) return;

    let dirUrl = this.props.model.getBytestreamURL(node.obj.digest);
    let digestString = node.obj.digest.hash ?? "";
    if (this.state.treeShaToExpanded.get(digestString)) {
      this.state.treeShaToExpanded.set(digestString, false);
      this.forceUpdate();
      return;
    }
    if (node.type == "file") {
      rpcService.downloadBytestreamFile(node.obj.name, dirUrl, this.props.model.getInvocationId());
      return;
    }

    rpcService
      .fetchBytestreamFile(dirUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer: ArrayBuffer) => new Uint8Array(buffer))
      .then((array: Uint8Array) =>
        node.type == "tree"
          ? build.bazel.remote.execution.v2.Tree.decode(array).root
          : build.bazel.remote.execution.v2.Directory.decode(array)
      )
      .then((dir: build.bazel.remote.execution.v2.Directory | null | undefined) => {
        if (!dir) {
          return;
        }

        this.state.treeShaToExpanded.set(digestString, true);
        const nodes = dir.directories
          .map<TreeNode>((node) => ({
            obj: node,
            type: "dir",
          }))
          .concat(
            dir.files.map((node) => ({
              obj: node,
              type: "file",
            }))
          )
          .concat(
            dir.symlinks.map((node) => ({
              obj: node,
              type: "symlink",
            }))
          );
        this.state.treeShaToChildrenMap.set(digestString, nodes);
        this.forceUpdate();
      })
      .catch((e) => console.error(e));
  }

  // For firecracker actions, VM metadata is stored in the auxiliary metadata field
  // of the execution metadata. Try to decode it into an object if it exists.
  private getFirecrackerVMMetadata(): firecracker.VMMetadata | null | undefined {
    const auxiliaryMetadata = this.state.actionResult?.executionMetadata?.auxiliaryMetadata;
    if (!auxiliaryMetadata || auxiliaryMetadata.length == 0) {
      return null;
    }
    for (const metadata of auxiliaryMetadata) {
      if (metadata.typeUrl === "type.googleapis.com/firecracker.VMMetadata") {
        return firecracker.VMMetadata.decode(metadata.value);
      }
    }
    return null;
  }

  private getVMPreviousTaskHref(): string {
    const vmMetadata = this.getFirecrackerVMMetadata();
    const task = vmMetadata?.lastExecutedTask;
    if (!task?.executeResponseDigest || !task?.invocationId || !task?.actionDigest) return "";
    return `/invocation/${task.invocationId}?actionDigest=${digestToString(
      task.actionDigest
    )}&executeResponseDigest=${digestToString(task.executeResponseDigest)}#action`;
  }

  private renderNotFoundDetails({ result = false }) {
    const hasRemoteUploadLocalResults = this.props.model.booleanCommandLineOption("remote_upload_local_results");
    const hasRemoteExecutor = Boolean(this.props.model.stringCommandLineOption("remote_executor"));
    const hasRemoteCache = Boolean(this.props.model.booleanCommandLineOption("remote_cache"));

    if (result && !hasRemoteCache && !hasRemoteExecutor) {
      return (
        <p>
          No result details found. Action results are available for builds with{" "}
          <TextLink href="/docs/setup">full cache or remote execution</TextLink> enabled.
        </p>
      );
    }
    if (result && !hasRemoteExecutor && !hasRemoteUploadLocalResults) {
      return (
        <p>
          No result details found. Action results for locally executed actions are available for builds with{" "}
          <TextLink href="/docs/setup">full cache</TextLink> enabled (
          <span className="inline-code">--remote_upload_local_results</span>).
        </p>
      );
    }
    return (
      <>
        {result && <p>Action result not found. The result may have expired from cache, or it may not be cacheable.</p>}
        {!result && <p>Action not found. The action may have expired from cache.</p>}
        {!hasRemoteExecutor &&
          !this.props.model.isAnonymousInvocation() &&
          !this.props.model.hasCacheWriteCapability() && (
            <p>This could also be because the invocation was authenticated with a read-only API key.</p>
          )}
      </>
    );
  }

  private onClickInvalidateSnapshot(snapshotKey: firecracker.SnapshotKey) {
    rpcService.service
      .invalidateSnapshot(
        new workflow.InvalidateSnapshotRequest({
          snapshotKey: snapshotKey,
        })
      )
      .then(() => {
        alert_service.success(`Successfully invalidated the VM snapshot.`);
      })
      .catch((e) => {
        errorService.handleError(e);
      })
      .finally(() => {
        this.setState({ showInvalidateSnapshotModal: false, isMenuOpen: false });
      });
  }

  private renderOutputDirectories(actionsResult: build.bazel.remote.execution.v2.ActionResult) {
    return (
      <div className="action-section">
        <div className="action-property-title">Output directories</div>
        {actionsResult.outputDirectories.length ? (
          <div className="action-list">
            {actionsResult.outputDirectories.map((dir) => (
              <TreeNodeComponent
                node={{
                  obj: new build.bazel.remote.execution.v2.DirectoryNode({ name: dir.path, digest: dir.treeDigest }),
                  type: "tree",
                }}
                treeShaToExpanded={this.state.treeShaToExpanded}
                treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                treeShaToTotalSizeMap={this.state.treeShaToTotalSizeMap}
                handleFileClicked={this.handleFileClicked.bind(this)}
              />
            ))}
          </div>
        ) : (
          <div>None</div>
        )}
      </div>
    );
  }

  private renderOutputSymlinks(actionsResult: build.bazel.remote.execution.v2.ActionResult) {
    const symlinks = actionsResult.outputSymlinks.length
      ? actionsResult.outputSymlinks
      : [...actionsResult.outputFileSymlinks, ...actionsResult.outputDirectorySymlinks];

    return (
      <div className="action-section">
        <div className="action-property-title">Output symlinks</div>
        {symlinks.length ? (
          <div className="action-list">
            {symlinks.map((symlink) => (
              <div className="tree-node-symlink">
                <span>
                  <FileSymlink className="icon symlink-icon" />
                </span>{" "}
                <span>{symlink.path}</span>{" "}
                <span>
                  <ArrowRight className="icon arrow-right-icon" />
                </span>{" "}
                <span>{symlink.target}</span>
              </div>
            ))}
          </div>
        ) : (
          <div>None</div>
        )}
      </div>
    );
  }

  render() {
    const digest = parseActionDigest(this.props.search.get("actionDigest") ?? "");
    const vmMetadata = this.getFirecrackerVMMetadata();
    const executionId = this.props.search.get("executionId");

    return (
      <div className="invocation-action-card">
        {this.state.loadingAction && (
          <div className="card">
            <div className="loading" />
          </div>
        )}
        {!this.state.loadingAction && (
          <div className="card">
            <Info className="icon purple" />
            <div className="content">
              {executionId && (
                <>
                  <div className="title">Execution details</div>
                  <div className="details">
                    <div className="action-section">
                      <div className="action-property-title">Execution ID</div>
                      <div>{executionId}</div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Stage</div>
                      <div>{this.state.lastOperation ? executionStatusLabel(this.state.lastOperation) : "Unknown"}</div>
                    </div>
                    {this.state.executeResponse && (
                      <>
                        <div className="action-section">
                          <div className="action-property-title">RPC status</div>
                          <div
                            className={(this.state.executeResponse.status?.code ?? 0) !== 0 ? "grpc-status-error" : ""}>
                            {this.state.executeResponse
                              ? grpcStatusCodeToString(this.state.executeResponse.status?.code ?? 0)
                              : "Unknown"}
                            {this.state.executeResponse?.status?.message && (
                              <>: {this.state.executeResponse?.status.message}</>
                            )}
                          </div>
                        </div>
                        <div className="action-section">
                          <div className="action-property-title">Served from cache</div>
                          <div>{this.state.executeResponse.cachedResult ? "Yes" : "No"}</div>
                        </div>
                      </>
                    )}
                  </div>
                </>
              )}
              <div className="title">Action details</div>
              {this.state.action ? (
                <div className="details">
                  <div>
                    <div className="action-section">
                      <div className="action-property-title">Digest</div>
                      <DigestComponent digest={digest} expanded={true} />
                    </div>
                    {this.state.action.inputRootDigest && (
                      <div className="action-section">
                        <div className="action-property-title">Input root digest</div>
                        <div>
                          <DigestComponent digest={this.state.action.inputRootDigest} expanded={true} />
                        </div>
                      </div>
                    )}
                    <div className="action-section">
                      <div className="action-property-title">Input files</div>
                      {this.state.inputNodes.length ? (
                        <div className="input-tree">
                          {this.state.inputNodes.map((node) => (
                            <TreeNodeComponent
                              node={node}
                              treeShaToExpanded={this.state.treeShaToExpanded}
                              treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                              treeShaToTotalSizeMap={this.state.treeShaToTotalSizeMap}
                              handleFileClicked={this.handleFileClicked.bind(this)}
                            />
                          ))}
                        </div>
                      ) : (
                        <div>None found</div>
                      )}
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Cacheable</div>
                      <div>{!this.state.action.doNotCache ? "Yes" : "No"}</div>
                    </div>
                    <div className="action-section">
                      <div className="action-property-title">Timeout</div>
                      <div>{this.state.action.timeout ? format.durationProto(this.state.action.timeout) : "None"}</div>
                    </div>
                  </div>
                  <div className="action-line">
                    <div className="action-title">Command details</div>
                    {this.state.command ? (
                      <div>
                        <div className="action-section">
                          <div className="action-property-title">Arguments</div>
                          {this.displayList(this.state.command.arguments)}
                        </div>
                        <div className="action-section">
                          <div className="action-property-title">Environment variables</div>
                          {this.state.command.environmentVariables.length ? (
                            <div className="action-list">
                              {this.state.command.environmentVariables.map((variable) => (
                                <div>
                                  <span className="prop-name">{variable.name}</span>
                                  <span className="prop-value">={variable.value}</span>
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                        <div className="action-section">
                          <div className="action-property-title">Platform properties</div>
                          {this.state.command?.platform?.properties.length ? (
                            <div className="action-list">
                              {this.state.command?.platform?.properties.map((property) => (
                                <div>
                                  <span className="prop-name">{property.name}</span>
                                  <span className="prop-value">={property.value}</span>
                                </div>
                              ))}
                              {!this.state.command?.platform?.properties.length && <div>(Default)</div>}
                            </div>
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                      </div>
                    ) : (
                      <div>No command details were found.</div>
                    )}
                  </div>
                </div>
              ) : (
                <div className="details">{this.renderNotFoundDetails({ result: false })}</div>
              )}
              <div>
                <div className="action-line">
                  <div className="action-title">Result details</div>
                  {this.state.actionResult ? (
                    <div>
                      <div className="action-section">
                        <div className="action-property-title">Exit code</div>
                        <div>{this.state.actionResult.exitCode}</div>
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Execution metadata</div>
                        {this.state.actionResult.executionMetadata ? (
                          <div className="action-list">
                            <div className="metadata-title">Executor Host ID</div>
                            <div className="metadata-detail">{this.state.actionResult.executionMetadata.worker} </div>
                            <div className="metadata-title">Executor ID</div>
                            <div className="metadata-detail">
                              {this.state.actionResult.executionMetadata.executorId}
                            </div>
                            {vmMetadata && (
                              <>
                                <div className="metadata-title">VM ID</div>
                                <div className="metadata-detail">{vmMetadata.vmId}</div>
                                {vmMetadata.lastExecutedTask && (
                                  <>
                                    <div className="metadata-title">VM resumed from invocation</div>
                                    <div className="metadata-detail">
                                      <TextLink
                                        href={this.getVMPreviousTaskHref()}
                                        title={vmMetadata.lastExecutedTask.executionId}>
                                        {vmMetadata.lastExecutedTask.invocationId}
                                      </TextLink>
                                    </div>
                                    <div className="metadata-title">VM resumed from snapshot ID</div>
                                    <div className="metadata-detail">{vmMetadata.lastExecutedTask.snapshotId}</div>
                                  </>
                                )}
                                {vmMetadata.snapshotId && (
                                  <>
                                    <div className="metadata-title">Saved to snapshot ID</div>
                                    <div className="snapshot-container">
                                      <div className="metadata-detail">{vmMetadata.snapshotId}</div>
                                      {vmMetadata.snapshotKey && (
                                        <div className="invocation-menu-container">
                                          <a
                                            className="invalidate-button"
                                            onClick={() => this.setState({ showInvalidateSnapshotModal: true })}>
                                            Invalidate VM snapshot
                                          </a>
                                          <Modal
                                            isOpen={this.state.showInvalidateSnapshotModal}
                                            onRequestClose={() =>
                                              this.setState({ showInvalidateSnapshotModal: false, isMenuOpen: false })
                                            }>
                                            <Dialog>
                                              <DialogHeader>
                                                <DialogTitle>Confirm invalidate VM snapshot</DialogTitle>
                                              </DialogHeader>
                                              <DialogBody>
                                                <p>
                                                  Are you sure you want to invalidate the VM snapshot used for this
                                                  action?
                                                </p>
                                                <p>
                                                  A new VM, instead of a recycled VM, will be used for the next run of
                                                  this action, which may result in longer execution time.
                                                </p>
                                              </DialogBody>
                                              <DialogFooter>
                                                <DialogFooterButtons>
                                                  <OutlinedButton
                                                    onClick={() =>
                                                      this.setState({
                                                        showInvalidateSnapshotModal: false,
                                                        isMenuOpen: false,
                                                      })
                                                    }>
                                                    Cancel
                                                  </OutlinedButton>
                                                  <Button
                                                    onClick={this.onClickInvalidateSnapshot.bind(
                                                      this,
                                                      vmMetadata.snapshotKey
                                                    )}>
                                                    Invalidate
                                                  </Button>
                                                </DialogFooterButtons>
                                              </DialogFooter>
                                            </Dialog>
                                          </Modal>
                                        </div>
                                      )}
                                    </div>
                                  </>
                                )}
                              </>
                            )}
                            {this.state.actionResult.executionMetadata.usageStats && (
                              <>
                                <div className="metadata-title">Resource usage</div>
                                <div>
                                  <div>
                                    Peak memory:{" "}
                                    {format.bytes(this.state.actionResult.executionMetadata.usageStats.peakMemoryBytes)}
                                  </div>
                                  <div>MilliCPU: {computeMilliCpu(this.state.actionResult)}</div>
                                  {this.state.actionResult.executionMetadata.usageStats.peakFileSystemUsage?.map(
                                    (fs) => (
                                      <div>
                                        Peak disk usage: {fs.target} ({fs.fstype}): {format.bytes(fs.usedBytes)} of{" "}
                                        {format.bytes(fs.totalBytes)}
                                      </div>
                                    )
                                  )}
                                </div>
                              </>
                            )}
                            {this.state.actionResult.executionMetadata.estimatedTaskSize && (
                              <>
                                <div className="metadata-title">Estimated resource usage</div>
                                <div>
                                  <div>
                                    Peak memory:{" "}
                                    {format.bytes(
                                      this.state.actionResult.executionMetadata.estimatedTaskSize.estimatedMemoryBytes
                                    )}
                                  </div>
                                  <div>
                                    MilliCPU:{" "}
                                    {this.state.actionResult.executionMetadata.estimatedTaskSize.estimatedMilliCpu}
                                  </div>
                                </div>
                              </>
                            )}
                            <div className="metadata-title">Timeline</div>
                            {this.renderTimelines()}
                          </div>
                        ) : (
                          <div>None found</div>
                        )}
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Output files</div>
                        {this.state.actionResult.outputFiles ? (
                          <div className="action-list">
                            {this.state.actionResult.outputFiles.map((file) => (
                              <div
                                className="file-name clickable"
                                onClick={this.handleOutputFileClicked.bind(this, file)}>
                                <span>
                                  <Download className="icon file-icon" />
                                </span>
                                <span className="prop-link">{file.path}</span>
                                {file.isExecutable && <span className="detail"> (executable)</span>}
                                {file.digest && <DigestComponent digest={file.digest} />}
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div>None found</div>
                        )}
                      </div>
                      {this.renderOutputDirectories(this.state.actionResult)}
                      {this.renderOutputSymlinks(this.state.actionResult)}
                      <div className="action-section">
                        <div className="action-property-title">Stderr</div>
                        <div>
                          {this.state.stderr ? (
                            <TerminalComponent
                              value={this.state.stderr}
                              lightTheme={this.props.preferences.lightTerminalEnabled}
                            />
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Stdout</div>
                        <div>
                          {this.state.stdout ? (
                            <TerminalComponent
                              value={this.state.stdout}
                              lightTheme={this.props.preferences.lightTerminalEnabled}
                            />
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Server logs</div>
                        {this.state.serverLogs ? (
                          this.state.serverLogs.map((log) => (
                            <div key={log.name}>
                              <TerminalComponent
                                title={<b className="server-log-title">{log.name}</b>}
                                value={log.text}
                                lightTheme={this.props.preferences.lightTerminalEnabled}
                              />
                            </div>
                          ))
                        ) : (
                          <div>None</div>
                        )}
                      </div>
                    </div>
                  ) : (
                    !this.state.executeResponse && <div>{this.renderNotFoundDetails({ result: true })}</div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }
}

function grpcStatusCodeToString(code: number): string {
  return google_grpc_code.rpc.Code[code] ?? "";
}

function computeMilliCpu(result: build.bazel.remote.execution.v2.ActionResult): number {
  const metadata = result?.executionMetadata;
  if (!metadata) return 0;
  const usage = metadata.usageStats;
  if (!usage) return 0;

  const execDurationSeconds = durationSeconds(metadata.executionStartTimestamp!, metadata.executionCompletedTimestamp!);
  const cpuMillis = Number(usage.cpuNanos) / 1e6;
  return Math.floor(cpuMillis / execDurationSeconds);
}

function durationSeconds(t1: ITimestamp, t2: ITimestamp): number {
  return Math.max(0, timestampToUnixSeconds(t2) - timestampToUnixSeconds(t1));
}

function timestampToUnixSeconds(timestamp: ITimestamp): number {
  return Number(timestamp.seconds) + Number(timestamp.nanos) / 1e9;
}
