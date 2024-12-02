import React, { ReactElement } from "react";
import format, { durationUsec } from "../format/format";
import InvocationModel from "./invocation_model";
import { ArrowRight, Download, File, FileQuestion, FileSymlink, Folder, Info } from "lucide-react";
import { build } from "../../proto/remote_execution_ts_proto";
import { firecracker } from "../../proto/firecracker_ts_proto";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { google as google_grpc_code } from "../../proto/grpc_code_ts_proto";
import TreeNodeComponent, { TreeNode } from "./invocation_action_tree_node";
import rpcService, { Cancelable, CancelablePromise } from "../service/rpc_service";
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
import { getErrorReason } from "../util/rpc";
import rpc_service from "../service/rpc_service";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { BuildBuddyError, HTTPStatusError } from "../util/errors";
import { Profile, readProfile } from "../trace/trace_events";
import TraceViewer from "../trace/trace_viewer";
import Spinner from "../components/spinner/spinner";
import { timestampToDate } from "../util/proto";

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
  executionId?: string;
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
  profileLoading: boolean;
  profile?: Profile;
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
    profileLoading: false,
  };

  componentDidMount() {
    this.fetchAction();
    this.fetchExecuteResponseOrActionResult();
    if (this.props.search.has("executionId")) {
      this.streamExecution();
    }
  }

  componentDidUpdate(prevProps: Readonly<Props>): void {
    if (prevProps.search.get("actionDigest") !== this.props.search.get("actionDigest")) {
      this.fetchAction();
      this.fetchExecuteResponseOrActionResult();
    } else if (prevProps.search.get("executeResponseDigest") !== this.props.search.get("executeResponseDigest")) {
      this.fetchExecuteResponseOrActionResult();
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
    if (!digest) {
      alert_service.error("Missing action digest URL param");
      return;
    }
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

    const service = rpcService.getRegionalServiceOrDefault(this.props.model.stringCommandLineOption("remote_executor"));

    this.operationStream = waitExecution(service, executionId, {
      next: (operation) => {
        // If the execution response is unavailable due to the pubsub channel
        // having gone away, then we can't rely on the execution stream for the
        // latest status, so just cancel the stream.
        if (operation.response?.status && getErrorReason(operation.response.status) == "PUBSUB_CHANNEL_ERROR") {
          this.operationStream?.cancel();
          return;
        }

        this.setState({ lastOperation: operation });
        if (operation.response && !this.state.executeResponse) {
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
    const service = rpcService.getRegionalServiceOrDefault(this.props.model.stringCommandLineOption("remote_cache"));

    service
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

  /**
   * Fetches the latest ActionResult from AC as a fallback in case we couldn't
   * locate the ExecuteResponse that was returned for this particular
   * invocation.
   */
  fetchActionResult() {
    let digestParam = this.props.search.get("actionDigest");
    const digest = parseActionDigest(digestParam ?? "");
    if (!digest) {
      alert_service.error("Missing action digest in URL");
      return;
    }
    const actionResultUrl = this.props.model.getActionCacheURL(digest);
    this.actionResultRPC = rpcService
      .fetchBytestreamFile(actionResultUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        this.setState({ actionResult });
        this.fetchStdout(actionResult);
        this.fetchStderr(actionResult);
      })
      .catch((e) => console.error("Failed to fetch action result:", e));
  }

  private executeResponseRPC?: CancelablePromise<build.bazel.remote.execution.v2.ExecuteResponse | null>;
  private actionResultRPC?: Cancelable;
  private stdoutRPC?: Cancelable;
  private stderrRPC?: Cancelable;
  private serverLogsRPCs?: Cancelable[];
  private profileRPC?: Cancelable;

  fetchExecuteResponseOrActionResult() {
    this.executeResponseRPC?.cancel();
    this.actionResultRPC?.cancel();
    this.stdoutRPC?.cancel();
    this.stderrRPC?.cancel();
    this.serverLogsRPCs?.forEach((rpc) => rpc.cancel());
    this.profileRPC?.cancel();

    this.setState({
      executeResponse: undefined,
      executionId: undefined,
      actionResult: undefined,
      stdout: undefined,
      stderr: undefined,
      serverLogs: undefined,
      profile: undefined,
    });

    const actionDigestParam = this.props.search.get("actionDigest");
    if (!actionDigestParam) {
      alert_service.error("Missing action digest in URL");
      return;
    }

    const executeResponseDigestParam = this.props.search.get("executeResponseDigest");
    if (executeResponseDigestParam) {
      // If we have the executeResponseDigest in the URL, we can skip the
      // execution table lookup.
      const executeResponseDigest = parseActionDigest(executeResponseDigestParam);
      if (!executeResponseDigest) {
        alert_service.error("Invalid execute response digest in URL");
        return;
      }
      this.executeResponseRPC = this.fetchExecuteResponseByDigest(executeResponseDigest);
    } else {
      const actionDigest = parseActionDigest(actionDigestParam);
      if (!actionDigest) {
        alert_service.error("Missing action digest in URL");
        return;
      }
      this.executeResponseRPC = this.fetchExecuteResponseByActionDigest(actionDigest);
    }
    // Whether to fall back to fetching the latest action result.
    let fallback = false;

    this.executeResponseRPC
      .then((executeResponse) => {
        if (!executeResponse) {
          fallback = true;
          return;
        }
        this.setState({ executeResponse });
        if (executeResponse.result) {
          const actionResult = executeResponse.result;
          this.setState({ actionResult });
          this.fetchStdout(actionResult);
          this.fetchStderr(actionResult);
          this.fetchServerLogs(executeResponse);
          const executionId = this.getExecutionId();
          if (executionId) {
            this.fetchProfile(executionId);
          }
        }
      })
      .catch((e) => {
        const error = BuildBuddyError.parse(e);
        if (error.code === "NotFound") {
          fallback = true;
          return;
        }
        errorService.handleError(e);
      })
      .finally(() => {
        if (fallback) {
          this.fetchActionResult();
        }
      });
  }

  fetchExecuteResponseByDigest(executeResponseDigest: build.bazel.remote.execution.v2.Digest) {
    return rpcService
      .fetchBytestreamFile(
        this.props.model.getActionCacheURL(executeResponseDigest),
        this.props.model.getInvocationId(),
        "arraybuffer"
      )
      .then((buffer) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        // ExecuteResponse is encoded in ActionResult.stdout_raw field. See
        // proto field docs on `Execution.execute_response_digest`.
        const executeResponseBytes = actionResult.stdoutRaw;
        const executeResponse = build.bazel.remote.execution.v2.ExecuteResponse.decode(executeResponseBytes);
        return executeResponse;
      });
  }

  fetchExecuteResponseByActionDigest(actionDigest: build.bazel.remote.execution.v2.Digest) {
    const service = rpc_service.getRegionalServiceOrDefault(
      this.props.model.stringCommandLineOption("remote_executor")
    );
    return service
      .getExecution({
        executionLookup: new execution_stats.ExecutionLookup({
          invocationId: this.props.model.getInvocationId(),
          actionDigestHash: actionDigest.hash,
        }),
        inlineExecuteResponse: true,
      })
      .then((response) => {
        const execution = response.execution?.[0];
        if (execution?.executionId) {
          this.setState({ executionId: execution.executionId });
        }
        return execution?.executeResponse ?? null;
      });
  }

  fetchStdout(actionResult: build.bazel.remote.execution.v2.ActionResult) {
    if (!actionResult.stdoutDigest) return;

    let stdoutUrl = this.props.model.getBytestreamURL(actionResult.stdoutDigest);
    this.stdoutRPC = rpcService
      .fetchBytestreamFile(stdoutUrl, this.props.model.getInvocationId())
      .then((stdout) => this.setState({ stdout }))
      .catch((e) => console.error("Failed to fetch stdout:", e));
  }

  fetchStderr(actionResult: build.bazel.remote.execution.v2.ActionResult) {
    if (!actionResult.stderrDigest) return;

    let stderrUrl = this.props.model.getBytestreamURL(actionResult.stderrDigest);
    this.stderrRPC = rpcService
      .fetchBytestreamFile(stderrUrl, this.props.model.getInvocationId())
      .then((stderr) => this.setState({ stderr }))
      .catch((e) => console.error("Failed to fetch stderr:", e));
  }

  fetchServerLogs(executeResponse: build.bazel.remote.execution.v2.ExecuteResponse) {
    this.serverLogsRPCs = [];
    for (const [name, file] of Object.entries(executeResponse.serverLogs)) {
      if (!file.digest) continue;
      const logsURL = this.props.model.getBytestreamURL(file.digest);
      const rpc = rpcService
        .fetchBytestreamFile(logsURL, this.props.model.getInvocationId())
        .then((text) => {
          const log: ServerLog = { name, text };
          const serverLogs = [...(this.state.serverLogs ?? []), log];
          serverLogs.sort((a, b) => a.name.localeCompare(b.name));
          this.setState({ serverLogs: serverLogs });
        })
        .catch((e) => console.error(`Failed to fetch server log ${name}: ${e}`));
      this.serverLogsRPCs.push(rpc);
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

  fetchProfile(executionId: string) {
    this.profileRPC = rpcService
      .fetchFile(
        rpcService.getDownloadUrl({
          invocation_id: this.props.model.getInvocationId(),
          execution_id: executionId,
          artifact: "execution_profile",
        }),
        "stream"
      )
      .then((response) => {
        if (response.body === null) {
          throw new Error("failed to read profile: response body is null");
        }
        return readProfile(response.body);
      })
      .then((profile) => this.setState({ profile }))
      .catch((e) => {
        // Ignore NotFound
        if (e instanceof HTTPStatusError && e.code === 404) {
          return;
        }
        errorService.handleError(e);
      })
      .finally(() => this.setState({ profileLoading: false }));
  }

  getExecutionId() {
    // If we got here from the executions page then we'll have the execution ID
    // in the URL; otherwise the execution ID gets fetched from the executions
    // linked to the invocation matching the actionDigest in the URL.
    return this.props.search.get("executionId") || this.state.executionId;
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

  private renderTiming(metadata: build.bazel.remote.execution.v2.ExecutedActionMetadata) {
    let timingDescription = null;
    if (metadata.queuedTimestamp && metadata.workerStartTimestamp && metadata.workerCompletedTimestamp) {
      const queuedDurationMillis = Math.max(
        0,
        timestampToDate(metadata.workerStartTimestamp).getTime() - timestampToDate(metadata.queuedTimestamp).getTime()
      );
      const workerDurationMillis =
        timestampToDate(metadata.workerCompletedTimestamp).getTime() -
        timestampToDate(metadata.workerStartTimestamp).getTime();
      timingDescription = (
        <div>
          <div>
            Queued for {format.durationMillis(queuedDurationMillis)} @{" "}
            {format.formatTimestamp(metadata.queuedTimestamp)}
          </div>
          <div>
            Executed in {format.durationMillis(workerDurationMillis)} @{" "}
            {format.formatTimestamp(metadata.workerStartTimestamp)}
          </div>
          <div>Completed @ {format.formatTimestamp(metadata.workerCompletedTimestamp)}</div>
        </div>
      );
    }

    return (
      <>
        <div className="metadata-title">Timing</div>
        {timingDescription}
        <div>
          {this.state.profileLoading ? (
            <Spinner />
          ) : this.state.profile ? (
            <TraceViewer profile={this.state.profile} fitToContent filterHidden />
          ) : null}
        </div>
      </>
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

  private renderExpectedOutputs(command: build.bazel.remote.execution.v2.Command) {
    const useOutputPaths =
      command.outputPaths.length && !(command.outputFiles.length + command.outputDirectories.length);

    const renderOutputPaths = () => (
      <>
        {command.outputPaths.map((expectedOutput) => (
          <div className="expected-output">
            <span>
              <FileQuestion className="icon file-question-icon" />
            </span>
            <span className="expected-output-label">{expectedOutput}</span>
          </div>
        ))}
      </>
    );

    const renderOutputFilesAndDirs = () => (
      <>
        {command.outputDirectories.map((expectedDir) => (
          <div className="expected-output">
            <span>
              <Folder className="icon folder-icon" />
            </span>
            <span className="expected-output-label">{expectedDir}</span>
          </div>
        ))}
        {command.outputFiles.map((expectedFile) => (
          <div className="expected-output">
            <span>
              <File className="icon file-icon" />
            </span>
            <span className="expected-output-label">{expectedFile}</span>
          </div>
        ))}
      </>
    );

    return (
      <div className="action-section">
        <div className="action-property-title">Expected Outputs</div>
        <div className="action-list">{useOutputPaths ? renderOutputPaths() : renderOutputFilesAndDirs()}</div>
      </div>
    );
  }

  private renderMissingOutputs(
    command: build.bazel.remote.execution.v2.Command,
    actionResult: build.bazel.remote.execution.v2.ActionResult
  ) {
    const useOutputPaths =
      command.outputPaths.length && !(command.outputFiles.length + command.outputDirectories.length);

    const renderMissingOutputPaths = (missingOutputs: Array<string>) => (
      <>
        {missingOutputs.map((missingOutput) => (
          <div className="missing-output">
            <span>
              <FileQuestion className="icon file-question-icon red" />
            </span>
            <span className="missing-output-label">{missingOutput}</span>
          </div>
        ))}
      </>
    );

    const renderMissingOutputFilesAndDirs = (missingFiles: Array<string>, missingDirs: Array<string>) => (
      <>
        {missingDirs.map((missingDir) => (
          <div className="missing-output">
            <span>
              <Folder className="icon file-question-icon red" />
            </span>
            <span className="missing-output-label">{missingDir}</span>
          </div>
        ))}
        {missingFiles.map((missingFile) => (
          <div className="missing-output">
            <span>
              <FileQuestion className="icon file-question-icon red" />
            </span>
            <span className="missing-output-label">{missingFile}</span>
          </div>
        ))}
      </>
    );

    const renderOutline = (content: ReactElement) => (
      <div className="action-section">
        <div className="action-property-title">Missing Outputs</div>
        <div className="action-list">{content}</div>
      </div>
    );

    if (useOutputPaths) {
      const actualOutputs = [
        ...actionResult.outputFiles,
        ...actionResult.outputDirectories,
        ...actionResult.outputSymlinks,
      ].map((output) => output.path);
      const missingOutputs = command.outputPaths.filter((expected) => !actualOutputs.includes(expected));
      return missingOutputs.length && renderOutline(renderMissingOutputPaths(missingOutputs));
    }

    const actualFiles = [
      ...actionResult.outputFiles,
      ...actionResult.outputFileSymlinks,
      ...actionResult.outputDirectorySymlinks,
    ].map((file) => file.path);
    const missingFiles = command.outputFiles.filter((expected) => !actualFiles.includes(expected));
    // In practice, Bazel creates the expected directories inside the input tree.
    // So it's unlikely that any of the expected dirs is missing.
    const actualDirs = actionResult.outputDirectories.map((dir) => dir.path);
    const missingDirs = command.outputDirectories.filter((expected) => !actualDirs.includes(expected));
    return (
      missingFiles.length + missingDirs.length > 0 &&
      renderOutline(renderMissingOutputFilesAndDirs(missingFiles, missingDirs))
    );
  }

  private renderUsageStats(usageStats: build.bazel.remote.execution.v2.UsageStats) {
    return (
      <>
        <div className="metadata-title">Resource usage</div>
        <div>
          <div>Peak memory: {format.bytes(usageStats.peakMemoryBytes)}</div>
          <div>MilliCPU: {computeMilliCpu(this.state.actionResult!)}</div>
          {usageStats.peakFileSystemUsage?.map((fs) => (
            <div>
              Peak disk usage: {fs.target} ({fs.fstype}): {format.bytes(fs.usedBytes)} of {format.bytes(fs.totalBytes)}
            </div>
          ))}
          {usageStats.cgroupIoStats && (
            <>
              <div>Disk bytes read: {format.bytes(usageStats.cgroupIoStats.rbytes)}</div>
              <div>Disk read operations: {format.count(usageStats.cgroupIoStats.rios)}</div>
              <div>Disk bytes written: {format.bytes(usageStats.cgroupIoStats.wbytes)}</div>
              <div>Disk write operations: {format.count(usageStats.cgroupIoStats.wios)}</div>
            </>
          )}
        </div>
        {usageStats.cpuPressure && this.renderPSI("CPU", usageStats.cpuPressure)}
        {usageStats.memoryPressure && this.renderPSI("Memory", usageStats.memoryPressure)}
        {usageStats.ioPressure && this.renderPSI("IO", usageStats.ioPressure)}
      </>
    );
  }

  private renderPSI(resource: string, psi: build.bazel.remote.execution.v2.PSI) {
    const metadata = this.state.actionResult?.executionMetadata;
    if (!metadata) return null;
    const execDurationSeconds = durationSeconds(
      metadata.executionStartTimestamp!,
      metadata.executionCompletedTimestamp!
    );
    if (!execDurationSeconds) return null;
    const execDurationUsec = execDurationSeconds * 1e6;
    const partialStallUsec = Number(psi.some?.total ?? 0);
    if (!partialStallUsec) return null;
    const completeStallUsec = Number(psi.full?.total ?? 0);

    // TODO: render percentages using a color scale corresponding to how bad the stalling is
    return (
      <>
        <div className="metadata-title">{resource} pressure stall duration</div>
        <div>
          Partially stalled: {durationUsec(partialStallUsec)} (
          {((partialStallUsec / execDurationUsec) * 100).toFixed(1)}
          %)
        </div>
        <div>
          Fully stalled: {durationUsec(completeStallUsec)} ({((completeStallUsec / execDurationUsec) * 100).toFixed(1)}
          %)
        </div>
      </>
    );
  }

  render() {
    const digest = parseActionDigest(this.props.search.get("actionDigest") ?? "");
    if (!digest) return <></>;
    const vmMetadata = this.getFirecrackerVMMetadata();
    const executionId = this.getExecutionId();

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
                      <div>
                        {this.state.executeResponse
                          ? "Completed"
                          : this.state.lastOperation
                            ? executionStatusLabel(this.state.lastOperation)
                            : "Unknown"}
                      </div>
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
                          {this.state.command.platform?.properties.length ? (
                            <div className="action-list">
                              {this.state.command.platform?.properties.map((property) => (
                                <div>
                                  <span className="prop-name">{property.name}</span>
                                  <span className="prop-value">={property.value}</span>
                                </div>
                              ))}
                              {!this.state.command.platform?.properties.length && <div>(Default)</div>}
                            </div>
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                        {!this.state.actionResult && this.renderExpectedOutputs(this.state.command)}
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
                            {this.state.actionResult.executionMetadata.usageStats &&
                              this.renderUsageStats(this.state.actionResult.executionMetadata.usageStats)}
                            {this.state.actionResult.executionMetadata &&
                              this.renderTiming(this.state.actionResult.executionMetadata)}
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
                      {this.state.command && this.renderMissingOutputs(this.state.command, this.state.actionResult)}
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
