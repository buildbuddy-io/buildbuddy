import {
  ArrowRight,
  ChevronDown,
  ChevronRight,
  Copy,
  Download,
  File,
  FileQuestion,
  FileSymlink,
  Folder,
  History,
  Info,
  MoreVertical,
} from "lucide-react";
import React, { ReactElement } from "react";
import { cache } from "../../proto/cache_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { firecracker } from "../../proto/firecracker_ts_proto";
import { google as google_grpc_code } from "../../proto/grpc_code_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { stored_invocation } from "../../proto/stored_invocation_ts_proto";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { workflow } from "../../proto/workflow_ts_proto";
import alert_service from "../alert/alert_service";
import capabilities from "../capabilities/capabilities";
import Button, { OutlinedButton } from "../components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import DigestComponent from "../components/digest/digest";
import { TextLink } from "../components/link/link";
import Menu, { MenuItem } from "../components/menu/menu";
import Modal from "../components/modal/modal";
import Popup from "../components/popup/popup";
import Spinner from "../components/spinner/spinner";
import HelpTooltip from "../components/tooltip/help_tooltip";
import errorService from "../errors/error_service";
import format, { durationUsec } from "../format/format";
import { FileIcon } from "../icons/file_icon";
import UserPreferences from "../preferences/preferences";
import router from "../router/router";
import { Cancelable, CancelablePromise, default as rpcService } from "../service/rpc_service";
import TerminalComponent from "../terminal/terminal";
import { Profile, readProfile } from "../trace/compact_trace";
import TraceViewer from "../trace/trace_viewer";
import { digestToString, parseActionDigest } from "../util/cache";
import { copyToClipboard } from "../util/clipboard";
import { BuildBuddyError, HTTPStatusError } from "../util/errors";
import { MessageClass, timestampToDate } from "../util/proto";
import { getErrorReason } from "../util/rpc";
import { quote } from "../util/shlex";
import ActionCompareButtonComponent from "./action_compare_button";
import {
  CompactExecLogActionEdge,
  CompactExecLogActionEdgesIndex,
  CompactExecLogActionSummary,
  compareTargetLabelsByRelevance,
  findCompactExecLogAction,
  getCompactExecLogActionEdgesIndex,
} from "./compact_exec_log_action_edges";
import { ExecuteOperation, executionStatusLabel, waitExecution } from "./execution_status";
import TreeNodeComponent, { TreeNode } from "./invocation_action_tree_node";
import InvocationModel from "./invocation_model";

type IDigest = build.bazel.remote.execution.v2.IDigest;
type ITimestamp = google_timestamp.protobuf.ITimestamp;

const executionDownloadsPageSize = 100;

interface InputFileReference {
  path: string;
  digest: IDigest;
}

type ParamFileState =
  | { expanded: boolean; status: "loading" }
  | { expanded: boolean; status: "loaded"; content: string }
  | { expanded: boolean; status: "error" };

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  preferences: UserPreferences;
}

interface State {
  action?: build.bazel.remote.execution.v2.Action;
  loadingAction: boolean;
  /**
   * The execution fetched from the `getExecution` API.
   * At minimum this should include the basic metadata that we store in the DB.
   */
  execution?: execution_stats.Execution | null;
  executeResponse?: build.bazel.remote.execution.v2.ExecuteResponse;
  actionResult?: build.bazel.remote.execution.v2.ActionResult;
  measuredMemoryPeakBytes?: number;
  inputFilePathToDigest: Map<string, IDigest | null>;
  argumentToInputFile: Map<string, InputFileReference | null>;
  paramFilePathToState: Map<string, ParamFileState>;
  // The first entry in the tuple is the size, the second is the number of files.
  treeShaToTotalSizeMap: Map<string, [Number, Number]>;
  command?: build.bazel.remote.execution.v2.Command;
  error?: string;
  inputRoot?: build.bazel.remote.execution.v2.Directory;
  inputNodes: TreeNode[];
  isMenuOpen: boolean;
  showInvalidateSnapshotModal: boolean;
  showSnapshotMenu: boolean;
  treeShaToExpanded: Map<string, boolean>;
  treeShaToChildrenMap: Map<string, TreeNode[]>;
  stderr?: string;
  stdout?: string;
  serverLogs?: ServerLog[];
  lastOperation?: ExecuteOperation;
  profileLoading: boolean;
  profile?: Profile;
  executionDownloads: cache.ExecutionDownload[];
  executionDownloadsLoading: boolean;
  executionDownloadsNextPageToken: string;
  actionEdgesIndex?: CompactExecLogActionEdgesIndex;
  actionEdgesLoading: boolean;
  actionEdgesError?: string;
  inputSourcesExpanded: boolean;
  expandedOutputConsumerPaths: Set<string>;
}

interface ServerLog {
  name: string;
  text: string;
}

interface ActionEdgeGroup {
  artifactPath: string;
  edges: CompactExecLogActionEdge[];
}

interface ActionEdgeTargetGroup {
  targetLabel: string;
  actions: CompactExecLogActionSummary[];
}

export default class InvocationActionCardComponent extends React.Component<Props, State> {
  state: State = {
    treeShaToExpanded: new Map<string, boolean>(),
    treeShaToChildrenMap: new Map<string, TreeNode[]>(),
    treeShaToTotalSizeMap: new Map<string, [Number, Number]>(),
    inputFilePathToDigest: new Map<string, IDigest | null>(),
    argumentToInputFile: new Map<string, InputFileReference | null>(),
    paramFilePathToState: new Map<string, ParamFileState>(),
    serverLogs: [],
    inputNodes: [],
    loadingAction: true,
    isMenuOpen: false,
    showInvalidateSnapshotModal: false,
    showSnapshotMenu: false,
    profileLoading: false,
    executionDownloads: [],
    executionDownloadsLoading: false,
    executionDownloadsNextPageToken: "",
    actionEdgesLoading: false,
    inputSourcesExpanded: false,
    expandedOutputConsumerPaths: new Set<string>(),
  };

  private executionDownloadsContainerRef = React.createRef<HTMLDivElement>();
  private treeShaToChildrenPromiseMap = new Map<string, Promise<TreeNode[]>>();
  private actionEdgesRequestId = 0;

  componentDidMount() {
    this.fetchAction();
    this.fetchExecuteResponseOrActionResult();
    this.fetchSpawnMetrics();
    this.fetchActionEdges();
    if (this.getExecutionId()) {
      this.fetchExecutionDownloads("");
    }
  }

  componentDidUpdate(prevProps: Readonly<Props>, prevState: Readonly<State>): void {
    if (prevProps.search.get("actionDigest") !== this.props.search.get("actionDigest")) {
      this.fetchAction();
      this.fetchExecuteResponseOrActionResult();
      this.fetchSpawnMetrics();
      this.fetchActionEdges();
      this.fetchExecutionDownloads("");
      return;
    }
    if (prevProps.model.getExecutionLogFileUri() !== this.props.model.getExecutionLogFileUri()) {
      this.fetchSpawnMetrics();
      this.fetchActionEdges();
    }
    if (prevProps.search.get("executeResponseDigest") !== this.props.search.get("executeResponseDigest")) {
      this.fetchExecuteResponseOrActionResult();
      this.fetchExecutionDownloads("");
      return;
    }

    const prevExecutionId = prevProps.search.get("executionId") || prevState.execution?.executionId;
    const executionId = this.getExecutionId();
    if (prevExecutionId !== executionId) {
      this.fetchExecutionDownloads("");
    }
  }

  componentWillUnmount() {
    this.actionEdgesRequestId++;
  }

  fetchSpawnMetrics() {
    const actionDigestParam = this.props.search.get("actionDigest");
    if (!actionDigestParam || !this.props.model.hasExecutionLog() || this.getExecutionId()) {
      this.setState({ measuredMemoryPeakBytes: undefined });
      return;
    }

    const actionDigest = parseActionDigest(actionDigestParam);
    if (!actionDigest) {
      this.setState({ measuredMemoryPeakBytes: undefined });
      return;
    }

    const model = this.props.model;
    const actionDigestString = digestToString(actionDigest);
    const actionDigestHasSize = actionDigestParam.includes("/");
    model
      .getExecutionLog()
      .then((log) => {
        if (this.props.model !== model || this.props.search.get("actionDigest") !== actionDigestParam) return;

        const spawn = log.find((entry) => {
          const spawnDigest = entry.spawn?.digest;
          if (!spawnDigest || spawnDigest.hash !== actionDigest.hash) return false;
          return !actionDigestHasSize || digestToString(spawnDigest) === actionDigestString;
        });
        this.setState({
          measuredMemoryPeakBytes: Number(spawn?.spawn?.metrics?.measuredMemoryPeakBytes || 0) || undefined,
        });
      })
      .catch((e) => console.error("Failed to fetch execution log:", e));
  }

  fetchActionEdges() {
    const requestId = ++this.actionEdgesRequestId;
    const actionDigestParam = this.props.search.get("actionDigest");
    const actionDigest = parseActionDigest(actionDigestParam || "");
    const model = this.props.model;
    const executionLogUri = model.getExecutionLogFileUri();
    if (!actionDigest || !executionLogUri) {
      this.setState({ actionEdgesIndex: undefined, actionEdgesLoading: false, actionEdgesError: undefined });
      return;
    }

    this.setState({
      actionEdgesLoading: true,
      actionEdgesError: undefined,
      inputSourcesExpanded: false,
      expandedOutputConsumerPaths: new Set<string>(),
    });
    getCompactExecLogActionEdgesIndex(executionLogUri, () => model.getExecutionLog())
      .then((index) => {
        if (
          requestId !== this.actionEdgesRequestId ||
          this.props.model !== model ||
          this.props.search.get("actionDigest") !== actionDigestParam ||
          this.props.model.getExecutionLogFileUri() !== executionLogUri
        ) {
          return;
        }
        this.setState({ actionEdgesIndex: index, actionEdgesLoading: false });
      })
      .catch((e) => {
        if (requestId !== this.actionEdgesRequestId) return;
        console.error("Failed to fetch compact execution log action edges:", e);
        this.setState({
          actionEdgesIndex: undefined,
          actionEdgesLoading: false,
          actionEdgesError: "Failed to load compact execution log action dependencies.",
        });
      });
  }

  fetchAction() {
    this.setState({
      loadingAction: true,
      action: undefined,
      command: undefined,
      inputRoot: undefined,
      inputNodes: [],
      inputFilePathToDigest: new Map<string, IDigest | null>(),
      argumentToInputFile: new Map<string, InputFileReference | null>(),
      paramFilePathToState: new Map<string, ParamFileState>(),
    });
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
        this.setState({ action });
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
        // Fetch the full response from cache, since it contains some additional
        // metadata not sent on the stream. Disallow stream fallback since at
        // this point we're already in the stream.
        if (operation.done && !this.state.actionResult) {
          this.fetchExecuteResponseOrActionResult({ streamFallback: false });
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
        const inputNodes = this.treeNodesForDirectory(inputRoot);
        this.setState({ inputRoot, inputNodes }, () => this.resolveArgumentInputFilesIfNeeded());
      })
      .catch((e) => console.error("Failed to fetch input root:", e));
  }

  /**
   * Fetches the latest ActionResult from AC as a fallback in case we couldn't
   * locate the ExecuteResponse that was returned for this particular
   * invocation.
   */
  fetchActionResult(actionDigest: IDigest) {
    const actionResultUrl = this.props.model.getActionCacheURL(actionDigest);
    this.actionResultRPC = rpcService
      .fetchBytestreamFile(actionResultUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer) => {
        const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
        this.setState({ actionResult });
        this.fetchStdout(actionResult);
        this.fetchStderr(actionResult);
      })
      .catch((e) => {
        const error = BuildBuddyError.parse(e);
        if (error.code !== "NotFound") {
          console.error("Error during AC fallback:", e);
          // Optionally handle other non-NotFound errors from AC fetch.
        } else {
          console.debug("Action result not found in AC.");
        }
      });
  }

  private executeResponseRPC?: CancelablePromise<build.bazel.remote.execution.v2.ExecuteResponse | null>;
  private executionRPC?: CancelablePromise<execution_stats.Execution | null>;
  private actionResultRPC?: Cancelable;
  private stdoutRPC?: Cancelable;
  private stderrRPC?: Cancelable;
  private serverLogsRPCs?: Cancelable[];
  private profileRPC?: Cancelable;

  fetchExecuteResponseOrActionResult({ streamFallback = true } = {}) {
    this.executeResponseRPC?.cancel();
    this.executionRPC?.cancel();
    this.actionResultRPC?.cancel();
    this.stdoutRPC?.cancel();
    this.stderrRPC?.cancel();
    this.serverLogsRPCs?.forEach((rpc) => rpc.cancel());
    this.profileRPC?.cancel();

    this.setState({
      executeResponse: undefined,
      execution: undefined,
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
      const executeResponseDigest = parseActionDigest(executeResponseDigestParam);
      if (!executeResponseDigest) {
        alert_service.error("Invalid execute response digest in URL");
        return;
      }
      // TODO: once all servers support executionId filtering, request
      // inlineExecuteResponse from the server instead of fetching the
      // ExecuteResponse separately on the client.
      this.fetchExecuteResponseByDigest(executeResponseDigest);
      // If we have an execution ID, also fetch the execution metadata from the DB.
      const executionId = this.getExecutionId();
      if (executionId) {
        this.fetchExecution(executionId);
      }
    } else {
      const actionDigest = parseActionDigest(actionDigestParam);
      if (!actionDigest) {
        alert_service.error("Missing action digest in URL");
        return;
      }
      // If we have an execution ID, it means that this was certainly an RBE action
      // and we can fetch the ExecuteResponse directly by action digest.
      //
      // If we don't have an execution ID, we can fall back to fetching the
      // ActionResult from the action cache.
      //
      // TODO: we should display a warning if we fetched the ActionResult from AC,
      // since the AC entry could potentially be from a newer invocation, not the
      // current one we're looking at, which is probably confusing.
      this.getExecutionId()
        ? this.fetchExecuteResponseByActionDigest(actionDigest)
        : this.fetchActionResult(actionDigest);
    }

    if (!this.executeResponseRPC) {
      return;
    }

    let executionFound = false;
    this.executeResponseRPC
      .then((executeResponse) => {
        if (!executeResponse) {
          return;
        }
        // If we found an execution, we can cancel the direct AC fetch.
        this.actionResultRPC?.cancel();
        executionFound = true;
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
          return;
        }
        errorService.handleError(e);
      })
      .finally(() => {
        if (executionFound) return;

        if (streamFallback) {
          console.debug("Falling back to WaitExecution");
          this.streamExecution();
        }
      });
  }

  fetchExecuteResponseByDigest(executeResponseDigest: build.bazel.remote.execution.v2.Digest) {
    this.executeResponseRPC = rpcService
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

  fetchExecution(executionId: string) {
    const service = rpcService.getRegionalServiceOrDefault(this.props.model.stringCommandLineOption("remote_executor"));
    // TODO: remove redundant actionDigestHash filtering once all servers
    // support executionId filtering.
    const actionDigestHash = parseActionDigestHashFromExecutionId(executionId);
    this.executionRPC = service
      .getExecution({
        executionLookup: new execution_stats.ExecutionLookup({
          invocationId: this.props.model.getInvocationId(),
          executionId,
          actionDigestHash,
        }),
      })
      .then((response) => {
        const execution = response.execution?.[0];
        if (execution) {
          this.setState({ execution });
        }
        return execution;
      });
  }

  private executionDownloadsRPC?: Cancelable;

  private fetchExecutionDownloads(pageToken = this.state.executionDownloadsNextPageToken) {
    this.executionDownloadsRPC?.cancel();
    this.executionDownloadsRPC = undefined;

    const executionId = this.getExecutionId();
    if (!executionId) {
      this.setState({
        executionDownloads: [],
        executionDownloadsLoading: false,
        executionDownloadsNextPageToken: "",
      });
      return;
    }

    const service = rpcService.getRegionalServiceOrDefault(this.props.model.stringCommandLineOption("remote_executor"));
    this.setState({ executionDownloadsLoading: true });
    if (!pageToken) {
      this.setState({ executionDownloads: [] });
    }
    this.executionDownloadsRPC = service
      .getExecutionDownloads({
        invocationId: this.props.model.getInvocationId(),
        executionId,
        pageSize: executionDownloadsPageSize,
        pageToken,
      })
      .then((response) => {
        this.setState((prevState) => ({
          executionDownloads: [...(pageToken ? prevState.executionDownloads : []), ...(response.downloads ?? [])],
          executionDownloadsNextPageToken: response.nextPageToken || "",
        }));
        return response;
      })
      .catch((e) => {
        const error = BuildBuddyError.parse(e);
        // If we're fetching a subsequent page, surface the error loudly (since
        // the user is actively trying to fetch); otherwise silently log it.
        if (pageToken) {
          errorService.handleError(error);
        } else {
          console.error(e);
        }
        // Clear the page token so we don't keep trying to fetch pages after we
        // hit an error.
        this.setState({ executionDownloadsNextPageToken: "" });
      })
      .finally(() => {
        // Mark the RPC done, and then check whether we need to fetch the next
        // page (if the user is scrolled to the bottom).
        this.executionDownloadsRPC = undefined;
        this.setState({ executionDownloadsLoading: false }, () => this.maybeFetchMoreExecutionDownloads());
      });
  }

  private maybeFetchMoreExecutionDownloads() {
    if (this.executionDownloadsRPC || !this.state.executionDownloadsNextPageToken) {
      return;
    }
    const container = this.executionDownloadsContainerRef.current;
    if (!container) {
      return;
    }
    const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
    if (container.scrollHeight <= container.clientHeight + 1 || distanceFromBottom <= 1) {
      this.fetchExecutionDownloads();
    }
  }

  fetchExecuteResponseByActionDigest(actionDigest: build.bazel.remote.execution.v2.Digest) {
    const service = rpcService.getRegionalServiceOrDefault(this.props.model.stringCommandLineOption("remote_executor"));
    this.executeResponseRPC = service
      .getExecution({
        executionLookup: new execution_stats.ExecutionLookup({
          invocationId: this.props.model.getInvocationId(),
          actionDigestHash: actionDigest.hash,
        }),
        inlineExecuteResponse: true,
      })
      .then((response) => {
        const execution = response.execution?.[0];
        if (execution) {
          this.setState({ execution });
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
        const command = build.bazel.remote.execution.v2.Command.decode(new Uint8Array(buffer));
        this.setState({ command }, () => this.resolveArgumentInputFilesIfNeeded());
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
        console.log("fetch profile failed", e);
        errorService.handleError(e);
      })
      .finally(() => this.setState({ profileLoading: false }));
  }

  getExecutionId(): string | undefined {
    // If we got here from the executions page then we'll have the execution ID
    // in the URL; otherwise the execution ID gets fetched from the executions
    // linked to the invocation matching the actionDigest in the URL.
    return this.props.search.get("executionId") || this.state.execution?.executionId;
  }

  private renderArguments(args: string[]) {
    if (args.length == 0) return <div>None found</div>;
    return (
      <div className="action-list">
        {args.map((argument, index) => (
          <div key={`${index}-${argument}`}>{this.renderArgument(argument)}</div>
        ))}
      </div>
    );
  }

  private renderArgument(argument: string) {
    const inputFile = this.state.argumentToInputFile.get(argument);
    if (!inputFile) return argument;

    const paramFilePath = getArgumentParamFilePath(argument);
    const paramFileDigest = paramFilePath ? this.state.inputFilePathToDigest.get(paramFilePath) : undefined;
    const paramFile = paramFilePath && paramFileDigest ? { path: paramFilePath, digest: paramFileDigest } : undefined;
    const paramFileState = paramFile ? this.state.paramFilePathToState.get(paramFile.path) : undefined;
    const expanded = paramFileState?.expanded ?? false;
    return (
      <>
        <div className="action-argument-row">
          {paramFile && (
            <button
              className="action-argument-expander"
              onClick={() => this.handleParamFileExpanded(paramFile.path, paramFile.digest)}
              title={expanded ? "Collapse params file" : "Expand params file"}
              type="button">
              {expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />}
            </button>
          )}
          <span>{argument}</span>
          <TextLink
            className="artifact-view"
            href={this.getFileViewUrl(inputFile.path, inputFile.digest)}
            target="_blank">
            <FileIcon extension={getPathBasename(inputFile.path)} /> View
          </TextLink>
        </div>
        {paramFileState?.expanded && this.renderParamFileContents(paramFileState)}
      </>
    );
  }

  private handleParamFileExpanded(path: string, digest: IDigest) {
    const paramFileState = this.state.paramFilePathToState.get(path);
    if (paramFileState?.expanded) {
      this.updateParamFileState(path, (state) =>
        state ? { ...state, expanded: false } : { expanded: false, status: "loading" }
      );
      return;
    }

    const shouldFetch = !paramFileState || paramFileState.status === "error";
    this.updateParamFileState(
      path,
      (state) =>
        !state || state.status === "error" ? { expanded: true, status: "loading" } : { ...state, expanded: true },
      () => {
        if (shouldFetch) this.fetchParamFileContent(path, digest);
      }
    );
  }

  private fetchParamFileContent(path: string, digest: IDigest) {
    const actionDigest = this.props.search.get("actionDigest") ?? "";
    this.updateParamFileState(path, (state) => ({ expanded: state?.expanded ?? true, status: "loading" }));

    rpcService
      .fetchBytestreamFile(this.props.model.getBytestreamURL(digest), this.props.model.getInvocationId(), "text")
      .then((content) => {
        if ((this.props.search.get("actionDigest") ?? "") !== actionDigest) return;
        this.updateParamFileState(path, (state) => ({
          expanded: state?.expanded ?? false,
          status: "loaded",
          content,
        }));
      })
      .catch((e) => {
        console.error(`Failed to fetch params file ${path}:`, e);
        if ((this.props.search.get("actionDigest") ?? "") !== actionDigest) return;
        this.updateParamFileState(path, (state) => ({ expanded: state?.expanded ?? false, status: "error" }));
      });
  }

  private updateParamFileState(
    path: string,
    update: (state: ParamFileState | undefined) => ParamFileState,
    callback?: () => void
  ) {
    this.setState((prevState) => {
      const paramFilePathToState = new Map(prevState.paramFilePathToState);
      paramFilePathToState.set(path, update(prevState.paramFilePathToState.get(path)));
      return { paramFilePathToState };
    }, callback);
  }

  private renderParamFileContents(paramFileState: ParamFileState) {
    switch (paramFileState.status) {
      case "loading":
        return <div className="action-argument-param-file-loading">Loading params file...</div>;
      case "error":
        return <div className="action-argument-param-file-error">Failed to load params file.</div>;
      case "loaded":
        return <pre className="action-argument-param-file">{paramFileState.content || "(empty)"}</pre>;
    }
  }

  private resolveArgumentInputFilesIfNeeded() {
    if (!this.state.command || !this.state.inputRoot) return;

    // Collect potential paths for command line arguments
    const actionDigest = this.props.search.get("actionDigest") ?? "";
    const argumentToCandidates = new Map<string, string[]>();
    for (const argument of this.state.command.arguments) {
      if (this.state.argumentToInputFile.has(argument)) continue;
      const candidates = getArgumentInputFilePathCandidates(argument);
      if (candidates.length) {
        argumentToCandidates.set(argument, candidates);
      }
    }
    if (!argumentToCandidates.size) return;

    // Resolve each potential path only once
    const inputFilePaths = new Set([...argumentToCandidates.values()].flat());
    const pathsToResolve = [...inputFilePaths].filter((path) => !this.state.inputFilePathToDigest.has(path));

    // For each argument attempt to resolve the path to a real input file digest
    Promise.all(pathsToResolve.map((path) => this.resolveInputFilePath(path))).then((results) => {
      if ((this.props.search.get("actionDigest") ?? "") !== actionDigest) return;
      this.setState((prevState) => {
        const inputFilePathToDigest = new Map(prevState.inputFilePathToDigest);
        for (const [path, digest] of results) {
          if (digest) {
            inputFilePathToDigest.set(path, digest);
          }
        }
        const argumentToInputFile = new Map(prevState.argumentToInputFile);
        for (const [argument, candidates] of argumentToCandidates) {
          for (const path of candidates) {
            const digest = inputFilePathToDigest.get(path);
            if (digest) {
              argumentToInputFile.set(argument, { path, digest });
              break;
            }
          }
        }
        return { inputFilePathToDigest, argumentToInputFile };
      });
    });
  }

  private async resolveInputFilePath(path: string): Promise<[string, IDigest | null]> {
    try {
      return [path, await this.resolveInputFileDigest(path)];
    } catch (e) {
      console.error(`Failed to resolve input file ${path}:`, e);
      return [path, null];
    }
  }

  private async resolveInputFileDigest(path: string): Promise<IDigest | null> {
    const segments = path.split("/");
    if (!segments.length || !this.state.inputNodes.length) return null;

    let nodes = this.state.inputNodes;
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      const isLeaf = i === segments.length - 1;
      if (isLeaf) {
        // View links are only useful for files so ignore anything else
        // At this point nodes contains only the children of the earlier path segments
        for (const node of nodes) {
          if (node.type === "file" && node.obj.name === segment) {
            return node.obj.digest ?? null;
          }
        }
        return null;
      }

      // The dirname of paths must be fetched first so the children exist in the inputNodes
      let child: Extract<TreeNode, { type: "dir" | "tree" }> | undefined;
      for (const node of nodes) {
        if ((node.type === "dir" || node.type === "tree") && node.obj.name === segment) {
          child = node;
          break;
        }
      }
      if (!child || !child.obj.digest) return null;
      nodes = await this.fetchDirectoryChildren(child.obj.digest, child.type);
    }
    return null;
  }

  handleOutputFileClicked(file: build.bazel.remote.execution.v2.OutputFile) {
    if (!file.digest) return;

    rpcService.downloadBytestreamFile(
      file.path,
      this.props.model.getBytestreamURL(file.digest),
      this.props.model.getInvocationId()
    );
  }

  private getFileViewUrl(path: string, digest: IDigest) {
    const params: Record<string, string> = {
      bytestream_url: this.props.model.getBytestreamURL(digest),
      invocation_id: this.props.model.getInvocationId(),
      filename: path,
    };
    return `/code/buildbuddy-io/buildbuddy/?${new URLSearchParams(params).toString()}`;
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
            <TraceViewer
              profile={this.state.profile}
              fitToContent
              filterHidden
              dark={this.props.preferences.darkModeEnabled}
            />
          ) : null}
        </div>
      </>
    );
  }

  private renderExecutionDownloads() {
    const ioStats = this.state.actionResult?.executionMetadata?.ioStats;
    const fetchSummary = ioStats && (
      <div className="action-downloads-summary">
        Downloaded {format.bytes(ioStats.fileDownloadSizeBytes ?? 0)} ({format.count(ioStats.fileDownloadCount ?? 0)}{" "}
        cache misses, {format.count(ioStats.localCacheHits ?? 0)} cache hits)
      </div>
    );
    if (!fetchSummary && !this.state.executionDownloadsLoading && !this.state.executionDownloads.length) {
      return null;
    }
    return (
      <>
        <div className="metadata-title">Inputs fetched</div>
        <div className="action-downloads">
          {fetchSummary}
          {this.state.executionDownloadsLoading && !this.state.executionDownloads.length ? (
            <div className="action-downloads-loading">
              <Spinner />
            </div>
          ) : this.state.executionDownloads.length ? (
            <>
              <div
                className="action-downloads-table-container"
                onScroll={() => this.maybeFetchMoreExecutionDownloads()}
                ref={this.executionDownloadsContainerRef}>
                <table className="action-downloads-table">
                  <thead>
                    <tr>
                      <th>Size</th>
                      <th>Path</th>
                      <th>Digest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {this.state.executionDownloads.map((download) => (
                      <tr key={`${download.path}|${download.digest?.hash}`}>
                        <td className="action-downloads-size" title={`${download.digest?.sizeBytes ?? 0}`}>
                          {format.bytes(download.digest?.sizeBytes ?? 0)}
                        </td>
                        <td className="action-downloads-path">
                          <div className="action-downloads-path-content">
                            <span className="action-downloads-path-text">{download.path}</span>
                            {download.digest?.hash && (
                              <a
                                className="action-downloads-download-link"
                                href={rpcService.getBytestreamUrl(
                                  this.props.model.getBytestreamURL(download.digest),
                                  this.props.model.getInvocationId(),
                                  { filename: download.path }
                                )}
                                title="Download">
                                <Download className="download-button" />
                              </a>
                            )}
                          </div>
                        </td>
                        <td className="action-downloads-digest">
                          <DigestComponent
                            digest={{ hash: download.digest?.hash, sizeBytes: null }}
                            hashWidth="112px"
                            expandOnHover={false}
                          />
                        </td>
                      </tr>
                    ))}
                    {this.state.executionDownloadsLoading && (
                      <tr className="action-downloads-loading-row">
                        <td colSpan={3}>
                          <Spinner />
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : null}
        </div>
      </>
    );
  }

  private getCurrentCompactLogAction(): CompactExecLogActionSummary | undefined {
    const digest = parseActionDigest(this.props.search.get("actionDigest") ?? "");
    return findCompactExecLogAction(
      this.state.actionEdgesIndex,
      digest,
      this.state.execution?.targetLabel,
      this.state.execution?.actionMnemonic
    );
  }

  private getRelatedCompactLogActionEdges(direction: "parents" | "children"): CompactExecLogActionEdge[] {
    const currentAction = this.getCurrentCompactLogAction();
    if (!currentAction) return [];
    return direction === "parents" ? currentAction.parentEdges : currentAction.childEdges;
  }

  private groupCompactLogActionEdges(edges: CompactExecLogActionEdge[]): ActionEdgeGroup[] {
    const groups = new Map<string, CompactExecLogActionEdge[]>();
    for (const edge of edges) {
      const group = groups.get(edge.artifactPath);
      if (group) {
        group.push(edge);
      } else {
        groups.set(edge.artifactPath, [edge]);
      }
    }
    return Array.from(groups.entries()).map(([artifactPath, edges]) => ({ artifactPath, edges }));
  }

  private groupCompactLogActionEdgesByTarget(
    edges: CompactExecLogActionEdge[],
    referenceTargetLabel?: string
  ): ActionEdgeTargetGroup[] {
    const groups = new Map<string, CompactExecLogActionSummary[]>();
    const seenActionIdsByTarget = new Map<string, Set<string>>();
    for (const edge of edges) {
      const action = this.state.actionEdgesIndex?.actionById.get(edge.actionId);
      if (!action) continue;
      const targetLabel = action.targetLabel || "";
      const seenActionIds = seenActionIdsByTarget.get(targetLabel) || new Set<string>();
      if (seenActionIds.has(action.id)) continue;
      seenActionIds.add(action.id);
      seenActionIdsByTarget.set(targetLabel, seenActionIds);
      const actions = groups.get(targetLabel) || [];
      actions.push(action);
      groups.set(targetLabel, actions);
    }
    return Array.from(groups.entries())
      .map(([targetLabel, actions]) => ({ targetLabel, actions }))
      .sort((a, b) => compareTargetLabelsByRelevance(a.targetLabel, b.targetLabel, referenceTargetLabel));
  }

  private getUniqueCompactLogActionCount(edges: CompactExecLogActionEdge[]): number {
    return new Set(edges.map((edge) => edge.actionId)).size;
  }

  private getOutputConsumerEdges(outputPath: string): CompactExecLogActionEdge[] {
    const currentAction = this.getCurrentCompactLogAction();
    if (!currentAction) return [];

    const exactMatches = currentAction.childEdges.filter((edge) => edge.artifactPath === outputPath);
    if (exactMatches.length) return exactMatches;

    const outputPathSuffix = this.getBazelOutBinSuffix(outputPath);
    if (!outputPathSuffix) return [];
    return currentAction.childEdges.filter((edge) => this.getBazelOutBinSuffix(edge.artifactPath) === outputPathSuffix);
  }

  private getBazelOutBinSuffix(path: string): string | undefined {
    const match = path.match(/^bazel-out\/[^/]+\/bin\/(.+)$/);
    return match?.[1];
  }

  private isOutputConsumerExpanded(outputPath: string): boolean {
    return this.state.expandedOutputConsumerPaths.has(outputPath);
  }

  private toggleOutputConsumerExpansion(outputPath: string) {
    this.setState((state) => {
      const expandedOutputConsumerPaths = new Set(state.expandedOutputConsumerPaths);
      if (expandedOutputConsumerPaths.has(outputPath)) {
        expandedOutputConsumerPaths.delete(outputPath);
      } else {
        expandedOutputConsumerPaths.add(outputPath);
      }
      return { expandedOutputConsumerPaths };
    });
  }

  private getDigestBackedCompactLogActionHref(action: CompactExecLogActionSummary): string | undefined {
    if (!action.digestHash) return undefined;
    const search = new URLSearchParams();
    search.set("actionDigest", `${action.digestHash}/${action.digestSizeBytes || 0}`);
    return `/invocation/${this.props.model.getInvocationId()}?${search.toString()}#action`;
  }

  private getSymlinkSourceAction(
    action: CompactExecLogActionSummary,
    visitedActionIds = new Set<string>()
  ): CompactExecLogActionSummary | undefined {
    if (visitedActionIds.has(action.id)) return undefined;
    visitedActionIds.add(action.id);

    for (const parentId of action.parentIds) {
      const parentAction = this.state.actionEdgesIndex?.actionById.get(parentId);
      if (!parentAction) continue;
      if (parentAction.digestHash) return parentAction;
      if (parentAction.kind === "symlink") {
        const sourceAction = this.getSymlinkSourceAction(parentAction, visitedActionIds);
        if (sourceAction) return sourceAction;
      }
    }
    return undefined;
  }

  private getCompactLogActionHref(action: CompactExecLogActionSummary): string | undefined {
    const digestBackedHref = this.getDigestBackedCompactLogActionHref(action);
    if (digestBackedHref) return digestBackedHref;

    const sourceAction = action.kind === "symlink" ? this.getSymlinkSourceAction(action) : undefined;
    return sourceAction ? this.getDigestBackedCompactLogActionHref(sourceAction) : undefined;
  }

  private renderCompactLogActionLink(action: CompactExecLogActionSummary, hideTargetLabel: boolean = false) {
    const actionText = action.cached ? `${action.label} (Cached)` : action.label;
    const symlinkSourceAction =
      action.kind === "symlink" && !action.digestHash ? this.getSymlinkSourceAction(action) : undefined;
    const href = this.getCompactLogActionHref(action);
    const actionTitle = [
      actionText,
      action.primaryOutputPath ? `Primary output: ${action.primaryOutputPath}` : "",
      symlinkSourceAction
        ? `Opens source action: ${symlinkSourceAction.targetLabel} > ${symlinkSourceAction.label}`
        : "",
      !href && action.kind === "symlink" ? "Symlink action source not found in the compact execution log." : "",
    ]
      .filter(Boolean)
      .join("\n");
    const content = (
      <>
        {!hideTargetLabel && (
          <>
            <span className="action-edge-target" title={action.targetLabel}>
              {action.targetLabel || "<unknown target>"}
            </span>
            <ChevronRight className="action-edge-separator icon" />
          </>
        )}
        <span className="action-edge-action" title={actionTitle}>
          {actionText}
        </span>
        {action.kind === "symlink" && <span className="action-edge-chip">Symlink</span>}
        {action.result === "failed" && <span className="action-edge-chip failed">Failed</span>}
      </>
    );
    if (!href) {
      return (
        <span className={`action-edge-row action-edge-row-static ${hideTargetLabel ? "action-edge-action-only" : ""}`}>
          {content}
        </span>
      );
    }
    return (
      <TextLink className={`action-edge-row ${hideTargetLabel ? "action-edge-action-only" : ""}`} href={href}>
        {content}
      </TextLink>
    );
  }

  private renderActionEdgeTargetGroups(edges: CompactExecLogActionEdge[]) {
    const targetGroups = this.groupCompactLogActionEdgesByTarget(edges, this.getCurrentCompactLogAction()?.targetLabel);
    return targetGroups.map((group) => (
      <div className="action-edge-target-group" key={group.targetLabel || "<unknown target>"}>
        <div className="action-edge-target-group-label" title={group.targetLabel}>
          {group.targetLabel || "<unknown target>"}
        </div>
        <div className="action-edge-target-group-actions">
          {group.actions.map((action) => (
            <div className="action-edge-target-group-action" key={action.id}>
              {this.renderCompactLogActionLink(action, true)}
            </div>
          ))}
        </div>
      </div>
    ));
  }

  private renderOutputConsumerDisclosure(outputPath: string) {
    const edges = this.getOutputConsumerEdges(outputPath);
    if (!edges.length) return null;

    const expanded = this.isOutputConsumerExpanded(outputPath);
    const consumerCount = this.groupCompactLogActionEdgesByTarget(
      edges,
      this.getCurrentCompactLogAction()?.targetLabel
    ).reduce((count, group) => count + group.actions.length, 0);
    const Chevron = expanded ? ChevronDown : ChevronRight;
    return (
      <button
        className="action-edge-output-toggle"
        onClick={(event) => {
          event.stopPropagation();
          this.toggleOutputConsumerExpansion(outputPath);
        }}
        type="button">
        <Chevron className="icon" />
        Consumed by {consumerCount}
      </button>
    );
  }

  private renderOutputConsumerSection(outputPath: string) {
    const disclosure = this.renderOutputConsumerDisclosure(outputPath);
    if (!disclosure) return null;
    return (
      <div className="action-edge-output-consumer-section">
        <div className="action-edge-output-consumer-line">{disclosure}</div>
        {this.renderOutputConsumerDetails(outputPath)}
      </div>
    );
  }

  private renderOutputConsumerDetails(outputPath: string) {
    if (!this.isOutputConsumerExpanded(outputPath)) return null;
    const edges = this.getOutputConsumerEdges(outputPath);
    if (!edges.length) return null;
    return <div className="action-edge-output-consumers">{this.renderActionEdgeTargetGroups(edges)}</div>;
  }

  private renderInputSourcesSection() {
    if (!this.props.model.hasExecutionLog()) return null;

    if (this.state.actionEdgesLoading) {
      return (
        <div className="action-section">
          <div className="action-property-title">Input sources</div>
          <div>
            <div className="action-edge-empty">Loading compact execution log dependencies...</div>
          </div>
        </div>
      );
    }
    if (this.state.actionEdgesError) return null;

    const currentAction = this.getCurrentCompactLogAction();
    if (!currentAction) return null;

    const relatedEdges = this.getRelatedCompactLogActionEdges("parents");
    const expanded = this.state.inputSourcesExpanded;
    const sourceCount = this.getUniqueCompactLogActionCount(relatedEdges);
    return (
      <div className="action-section">
        <div className="action-property-title">Input sources</div>
        <div>
          {relatedEdges.length ? (
            <>
              <button
                className="action-edge-section-toggle"
                onClick={() => this.setState({ inputSourcesExpanded: !expanded })}
                type="button">
                {expanded ? <ChevronDown className="icon" /> : <ChevronRight className="icon" />}
                {expanded ? "Hide" : "Show"} {sourceCount} input source{sourceCount === 1 ? "" : "s"}
              </button>
              {expanded && this.renderInputSourceDetails(relatedEdges)}
            </>
          ) : (
            <div className="action-edge-empty">No input sources found in the compact execution log.</div>
          )}
        </div>
      </div>
    );
  }

  private renderInputSourceDetails(edges: CompactExecLogActionEdge[]) {
    const edgeGroups = this.groupCompactLogActionEdges(edges);
    return (
      <div className="action-edge-section-panel">
        <div className="action-edge-list">
          {edgeGroups.map((group) => (
            <div className="action-edge-group" key={group.artifactPath}>
              <div className="action-edge-artifact-path" title={group.artifactPath}>
                {group.artifactPath}
              </div>
              <div className="action-edge-group-list">{this.renderActionEdgeTargetGroups(group.edges)}</div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  private renderCompactLogActionIdentity() {
    const action = this.getCurrentCompactLogAction();
    if (!action) return null;

    const targetLabel = action.targetLabel || "<unknown target>";
    const actionText = action.cached ? `${action.label} (Cached)` : action.label;
    return (
      <>
        <div className="action-section">
          <div className="action-property-title">Target label</div>
          <div>
            {action.targetLabel ? (
              <TextLink
                className="target-label-link"
                href={`/invocation/${this.props.model.getInvocationId()}?${new URLSearchParams({
                  target: action.targetLabel,
                })}`}>
                {targetLabel}
              </TextLink>
            ) : (
              targetLabel
            )}
          </div>
        </div>
        <div className="action-section">
          <div className="action-property-title">Action</div>
          <div>{actionText}</div>
        </div>
      </>
    );
  }

  /** Build a fully-formed `bb execute` command for this action. */
  private buildBbExecuteCommand(): string {
    const { action, command } = this.state;
    if (!action || !command) return "";

    const unquotedIndexes = new Set();

    const parts: string[] = ["bb", "execute"];
    parts.push("--remote_header=x-buildbuddy-api-key=${BB_API_KEY?}");
    // Don't quote this arg, since we want ${BB_API_KEY?} to be evaluated by
    // the shell.
    unquotedIndexes.add(parts.length - 1);

    // Remote executor / instance (derived from invocation options if present)
    const remoteExec = this.props.model.stringCommandLineOption("remote_executor");
    if (remoteExec) parts.push(`--remote_executor=${remoteExec}`);

    const digestFn =
      build.bazel.remote.execution.v2.DigestFunction.Value[this.props.model.getDigestFunction()].toLowerCase();
    if (digestFn === "blake3" || digestFn === "sha256") parts.push(`--digest_function=${digestFn}`);

    const invocationId = this.props.model.getInvocationId();
    if (invocationId) parts.push(`--invocation_id=${invocationId}`);

    const remoteInstance = this.props.model.optionsMap.get("remote_instance_name");
    if (remoteInstance) parts.push(`--remote_instance_name=${remoteInstance}`);

    // Timeout
    if (action.timeout?.seconds) parts.push(`--remote_timeout=${action.timeout.seconds}s`);

    if (action.inputRootDigest) parts.push(`--input_root_digest=${digestToString(action.inputRootDigest)}`);

    // Env vars
    for (const env of command.environmentVariables) {
      parts.push(`--action_env=${env.name}=${env.value}`);
    }

    // Platform props
    for (const prop of command.platform?.properties ?? []) {
      parts.push(`--exec_properties=${prop.name}=${prop.value}`);
    }

    // Expected outputs
    const addOutPath = (p: string) => parts.push(`--output_path=${p}`);
    if (command.outputPaths.length) {
      command.outputPaths.forEach(addOutPath);
    } else {
      command.outputFiles.forEach(addOutPath);
      command.outputDirectories.forEach(addOutPath);
    }

    // Separator and original argv
    parts.push("--", ...command.arguments);

    return parts.map((arg, i) => (unquotedIndexes.has(i) ? arg : quote(arg))).join(" \\\n\t");
  }

  /** Copy the command to clipboard and toast the user. */
  private onClickCopyBbExecute = () => {
    const cmd = this.buildBbExecuteCommand();
    if (!cmd) return alert_service.error("Unable to build command");
    copyToClipboard(cmd);
    alert_service.success("`bb execute` command copied to clipboard");
  };

  private fetchAndExpandDir(node: TreeNode): Promise<TreeNode[]> {
    if (node.type !== "dir" && node.type !== "tree") return Promise.resolve([]);
    if (!node.obj.digest) return Promise.resolve([]);

    const digestString = node.obj.digest.hash ?? "";
    return this.fetchDirectoryChildren(node.obj.digest, node.type).then((nodes) => {
      if (digestString) {
        this.state.treeShaToExpanded.set(digestString, true);
      }
      return nodes;
    });
  }

  private fetchDirectoryChildren(digest: IDigest, type: "dir" | "tree"): Promise<TreeNode[]> {
    const digestString = digest.hash ?? "";
    const cachedChildren = digestString ? this.state.treeShaToChildrenMap.get(digestString) : undefined;
    if (cachedChildren) return Promise.resolve(cachedChildren);
    const cachedPromise = digestString ? this.treeShaToChildrenPromiseMap.get(digestString) : undefined;
    if (cachedPromise) return cachedPromise;

    const dirUrl = this.props.model.getBytestreamURL(digest);
    const fetchPromise = rpcService
      .fetchBytestreamFile(dirUrl, this.props.model.getInvocationId(), "arraybuffer")
      .then((buffer: ArrayBuffer) => new Uint8Array(buffer))
      .then((array: Uint8Array) =>
        type == "tree"
          ? build.bazel.remote.execution.v2.Tree.decode(array).root
          : build.bazel.remote.execution.v2.Directory.decode(array)
      )
      .then((dir: build.bazel.remote.execution.v2.Directory | null | undefined) => {
        if (!dir) return [];

        const nodes = this.treeNodesForDirectory(dir);
        if (digestString) {
          this.state.treeShaToChildrenMap.set(digestString, nodes);
        }
        return nodes;
      });
    if (digestString) {
      this.treeShaToChildrenPromiseMap.set(digestString, fetchPromise);
      fetchPromise.finally(() => this.treeShaToChildrenPromiseMap.delete(digestString));
    }
    return fetchPromise;
  }

  private treeNodesForDirectory(dir: build.bazel.remote.execution.v2.Directory): TreeNode[] {
    return dir.directories
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
  }

  private autoExpandSingleChildDirs(nodes: TreeNode[]): Promise<void> {
    const dirNodes = nodes.filter((n) => n.type === "dir" || n.type === "tree");
    if (dirNodes.length === 1 && dirNodes.length === nodes.length) {
      return this.fetchAndExpandDir(dirNodes[0]).then((children) => this.autoExpandSingleChildDirs(children));
    }
    return Promise.resolve();
  }

  handleFileClicked(node: TreeNode) {
    if (!("digest" in node.obj)) return;
    if (!node.obj?.digest) return;

    let digestString = node.obj.digest.hash ?? "";
    if (this.state.treeShaToExpanded.get(digestString)) {
      this.state.treeShaToExpanded.set(digestString, false);
      this.forceUpdate();
      return;
    }
    if (node.type == "file") {
      let dirUrl = this.props.model.getBytestreamURL(node.obj.digest);
      rpcService.downloadBytestreamFile(node.obj.name, dirUrl, this.props.model.getInvocationId());
      return;
    }

    this.fetchAndExpandDir(node)
      .then((children) => this.autoExpandSingleChildDirs(children))
      .then(() => this.forceUpdate())
      .catch((e) => console.error(e));
  }

  /**
   * Looks for the given message type in auxiliary metadata and returns the
   * decoded message if found.
   */
  private getAuxiliaryMetadata<T>(messageClass: MessageClass<T>): T | null | undefined {
    for (const metadata of this.state.actionResult?.executionMetadata?.auxiliaryMetadata ?? []) {
      if (metadata.typeUrl === messageClass.getTypeUrl()) {
        return messageClass.decode(metadata.value);
      }
    }
    return null;
  }

  private getVMPreviousTaskHref(): string {
    const vmMetadata = this.getAuxiliaryMetadata(firecracker.VMMetadata);
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

  private onClickCopySnapshotKey(vmMetadata: firecracker.VMMetadata) {
    const snapshotKey = this.getSnapshotKeyForSnapshotID(vmMetadata);
    copyToClipboard(JSON.stringify(snapshotKey));
    alert_service.success("Snapshot key copied to clipboard");
    this.setState({ showSnapshotMenu: false });
  }

  private onClickCopyRemoteBazelCommand(
    vmMetadata: firecracker.VMMetadata,
    executionMetadata: build.bazel.remote.execution.v2.ExecutedActionMetadata
  ) {
    const snapshotKey = this.getSnapshotKeyForSnapshotID(vmMetadata);
    const snapshotKeyJSON = JSON.stringify(snapshotKey);
    const cmd = `bb remote --run_from_snapshot='${snapshotKeyJSON}' --runner_exec_properties=debug-executor-id=${executionMetadata.executorId} --script='echo "My custom bash command!"'`;
    copyToClipboard(cmd);
    alert_service.success("Command copied to clipboard");
    this.setState({ showSnapshotMenu: false });
  }

  // Rather than using the snapshot key from the VMMetadata, which refers to the
  // master key that can be overridden by future workflow runs, use a snapshot
  // key containing the snapshot ID, which will guarantee the key refers to the
  // specific snapshot saved by this invocation.
  private getSnapshotKeyForSnapshotID(vmMetadata: firecracker.VMMetadata): firecracker.SnapshotKey {
    return new firecracker.SnapshotKey({
      snapshotId: vmMetadata.snapshotId,
      instanceName: vmMetadata.snapshotKey?.instanceName,
    });
  }

  private renderOutputDirectories(actionsResult: build.bazel.remote.execution.v2.ActionResult) {
    return (
      <div className="action-section">
        <div className="action-property-title">Output directories</div>
        {actionsResult.outputDirectories.length ? (
          <div className="action-list">
            {actionsResult.outputDirectories.map((dir) => (
              <div className="action-output-entry" key={dir.path}>
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
                {this.renderOutputConsumerSection(dir.path)}
              </div>
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
              <div className="action-output-entry" key={symlink.path}>
                <div className="tree-node-symlink">
                  <span>
                    <FileSymlink className="symlink-icon" />
                  </span>{" "}
                  <span>{symlink.path}</span>{" "}
                  <span>
                    <ArrowRight className="arrow-right-icon" />
                  </span>{" "}
                  <span>{symlink.target}</span>
                </div>
                {this.renderOutputConsumerSection(symlink.path)}
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
              <FileQuestion className="file-question-icon" />
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
              <Folder className="folder-icon" />
            </span>
            <span className="expected-output-label">{expectedDir}</span>
          </div>
        ))}
        {command.outputFiles.map((expectedFile) => (
          <div className="expected-output">
            <span>
              <File className="file-icon" />
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
              <FileQuestion className="file-question-icon red" />
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
              <Folder className="file-question-icon red" />
            </span>
            <span className="missing-output-label">{missingDir}</span>
          </div>
        ))}
        {missingFiles.map((missingFile) => (
          <div className="missing-output">
            <span>
              <FileQuestion className="file-question-icon red" />
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
      return !!missingOutputs.length && renderOutline(renderMissingOutputPaths(missingOutputs));
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

  private renderSpawnResourceUsage() {
    if (this.getExecutionId()) return null;

    const measuredMemoryPeakBytes = this.state.measuredMemoryPeakBytes;
    if (!measuredMemoryPeakBytes || measuredMemoryPeakBytes <= 0) return null;

    return (
      <>
        <div className="metadata-title">Resource usage</div>
        <div>
          <div>Peak memory: {format.bytesIEC(measuredMemoryPeakBytes)}</div>
        </div>
      </>
    );
  }

  private renderUsageStats(usageStats: build.bazel.remote.execution.v2.UsageStats) {
    return (
      <>
        <div className="metadata-title">Resource usage</div>
        <div>
          <div>Peak memory: {format.bytesIEC(usageStats.peakMemoryBytes)}</div>
          <div>MilliCPU: {computeMilliCpu(this.state.actionResult!)}</div>
          {usageStats.peakFileSystemUsage?.map((fs) => (
            <div>
              Peak disk usage: {fs.target} ({fs.fstype}): {format.bytesIEC(fs.usedBytes)} of{" "}
              {format.bytesIEC(fs.totalBytes)}
            </div>
          ))}
          {usageStats.cgroupIoStats && (
            <>
              <div>Disk bytes read: {format.bytesIEC(usageStats.cgroupIoStats.rbytes)}</div>
              <div>Disk read operations: {format.count(usageStats.cgroupIoStats.rios)}</div>
              <div>Disk bytes written: {format.bytesIEC(usageStats.cgroupIoStats.wbytes)}</div>
              <div>Disk write operations: {format.count(usageStats.cgroupIoStats.wios)}</div>
            </>
          )}
          {usageStats.networkStats && (
            <>
              <div>Network bytes received: {format.bytesIEC(usageStats.networkStats.bytesReceived)}</div>
              <div>Network packets received: {format.count(usageStats.networkStats.packetsReceived)}</div>
              <div>Network bytes sent: {format.bytesIEC(usageStats.networkStats.bytesSent)}</div>
              <div>Network packets sent: {format.count(usageStats.networkStats.packetsSent)}</div>
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

  private getPlatformOverrides(): Map<string, string> {
    const overrides = new Map<string, string>();
    const executionAuxiliaryMetadata = this.getAuxiliaryMetadata(execution_stats.ExecutionAuxiliaryMetadata);
    for (const prop of executionAuxiliaryMetadata?.platformOverrides?.properties ?? []) {
      let value = prop.value ?? "";
      // TODO: this redaction is also done on the server and can be removed
      // after some time.
      const nameLower = prop.name.toLowerCase();
      if (nameLower.includes("username") || nameLower.includes("password") || nameLower.includes("env-overrides")) {
        value = "<REDACTED>";
      }
      overrides.set(prop.name, value);
    }
    return overrides;
  }

  render() {
    const digest = parseActionDigest(this.props.search.get("actionDigest") ?? "");
    if (!digest) return <></>;
    const vmMetadata = this.getAuxiliaryMetadata(firecracker.VMMetadata);
    const executionId = this.getExecutionId();
    const platformOverrides = this.getPlatformOverrides();
    const spawnResourceUsage = this.renderSpawnResourceUsage();

    return (
      <div className="invocation-action-card">
        {this.state.loadingAction && (
          <div className="card">
            <div className="loading" />
          </div>
        )}
        {!this.state.loadingAction && (
          <div className="card">
            <Info className="purple" />
            <div className="content">
              {executionId && (
                <>
                  <div className="action-header">
                    <div className="title">Execution details</div>
                    {this.state.execution?.targetLabel && this.state.execution?.actionMnemonic && (
                      <OutlinedButton
                        className="view-history-button"
                        onClick={() =>
                          router.navigateTo(
                            getDrilldownUrl(this.state.execution?.targetLabel, this.state.execution?.actionMnemonic)
                          )
                        }>
                        <History />
                        <span>View history</span>
                      </OutlinedButton>
                    )}
                  </div>
                  <div className="details">
                    {this.state.execution?.targetLabel && (
                      <div className="action-section">
                        <div className="action-property-title">Target label</div>
                        <div debug-id="target-label">
                          <TextLink
                            className="target-label-link"
                            href={`/invocation/${this.props.model.getInvocationId()}?${new URLSearchParams({
                              target: this.state.execution.targetLabel,
                            })}`}>
                            {this.state.execution.targetLabel}
                          </TextLink>
                        </div>
                      </div>
                    )}
                    {this.state.execution?.actionMnemonic && (
                      <div className="action-section">
                        <div className="action-property-title">Action mnemonic</div>
                        <div>{this.state.execution?.actionMnemonic}</div>
                      </div>
                    )}
                    <div className="action-section">
                      <div className="action-property-title">Execution ID</div>
                      <div debug-id="execution-id">{executionId}</div>
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
                        {(this.state.execution?.invocationLinkType ??
                          stored_invocation.StoredInvocationLink.Type.UNKNOWN_TYPE) !==
                          stored_invocation.StoredInvocationLink.Type.UNKNOWN_TYPE && (
                          <div className="action-section">
                            <div className="action-property-title">Merged</div>
                            <div className="value-with-help-tooltip">
                              {this.state.execution?.invocationLinkType ===
                              stored_invocation.StoredInvocationLink.Type.MERGED
                                ? "Yes"
                                : "No"}
                              <HelpTooltip>
                                <p>
                                  If merged, this execution reused an in-flight execution attempt of the same action,
                                  triggered by an earlier invocation. The details shown on this page are from the
                                  original execution attempt.
                                </p>
                              </HelpTooltip>
                            </div>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                </>
              )}
              <div className="action-header">
                <div className="action-title">Action details</div>
                {digest && (
                  <ActionCompareButtonComponent
                    invocationId={this.props.model.getInvocationId()}
                    actionDigest={digestToString(digest)}
                  />
                )}
              </div>
              {this.state.action ? (
                <div className="details">
                  <div>
                    {this.renderCompactLogActionIdentity()}
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
                      <div>
                        {this.state.inputNodes.length ? (
                          <div className="input-tree">
                            {this.state.inputNodes.map((node) => (
                              <TreeNodeComponent
                                node={node}
                                treeShaToExpanded={this.state.treeShaToExpanded}
                                treeShaToChildrenMap={this.state.treeShaToChildrenMap}
                                treeShaToTotalSizeMap={this.state.treeShaToTotalSizeMap}
                                handleFileClicked={this.handleFileClicked.bind(this)}
                                getFileViewUrl={this.getFileViewUrl.bind(this)}
                              />
                            ))}
                          </div>
                        ) : (
                          <div>None found</div>
                        )}
                      </div>
                    </div>
                    {this.renderInputSourcesSection()}
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
                    <div className="action-header">
                      <div className="action-title">Command details</div>
                      {this.state.command && (
                        <OutlinedButton className="copy-bb-execute-button" onClick={this.onClickCopyBbExecute}>
                          <Copy className="copy-icon" />
                          Copy as bb-execute
                        </OutlinedButton>
                      )}
                    </div>
                    {this.state.command ? (
                      <div>
                        <div className="action-section">
                          <div className="action-property-title">Arguments</div>
                          {this.renderArguments(this.state.command.arguments)}
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
                                <div
                                  className={
                                    platformOverrides.has(property?.name ?? "") ? "platform-property-overridden" : ""
                                  }>
                                  <span className="prop-name">{property.name}</span>
                                  <span className="prop-value">={property.value}</span>
                                  {platformOverrides.has(property?.name ?? "") && <span> (overridden)</span>}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div>None</div>
                          )}
                        </div>
                        {platformOverrides.size > 0 && (
                          <div className="action-section">
                            <div className="action-property-title">Platform overrides</div>
                            <div className="action-list">
                              {[...platformOverrides.entries()].map(([name, value]) => (
                                <div>
                                  <span className="prop-name">{name}</span>
                                  <span className="prop-value">={value}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
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
                            <div className="metadata-detail metadata-detail-inline-action">
                              <span>{this.state.actionResult.executionMetadata.worker}</span>
                              {this.state.actionResult.executionMetadata.worker && (
                                <TextLink
                                  className="artifact-view metadata-history-link"
                                  href={getExecutorDrilldownUrl(this.state.actionResult.executionMetadata.worker)}>
                                  <History /> History
                                </TextLink>
                              )}
                            </div>
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
                                  <div className="snapshot-id-container">
                                    <div className="snapshot-id-details">
                                      {vmMetadata.savedLocalSnapshot || vmMetadata.savedRemoteSnapshot ? (
                                        <>
                                          <div className="metadata-title">Saved to snapshot ID</div>
                                          <div className="metadata-detail">{vmMetadata.snapshotId}</div>
                                        </>
                                      ) : (
                                        <div className="metadata-title">No snapshot saved for this run</div>
                                      )}
                                    </div>
                                    <div>
                                      {vmMetadata.snapshotKey &&
                                        (vmMetadata.savedLocalSnapshot || vmMetadata.savedRemoteSnapshot) && (
                                          <div className="invocation-menu-container">
                                            <a
                                              className="invalidate-button"
                                              onClick={() => this.setState({ showInvalidateSnapshotModal: true })}>
                                              Invalidate VM snapshot
                                            </a>
                                            <OutlinedButton
                                              title="Snapshot options"
                                              className="snapshot-more-button"
                                              onClick={() => this.setState({ showSnapshotMenu: true })}>
                                              <MoreVertical />
                                            </OutlinedButton>
                                            <Popup
                                              isOpen={this.state.showSnapshotMenu}
                                              onRequestClose={() => this.setState({ showSnapshotMenu: false })}>
                                              <Menu className="workflow-dropdown-menu">
                                                <MenuItem onClick={this.onClickCopySnapshotKey.bind(this, vmMetadata)}>
                                                  Copy snapshot key
                                                </MenuItem>
                                                <MenuItem
                                                  onClick={this.onClickCopyRemoteBazelCommand.bind(
                                                    this,
                                                    vmMetadata,
                                                    this.state.actionResult.executionMetadata
                                                  )}>
                                                  Copy Remote Bazel command to run commands in snapshot
                                                </MenuItem>
                                              </Menu>
                                            </Popup>
                                            <Modal
                                              isOpen={this.state.showInvalidateSnapshotModal}
                                              onRequestClose={() =>
                                                this.setState({
                                                  showInvalidateSnapshotModal: false,
                                                  isMenuOpen: false,
                                                })
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
                                  </div>
                                )}
                              </>
                            )}
                            {this.state.actionResult.executionMetadata.estimatedTaskSize && (
                              <>
                                <div className="metadata-title">Estimated resource usage</div>
                                <div>
                                  <div>
                                    Peak memory:{" "}
                                    {format.bytesIEC(
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
                            {this.state.actionResult.executionMetadata.usageStats
                              ? this.renderUsageStats(this.state.actionResult.executionMetadata.usageStats)
                              : spawnResourceUsage}
                            {this.renderExecutionDownloads()}
                            {this.state.actionResult.executionMetadata &&
                              this.renderTiming(this.state.actionResult.executionMetadata)}
                          </div>
                        ) : (
                          (spawnResourceUsage && <div className="action-list">{spawnResourceUsage}</div>) || (
                            <div>None found</div>
                          )
                        )}
                      </div>
                      <div className="action-section">
                        <div className="action-property-title">Output files</div>
                        <div>
                          {this.state.actionResult.outputFiles ? (
                            <div className="action-list">
                              {this.state.actionResult.outputFiles.map((file) => (
                                <div className="action-output-entry" key={file.path}>
                                  <div
                                    className="file-name clickable"
                                    onClick={this.handleOutputFileClicked.bind(this, file)}>
                                    <span>
                                      <Download className="file-icon" />
                                    </span>
                                    <span className="prop-link" title={file.path}>
                                      {file.path}
                                    </span>
                                    {file.digest && (
                                      <TextLink
                                        className="artifact-view"
                                        href={this.getFileViewUrl(file.path, file.digest)}
                                        // Otherwise the file will be downloaded instead
                                        onClick={(e) => e.stopPropagation()}
                                        target="_blank">
                                        <FileIcon extension={file.path} /> View
                                      </TextLink>
                                    )}
                                    {file.isExecutable && <span className="detail"> (executable)</span>}
                                    {file.digest && <DigestComponent digest={file.digest} />}
                                  </div>
                                  {this.renderOutputConsumerSection(file.path)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div>None found</div>
                          )}
                        </div>
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
                    <>
                      {spawnResourceUsage && (
                        <div className="action-section">
                          <div className="action-property-title">Execution metadata</div>
                          <div className="action-list">{spawnResourceUsage}</div>
                        </div>
                      )}
                      {!this.state.executeResponse && <div>{this.renderNotFoundDetails({ result: true })}</div>}
                    </>
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

function getArgumentInputFilePathCandidates(argument: string): string[] {
  const candidates: string[] = [];
  const assignmentValue = getAssignmentValue(argument);
  if (assignmentValue) {
    candidates.push(...getNormalizedArgfilePathCandidates(assignmentValue), assignmentValue);
  }
  if (!argument.startsWith("-")) {
    candidates.push(...getNormalizedArgfilePathCandidates(argument), argument);
  }
  return normalizeInputFilePathCandidates(candidates);
}

function getArgumentParamFilePath(argument: string): string | undefined {
  const assignmentValue = getAssignmentValue(argument);
  const paramFileArgument = assignmentValue?.startsWith("@") ? assignmentValue : argument;
  return getNormalizedArgfilePathCandidates(paramFileArgument)[0];
}

// --include-something=path/to/file
function getAssignmentValue(argument: string): string | undefined {
  const assignmentIndex = argument.indexOf("=");
  if (assignmentIndex <= 0 || assignmentIndex === argument.length - 1) return undefined;
  return argument.substring(assignmentIndex + 1);
}

// @path/to/foo.params
function getArgfilePathCandidates(argument: string): string[] {
  if (!argument.startsWith("@") || argument.length <= 1) return [];
  return [argument.substring(1)];
}

function getNormalizedArgfilePathCandidates(argument: string): string[] {
  return normalizeInputFilePathCandidates(getArgfilePathCandidates(argument));
}

function normalizeInputFilePathCandidates(candidates: string[]): string[] {
  return [...new Set(candidates.map(normalizeInputFilePathCandidate).filter((path): path is string => !!path))];
}

function normalizeInputFilePathCandidate(path: string): string | undefined {
  if (isAbsolutePathCandidate(path)) return undefined;

  path = path.replace(/\\/g, "/").replace(/^(\.\/)+/, "");
  const segments = path.split("/");
  if (
    !path ||
    path.startsWith("@") ||
    path.includes("\0") ||
    segments.some((segment) => segment === "" || segment === "." || segment === "..")
  ) {
    return undefined;
  }
  return path;
}

function isAbsolutePathCandidate(path: string): boolean {
  return path.startsWith("/") || path.startsWith("\\") || /^[A-Za-z]:/.test(path);
}

function getPathBasename(path: string): string {
  return path.split("/").pop() || path;
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

function parseActionDigestHashFromExecutionId(executionId: string): string | undefined {
  const parts = executionId.split("/");
  return parts[parts.length - 2];
}

function getDrilldownUrl(targetLabel?: string, actionMnemonic?: string): string {
  if (!targetLabel || !actionMnemonic) {
    return "";
  }
  const dimensionParam = `${encodeTargetLabelUrlParam(targetLabel)}|${encodeActionMnemonicUrlParam(actionMnemonic)}`;
  return `/trends/?d=${encodeURIComponent(dimensionParam)}&ddMetric=e4#drilldown`;
}

function getExecutorDrilldownUrl(executorHostId?: string): string {
  if (!executorHostId) {
    return "";
  }
  return `/trends/?d=${encodeURIComponent(encodeWorkerUrlParam(executorHostId))}&ddMetric=e9#drilldown`;
}

export function encodeWorkerUrlParam(workerId: string): string {
  return `e1|${workerId.length}|${workerId}`;
}

export function encodeTargetLabelUrlParam(targetLabel: string): string {
  return `e2|${targetLabel.length}|${targetLabel}`;
}

export function encodeActionMnemonicUrlParam(actionMnemonic: string): string {
  return `e3|${actionMnemonic.length}|${actionMnemonic}`;
}
