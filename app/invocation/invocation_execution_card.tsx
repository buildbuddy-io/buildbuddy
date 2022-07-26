import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import router from "../router/router";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import Select, { Option } from "../components/select/select";
import { build } from "../../proto/remote_execution_ts_proto";
import { google } from "../../proto/grpc_code_ts_proto";
import rpcService from "../service/rpc_service";
import { RotateCw, Package, Clock, AlertCircle, XCircle, CheckCircle } from "lucide-react";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  executions: execution_stats.IExecution[];
  sort: string;
  direction: "asc" | "desc";
  statusFilter: string;
}

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage;

type ExecutionStatus = {
  name: string;
  icon: JSX.Element;
  className?: string;
};

const STATUSES_BY_STAGE: Record<number, ExecutionStatus> = {
  [ExecutionStage.Value.UNKNOWN]: { name: "Starting", icon: <RotateCw className="icon blue rotating" /> },
  [ExecutionStage.Value.CACHE_CHECK]: { name: "Cache check", icon: <Package className="icon brown" /> },
  [ExecutionStage.Value.QUEUED]: { name: "Queued", icon: <Clock className="icon" /> },
  [ExecutionStage.Value.EXECUTING]: { name: "Executing", icon: <RotateCw className="icon blue rotating" /> },
  // COMPLETED is not included here because it depends on the gRPC status and exit code.
};

const GRPC_STATUS_LABEL_BY_CODE: Record<number, string> = Object.fromEntries(
  Object.entries(google.rpc.Code).map(([name, value]) => [value, name])
);

function getExecutionStatus(execution: execution_stats.IExecution): ExecutionStatus {
  if (execution.stage === ExecutionStage.Value.COMPLETED) {
    if (execution.status.code !== 0) {
      return {
        name: `Error (${GRPC_STATUS_LABEL_BY_CODE[execution.status.code] || "UNKNOWN"})`,
        icon: <AlertCircle className="icon red" />,
      };
    }
    if (execution.exitCode !== 0) {
      return {
        name: `Failed (exit code ${execution.exitCode})`,
        icon: <XCircle className="icon red" />,
      };
    }
    return { name: "Succeeded", icon: <CheckCircle className="icon green" /> };
  }

  return STATUSES_BY_STAGE[execution.stage];
}

export default class ExecutionCardComponent extends React.Component<Props, State> {
  state: State = {
    executions: [],
    loading: true,
    sort: "status",
    direction: "desc",
    statusFilter: "all",
  };

  timeoutRef: number;

  componentDidMount() {
    this.fetchExecution();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.model !== prevProps.model) {
      this.fetchExecution();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  fetchExecution() {
    let request = new execution_stats.GetExecutionRequest();
    request.executionLookup = new execution_stats.ExecutionLookup();
    request.executionLookup.invocationId = this.props.model.getId();
    let inProgressBeforeRequestWasMade = this.props.inProgress;
    rpcService.service.getExecution(request).then((response) => {
      this.setState({ executions: response.execution, loading: false });

      if (inProgressBeforeRequestWasMade) {
        this.fetchUpdatedProgress();
      }

      console.log(response);
    });
  }

  fetchUpdatedProgress() {
    clearTimeout(this.timeoutRef);

    // Refetch execution data in 3 seconds to update status.
    this.timeoutRef = setTimeout(() => {
      this.fetchExecution();
    }, 3000);
  }

  subtractTimestamp(
    timestampA: { nanos?: number | Long; seconds?: number | Long },
    timestampB: { nanos?: number | Long; seconds?: number | Long }
  ) {
    let microsA = +timestampA.seconds * 1000000 + +timestampA.nanos / 1000;
    let microsB = +timestampB.seconds * 1000000 + +timestampB.nanos / 1000;
    return microsA - microsB;
  }

  totalDuration(execution: execution_stats.IExecution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.workerCompletedTimestamp,
      execution?.executedActionMetadata?.queuedTimestamp
    );
  }

  queuedDuration(execution: execution_stats.IExecution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.workerStartTimestamp,
      execution?.executedActionMetadata?.queuedTimestamp
    );
  }

  downloadDuration(execution: execution_stats.IExecution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.inputFetchCompletedTimestamp,
      execution?.executedActionMetadata?.inputFetchStartTimestamp
    );
  }

  executionDuration(execution: execution_stats.IExecution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.executionCompletedTimestamp,
      execution?.executedActionMetadata?.executionStartTimestamp
    );
  }

  uploadDuration(execution: execution_stats.IExecution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.outputUploadCompletedTimestamp,
      execution?.executedActionMetadata?.outputUploadStartTimestamp
    );
  }

  sort(a: execution_stats.IExecution, b: execution_stats.IExecution) {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "total-duration":
        return this.totalDuration(first) - this.totalDuration(second);
      case "queued-duration":
        return this.queuedDuration(first) - this.queuedDuration(second);
      case "download-duration":
        return this.downloadDuration(first) - this.downloadDuration(second);
      case "execution-duration":
        return this.executionDuration(first) - this.executionDuration(second);
      case "upload-duration":
        return this.uploadDuration(first) - this.uploadDuration(second);
      case "files-downloaded":
        return (
          +first?.executedActionMetadata?.ioStats?.fileDownloadCount -
          +second?.executedActionMetadata?.ioStats?.fileDownloadCount
        );
      case "files-uploaded":
        return (
          +first?.executedActionMetadata?.ioStats?.fileUploadCount -
          +second?.executedActionMetadata?.ioStats?.fileUploadCount
        );
      case "file-size-downloaded":
        return (
          +first?.executedActionMetadata?.ioStats?.fileDownloadSizeBytes -
          +second?.executedActionMetadata?.ioStats?.fileDownloadSizeBytes
        );
      case "file-size-uploaded":
        return (
          +first?.executedActionMetadata?.ioStats?.fileUploadSizeBytes -
          +second?.executedActionMetadata?.ioStats?.fileUploadSizeBytes
        );
      case "queue-start":
        return this.subtractTimestamp(
          first?.executedActionMetadata?.queuedTimestamp,
          second?.executedActionMetadata?.queuedTimestamp
        );
      case "execution-start":
        return this.subtractTimestamp(
          first?.executedActionMetadata?.workerStartTimestamp,
          second?.executedActionMetadata?.workerStartTimestamp
        );
      case "execution-end":
        return this.subtractTimestamp(
          first?.executedActionMetadata?.workerCompletedTimestamp,
          second?.executedActionMetadata?.workerCompletedTimestamp
        );
      case "worker":
        return second?.executedActionMetadata?.worker.localeCompare(first?.executedActionMetadata?.worker);
      case "command":
        return second?.commandSnippet.localeCompare(first?.commandSnippet);
      case "action":
        return second?.actionDigest?.hash.localeCompare(first?.actionDigest?.hash);
      default:
        // Within COMPLETED actions, sort first by gRPC code (OK, DEADLINE_EXCEEDED, etc.)
        // then by exit code.
        if (first.stage === ExecutionStage.Value.COMPLETED && first.stage === second.stage) {
          if (second.status.code !== first.status.code) {
            return second.status.code - first.status.code;
          }
          return second.exitCode - first.exitCode;
        }
        return second.stage - first.stage;
    }
  }

  handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.value,
    } as Record<keyof State, any>);
  }

  handleSortChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleStatusFilterChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({
      statusFilter: event.target.value,
    });
  }

  getActionPageLink(execution: execution_stats.IExecution) {
    const search = new URLSearchParams({
      actionDigest: `${execution.actionDigest.hash}/${execution.actionDigest.sizeBytes}`,
    });
    if (execution.actionResultDigest) {
      search.set(
        "actionResultDigest",
        `${execution.actionResultDigest.hash}/${execution.actionResultDigest.sizeBytes}`
      );
    }
    return `/invocation/${this.props.model.getId()}?${search}#action`;
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.executions.length) {
      return (
        <div className="invocation-execution-empty-state">
          No actions remotely executed by BuildBuddy RBE for this invocation{this.props.inProgress && <span> yet</span>}
          .
        </div>
      );
    }

    let completedCount = 0;
    let incompleteCount = 0;
    for (let execution of this.state.executions) {
      if (execution.stage === ExecutionStage.Value.COMPLETED) {
        completedCount++;
      } else {
        incompleteCount++;
      }
    }

    const filteredActions = this.state.executions
      .filter(
        (action) =>
          !this.props.filter ||
          `${action.actionDigest.hash}/${action.actionDigest.sizeBytes}`
            .toLowerCase()
            .includes(this.props.filter.toLowerCase()) ||
          action.commandSnippet.toLowerCase().includes(this.props.filter.toLowerCase())
      )
      .filter(
        (action) =>
          this.state.statusFilter === "all" ||
          getExecutionStatus(action).name.toLowerCase().startsWith(this.state.statusFilter)
      );

    return (
      <div>
        <div className={`card expanded`}>
          <div className="content">
            <div className="invocation-content-header">
              <div className="title">
                Remotely executed actions ({!!incompleteCount && <span>{incompleteCount} in progress, </span>}
                {completedCount} completed)
              </div>

              <div className="invocation-sort-controls">
                <span className="invocation-filter-title">Show</span>
                <Select onChange={this.handleStatusFilterChange.bind(this)} value={this.state.statusFilter}>
                  <Option value="all">All</Option>
                  <Option value="starting">Starting</Option>
                  <Option value="cache check">Cache Check</Option>
                  <Option value="queued">Queued</Option>
                  <Option value="executing">Executing</Option>
                  <Option value="succeeded">Succeeded</Option>
                  <Option value="failed">Failed</Option>
                  <Option value="error">Errored</Option>
                </Select>
                <span className="invocation-sort-title">Sort by</span>
                <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                  <Option value="total-duration">Total Duration</Option>
                  <Option value="queued-duration">Queued Duration</Option>
                  <Option value="download-duration">Download Duration</Option>
                  <Option value="execution-duration">Execution Duration</Option>
                  <Option value="upload-duration">Upload Duration</Option>
                  <Option value="files-downloaded">Files Downloaded</Option>
                  <Option value="files-uploaded">Files Uploaded</Option>
                  <Option value="file-size-downloaded">File Size Downloaded</Option>
                  <Option value="file-size-uploaded">File Size Uploaded</Option>
                  <Option value="queue-start">Queue Start Time</Option>
                  <Option value="execution-start">Execution Start Time</Option>
                  <Option value="execution-end">Execution End Time</Option>
                  <Option value="worker">Worker</Option>
                  <Option value="command">Command Snippet</Option>
                  <Option value="action">Action Digest</Option>
                  <Option value="status">Status</Option>
                </Select>
                <span className="group-container">
                  <div>
                    <input
                      id="direction-asc"
                      checked={this.state.direction == "asc"}
                      onChange={this.handleInputChange.bind(this)}
                      value="asc"
                      name="direction"
                      type="radio"
                    />
                    <label htmlFor="direction-asc">Asc</label>
                  </div>
                  <div>
                    <input
                      id="direction-desc"
                      checked={this.state.direction == "desc"}
                      onChange={this.handleInputChange.bind(this)}
                      value="desc"
                      name="direction"
                      type="radio"
                    />
                    <label htmlFor="direction-desc">Desc</label>
                  </div>
                </span>
              </div>
            </div>
            <div>
              {filteredActions.length ? (
                <div className="invocation-execution-table">
                  {filteredActions.sort(this.sort.bind(this)).map((execution, index) => {
                    const status = getExecutionStatus(execution);
                    return (
                      <Link key={index} className="invocation-execution-row" href={this.getActionPageLink(execution)}>
                        <div className="invocation-execution-row-image">{status.icon}</div>
                        <div>
                          <div className="invocation-execution-row-header">
                            <span className="invocation-execution-row-header-status">{status.name}</span>
                            <DigestComponent digest={execution?.actionDigest} expanded={true} />
                          </div>
                          <div>{execution.commandSnippet}</div>
                          <div className="invocation-execution-row-stats">
                            <div>Executor Host ID: {execution?.executedActionMetadata?.worker}</div>
                            <div>Total duration: {format.durationUsec(this.totalDuration(execution))}</div>
                            <div>Queued duration: {format.durationUsec(this.queuedDuration(execution))}</div>
                            <div>
                              File download duration: {format.durationUsec(this.downloadDuration(execution))} (
                              {format.bytes(execution?.executedActionMetadata?.ioStats?.fileDownloadSizeBytes)} across{" "}
                              {execution?.executedActionMetadata?.ioStats?.fileDownloadCount} files)
                            </div>
                            <div>Execution duration: {format.durationUsec(this.executionDuration(execution))}</div>
                            <div>
                              File upload duration: {format.durationUsec(this.uploadDuration(execution))} (
                              {format.bytes(execution?.executedActionMetadata?.ioStats?.fileUploadSizeBytes)} across{" "}
                              {execution?.executedActionMetadata?.ioStats?.fileUploadCount} files)
                            </div>
                          </div>
                        </div>
                      </Link>
                    );
                  })}
                </div>
              ) : (
                <div className="invocation-execution-empty-actions">No matching actions.</div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
