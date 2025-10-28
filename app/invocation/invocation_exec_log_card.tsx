import React from "react";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { OutlinedButton } from "../components/button/button";
import Select, { Option } from "../components/select/select";
import errorService from "../errors/error_service";
import format from "../format/format";
import rpcService from "../service/rpc_service";
import InvocationExecutionTable from "./invocation_execution_table";
import {
  downloadDuration,
  executionDuration,
  getExecutionStatus,
  queuedDuration,
  subtractTimestamp,
  totalDuration,
  uploadDuration,
} from "./invocation_execution_util";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  search: URLSearchParams;
  filter: string;
}

interface State {
  loading: boolean;
  executions: execution_stats.Execution[];
  sort: string;
  direction: "asc" | "desc";
  statusFilter: string;
  limit: number;
}

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage;

export default class SpawnCardComponent extends React.Component<Props, State> {
  state: State = {
    executions: [],
    loading: true,
    sort: "status",
    direction: "desc",
    statusFilter: "all",
    limit: 100,
  };

  timeoutRef?: number;

  componentDidMount() {
    this.fetchExecution();
  }

  componentDidUpdate(prevProps: Props) {
    const invocationIdChanged = this.props.model.getInvocationId() !== prevProps.model.getInvocationId();
    const invocationStatusChanged =
      this.props.model.invocation.invocationStatus !== prevProps.model.invocation.invocationStatus;

    if (invocationIdChanged || invocationStatusChanged) {
      clearTimeout(this.timeoutRef);
      this.timeoutRef = undefined;
      this.fetchExecution();
    }
  }

  componentWillUnmount() {
    clearTimeout(this.timeoutRef);
  }

  fetchExecution() {
    let request = new execution_stats.GetExecutionRequest();
    request.executionLookup = new execution_stats.ExecutionLookup();
    request.executionLookup.invocationId = this.props.model.getInvocationId();
    let inProgressBeforeRequestWasMade = this.props.model.isInProgress();
    rpcService.service
      .getExecution(request)
      .then((response) => {
        this.setState({ executions: response.execution, loading: false });

        if (inProgressBeforeRequestWasMade) {
          this.fetchUpdatedProgress();
        }

        console.log(response);
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  fetchUpdatedProgress() {
    clearTimeout(this.timeoutRef);

    // Refetch execution data in 3 seconds to update status.
    this.timeoutRef = window.setTimeout(() => {
      this.fetchExecution();
    }, 3000);
  }

  sort(a: execution_stats.Execution, b: execution_stats.Execution): number {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    switch (this.state.sort) {
      case "total-duration":
        return totalDuration(first) - totalDuration(second);
      case "queued-duration":
        return queuedDuration(first) - queuedDuration(second);
      case "download-duration":
        return downloadDuration(first) - downloadDuration(second);
      case "execution-duration":
        return executionDuration(first) - executionDuration(second);
      case "upload-duration":
        return uploadDuration(first) - uploadDuration(second);
      case "files-downloaded":
        return (
          +(first.executedActionMetadata?.ioStats?.fileDownloadCount ?? 0) -
          +(second.executedActionMetadata?.ioStats?.fileDownloadCount ?? 0)
        );
      case "files-uploaded":
        return (
          +(first.executedActionMetadata?.ioStats?.fileUploadCount ?? 0) -
          +(second.executedActionMetadata?.ioStats?.fileUploadCount ?? 0)
        );
      case "file-size-downloaded":
        return (
          +(first.executedActionMetadata?.ioStats?.fileDownloadSizeBytes ?? 0) -
          +(second.executedActionMetadata?.ioStats?.fileDownloadSizeBytes ?? 0)
        );
      case "file-size-uploaded":
        return (
          +(first.executedActionMetadata?.ioStats?.fileUploadSizeBytes ?? 0) -
          +(second.executedActionMetadata?.ioStats?.fileUploadSizeBytes ?? 0)
        );
      case "queue-start":
        return subtractTimestamp(
          first.executedActionMetadata?.queuedTimestamp,
          second.executedActionMetadata?.queuedTimestamp
        );
      case "execution-start":
        return subtractTimestamp(
          first.executedActionMetadata?.workerStartTimestamp,
          second.executedActionMetadata?.workerStartTimestamp
        );
      case "execution-end":
        return subtractTimestamp(
          first.executedActionMetadata?.workerCompletedTimestamp,
          second.executedActionMetadata?.workerCompletedTimestamp
        );
      case "worker":
        return second.executedActionMetadata?.worker.localeCompare(first.executedActionMetadata?.worker ?? "") ?? NaN;
      case "command":
        return second.commandSnippet.localeCompare(first.commandSnippet);
      case "action":
        return second?.actionDigest?.hash.localeCompare(first?.actionDigest?.hash ?? "") ?? NaN;
      default:
        // Within COMPLETED actions, sort first by gRPC code (OK, DEADLINE_EXCEEDED, etc.)
        // then by exit code.
        if (first.stage === ExecutionStage.Value.COMPLETED && first.stage === second.stage) {
          if (second.status?.code !== first.status?.code) {
            return (second.status?.code ?? NaN) - (first.status?.code ?? NaN);
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

  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleStatusFilterChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({
      statusFilter: event.target.value,
    });
  }

  handleMoreClicked() {
    this.setState({ limit: this.state.limit + 100 });
  }

  handleAllClicked() {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
  }

  render() {
    if (this.state.loading) {
      return <div className="loading loading-slim invocation-tab-loading" />;
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

    const filter = this.props.filter?.toLowerCase();
    const filteredExecutions = this.state.executions
      .filter(
        (execution) =>
          !filter ||
          execution.targetLabel?.toLowerCase()?.includes(filter) ||
          execution.actionMnemonic?.toLowerCase()?.includes(filter) ||
          execution.commandSnippet?.toLowerCase()?.includes(filter) ||
          execution.primaryOutputPath?.toLowerCase()?.includes(filter) ||
          `${execution.actionDigest?.hash ?? ""}/${execution.actionDigest?.sizeBytes ?? ""}`
            .toLowerCase()
            .includes(filter)
      )
      .filter(
        (execution) =>
          this.state.statusFilter === "all" ||
          getExecutionStatus(execution).name.toLowerCase().startsWith(this.state.statusFilter)
      );

    return (
      <div>
        <div className={`card expanded`}>
          <div className="content">
            <div className="invocation-content-header">
              <div className="title">
                Remotely executed actions (
                {!!incompleteCount && `${format.formatWithCommas(incompleteCount)} in progress, `}
                {format.formatWithCommas(completedCount)} completed)
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
              {filteredExecutions.length ? (
                <InvocationExecutionTable
                  executions={filteredExecutions.sort(this.sort.bind(this)).slice(0, this.state.limit)}
                  invocationIdProvider={() => this.props.model.getInvocationId()}></InvocationExecutionTable>
              ) : (
                <div className="invocation-execution-empty-actions">No matching actions.</div>
              )}
            </div>
            {filteredExecutions.length > this.state.limit && (
              <div className="more-buttons">
                <OutlinedButton onClick={this.handleMoreClicked.bind(this)}>See more executions</OutlinedButton>
                <OutlinedButton onClick={this.handleAllClicked.bind(this)}>See all executions</OutlinedButton>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }
}
