import React from "react";
import InvocationModel from "./invocation_model";
import InvocationExecutionTable from "./invocation_execution_table";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import Select, { Option } from "../components/select/select";
import { build } from "../../proto/remote_execution_ts_proto";
import rpcService from "../service/rpc_service";
import { OutlinedButton } from "../components/button/button";
import {
  downloadDuration,
  executionDuration,
  getExecutionStatus,
  queuedDuration,
  subtractTimestamp,
  totalDuration,
  uploadDuration,
} from "./invocation_execution_util";

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
  limit: number;
}

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage;

export default class ExecutionCardComponent extends React.Component<Props, State> {
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
    this.timeoutRef = window.setTimeout(() => {
      this.fetchExecution();
    }, 3000);
  }

  sort(a: execution_stats.IExecution, b: execution_stats.IExecution) {
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
        return subtractTimestamp(
          first?.executedActionMetadata?.queuedTimestamp,
          second?.executedActionMetadata?.queuedTimestamp
        );
      case "execution-start":
        return subtractTimestamp(
          first?.executedActionMetadata?.workerStartTimestamp,
          second?.executedActionMetadata?.workerStartTimestamp
        );
      case "execution-end":
        return subtractTimestamp(
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

  handleMoreClicked() {
    this.setState({ limit: this.state.limit + 100 });
  }

  handleAllClicked() {
    this.setState({ limit: Number.MAX_SAFE_INTEGER });
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
                <InvocationExecutionTable
                  executions={filteredActions.sort(this.sort.bind(this)).slice(0, this.state.limit)}
                  invocationIdProvider={() => this.props.model.getId()}></InvocationExecutionTable>
              ) : (
                <div className="invocation-execution-empty-actions">No matching actions.</div>
              )}
            </div>
            {filteredActions.length > this.state.limit && (
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
