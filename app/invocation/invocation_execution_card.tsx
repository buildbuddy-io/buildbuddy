import React from "react";
import format from "../format/format";
import capabilities from "../capabilities/capabilities";
import InvocationModel from "./invocation_model";
import router from "../router/router";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import Select, { Option } from "../components/select/select";

import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
}

interface State {
  loading: boolean;
  executions: execution_stats.Execution[];
  sort: string;
  direction: "asc" | "desc";
}

const stages = {
  0: { name: "Starting", image: "/image/running.svg", class: "rotating" },
  1: { name: "Cache check", image: "/image/cache-check.svg", class: "" },
  2: { name: "Queued", image: "/image/queued.svg", class: "" },
  3: { name: "Executing", image: "/image/running.svg", class: "rotating" },
  4: { name: "Completed", image: "/image/complete.svg", class: "" },
};

export default class ExecutionCardComponent extends React.Component {
  props: Props;

  state: State = {
    executions: [],
    loading: true,
    sort: "status",
    direction: "desc",
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

  totalDuration(execution: execution_stats.Execution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.workerCompletedTimestamp,
      execution?.executedActionMetadata?.queuedTimestamp
    );
  }

  queuedDuration(execution: execution_stats.Execution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.workerStartTimestamp,
      execution?.executedActionMetadata?.queuedTimestamp
    );
  }

  downloadDuration(execution: execution_stats.Execution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.inputFetchCompletedTimestamp,
      execution?.executedActionMetadata?.inputFetchStartTimestamp
    );
  }

  executionDuration(execution: execution_stats.Execution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.executionCompletedTimestamp,
      execution?.executedActionMetadata?.executionStartTimestamp
    );
  }

  uploadDuration(execution: execution_stats.Execution) {
    return this.subtractTimestamp(
      execution?.executedActionMetadata?.outputUploadCompletedTimestamp,
      execution?.executedActionMetadata?.outputUploadStartTimestamp
    );
  }

  sort(a: execution_stats.Execution, b: execution_stats.Execution) {
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
        return +first?.ioStats?.fileDownloadCount - +second?.ioStats?.fileDownloadCount;
      case "files-uploaded":
        return +first?.ioStats?.fileUploadCount - +second?.ioStats?.fileUploadCount;
      case "file-size-downloaded":
        return +first?.ioStats?.fileDownloadSizeBytes - +second?.ioStats?.fileDownloadSizeBytes;
      case "file-size-uploaded":
        return +first?.ioStats?.fileUploadSizeBytes - +second?.ioStats?.fileUploadSizeBytes;
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
        return second.stage - first.stage;
    }
  }

  handleInputChange(event: any) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.value,
    });
  }

  handleSortChange(event: any) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleActionDigestClick(execution: execution_stats.Execution) {
    router.navigateTo(
      "/invocation/" +
        this.props.model.getId() +
        "?actionDigest=" +
        execution.actionDigest.hash +
        "/" +
        execution.actionDigest.sizeBytes +
        "#action"
    );
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
      // Completed
      if (execution.stage == 4) {
        completedCount++;
      } else {
        incompleteCount++;
      }
    }

    return (
      <div className={`card expanded`}>
        <div className="content">
          <div className="invocation-content-header">
            <div className="title">
              Remotely executed actions ({!!incompleteCount && <span>{incompleteCount} in progress, </span>}
              {completedCount} completed)
            </div>

            <div className="invocation-sort-controls">
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
          <div className="invocation-execution-table">
            {this.state.executions.sort(this.sort.bind(this)).map((execution, index) => (
              <div key={index} className="invocation-execution-row">
                <div className="invocation-execution-row-image">
                  <img
                    className={stages[execution.stage].class}
                    src={stages[execution.stage].image}
                    alt={stages[execution.stage].name}
                  />
                </div>
                <div className="clickable" onClick={this.handleActionDigestClick.bind(this, execution)}>
                  <div className="invocation-execution-row-digest">
                    {stages[execution.stage].name} {execution?.actionDigest?.hash}/{execution?.actionDigest?.sizeBytes}
                  </div>
                  <div>{execution.commandSnippet}</div>
                  <div className="invocation-execution-row-stats">
                    <div>Worker: {execution?.executedActionMetadata?.worker}</div>
                    <div>Total duration: {format.durationUsec(this.totalDuration(execution))}</div>
                    <div>Queued duration: {format.durationUsec(this.queuedDuration(execution))}</div>
                    <div>
                      File download duration: {format.durationUsec(this.downloadDuration(execution))} (
                      {format.bytes(execution?.ioStats?.fileDownloadSizeBytes)} across{" "}
                      {execution?.ioStats?.fileDownloadCount} files)
                    </div>
                    <div>Execution duration: {format.durationUsec(this.executionDuration(execution))}</div>
                    <div>
                      File upload duration: {format.durationUsec(this.uploadDuration(execution))} (
                      {format.bytes(execution?.ioStats?.fileUploadSizeBytes)} across{" "}
                      {execution?.ioStats?.fileUploadCount} files)
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
