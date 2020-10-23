import React from "react";
import format from "../format/format";
import InvocationModel from "./invocation_model";
import { execution_stats } from "../../proto/execution_stats_ts_proto";

import rpcService from "../service/rpc_service";

interface Props {
  model: InvocationModel;
  inProgress: boolean;
}

interface State {
  loading: boolean;
  executions: execution_stats.Execution[];
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

  sort(a: execution_stats.Execution, b: execution_stats.Execution) {
    return a.stage - b.stage;
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.executions.length) {
      return <div>No remotely executed actions.</div>;
    }

    let completedCount = 0;
    let incompleteCount = 0;
    for (let exection of this.state.executions) {
      // Completed
      if (exection.stage == 4) {
        completedCount++;
      } else {
        incompleteCount++;
      }
    }

    return (
      <div className={`card expanded`}>
        <div className="content">
          <div className="title">
            Remotely executed actions ({!!incompleteCount && <span>{incompleteCount} in progress, </span>}
            {completedCount} completed)
          </div>
          <div className="invocation-execution-table">
            {this.state.executions.sort(this.sort).map((execution) => (
              <div key={execution?.actionDigest?.hash} className="invocation-execution-row">
                <div className="invocation-execution-row-image">
                  <img
                    className={stages[execution.stage].class}
                    src={stages[execution.stage].image}
                    alt={stages[execution.stage].name}
                  />
                </div>
                <div>
                  <div className="invocation-execution-row-digest">
                    {stages[execution.stage].name} {execution?.actionDigest?.hash}/{execution?.actionDigest?.sizeBytes}
                  </div>
                  <div>{execution.commandSnippet}</div>
                  <div className="invocation-execution-row-stats">
                    <div>Worker: {execution?.executedActionMetadata?.worker}</div>
                    <div>
                      Total duration:{" "}
                      {format.durationUsec(
                        this.subtractTimestamp(
                          execution?.executedActionMetadata?.workerCompletedTimestamp,
                          execution?.executedActionMetadata?.queuedTimestamp
                        )
                      )}
                    </div>
                    <div>
                      Queued duration:{" "}
                      {format.durationUsec(
                        this.subtractTimestamp(
                          execution?.executedActionMetadata?.workerStartTimestamp,
                          execution?.executedActionMetadata?.queuedTimestamp
                        )
                      )}
                    </div>
                    <div>
                      File download duration:{" "}
                      {format.durationUsec(
                        this.subtractTimestamp(
                          execution?.executedActionMetadata?.inputFetchCompletedTimestamp,
                          execution?.executedActionMetadata?.inputFetchStartTimestamp
                        )
                      )}{" "}
                      ({format.bytes(execution?.ioStats?.fileDownloadSizeBytes)} across{" "}
                      {execution?.ioStats?.fileDownloadCount} files)
                    </div>
                    <div>
                      Execution duration:{" "}
                      {format.durationUsec(
                        this.subtractTimestamp(
                          execution?.executedActionMetadata?.executionCompletedTimestamp,
                          execution?.executedActionMetadata?.executionStartTimestamp
                        )
                      )}
                    </div>
                    <div>
                      File upload duration:{" "}
                      {format.durationUsec(
                        this.subtractTimestamp(
                          execution?.executedActionMetadata?.outputUploadCompletedTimestamp,
                          execution?.executedActionMetadata?.outputUploadStartTimestamp
                        )
                      )}{" "}
                      ({format.bytes(execution?.ioStats?.fileUploadSizeBytes)} across{" "}
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
