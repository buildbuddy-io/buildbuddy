import React from "react";
import format from "../format/format";
import {
  getExecutionStatus,
  totalDuration,
  queuedDuration,
  downloadDuration,
  executionDuration,
  uploadDuration,
} from "./invocation_execution_util";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";

interface Props {
  executions: execution_stats.IExecution[];
  invocationIdProvider: (exec: execution_stats.IExecution) => string;
}

export default class InvocationExecutionTable extends React.Component<Props> {
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
    return `/invocation/${this.props.invocationIdProvider(execution)}?${search}#action`;
  }

  render() {
    return (
      <div className="invocation-execution-table">
        {this.props.executions.map((execution, index) => {
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
                  <div>Total duration: {format.durationUsec(totalDuration(execution))}</div>
                  <div>Queued duration: {format.durationUsec(queuedDuration(execution))}</div>
                  <div>
                    File download duration: {format.durationUsec(downloadDuration(execution))} (
                    {format.bytes(execution?.executedActionMetadata?.ioStats?.fileDownloadSizeBytes)} across{" "}
                    {execution?.executedActionMetadata?.ioStats?.fileDownloadCount} files)
                  </div>
                  <div>Execution duration: {format.durationUsec(executionDuration(execution))}</div>
                  <div>
                    File upload duration: {format.durationUsec(uploadDuration(execution))} (
                    {format.bytes(execution?.executedActionMetadata?.ioStats?.fileUploadSizeBytes)} across{" "}
                    {execution?.executedActionMetadata?.ioStats?.fileUploadCount} files)
                  </div>
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    );
  }
}
