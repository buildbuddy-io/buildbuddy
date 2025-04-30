import React from "react";
import format from "../format/format";
import {
  getExecutionStatus,
  totalDuration,
  queuedDuration,
  downloadDuration,
  executionDuration,
  uploadDuration,
  getActionPageLink,
  workerDuration,
} from "./invocation_execution_util";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";

interface Props {
  executions: execution_stats.Execution[];
  invocationIdProvider: (exec: execution_stats.Execution) => string;
}

export default class InvocationExecutionTable extends React.Component<Props> {
  render() {
    return (
      <div className="invocation-execution-table">
        {this.props.executions.map((execution, index) => {
          const status = getExecutionStatus(execution);
          if (!execution.actionDigest) {
            return;
          }
          return (
            <Link
              key={index}
              className="invocation-execution-row"
              href={getActionPageLink(this.props.invocationIdProvider(execution), execution)}>
              <div className="invocation-execution-row-image">{status.icon}</div>
              <div>
                <div className="execution-header">
                  {execution.targetLabel && <span className="target-label">{execution.targetLabel}</span>}
                  <DigestComponent digest={execution.actionDigest} />
                </div>
                <div className="command-snippet">$ {execution.commandSnippet}</div>
                <div className="status">
                  {!execution.status?.code && (
                    <span>
                      {execution.exitCode ? (
                        <>
                          <span className="status-name failed">{status.name}</span> in{" "}
                          {format.durationUsec(workerDuration(execution))} (exit code {execution.exitCode})
                        </>
                      ) : (
                        <>
                          <span className="status-name success">{status.name}</span> in{" "}
                          {format.durationUsec(workerDuration(execution))}
                        </>
                      )}
                    </span>
                  )}
                  {!!execution.status?.code && (
                    <span className="status-code">
                      <span className="status-name error">{status.name}:</span> {execution.status.message}
                    </span>
                  )}
                </div>
                <div className="invocation-execution-row-stats">
                  <div>Executor Host ID: {execution.executedActionMetadata?.worker}</div>
                  <div>Total duration: {format.durationUsec(totalDuration(execution))}</div>
                  <div>Queued duration: {format.durationUsec(queuedDuration(execution))}</div>
                  <div>
                    File download duration: {format.durationUsec(downloadDuration(execution))} (
                    {format.bytes(execution.executedActionMetadata?.ioStats?.fileDownloadSizeBytes ?? 0)} across{" "}
                    {format.formatWithCommas(execution?.executedActionMetadata?.ioStats?.fileDownloadCount)} files)
                  </div>
                  <div>Execution duration: {format.durationUsec(executionDuration(execution))}</div>
                  <div>
                    File upload duration: {format.durationUsec(uploadDuration(execution))} (
                    {format.bytes(execution.executedActionMetadata?.ioStats?.fileUploadSizeBytes ?? 0)} across{" "}
                    {format.formatWithCommas(execution?.executedActionMetadata?.ioStats?.fileUploadCount)} files)
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
