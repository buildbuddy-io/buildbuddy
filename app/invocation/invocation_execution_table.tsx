import { ChevronRight } from "lucide-react";
import React from "react";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import DigestComponent from "../components/digest/digest";
import Link from "../components/link/link";
import format from "../format/format";
import { digestToString } from "../util/cache";
import { joinReactNodes } from "../util/react";
import ActionCompareButtonComponent from "./action_compare_button";
import {
  downloadDuration,
  executionDuration,
  getActionPageLink,
  getExecutionStatus,
  queuedDuration,
  totalDuration,
  uploadDuration,
  workerDuration,
} from "./invocation_execution_util";

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
          const workerDurationUsec = workerDuration(execution);
          const durationSuffix = workerDurationUsec ? <> in {format.durationUsec(workerDurationUsec)}</> : null;
          return (
            <Link
              key={index}
              className="invocation-execution-row"
              href={getActionPageLink(this.props.invocationIdProvider(execution), execution)}>
              <div className="invocation-execution-row-image">{status.icon}</div>
              <div>
                <div className="execution-header">
                  {renderExecutionLabel(execution)}
                  <DigestComponent digest={execution.actionDigest} expanded={execution.targetLabel === ""} />
                </div>
                <div className="command-snippet">$ {execution.commandSnippet}</div>
                <div className="status">
                  {!execution.status?.code && (
                    <span>
                      <span className={`status-name ${execution.exitCode ? "failed" : "success"}`}>{status.name}</span>
                      {durationSuffix}
                    </span>
                  )}
                  {!!execution.status?.code && (
                    <span className="status-code">
                      <span className="status-name error">{status.name}:</span> {execution.status.message}
                    </span>
                  )}
                </div>
                <ActionCompareButtonComponent
                  invocationId={this.props.invocationIdProvider(execution)}
                  actionDigest={digestToString(execution.actionDigest)}
                  mini={true}
                />
                <div className="invocation-execution-row-stats">
                  {!!execution.primaryOutputPath && (
                    <div>
                      Primary output:{" "}
                      <span className="primary-output" title={execution.primaryOutputPath}>
                        {formatPrimaryOutput(execution.primaryOutputPath)}
                      </span>
                    </div>
                  )}
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

function renderExecutionLabel(execution: execution_stats.Execution) {
  const nodes = [
    execution.targetLabel && <span className="target-label">{execution.targetLabel}</span>,
    execution.actionMnemonic && <span className="action-mnemonic">{execution.actionMnemonic}</span>,
  ].filter((node) => node);
  if (!nodes.length) {
    return null;
  }
  return (
    <span className="execution-label">
      {joinReactNodes(nodes, <ChevronRight className="icon breadcrumb-separator" />)}
    </span>
  );
}

function formatPrimaryOutput(path: string) {
  const segments = path.split("/").filter(Boolean);
  if (segments.length <= 2) {
    return path;
  }
  const tail = segments.slice(-2).join("/");
  return `…/${tail}`;
}
