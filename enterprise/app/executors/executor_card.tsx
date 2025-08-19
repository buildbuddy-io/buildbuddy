import { BarChart2, Cloud } from "lucide-react";
import React from "react";
import Link from "../../../app/components/link/link";
import format from "../../../app/format/format";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";
import { encodeMetricUrlParam, encodeWorkerUrlParam } from "../trends/common";

interface Props {
  node: scheduler.ExecutionNode;
  isDefault: boolean;
  lastCheckInTime?: google_timestamp.protobuf.Timestamp | null;
}

export default class ExecutorCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className={`card ${this.props.isDefault ? "card-success" : "card-neutral"}`}>
        <Cloud className="icon" />
        <div className="content">
          <div className="details">
            <div className="executor-section">
              <div className="executor-section-title">Hostname:</div>
              <div>{this.props.node.host}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Executor Instance ID:</div>
              <div>{this.props.node.executorId}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Executor Host ID:</div>
              <div>{this.props.node.executorHostId}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Assignable Memory:</div>
              <div>{format.bytes(+this.props.node.assignableMemoryBytes)}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Assignable Milli CPU:</div>
              <div>{this.props.node.assignableMilliCpu}</div>
            </div>
            {this.props.node.assignableCustomResources && this.props.node.assignableCustomResources.length > 0 && (
              <div className="executor-section">
                <div className="executor-section-title">Assignable Resources:</div>
                <div className="executor-custom-resource">
                  {this.props.node.assignableCustomResources.map((r) => {
                    return (
                      <div className="executor-custom-resource-wrapper">
                        <div className="executor-custom-resource-key">{r.name}: </div>
                        <div>{r.value}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
            {this.props.node.supportedIsolationTypes && this.props.node.supportedIsolationTypes.length > 0 && (
              <div className="executor-section">
                <div className="executor-section-title">Isolation Types:</div>
                <div>{this.props.node.supportedIsolationTypes.join(", ")}</div>
              </div>
            )}
            <div className="executor-section">
              <div className="executor-section-title">Version:</div>
              <div>{this.props.node.version}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Default:</div>
              <div>{this.props.isDefault ? "True" : "False"}</div>
            </div>

            {this.props.lastCheckInTime && (
              <>
                <div className="executor-section">
                  <div className="executor-section-title">Last Check-in:</div>
                  <div>{format.relativeTimeSeconds(this.props.lastCheckInTime)}</div>
                </div>
                <div className="executor-section">
                  <div className="executor-section-title">Queue Length:</div>
                  <div>{this.props.node.currentQueueLength || 0}</div>
                </div>
                <div className="executor-section">
                  <div className="executor-section-title">Active Action Count:</div>
                  <div>{this.props.node.activeActionCount || 0}</div>
                </div>
              </>
            )}

            <div className="executor-section">
              <Link
                className="executor-history-button history-button"
                href={`/trends/?d=${encodeURIComponent(
                  encodeWorkerUrlParam(this.props.node.executorHostId)
                )}&ddMetric=${encodeMetricUrlParam(
                  stat_filter.Metric.create({
                    execution: stat_filter.ExecutionMetricType.EXECUTION_WALL_TIME_EXECUTION_METRIC,
                  })
                )}#drilldown`}>
                <BarChart2 /> View executions
              </Link>
              <div></div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
