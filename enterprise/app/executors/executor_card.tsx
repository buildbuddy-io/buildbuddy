import React from "react";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import format from "../../../app/format/format";
import { BarChart2, Cloud } from "lucide-react";
import Link from "../../../app/components/link/link";
import { encodeMetricUrlParam, encodeWorkerUrlParam } from "../trends/common";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";

interface Props {
  node: scheduler.ExecutionNode;
  isDefault: boolean;
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
            <div className="executor-section">
              <div className="executor-section-title">Version:</div>
              <div>{this.props.node.version}</div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Default:</div>
              <div>{this.props.isDefault ? "True" : "False"}</div>
            </div>
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
