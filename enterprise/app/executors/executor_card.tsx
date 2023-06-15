import React from "react";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import format from "../../../app/format/format";
import { Cloud } from "lucide-react";

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
          </div>
        </div>
      </div>
    );
  }
}
