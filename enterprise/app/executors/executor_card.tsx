import React from "react";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import format from "../../../app/format/format";
import { Cloud } from "lucide-react";

interface Props {
  executor: scheduler.GetExecutionNodesResponse.IExecutor;
}

export default class ExecutorCardComponent extends React.Component<Props> {
  render() {
    const node = this.props.executor.node;
    const enabled = this.props.executor.enabled;

    return (
      <div className={`card ${enabled ? "card-success" : "card-failure"}`}>
        <Cloud className="icon" />
        <div className="details">
          <div className="executor-section">
            <div className="executor-section-title">Address:</div>
            <div>
              {node.host}:{node.port}
            </div>
          </div>
          <div className="executor-section">
            <div className="executor-section-title">ID:</div>
            <div>{node.executorId}</div>
          </div>
          <div className="executor-section">
            <div className="executor-section-title">Assignable Memory:</div>
            <div>{format.bytes(+node.assignableMemoryBytes)}</div>
          </div>
          <div className="executor-section">
            <div className="executor-section-title">Assignable Milli CPU:</div>
            <div>{node.assignableMilliCpu}</div>
          </div>
          <div className="executor-section">
            <div className="executor-section-title">Version:</div>
            <div>{node.version}</div>
          </div>
          <div className="executor-section">
            <div className="executor-section-title">Status:</div>
            <div>{enabled ? "Enabled" : "Disabled"}</div>
          </div>
        </div>
      </div>
    );
  }
}
