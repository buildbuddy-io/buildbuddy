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
    const isDefault = this.props.executor.isDefault;

    return (
      <div className={`card ${isDefault ? "card-success" : "card-neutral"}`}>
        <Cloud className="icon" />
        <div className="content">
          <div className="details">
            <div className="executor-section">
              <div className="executor-section-title">Address:</div>
              <div>
                {node.host}:{node.port}
              </div>
            </div>
            <div className="executor-section">
              <div className="executor-section-title">Executor Host ID:</div>
              <div>{node.executorHostId}</div>
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
              <div className="executor-section-title">Default:</div>
              <div>{isDefault ? "True" : "False"}</div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
