import React from "react";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import format from "../../../app/format/format";
import { Cloud } from "lucide-react";

interface Props {
  node: scheduler.ExecutionNode;
  stats: scheduler.ExecutionNodeStats;
  isDefault: boolean;
}

export default class ExecutorCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className={`card ${this.props.isDefault ? "card-success" : "card-neutral"} single-executor`}>
        <Cloud className="icon" />
        <div className="content">
          <div style={{display: "flex"}}>
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
            <div style={{marginLeft: "96px", display: "flex", flexDirection: "column"}}>
              <div style={{fontWeight: 700}}>Executing</div>
              <div style={{fontSize: "36"}}>{this.props.stats.numExecuting}</div>
            </div>
            <div style={{marginLeft: "48px", display: "flex", flexDirection: "column"}}>
              <div style={{fontWeight: 700}}>Queued</div>
              <div style={{fontSize: "36"}}>{this.props.stats.numQueued}</div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
