import React from "react";
import { scheduler } from "../../../proto/scheduler_ts_proto";
import format from "../../../app/format/format";

interface Props {
  node: scheduler.IExecutionNode;
}

export default class ExecutorCardComponent extends React.Component<Props> {
  render() {
    return (
      <div className="card">
        <img className="icon" src="/image/cloud-regular.svg" />
        <div className="details">
          <div className="executor-section">
            <div className="executor-section-title">Address:</div>
            <div>
              {this.props.node.host}:{this.props.node.port}
            </div>
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
            <div className="executor-section-title">OS/Arch:</div>
            <div>
              {this.props.node.os}/{this.props.node.arch}
            </div>
          </div>
          <div className="executor-section">
            <div className="executor-section">
              <div className="executor-section-title">Pool:</div>
              <div>{this.props.node.pool || "Default Pool"}</div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
