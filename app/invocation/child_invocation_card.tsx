import React from "react";
import format from "../format/format";
import router from "../router/router";
import { BazelCommandResult } from "./child_invocations";

export type ChildInvocationCardProps = {
  status: string;
  results: BazelCommandResult[];
  className: string;
  icon: JSX.Element;
  linksDisabled?: boolean;
};

export default class ChildInvocationCard extends React.Component<ChildInvocationCardProps> {
  private handleCommandClicked(invocationId: string, e: React.MouseEvent) {
    e.preventDefault();
    router.navigateTo(`/invocation/${invocationId}`);
  }

  render() {
    return (
      <div className={`card ${this.props.className}`}>
        {this.props.icon}
        <div className="content">
          <div className="title">
            {this.props.results.length} command{this.props.results.length === 1 || "s"} {this.props.status}
          </div>
          <div className="details">
            {this.props.results.map((result) => (
              <div
                className="list-grid"
                onClick={
                  !this.props.linksDisabled && this.handleCommandClicked.bind(this, result.invocation.invocationId)
                }>
                <div className={`${!this.props.linksDisabled && "clickable"} target`}>
                  <span className="target-status-icon">{this.props.icon}</span> {result.invocation.bazelCommand}
                </div>
                <div>{typeof result.durationMillis === "number" && format.durationMillis(result.durationMillis)}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
