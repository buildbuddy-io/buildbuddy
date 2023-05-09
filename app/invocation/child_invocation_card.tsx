import React from "react";
import format from "../format/format";
import router from "../router/router";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";

export type BazelCommandResult = {
  invocation:
    | build_event_stream.WorkflowConfigured.IInvocationMetadata
    | build_event_stream.ChildInvocationsConfigured.IInvocationMetadata;
  durationMillis?: number;
};

export type ChildInvocationCardProps = {
  status: string;
  results: BazelCommandResult[];
  className: string;
  icon: JSX.Element;
  linksDisabled?: boolean;
};

export default class ChildInvocationCard extends React.Component<ChildInvocationCardProps> {
  private handleCommandClicked(invocationId: string, e: React.MouseEvent) {
    // TODO(siggisim): Switch this to using the <Link> component
    if (e.metaKey || e.ctrlKey) {
      return;
    }
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
              <a
                href={`/invocation/${result.invocation.invocationId}`}
                className="list-grid"
                onClick={
                  !this.props.linksDisabled && this.handleCommandClicked.bind(this, result.invocation.invocationId)
                }>
                <div className={`${!this.props.linksDisabled && "clickable"} target`}>
                  <span className="target-status-icon">{this.props.icon}</span> {result.invocation.bazelCommand}
                </div>
                <div>{typeof result.durationMillis === "number" && format.durationMillis(result.durationMillis)}</div>
              </a>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
