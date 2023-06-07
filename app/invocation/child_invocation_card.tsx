import React from "react";
import format from "../format/format";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { CheckCircle, PlayCircle, XCircle, CircleSlash, Timer } from "lucide-react";
import Link from "../components/link/link";

export type CommandStatus = "failed" | "succeeded" | "in-progress" | "queued" | "not-run";

export type BazelCommandResult = {
  status: CommandStatus;
  invocation: InvocationMetadata;
  durationMillis?: number;
};

export type InvocationMetadata =
  | build_event_stream.WorkflowConfigured.IInvocationMetadata
  | build_event_stream.ChildInvocationsConfigured.IInvocationMetadata;

export type ChildInvocationCardProps = {
  result: BazelCommandResult;
};

export default class ChildInvocationCard extends React.Component<ChildInvocationCardProps> {
  private isClickable() {
    return this.props.result.status !== "queued" && this.props.result.status !== "not-run";
  }

  private renderStatusIcon() {
    switch (this.props.result.status) {
      case "succeeded":
        return <CheckCircle className="icon" />;
      case "failed":
        return <XCircle className="icon" />;
      case "in-progress":
        return <PlayCircle className="icon" />;
      case "queued":
        return <Timer className="icon" />;
      case "not-run":
        return <CircleSlash className="icon" />;
      default:
        // Render nothing.
        return undefined;
    }
  }

  render() {
    return (
      <Link
        className={`child-invocation-card status-${this.props.result.status} ${this.isClickable() ? "clickable" : ""}`}
        href={this.isClickable() ? `/invocation/${this.props.result.invocation.invocationId}` : undefined}>
        <div className="icon-container">{this.renderStatusIcon()}</div>
        <div className="command">{this.props.result.invocation.bazelCommand}</div>
        <div className="duration">
          {this.props.result.durationMillis !== undefined && format.durationMillis(this.props.result.durationMillis)}
        </div>
      </Link>
    );
  }
}
