import React from "react";
import format from "../format/format";
import { invocation } from "../../proto/invocation_ts_proto";
import { CheckCircle, PlayCircle, XCircle, CircleSlash } from "lucide-react";
import Link from "../components/link/link";
import { invocation_status } from "../../proto/invocation_status_ts_proto";
import InvocationModel from "./invocation_model";

type CommandStatus = "failed" | "succeeded" | "in-progress" | "not-run";

export type ChildInvocationCardProps = {
  invocation: invocation.Invocation;
};

export default class ChildInvocationCard extends React.Component<ChildInvocationCardProps> {
  private getStatus(): CommandStatus {
    const inv = this.props.invocation;
    switch (inv.invocationStatus) {
      case invocation_status.InvocationStatus.COMPLETE_INVOCATION_STATUS:
      case invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS:
        return inv.bazelExitCode == "SUCCESS" ? "succeeded" : "failed";
      case invocation_status.InvocationStatus.PARTIAL_INVOCATION_STATUS:
        return "in-progress";
      default:
        return "not-run";
    }
  }

  private isClickable(status: CommandStatus): boolean {
    return status !== "not-run";
  }

  private getDurationLabel(status: CommandStatus): string {
    if (status == "failed" || status == "succeeded") {
      return format.durationUsec(this.props.invocation.durationUsec);
    }
    return "";
  }

  private renderStatusIcon(status: CommandStatus) {
    switch (status) {
      case "succeeded":
        return <CheckCircle className="icon" />;
      case "failed":
        return <XCircle className="icon" />;
      case "in-progress":
        return <PlayCircle className="icon" />;
      case "not-run":
        return <CircleSlash className="icon" />;
      default:
        // Render nothing.
        return undefined;
    }
  }

  private simplifyCommandLine(commandLine: string): string {
    // Remove the bazel command string
    if (commandLine.startsWith("bazel ")) {
      commandLine = commandLine.slice("bazel ".length);
    }

    // Strip all --remote_header flags
    commandLine = commandLine.replace(/\s*--remote_header=[^\s]+/g, "");
    // Strip the options we manually added
    commandLine = commandLine.replace(" --config=buildbuddy_bes_backend", "");
    commandLine = commandLine.replace(" --config=buildbuddy_bes_results_url", "");
    commandLine = commandLine.replace(" --config=buildbuddy_remote_cache", "");
    commandLine = commandLine.replace(/\s*--invocation_id=[^\s]+/g, "");

    return commandLine;
  }

  render() {
    const inv = this.props.invocation;
    const invModel = new InvocationModel(inv);

    const status = this.getStatus();
    let command = invModel.explicitCommandLine();
    if (command == "") {
      command = `${inv.command} ${inv.pattern.join(" ")}`;
    }
    command = this.simplifyCommandLine(command);

    return (
      <Link
        className={`child-invocation-card status-${status} ${this.isClickable(status) ? "clickable" : ""}`}
        href={this.isClickable(status) ? `/invocation/${inv.invocationId}` : undefined}>
        <div className="icon-container">{this.renderStatusIcon(status)}</div>
        <div className="command">{command}</div>
        <div className="duration">{this.getDurationLabel(status)}</div>
      </Link>
    );
  }
}
