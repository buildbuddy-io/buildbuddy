import {
  CheckCircle,
  Clock,
  Github,
  GitBranch,
  GitCommit,
  HardDrive,
  HelpCircle,
  LayoutGrid,
  PlayCircle,
  Tag,
  User,
  Wrench,
  XCircle,
  Circle,
} from "lucide-react";
import React from "react";
import format from "../../../app/format/format";
import router from "../../../app/router/router";
import Link from "../../../app/components/link/link";
import { invocation } from "../../../proto/invocation_ts_proto";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { exitCode } from "../../../app/util/exit_codes";

const durationRefreshIntervalMillis = 3000;

interface Props {
  invocation: invocation.Invocation;
  onMouseOver?: any;
  onMouseOut?: any;
  className?: string;
  hover?: boolean;
  isSelectedForCompare?: boolean;
  isSelectedWithKeyboard?: boolean;
}

interface State {
  time: number;
}

export default class HistoryInvocationCardComponent extends React.Component<Props, State> {
  state: State = {
    time: Date.now(),
  };

  interval?: number;

  componentDidMount() {
    this.updateTimeIfInProgress();
  }

  updateTimeIfInProgress() {
    if (!this.isInProgress()) {
      if (this.interval) {
        window.clearInterval(this.interval);
      }
      return;
    }
    this.setState({ time: Date.now() });
    if (!this.interval) {
      this.interval = window.setInterval(() => {
        this.updateTimeIfInProgress();
      }, durationRefreshIntervalMillis);
    }
  }

  componentWillUnmount() {
    window.clearInterval(this.interval);
  }

  // Beware, this method isn't bound to this - so don't use any this. stuff. Event propagation is a nightmare.
  handleUserClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToUserHistory(invocation.user);
    event.stopPropagation();
    event.preventDefault();
  }

  // Beware, this method isn't bound to this - so don't use any this. stuff. Event propagation is a nightmare.
  handleHostClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToHostHistory(invocation.host);
    event.stopPropagation();
    event.preventDefault();
  }

  // Beware, this method isn't bound to this - so don't use any this. stuff. Event propagation is a nightmare.
  handleCommitClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToCommitHistory(invocation.commitSha);
    event.stopPropagation();
    event.preventDefault();
  }

  // Beware, this method isn't bound to this - so don't use any this. stuff. Event propagation is a nightmare.
  handleBranchClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToBranchHistory(invocation.branchName);
    event.stopPropagation();
    event.preventDefault();
  }

  // Beware, this method isn't bound to this - so don't use any this. stuff. Event propagation is a nightmare.
  handleRepoClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToRepoHistory(invocation.repoUrl);
    event.stopPropagation();
    event.preventDefault();
  }

  isInProgress() {
    return this.props.invocation.invocationStatus == invocation_status.InvocationStatus.PARTIAL_INVOCATION_STATUS;
  }

  isDisconnected() {
    return this.props.invocation.invocationStatus == invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS;
  }

  getStatusClass() {
    if (this.isInProgress()) {
      return "card-in-progress";
    }

    if (this.isDisconnected()) {
      return "card-disconnected";
    }

    if (this.props.invocation.bazelExitCode == "NO_TESTS_FOUND") {
      return "card-neutral";
    }

    return this.props.invocation.success ? "card-success" : "card-failure";
  }

  renderStatusIcon() {
    if (this.isInProgress()) {
      return <PlayCircle className="icon blue" />;
    }

    if (this.isDisconnected()) {
      return <HelpCircle className="icon" />;
    }

    if (this.props.invocation.bazelExitCode == "NO_TESTS_FOUND") {
      return <Circle className="icon gray" />;
    }

    return this.props.invocation.success ? <CheckCircle className="icon green" /> : <XCircle className="icon red" />;
  }

  getStatusLabel() {
    if (this.isInProgress()) {
      return "In progress...";
    }

    if (this.isDisconnected()) {
      return "Disconnected";
    }
    return this.props.invocation.success ? "Succeeded" : exitCode(this.props.invocation.bazelExitCode);
  }

  private getTitleForWorkflow() {
    const actionName = this.props.invocation.pattern;
    return actionName;
  }

  getTitle() {
    if (this.props.invocation.role === "CI_RUNNER") {
      return this.getTitleForWorkflow();
    }

    if (this.isInProgress()) {
      return this.props.invocation?.user
        ? `${this.props.invocation.user}'s in progress ${
            this.props.invocation.command || "build"
          } ${format.truncateList(this.props.invocation.pattern)}`
        : "In progress build...";
    }

    if (this.isDisconnected()) {
      return this.props.invocation?.user
        ? `${this.props.invocation.user}'s disconnected ${
            this.props.invocation.command || "build"
          } ${format.truncateList(this.props.invocation.pattern)}`
        : "Disconnected build";
    }

    return `${this.props.invocation.user || "Unknown user"}'s ${this.props.invocation.command} ${format.truncateList(
      this.props.invocation.pattern
    )}`;
  }

  getDuration() {
    if (this.isInProgress()) {
      return format.durationUsec(this.state.time * 1000 - +this.props.invocation.createdAtUsec);
    }

    return format.durationUsec(this.props.invocation.durationUsec);
  }

  render() {
    const roleLabel = format.formatRole(this.props.invocation.role);
    const tags = (this.props.invocation.tags || []).map((t) => t.name).join(", ");

    return (
      <Link
        key={this.props.invocation.invocationId}
        href={`/invocation/${this.props.invocation.invocationId}`}
        onMouseOver={this.props.onMouseOver}
        onMouseOut={this.props.onMouseOut}
        className={`clickable card history-invocation-card
          ${this.props.className}
          ${this.props.hover ? "card-hover" : ""}
          ${this.getStatusClass()}
          ${this.props.isSelectedWithKeyboard ? "selected-keyboard-shortcuts" : ""}`}>
        <div className="content">
          {this.props.isSelectedForCompare && (
            <div className="comparison-buffer-illustration buffered" title="Selected for compare">
              <div className="comparison-buffer-icon comparison-buffer-icon-a" />
              <div className="comparison-buffer-icon comparison-buffer-icon-b" />
            </div>
          )}
          <div className="titles">
            <div className="title">{this.getTitle()}</div>
            {roleLabel && <div className={`role-badge ${this.props.invocation.role}`}>{roleLabel}</div>}
            <div className="subtitle">{format.formatTimestampUsec(this.props.invocation.createdAtUsec)}</div>
          </div>
          <div className="details">
            {!this.props.hover && (
              <div className="detail">
                {this.renderStatusIcon()}
                {this.getStatusLabel()}
              </div>
            )}
            {!this.props.hover && (
              <div className="detail">
                <Clock className="icon" />
                {this.getDuration()}
              </div>
            )}
            {!this.props.hover && this.props.invocation.user && (
              <div
                className="detail clickable"
                onClick={(e) => {
                  this.handleUserClicked(e, this.props.invocation);
                }}>
                <User className="icon" />
                {this.props.invocation.user}
              </div>
            )}
            {!this.props.hover && this.props.invocation.host && (
              <div
                className="detail clickable"
                onClick={(e) => {
                  this.handleHostClicked(e, this.props.invocation);
                }}>
                <HardDrive className="icon" />
                {this.props.invocation.host}
              </div>
            )}
            {!this.props.hover && this.props.invocation.command && (
              <div className="detail">
                <Wrench className="icon" />
                {this.props.invocation.command}
              </div>
            )}
            {!this.props.hover && this.props.invocation.pattern.length > 0 && (
              <div className="detail">
                <LayoutGrid className="icon" />
                {format.truncateList(this.props.invocation.pattern)}
              </div>
            )}
            {this.props.invocation.repoUrl && (
              <div
                className="detail clickable"
                onClick={(e) => {
                  this.handleRepoClicked(e, this.props.invocation);
                }}>
                <Github className="icon" />
                {format.formatGitUrl(this.props.invocation.repoUrl)}
              </div>
            )}
            {this.props.invocation.branchName && (
              <div
                className="detail clickable"
                onClick={(e) => {
                  this.handleBranchClicked(e, this.props.invocation);
                }}>
                <GitBranch className="icon" />
                {this.props.invocation.branchName}
              </div>
            )}
            {tags && (
              <div className="detail clickable">
                <Tag className="icon" />
                {tags}
              </div>
            )}
            {!this.props.hover && this.props.invocation.commitSha && (
              <div
                className="detail clickable"
                onClick={(e) => {
                  this.handleCommitClicked(e, this.props.invocation);
                }}>
                <GitCommit className="icon" />
                {format.formatCommitHash(this.props.invocation.commitSha)}
              </div>
            )}
          </div>
        </div>
      </Link>
    );
  }
}
