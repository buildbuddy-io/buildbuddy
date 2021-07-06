import React from "react";
import format from "../../../app/format/format";
import router from "../../../app/router/router";
import { invocation } from "../../../proto/invocation_ts_proto";

const durationRefreshIntervalMillis = 3000;

interface Props {
  invocation: invocation.Invocation;
  onMouseOver?: any;
  onMouseOut?: any;
  className?: string;
  hover?: boolean;
  isSelectedForCompare?: boolean;
}

interface State {
  time: number;
}

export default class HistoryInvocationCardComponent extends React.Component {
  props: Props;
  state: State = {
    time: Date.now(),
  };

  interval: number;

  componentDidMount() {
    this.updateTimeIfInProgress();
  }

  updateTimeIfInProgress() {
    if (!this.isInProgress()) {
      return;
    }
    this.setState({ time: Date.now() });
    this.interval = setTimeout(() => this.updateTimeIfInProgress(), durationRefreshIntervalMillis);
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  handleInvocationClicked() {
    router.navigateToInvocation(this.props.invocation.invocationId);
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
  handleRepoClicked(event: any, invocation: invocation.Invocation) {
    router.navigateToRepoHistory(invocation.repoUrl);
    event.stopPropagation();
    event.preventDefault();
  }

  isInProgress() {
    return this.props.invocation.invocationStatus == invocation.Invocation.InvocationStatus.PARTIAL_INVOCATION_STATUS;
  }

  isDisconnected() {
    return (
      this.props.invocation.invocationStatus == invocation.Invocation.InvocationStatus.DISCONNECTED_INVOCATION_STATUS
    );
  }

  getStatusClass() {
    if (this.isInProgress()) {
      return "card-in-progress";
    }

    if (this.isDisconnected()) {
      return "card-disconnected";
    }

    return this.props.invocation.success ? "card-success" : "card-failure";
  }

  getStatusIcon() {
    if (this.isInProgress()) {
      return "/image/play-circle.svg";
    }

    if (this.isDisconnected()) {
      return "/image/help-circle.svg";
    }

    return this.props.invocation.success ? "/image/check-circle.svg" : "/image/x-circle.svg";
  }

  getStatusLabel() {
    if (this.isInProgress()) {
      return "In progress...";
    }

    if (this.isDisconnected()) {
      return "Disconnected";
    }

    return this.props.invocation.success ? "Succeeded" : "Failed";
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
          } ${format.truncateList(this.props.invocation.pattern)}...`
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

    return (
      <div
        key={this.props.invocation.invocationId}
        onClick={this.handleInvocationClicked.bind(this, this.props.invocation)}
        onMouseOver={this.props.onMouseOver}
        onMouseOut={this.props.onMouseOut}
        className={`clickable card history-invocation-card ${this.props.className} ${
          this.props.hover ? "card-hover" : ""
        } ${this.getStatusClass()}`}>
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
          {!this.props.hover && (
            <div className="details">
              <div className="detail">
                <img className="icon" src={this.getStatusIcon()} />
                {this.getStatusLabel()}
              </div>
              <div className="detail">
                <img className="icon" src="/image/clock-regular.svg" />
                {this.getDuration()}
              </div>
              {this.props.invocation.user && (
                <div
                  className="detail clickable"
                  onClick={(e) => {
                    this.handleUserClicked(e, this.props.invocation);
                  }}>
                  <img className="icon" src="/image/user-regular.svg" />
                  {this.props.invocation.user}
                </div>
              )}
              {this.props.invocation.host && (
                <div
                  className="detail clickable"
                  onClick={(e) => {
                    this.handleHostClicked(e, this.props.invocation);
                  }}>
                  <img className="icon" src="/image/hard-drive-regular.svg" />
                  {this.props.invocation.host}
                </div>
              )}
              {this.props.invocation.command && (
                <div className="detail">
                  <img className="icon" src="/image/tool-regular.svg" />
                  {this.props.invocation.command}
                </div>
              )}
              {this.props.invocation.pattern && (
                <div className="detail">
                  <img className="icon" src="/image/grid-regular.svg" />
                  {format.truncateList(this.props.invocation.pattern)}
                </div>
              )}
              {this.props.invocation.repoUrl && (
                <div
                  className="detail clickable"
                  onClick={(e) => {
                    this.handleRepoClicked(e, this.props.invocation);
                  }}>
                  <img className="icon" src="/image/github-regular.svg" />
                  {format.formatGitUrl(this.props.invocation.repoUrl)}
                </div>
              )}
              {this.props.invocation.commitSha && (
                <div
                  className="detail clickable"
                  onClick={(e) => {
                    this.handleCommitClicked(e, this.props.invocation);
                  }}>
                  <img className="icon" src="/image/git-commit-regular.svg" />
                  {format.formatCommitHash(this.props.invocation.commitSha)}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    );
  }
}
