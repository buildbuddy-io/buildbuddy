import React from "react";

import {
  Activity,
  CheckCircle,
  Clock,
  CodeIcon,
  Hash,
  Network,
  Pencil,
  PlayCircle,
  Settings,
  XCircle,
} from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import format from "../../../app/format/format";
import router from "../../../app/router/router";
import { invocation } from "../../../proto/invocation_ts_proto";

interface Props {
  invocationStat: invocation.InvocationStat;
  type: invocation.AggType;
}

export default class HistoryInvocationStatCardComponent extends React.Component<Props> {
  handleStatClicked(): void {
    console.log(this.props.invocationStat);
    if (this.props.type == invocation.AggType.USER_AGGREGATION_TYPE) {
      router.navigateToUserHistory(this.props.invocationStat.name);
    }
    if (this.props.type == invocation.AggType.HOSTNAME_AGGREGATION_TYPE) {
      router.navigateToHostHistory(this.props.invocationStat.name);
    }
    if (this.props.type == invocation.AggType.REPO_URL_AGGREGATION_TYPE) {
      router.navigateToRepoHistory(this.props.invocationStat.name);
    }
    if (this.props.type == invocation.AggType.BRANCH_AGGREGATION_TYPE) {
      router.navigateToBranchHistory(this.props.invocationStat.name);
    }
    if (this.props.type == invocation.AggType.COMMIT_SHA_AGGREGATION_TYPE) {
      router.navigateToCommitHistory(this.props.invocationStat.name);
    }
  }

  getTitle(): string {
    if (this.props.type == invocation.AggType.USER_AGGREGATION_TYPE) {
      return this.props.invocationStat.name || "Unknown user";
    }
    if (this.props.type == invocation.AggType.HOSTNAME_AGGREGATION_TYPE) {
      return this.props.invocationStat.name || "Unknown host";
    }
    if (this.props.type == invocation.AggType.REPO_URL_AGGREGATION_TYPE) {
      return format.formatGitUrl(this.props.invocationStat.name) || "Unknown repo";
    }
    if (this.props.type == invocation.AggType.BRANCH_AGGREGATION_TYPE) {
      return this.props.invocationStat.name || "Unknown branch";
    }
    if (this.props.type == invocation.AggType.COMMIT_SHA_AGGREGATION_TYPE) {
      return this.props.invocationStat.name
        ? `Commit ${format.formatCommitHash(this.props.invocationStat.name)}`
        : "Unknown commit";
    }

    return this.props.invocationStat.name || "Unknown";
  }

  renderStatusIcon(): JSX.Element {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec) {
      return <CheckCircle className="icon green" />;
    }
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec) {
      return <XCircle className="icon red" />;
    }
    return <PlayCircle className="icon blue" />;
  }

  getStatus(): string {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "Last build succeeded";
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "Last build failed";
    return "Last build in progress";
  }

  getClass(): string {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "card-success";
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "card-failure";
    return "card-in-progress";
  }

  render(): JSX.Element {
    return (
      <div onClick={this.handleStatClicked.bind(this)} className={`clickable card ${this.getClass()}`}>
        <div className="content">
          <div className="titles">
            <div className="title">{this.getTitle()}</div>
            <div className="subtitle">
              Last build on {format.formatTimestampUsec(this.props.invocationStat.latestBuildTimeUsec)}
            </div>
          </div>
          <div className="details">
            <div className="detail">
              {this.renderStatusIcon()}
              {this.getStatus()}
            </div>
            <div className="detail">
              <Hash className="icon" />
              {format.formatWithCommas(this.props.invocationStat.totalNumBuilds)} total builds
            </div>
            <div className="detail">
              <Clock className="icon" />
              {format.durationUsec(this.props.invocationStat?.totalBuildTimeUsec)} total
            </div>
            <div className="detail">
              <Activity className="icon" />
              {format.formatWithCommas(this.props.invocationStat.totalActions)} total actions
            </div>
          </div>
          {this.props.type == invocation.AggType.REPO_URL_AGGREGATION_TYPE && capabilities.config.codeEditorEnabled && (
            <div className="actions">
              <button
                onClick={(e) => {
                  router.navigateTo("/code/" + format.formatGitUrl(this.props.invocationStat.name));
                  e.stopPropagation();
                }}>
                <CodeIcon /> Open in code editor
              </button>
              <button
                onClick={(e) => {
                  router.navigateTo("/code/" + format.formatGitUrl(this.props.invocationStat.name) + "/.bazelrc");
                  e.stopPropagation();
                }}>
                <Settings /> Edit .bazelrc
              </button>
              <button
                onClick={(e) => {
                  router.navigateTo("/code/" + format.formatGitUrl(this.props.invocationStat.name) + "/MODULE.bazel");
                  e.stopPropagation();
                }}>
                <Network /> Edit MODULE.bazel
              </button>
              <button
                onClick={(e) => {
                  router.navigateTo("/code/" + format.formatGitUrl(this.props.invocationStat.name) + "/.bazelversion");
                  e.stopPropagation();
                }}>
                <Pencil /> Edit .bazelversion
              </button>
            </div>
          )}
        </div>
      </div>
    );
  }
}
