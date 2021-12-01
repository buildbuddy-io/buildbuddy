import React from "react";

import { invocation } from "../../../proto/invocation_ts_proto";
import router from "../../../app/router/router";
import format from "../../../app/format/format";
import { Clock, Hash, XCircle, PlayCircle, CheckCircle, Activity } from "lucide-react";

interface Props {
  invocationStat: invocation.IInvocationStat;
  type: invocation.AggType;
}

export default class HistoryInvocationStatCardComponent extends React.Component<Props> {
  handleStatClicked() {
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

  getTitle() {
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

  renderStatusIcon() {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec) {
      return <CheckCircle className="icon green" />;
    }
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec) {
      return <XCircle className="icon red" />;
    }
    return <PlayCircle className="icon blue" />;
  }

  getStatus() {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "Last build succeeded";
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "Last build failed";
    return "Last build in progress";
  }

  getClass() {
    if (this.props.invocationStat.lastGreenBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "card-success";
    if (this.props.invocationStat.lastRedBuildUsec == this.props.invocationStat.latestBuildTimeUsec)
      return "card-failure";
    return "card-in-progress";
  }

  render() {
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
              {this.props.invocationStat.totalNumBuilds} total builds
            </div>
            <div className="detail">
              <Clock className="icon" />
              {format.durationUsec(this.props.invocationStat.totalBuildTimeUsec)} total
            </div>
            <div className="detail">
              <Activity className="icon" />
              {this.props.invocationStat.totalActions} total actions
            </div>
          </div>
        </div>
      </div>
    );
  }
}
