import React from "react";
import InvocationModel from "./invocation_model";
import router from "../router/router";
import format from "../format/format";
import { User } from "../auth/auth_service";

interface Props {
  model: InvocationModel;
  invocationId: string;
  user?: User;
}
export default class InvocationOverviewComponent extends React.Component {
  props: Props;

  handleOrganizationClicked() {
    router.navigateHome();
  }

  handleUserClicked() {
    router.navigateToUserHistory(this.props.model.getUser(false));
  }

  handleHostClicked() {
    router.navigateToHostHistory(this.props.model.getHost());
  }

  handleRepoClicked() {
    router.navigateToRepoHistory(this.props.model.getRepo());
  }

  handleCommitClicked() {
    router.navigateToCommitHistory(this.props.model.getCommit());
  }

  handleCacheClicked() {
    router.navigateToSetup();
  }

  handleRBEClicked() {
    router.navigateToSetup();
  }

  render() {
    return (
      <div className="container">
        <div className="breadcrumbs">
          {this.props.user && (
            <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
              {this.props.user?.selectedGroupName()}
            </span>
          )}
          {this.props.user && (
            <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
              Builds
            </span>
          )}
          <span>Invocation {this.props.invocationId}</span>
        </div>
        <div className="titles">
          <div className="title" title={this.props.model.getAllPatterns()}>
            {format.sentenceCase(this.props.model.getUser(true))} {this.props.model.getCommand()}{" "}
            {this.props.model.getPattern()}
          </div>
          <div className="subtitle">
            {this.props.model.getStartDate()} at {this.props.model.getStartTime()}
          </div>
        </div>
        <div className="details">
          <div className="detail">
            {this.props.model.getStatusIcon()}
            {this.props.model.getStatus()}
          </div>
          <div className="detail" title={this.props.model.getDurationSeconds()}>
            <img className="icon" src="/image/clock-regular.svg" />
            {this.props.model.getTiming()}
          </div>
          <div className="detail clickable" onClick={this.handleUserClicked.bind(this)}>
            <img className="icon" src="/image/user-regular.svg" />
            {this.props.model.getUser(false)}
          </div>
          <div className="detail clickable" onClick={this.handleHostClicked.bind(this)}>
            <img className="icon" src="/image/hard-drive-regular.svg" />
            {this.props.model.getHost()}
          </div>
          <div className="detail">
            <img className="icon" src="/image/tool-regular.svg" />
            {this.props.model.getTool()}
          </div>
          <div className="detail" title={this.props.model.getAllPatterns()}>
            <img className="icon" src="/image/grid-regular.svg" />
            {this.props.model.getPattern()}
          </div>
          <div
            className="detail"
            title={`${this.props.model.buildMetrics?.targetMetrics.targetsConfigured} configured / ${this.props.model.buildMetrics?.targetMetrics.targetsLoaded} loaded`}
          >
            <img className="icon" src="/image/target-regular.svg" />
            {this.props.model.targets.length}{" "}
            {this.props.model.targets.length == 1 ? "target" : "targets"}
          </div>
          <div
            title={`${this.props.model.buildMetrics?.actionSummary.actionsCreated} created`}
            className="detail"
          >
            <img className="icon" src="/image/activity-regular.svg" />
            {this.props.model.buildMetrics?.actionSummary.actionsExecuted} actions
          </div>
          <div className="detail">
            <img className="icon" src="/image/box-regular.svg" />
            {this.props.model.buildMetrics?.packageMetrics.packagesLoaded} packages
          </div>
          <div className="detail">
            <img className="icon" src="/image/cpu-regular.svg" />
            {this.props.model.getCPU()}
          </div>
          <div className="detail">
            <img className="icon" src="/image/zap-regular.svg" />
            {this.props.model.getMode()}
          </div>
          {this.props.model.getRepo() && (
            <div className="detail clickable" onClick={this.handleRepoClicked.bind(this)}>
              <img className="icon" src="/image/github-regular.svg" />
              {format.formatGitUrl(this.props.model.getRepo())}
            </div>
          )}
          {this.props.model.getCommit() && (
            <div className="detail clickable" onClick={this.handleCommitClicked.bind(this)}>
              <img className="icon" src="/image/git-commit-regular.svg" />
              {format.formatCommitHash(this.props.model.getCommit())}
            </div>
          )}
          <div className="detail clickable" onClick={this.handleCacheClicked.bind(this)}>
            <img className="icon" src="/image/package-regular.svg" />
            {this.props.model.getCache()}
          </div>
          <div className="detail clickable" onClick={this.handleRBEClicked.bind(this)}>
            <img className="icon" src="/image/cloud-regular.svg" />
            {this.props.model.getRBE()}
          </div>
        </div>
      </div>
    );
  }
}
