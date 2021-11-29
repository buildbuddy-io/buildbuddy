import {
  Activity,
  Box,
  Clock,
  Cpu,
  DownloadCloud,
  Github,
  GitBranch,
  GitCommit,
  Package,
  Cloud,
  HardDrive,
  LayoutGrid,
  Link,
  Target,
  User as UserIcon,
  Wrench,
  Zap,
} from "lucide-react";
import React from "react";
import { User } from "../auth/auth_service";
import format from "../format/format";
import router from "../router/router";
import InvocationButtons from "./invocation_buttons";
import InvocationModel from "./invocation_model";

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

  handleBranchClicked() {
    router.navigateToBranchHistory(this.props.model.getBranchName());
  }

  handleCommitClicked() {
    router.navigateToCommitHistory(this.props.model.getCommit());
  }

  handleCacheClicked() {
    router.navigateToSetup();
  }

  handleRBEClicked() {
    if (this.props.model.getIsRBEEnabled()) {
      window.location.hash = "#execution";
      return;
    }
    router.navigateToSetup();
  }

  handleFetchesClicked() {
    if (this.props.model.getFetchURLs().length > 0) {
      window.location.hash = "#fetches";
      return;
    }
  }

  handleBuildkiteClicked() {
    window.open(this.props.model.getBuildkiteUrl(), "_blank");
  }

  render() {
    const ownerGroup = this.props.model.findOwnerGroup(this.props.user?.groups);
    const isBazelInvocation = this.props.model.isBazelInvocation();
    const roleLabel = format.formatRole(this.props.model.getRole());

    return (
      <div className="container">
        <div className="breadcrumbs-and-buttons">
          <div className="breadcrumbs">
            {this.props.user && ownerGroup && (
              <>
                <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
                  {ownerGroup.name}
                </span>
                <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
                  Builds
                </span>
              </>
            )}
            <span>Invocation {this.props.invocationId}</span>
          </div>
          <InvocationButtons invocationId={this.props.invocationId} model={this.props.model} user={this.props.user} />
        </div>
        <div className="titles">
          {isBazelInvocation && (
            <div className="title" title={this.props.model.getAllPatterns()}>
              {this.props.model.getUser(/*possessive=*/ true)} {this.props.model.getCommand()}{" "}
              {this.props.model.getPattern()}
            </div>
          )}
          {this.props.model.workflowConfigured && (
            <div className="title">{this.props.model.workflowConfigured.actionName}</div>
          )}
          {roleLabel && <div className={`role-badge ${this.props.model.getRole()}`}>{roleLabel}</div>}
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
            <Clock className="icon" />
            {this.props.model.getTiming()}
          </div>
          <div className="detail clickable" onClick={this.handleUserClicked.bind(this)}>
            <UserIcon className="icon" />
            {this.props.model.getUser(false)}
          </div>
          <div className="detail clickable" onClick={this.handleHostClicked.bind(this)}>
            <HardDrive className="icon" />
            {this.props.model.getHost()}
          </div>
          <div className="detail">
            <Wrench className="icon" />
            {this.props.model.getTool()}
          </div>
          {isBazelInvocation && (
            <div className="detail" title={this.props.model.getAllPatterns()}>
              <LayoutGrid className="icon" />
              {this.props.model.getPattern()}
            </div>
          )}
          {isBazelInvocation && (
            <div
              className="detail"
              title={`${this.props.model.buildMetrics?.targetMetrics.targetsConfigured} configured / ${this.props.model.buildMetrics?.targetMetrics.targetsLoaded} loaded`}>
              <Target className="icon" />
              {this.props.model.targets.length} {this.props.model.targets.length == 1 ? "target" : "targets"}
            </div>
          )}
          {isBazelInvocation && (
            <div title={`${this.props.model.buildMetrics?.actionSummary.actionsCreated} created`} className="detail">
              <Activity className="icon" />
              {this.props.model.buildMetrics?.actionSummary.actionsExecuted} actions
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail">
              <Box className="icon" />
              {this.props.model.buildMetrics?.packageMetrics.packagesLoaded} packages
            </div>
          )}
          {isBazelInvocation && (
            <div
              className={this.props.model.getFetchURLs().length ? "detail clickable" : "detail"}
              onClick={this.handleFetchesClicked.bind(this)}>
              <DownloadCloud className="icon" />
              {this.props.model.getFetchURLs().length} fetches
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail">
              <Cpu className="icon" />
              {this.props.model.getCPU()}
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail">
              <Zap className="icon" />
              {this.props.model.getMode()}
            </div>
          )}
          {this.props.model.getRepo() && (
            <div className="detail clickable" onClick={this.handleRepoClicked.bind(this)}>
              <Github className="icon" />
              {format.formatGitUrl(this.props.model.getRepo())}
            </div>
          )}
          {this.props.model.getBranchName() && (
            <div className="detail clickable" onClick={this.handleBranchClicked.bind(this)}>
              <GitBranch className="icon" />
              {this.props.model.getBranchName()}
            </div>
          )}
          {this.props.model.getCommit() && (
            <div className="detail clickable" onClick={this.handleCommitClicked.bind(this)}>
              <GitCommit className="icon" />
              {format.formatCommitHash(this.props.model.getCommit())}
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail clickable" onClick={this.handleCacheClicked.bind(this)}>
              <Package className="icon" />
              {this.props.model.getCache()}
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail clickable" onClick={this.handleRBEClicked.bind(this)}>
              <Cloud className="icon" />
              {this.props.model.getRBE()}
            </div>
          )}
          {this.props.model.getBuildkiteUrl() && (
            <div className="detail clickable" onClick={this.handleBuildkiteClicked.bind(this)}>
              <img className="icon buildkite" src="/image/buildkite.svg" />
              Buildkite
            </div>
          )}
          {this.props.model.getLinks().map((link) => (
            <a className="detail clickable" href={link.linkUrl} target="_blank">
              <Link className="icon" />
              {link.linkText}
            </a>
          ))}
        </div>
      </div>
    );
  }
}
