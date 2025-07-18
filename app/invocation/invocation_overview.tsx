import {
  Activity,
  Box,
  Clock,
  Cloud,
  Cpu,
  DownloadCloud,
  GitBranch,
  GitCommit,
  Github,
  GitPullRequest,
  HardDrive,
  LayoutGrid,
  Link as LinkIcon,
  Package,
  Tag,
  Target,
  Terminal,
  User as UserIcon,
  Wrench,
  Zap,
} from "lucide-react";
import React from "react";
import { User } from "../auth/auth_service";
import { Link } from "../components/link/link";
import format from "../format/format";
import { getRepoUrlPathParam, Path } from "../router/router";
import { RepoURL } from "../util/git";
import InvocationButtons from "./invocation_buttons";
import InvocationModel from "./invocation_model";

interface Props {
  model: InvocationModel;
  user?: User;
}
export default class InvocationOverviewComponent extends React.Component<Props> {
  invocationType() {
    if (this.props.model.isWorkflowInvocation()) {
      return "Workflow";
    }
    if (this.props.model.isHostedBazelInvocation()) {
      return "Remote Bazel";
    }
    return "Invocation";
  }

  render() {
    const ownerGroup = this.props.model.findOwnerGroup(this.props.user?.groups);
    const isBazelInvocation = this.props.model.isBazelInvocation();
    const roleLabel = format.formatRole(this.props.model.getRole());
    const parentInvocationId = this.props.model.buildMetadataMap.get("PARENT_INVOCATION_ID");
    const parentWorkflowId = this.props.model.buildMetadataMap.get("WORKFLOW_ID");

    return (
      <div className="container">
        <div className="breadcrumbs-and-buttons">
          <div className="breadcrumbs">
            {this.props.user && ownerGroup && (
              <>
                <Link href="/">{ownerGroup.name}</Link>
                <Link href="/">Builds</Link>
              </>
            )}
            {parentInvocationId && (
              <Link href={`/invocation/${parentInvocationId}`}>
                {parentWorkflowId ? "Workflow" : "Bazel invocation"} {parentInvocationId}
              </Link>
            )}
            <span>
              {this.invocationType()} {this.props.model.getInvocationId()}
            </span>
          </div>
          <InvocationButtons model={this.props.model} user={this.props.user} />
        </div>
        <div className="titles">
          {(this.props.model.isBazelInvocation() || this.props.model.isHostedBazelInvocation()) && (
            <div className="title" title={this.props.model.getAllPatterns()}>
              {this.props.model.getUserPossessivePrefix()}
              {this.props.model.getCommand()} {this.props.model.getPattern()}
            </div>
          )}
          {this.props.model.workflowConfigured && (
            <div className="title">{this.props.model.workflowConfigured.actionName}</div>
          )}
          {roleLabel && <div className={`role-badge ${this.props.model.getRole()}`}>{roleLabel}</div>}
          <div className="subtitle">{this.props.model.getFormattedStartedDate()}</div>
        </div>
        <div debug-id="invocation-details" className="details">
          <div className="detail">
            {this.props.model.getStatusIcon()}
            {this.props.model.getStatus()}
          </div>
          <div className="detail" title={this.props.model.getDurationSeconds()}>
            <Clock className="icon" />
            {this.props.model.getTiming()}
          </div>
          {isBazelInvocation && !!this.props.model.getUser() && (
            <Link className="detail clickable" href={Path.userHistoryPath + this.props.model.getUser()}>
              <UserIcon className="icon" />
              {this.props.model.getUser()}
            </Link>
          )}
          {isBazelInvocation && !!this.props.model.getHost() && (
            <Link className="detail clickable" href={Path.hostHistoryPath + this.props.model.getHost()}>
              <HardDrive className="icon" />
              {this.props.model.getHost()}
            </Link>
          )}
          {isBazelInvocation && (
            <div className="detail">
              <Wrench className="icon" />
              {this.props.model.getTool()}
            </div>
          )}
          {this.props.model.getToolTag() && (
            <div className="detail">
              <Terminal className="icon" />
              {this.props.model.getToolTag()}
            </div>
          )}
          {isBazelInvocation && (
            <div className="detail" title={this.props.model.getAllPatterns()}>
              <LayoutGrid className="icon" />
              {this.props.model.getPattern()}
            </div>
          )}
          {isBazelInvocation && (
            <div
              className="detail"
              title={`${this.props.model.buildMetrics?.targetMetrics?.targetsConfigured} configured`}>
              <Target className="icon" />
              {format.formatWithCommas(this.props.model.getTargetConfiguredCount())}{" "}
              {this.props.model.getTargetConfiguredCount() == 1 ? "target" : "targets"}
            </div>
          )}
          {isBazelInvocation && Boolean(this.props.model.buildMetrics?.actionSummary) && (
            <div title={`${this.props.model.buildMetrics?.actionSummary?.actionsCreated} created`} className="detail">
              <Activity className="icon" />
              {format.formatWithCommas(this.props.model.buildMetrics?.actionSummary?.actionsExecuted)} actions
            </div>
          )}
          {isBazelInvocation && Boolean(this.props.model.buildMetrics?.packageMetrics) && (
            <div className="detail">
              <Box className="icon" />
              {format.formatWithCommas(this.props.model.buildMetrics?.packageMetrics?.packagesLoaded)} packages
            </div>
          )}
          {isBazelInvocation && (
            <Link className={this.props.model.getFetchURLs().length ? "detail clickable" : "detail"} href={"#fetches"}>
              <DownloadCloud className="icon" />
              {format.formatWithCommas(this.props.model.getFetchURLs().length)} fetches
            </Link>
          )}
          <div className="detail">
            <Cpu className="icon" />
            {this.props.model.getCPU()}
          </div>
          {isBazelInvocation && (
            <div className="detail">
              <Zap className="icon" />
              {this.props.model.getMode()}
            </div>
          )}
          {this.props.model.getRepo() && (
            <Link
              className="detail clickable"
              href={Path.repoHistoryPath + getRepoUrlPathParam(this.props.model.getRepo())}>
              <Github className="icon" />
              {format.formatGitUrl(this.props.model.getRepo())}
            </Link>
          )}
          {this.props.model.getRepo() && this.props.model.getPullRequestNumber() && (
            <Link
              className="detail"
              href={RepoURL.parse(this.props.model.getRepo())?.pullRequestLink(
                this.props.model.getPullRequestNumber()!
              )}>
              <GitPullRequest className="icon" />#{this.props.model.getPullRequestNumber()}
            </Link>
          )}
          {/* For branches that aren't in forked repos, show a link to the branch history. */}
          {this.props.model.getBranchName() && !this.props.model.getForkRepoURL() && (
            <Link className="detail clickable" href={Path.branchHistoryPath + this.props.model.getBranchName()}>
              <GitBranch className="icon" />
              {this.props.model.getBranchName()}
            </Link>
          )}
          {/* For branches in forked repos, just render "{forkName}:{branchName}" */}
          {this.props.model.getBranchName() && this.props.model.getForkRepoURL() && (
            <div className="detail">
              <GitBranch className="icon" />
              {RepoURL.parse(this.props.model.getForkRepoURL()!)?.owner}:{this.props.model.getBranchName()!}
            </div>
          )}
          {this.props.model.getCommit() && (
            <Link className="detail clickable" href={Path.commitHistoryPath + this.props.model.getCommit()}>
              <GitCommit className="icon" />
              {format.formatCommitHash(this.props.model.getCommit())}
            </Link>
          )}
          {isBazelInvocation && (
            <Link className="detail clickable" href={Path.setupPath}>
              <Package className="icon" />
              {this.props.model.getCache()}
            </Link>
          )}
          {isBazelInvocation && (
            <Link
              className="detail clickable"
              href={this.props.model.getIsRBEEnabled() ? "#execution" : Path.setupPath}>
              <Cloud className="icon" />
              {this.props.model.getRBE()}
            </Link>
          )}
          {this.props.model.getBuildkiteUrl() && (
            <Link className="detail clickable" href={this.props.model.getBuildkiteUrl()} target="_blank">
              <img className="icon buildkite" src="/image/buildkite.svg" />
              Buildkite
            </Link>
          )}
          {this.props.model.getLinks().map((link) => (
            <Link className="detail clickable" href={link.linkUrl} target="_blank">
              <LinkIcon className="icon" />
              {link.linkText}
            </Link>
          ))}
          {this.props.model.getTags().map((tag) => (
            <Link className="detail clickable" href={Path.home + "?tag=" + tag.name}>
              <Tag className="icon" />
              {tag.name}
            </Link>
          ))}
        </div>
      </div>
    );
  }
}
