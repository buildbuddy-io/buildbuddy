import React from "react";
import { target } from "../../proto/target_ts_proto";
import { api as api_common } from "../../proto/api/v1/common_ts_proto";
import {
  ArrowDownCircle,
  CheckCircle,
  ChevronRight,
  Clock,
  Copy,
  FileCode,
  HelpCircle,
  SkipForward,
  XCircle,
} from "lucide-react";
import { copyToClipboard } from "../util/clipboard";
import { renderDuration, renderTestSize } from "./target_util";
import Link from "../components/link/link";
import rpc_service, { CancelablePromise } from "../service/rpc_service";
import error_service from "../errors/error_service";
import Spinner from "../components/spinner/spinner";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import format from "../format/format";

export interface TargetGroupCardProps {
  invocationId: string;
  group: target.TargetGroup;
  filter: string;
}

interface State {
  loading: boolean;
  fetchedTargets: target.Target[];
  nextPageToken: string | null;
}

const Status = api_common.v1.Status;

/**
 * Renders a single `target.TargetGroup`, with the ability to fetch more pages
 * in the group.
 */
export default class TargetGroupCard extends React.Component<TargetGroupCardProps, State> {
  state: State = {
    loading: false,
    fetchedTargets: [],
    nextPageToken: null,
  };

  private fetchRPC?: CancelablePromise;

  private getTargetURL(target: target.Target) {
    return `?${new URLSearchParams({
      target: target.metadata?.label ?? "",
      targetStatus: String(target.status),
    })}`;
  }

  private nextPageToken() {
    return this.state.nextPageToken === null ? this.props.group.nextPageToken : this.state.nextPageToken;
  }

  private hasMoreTargets() {
    return Boolean(this.nextPageToken());
  }

  private loadMore() {
    this.fetchRPC?.cancel();
    this.setState({ loading: true });
    rpc_service.service
      .getTarget({
        invocationId: this.props.invocationId,
        status: this.props.group.status,
        pageToken: this.nextPageToken(),
        filter: this.props.filter,
      })
      .then((response) => {
        const page = response.targetGroups[0];
        if (!page) return;
        this.setState({
          fetchedTargets: [...this.state.fetchedTargets, ...page.targets],
          nextPageToken: page.nextPageToken,
        });
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onClickFile(event: React.MouseEvent<HTMLAnchorElement>, file: build_event_stream.File) {
    event.preventDefault();
    if (!file.uri) return false;

    if (file.uri.startsWith("file://")) {
      window.prompt("Copy artifact path to clipboard: Cmd+C, Enter", file.uri);
    } else if (file.uri.startsWith("bytestream://")) {
      rpc_service.downloadBytestreamFile(file.name, file.uri, this.props.invocationId);
    }
  }

  private getCodeURL(file: build_event_stream.File): string {
    return `/code/buildbuddy-io/buildbuddy/?${new URLSearchParams({
      invocation_id: this.props.invocationId,
      bytestream_url: file.uri,
      filename: file.name,
    })}`;
  }

  render() {
    let targets = this.props.group.targets.concat(this.state.fetchedTargets);
    let className = "";
    let icon: React.ReactNode = null;
    let presentVerb = "";
    let pastVerb = "";
    switch (this.props.group.status) {
      case 0:
        // Showing the target listing only.
        className = "artifacts";
        icon = <ArrowDownCircle className="icon brown" />;
        presentVerb = `${targets.length === 1 ? "target" : "targets"} with artifacts`;
        pastVerb = presentVerb;
        break;
      case Status.FAILED:
        className = "card-failure";
        icon = <XCircle className="icon red" />;
        presentVerb = `failing ${targets.length === 1 ? "test" : "tests"}`;
        pastVerb = `${targets.length === 1 ? "test" : "tests"} failed`;
        break;
      case Status.FAILED_TO_BUILD:
        className = "card-failure";
        icon = <XCircle className="icon red" />;
        presentVerb = `${targets.length === 1 ? "target" : "targets"} failed to build`;
        pastVerb = `${targets.length === 1 ? "target" : "targets"} failed to build`;
        break;
      case Status.TIMED_OUT:
        className = "card-timeout";
        icon = <Clock className="icon" />;
        presentVerb = `timed out ${targets.length == 1 ? "test" : "tests"}`;
        pastVerb = `${targets.length == 1 ? "test" : "tests"} timed out`;
        break;
      case Status.FLAKY:
        className = "card-flaky";
        icon = <HelpCircle className="icon orange" />;
        presentVerb = `flaky ${targets.length == 1 ? "test" : "tests"}`;
        pastVerb = `flaky ${targets.length == 1 ? "test" : "tests"}`;
        break;
      case Status.PASSED:
        className = "card-success";
        icon = <CheckCircle className="icon green" />;
        presentVerb = `passing ${targets.length == 1 ? "test" : "tests"}`;
        pastVerb = `${targets.length == 1 ? "test" : "tests"} passed`;
        break;
      case Status.BUILT:
        className = "card-success";
        icon = <CheckCircle className="icon green" />;
        presentVerb = `${targets.length == 1 ? "target" : "targets"}`;
        pastVerb = `${targets.length == 1 ? "target" : "targets"} built successfully`;
        break;
      case Status.SKIPPED:
        className = "card-skipped";
        icon = <SkipForward className="icon purple" />;
        presentVerb = `${targets.length == 1 ? "target" : "targets"}`;
        pastVerb = `${targets.length == 1 ? "target" : "targets"} skipped`;
        break;
      default:
        console.error("Unsupported status", Status[this.props.group.status]);
        return null;
    }

    return (
      <div className={`card ${className} invocation-targets-card target-group-card`}>
        <div className="icon">{icon}</div>
        <div className="content">
          <div className="title">
            {format.formatWithCommas(this.props.group.totalCount)}
            {this.props.filter ? " matching" : ""} {pastVerb}{" "}
            <Copy
              className="copy-icon"
              onClick={() => copyToClipboard(targets.map((target) => target.metadata?.label ?? "").join(" "))}
            />
          </div>
          <div className="details">
            {this.props.group.status !== 0 && (
              <div className="targets-table">
                {targets.map((target) => (
                  <Link className="target-row" href={this.getTargetURL(target)}>
                    <div title={targetTitleAttr(target)} className="target">
                      <span className="target-status-icon">{icon}</span>{" "}
                      <span className="chevron-icon">
                        <ChevronRight className="icon" />
                      </span>
                      <span className="target-label">{target.metadata?.label}</span>{" "}
                      {target.rootCause && <span className="root-cause-badge">Root cause</span>}
                    </div>
                    <div className="target-duration">{!!target.timing?.duration && renderDuration(target.timing)}</div>
                  </Link>
                ))}
              </div>
            )}
            {this.props.group.status === 0 &&
              targets.map((target) => (
                <div>
                  <div className="artifact-section-title">{target.metadata?.label}</div>
                  <div className="artifact-list">
                    {target.files.map((output) => (
                      <div className="artifact-line">
                        <a
                          href={rpc_service.getBytestreamUrl(output.uri, this.props.invocationId, {
                            filename: output.name,
                          })}
                          className="artifact-name"
                          onClick={(event) => this.onClickFile(event, output)}>
                          {output.name}
                        </a>
                        {output.uri?.startsWith("bytestream://") && (
                          <a className="artifact-view" href={this.getCodeURL(output)}>
                            <FileCode className="icon" /> View
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              ))}
          </div>
          {this.hasMoreTargets() && !this.state.loading && (
            <div className="more" onClick={() => this.loadMore()}>
              Load more {presentVerb}
            </div>
          )}
          {this.state.loading && (
            <div className="more-loading">
              Load more {presentVerb} <Spinner className="small-spinner" />
            </div>
          )}
        </div>
      </div>
    );
  }
}

function targetTitleAttr(target: target.Target) {
  return [target.metadata?.ruleType ?? "", renderTestSize(target.metadata?.testSize || 0)].filter((x) => x).join(" | ");
}
