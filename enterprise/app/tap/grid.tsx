import React from "react";
import ReactDOM from "react-dom";
import Long from "long";
import moment from "moment";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { api } from "../../../proto/api/v1/common_ts_proto";
import { target } from "../../../proto/target_ts_proto";
import { Subscription } from "rxjs";
import router from "../../../app/router/router";
import { google as google_duration } from "../../../proto/duration_ts_proto";
import format from "../../../app/format/format";
import { clamp } from "../../../app/util/math";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Spinner from "../../../app/components/spinner/spinner";
import {
  Activity,
  AlarmClock,
  AlertTriangle,
  Check,
  ChevronsRight,
  Clock,
  Hammer,
  HelpCircle,
  SkipForward,
  Slash,
  Snowflake,
  Wrench,
  X,
} from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import {
  COLOR_MODE_PARAM,
  ColorMode,
  SORT_DIRECTION_PARAM,
  SORT_MODE_PARAM,
  SortDirection,
  SortMode,
} from "./grid_common";
import TapEmptyStateComponent from "./tap_empty_state";

interface Props {
  user: User;
  search: URLSearchParams;
  repo: string;
}

interface State extends CommitGrouping {
  targetHistory: target.TargetHistory[];
  nextPageToken: string;
  loading: boolean;
  targetLimit: number;
  invocationLimit: number;
  stats: Map<string, Stat>;
  maxInvocations: number;
  maxDuration: number;
}

interface CommitGrouping {
  commits: string[] | null;
  commitToMaxInvocationCreatedAtUsec: Map<string, number> | null;
  commitToTargetLabelToStatuses: Map<string, Map<string, target.ITargetStatus[]>> | null;
}

interface CommitStatus {
  commitSha: string;
  statuses: target.ITargetStatus[] | null;
}

interface Stat {
  count: number;
  pass: number;
  totalDuration: number;
  maxDuration: number;
  avgDuration: number;
  flake: number;
}

const Status = api.v1.Status;

const MIN_OPACITY = 0.1;
const DAYS_OF_DATA_TO_FETCH = 7;

export default class TestGridComponent extends React.Component<Props, State> {
  state: State = {
    targetHistory: [],
    nextPageToken: "",
    commits: null,
    commitToTargetLabelToStatuses: null,
    commitToMaxInvocationCreatedAtUsec: null,
    loading: true,
    targetLimit: 100,
    invocationLimit: capabilities.config.testGridV2Enabled
      ? Number.MAX_SAFE_INTEGER
      : Math.min(100, Math.max(20, (window.innerWidth - 312) / 34)),
    stats: new Map<string, Stat>(),
    maxInvocations: 0,
    maxDuration: 1,
  };

  isV2 = Boolean(capabilities.config.testGridV2Enabled);

  subscription?: Subscription;
  targetsRPC?: CancelablePromise;

  componentWillMount() {
    document.title = `Tests | BuildBuddy`;

    this.fetchTargets(/*initial=*/ true);

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetchTargets(/*initial=*/ true),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.repo !== prevProps.repo) {
      // Repo-changed; re-fetch targets starting from scratch.
      this.fetchTargets(/*initial=*/ true);
    }
  }

  /**
   * Fetches targets. If `initial`, clear any existing target history and fetch
   * from scratch. Otherwise, append the fetched history to the existing
   * history.
   */
  fetchTargets(initial: boolean) {
    this.targetsRPC?.cancel();
    this.setState({ loading: false });

    const repoUrl = this.props.repo;
    if (!repoUrl) {
      this.updateState(new target.GetTargetHistoryResponse(), initial);
      return;
    }

    let request = new target.GetTargetHistoryRequest();

    request.startTimeUsec = Long.fromNumber(moment().subtract(DAYS_OF_DATA_TO_FETCH, "day").utc().valueOf() * 1000);
    request.endTimeUsec = Long.fromNumber(moment().utc().valueOf() * 1000);
    request.serverSidePagination = this.isV2;
    request.pageToken = initial ? "" : this.state.nextPageToken;
    if (this.isV2) {
      request.query = target.TargetQuery.create({ repoUrl });
    }

    this.setState({ loading: true });
    this.targetsRPC = rpcService.service
      .getTargetHistory(request)
      .then((response) => {
        this.updateState(response, initial);
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  updateState(response: target.GetTargetHistoryResponse, initial: boolean) {
    this.state.stats.clear();

    let histories = response.invocationTargets;
    if (this.isV2 && !initial) {
      histories = mergeHistories(this.state.targetHistory, response.invocationTargets);
    }

    let maxInvocations = 0;
    let maxDuration = 1;
    for (let targetHistory of histories) {
      let stats: Stat = { count: 0, pass: 0, totalDuration: 0, maxDuration: 0, avgDuration: 0, flake: 0 };
      for (let status of targetHistory.targetStatus) {
        stats.count += 1;
        let duration = this.durationToNum(status.timing?.duration || undefined);
        stats.totalDuration += duration;
        stats.maxDuration = Math.max(stats.maxDuration, duration);
        if (status.status == Status.PASSED) {
          stats.pass += 1;
        } else if (status.status == Status.FLAKY) {
          stats.flake += 1;
        }
      }
      stats.avgDuration = stats.totalDuration / stats.count;
      maxInvocations = Math.max(maxInvocations, stats.count);
      maxDuration = Math.max(maxDuration, stats.maxDuration);
      this.state.stats.set(targetHistory.target?.label || "undefined", stats);
    }

    this.setState({
      loading: false,
      targetHistory: histories,
      nextPageToken: response.nextPageToken,
      stats: this.state.stats,
      maxInvocations: maxInvocations,
      maxDuration: maxDuration,
    });
    if (this.isV2) {
      this.setState({
        ...this.groupByCommit(histories),
      });
    }
  }

  groupByCommit(targetHistories: target.ITargetHistory[]): CommitGrouping {
    const commitToMaxInvocationCreatedAtUsec = new Map<string, number>();
    const commitToTargetLabelToStatuses = new Map<string, Map<string, target.ITargetStatus[]>>();

    for (const history of targetHistories) {
      for (const targetStatus of history.targetStatus || []) {
        const timestamp = Number(targetStatus.invocationCreatedAtUsec);
        const commitMaxTimestamp = commitToMaxInvocationCreatedAtUsec.get(targetStatus.commitSha) || 0;
        if (timestamp > commitMaxTimestamp) {
          commitToMaxInvocationCreatedAtUsec.set(targetStatus.commitSha, timestamp);
        }

        let targetLabelToStatus = commitToTargetLabelToStatuses.get(targetStatus.commitSha);
        if (!targetLabelToStatus) {
          targetLabelToStatus = new Map<string, target.ITargetStatus[]>();
          commitToTargetLabelToStatuses.set(targetStatus.commitSha, targetLabelToStatus);
        }

        if (history.target) {
          const existing = targetLabelToStatus.get(history.target!.label);
          if (existing) {
            existing.push(targetStatus);
          } else {
            targetLabelToStatus.set(history.target!.label, [targetStatus]);
          }
        }
      }
    }
    const commits = [...commitToMaxInvocationCreatedAtUsec.keys()].sort(
      (a, b) => commitToMaxInvocationCreatedAtUsec.get(b)! - commitToMaxInvocationCreatedAtUsec.get(a)!
    );

    return { commits, commitToMaxInvocationCreatedAtUsec, commitToTargetLabelToStatuses };
  }

  navigateTo(destination: string, event: React.MouseEvent) {
    event.preventDefault();
    router.navigateTo(destination);
  }

  handleFilterChange(event: React.ChangeEvent<HTMLInputElement>) {
    router.setQueryParam("filter", event.target.value);
  }

  durationToNum(duration?: google_duration.protobuf.Duration) {
    if (!duration) {
      return 0;
    }
    return +duration.seconds + +duration.nanos / 1000000000;
  }

  statusToString(s: api.v1.Status) {
    switch (s) {
      case Status.STATUS_UNSPECIFIED:
        return "Unknown";
      case Status.BUILDING:
        return "Building";
      case Status.BUILT:
        return "Built";
      case Status.FAILED_TO_BUILD:
        return "Failed to build";
      case Status.TESTING:
        return "Testing";
      case Status.PASSED:
        return "Passed";
      case Status.FAILED:
        return "Failed";
      case Status.TIMED_OUT:
        return "Timed out";
      case Status.CANCELLED:
        return "Cancelled";
      case Status.TOOL_FAILED:
        return "Tool failed";
      case Status.INCOMPLETE:
        return "Incomplete";
      case Status.FLAKY:
        return "Flaky";
      case Status.UNKNOWN:
        return "Unknown";
      case Status.SKIPPED:
        return "Skipped";
    }
  }

  statusToIcon(s: api.v1.Status) {
    switch (s) {
      case Status.STATUS_UNSPECIFIED:
        return <HelpCircle />;
      case Status.BUILDING:
        return <Clock />;
      case Status.BUILT:
        return <Hammer />;
      case Status.FAILED_TO_BUILD:
        return <AlertTriangle />;
      case Status.TESTING:
        return <Clock />;
      case Status.PASSED:
        return <Check />;
      case Status.FAILED:
        return <X />;
      case Status.TIMED_OUT:
        return <AlarmClock />;
      case Status.CANCELLED:
        return <Slash />;
      case Status.TOOL_FAILED:
        return <Wrench />;
      case Status.INCOMPLETE:
        return <Activity />;
      case Status.FLAKY:
        return <Snowflake />;
      case Status.UNKNOWN:
        return <HelpCircle />;
      case Status.SKIPPED:
        return <SkipForward />;
    }
  }

  loadMoreTargets() {
    this.setState({ targetLimit: this.state.targetLimit + 50 });
  }

  loadMoreInvocations() {
    if (this.isV2) {
      this.fetchTargets(/*initial=*/ false);
      return;
    }

    this.setState({ invocationLimit: this.state.invocationLimit + 50 });
  }

  handleSortChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam("sort", event.target.value);
  }

  handleDirectionChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam("direction", event.target.value);
  }

  handleColorChange(event: React.ChangeEvent<HTMLSelectElement>) {
    router.setQueryParam("color", event.target.value);
  }

  getSortMode(): SortMode {
    return (this.props.search.get(SORT_MODE_PARAM) as SortMode) || "pass";
  }

  getSortDirection(): SortDirection {
    return (this.props.search.get(SORT_DIRECTION_PARAM) as SortDirection) || "asc";
  }

  getColorMode(): ColorMode {
    return (this.props.search.get(COLOR_MODE_PARAM) as ColorMode) || "status";
  }

  sort(a: target.ITargetHistory, b: target.ITargetHistory) {
    let first = this.getSortDirection() == "asc" ? a : b;
    let second = this.getSortDirection() == "asc" ? b : a;

    let firstTarget = first.target;
    let secondTarget = second.target;

    if (this.getSortMode() == "target") {
      if (!firstTarget && !secondTarget) {
        return 0;
      } else if (!firstTarget) {
        return 1;
      } else if (!secondTarget) {
        return -1;
      } else {
        return firstTarget!.label.localeCompare(secondTarget!.label);
      }
    }

    let firstStats = this.state.stats.get(first?.target?.label || "");
    let secondStats = this.state.stats.get(second?.target?.label || "");

    if (!firstStats && !secondStats) {
      return 0;
    } else if (!firstStats) {
      return 1;
    } else if (!secondStats) {
      return -1;
    }

    switch (this.getSortMode()) {
      case "count":
        return firstStats!.count - secondStats!.count;
      case "pass":
        return firstStats!.pass / firstStats!.count - secondStats!.pass / secondStats!.count;
      case "avgDuration":
        return firstStats!.avgDuration - secondStats!.avgDuration;
      case "maxDuration":
        return firstStats!.maxDuration - secondStats!.maxDuration;
      case "flake":
        return firstStats!.flake / firstStats!.count - secondStats!.flake / secondStats!.count;
    }
    return 0;
  }

  getTargetStatuses(history: target.ITargetHistory): CommitStatus[] {
    if (!this.isV2) {
      return (
        history.targetStatus?.map((status) => ({ commitSha: status.commitSha, statuses: [status], count: 1 })) || []
      );
    }
    // For test grid V2, ignore the incoming history (for now) and use the indexes we
    // built to order by commit.
    return (
      this.state.commits?.map((commitSha) => ({
        commitSha,
        statuses: this.state.commitToTargetLabelToStatuses?.get(commitSha)?.get(history.target?.label || "") || null,
      })) || []
    );
  }

  render() {
    if (this.state.loading && !this.state.targetHistory?.length) {
      return <div className="loading"></div>;
    }

    if (!this.state.targetHistory.length) {
      return (
        <TapEmptyStateComponent
          title="No CI tests found in the last week!"
          message="To see a CI test grid, make sure your CI tests are configured as follows:"
          showV2Instructions={this.isV2}></TapEmptyStateComponent>
      );
    }

    let filter: string = this.props.search.get("filter") || "";

    const showMoreInvocationsButton = this.isV2
      ? this.state.nextPageToken
      : this.state.maxInvocations > this.state.invocationLimit;
    const moreInvocationsButton = (
      <>
        {showMoreInvocationsButton && (
          <FilledButton
            className="more-invocations-button"
            onClick={this.loadMoreInvocations.bind(this)}
            disabled={this.state.loading}>
            <span>Load more</span>
            {this.state.loading ? <Spinner className="white" /> : <ChevronsRight className="icon white" />}
          </FilledButton>
        )}
      </>
    );

    const filteredTargets = this.state.targetHistory
      .filter((targetHistory) => (filter ? targetHistory.target?.label.includes(filter) : true))
      .sort(this.sort.bind(this));

    return (
      <>
        <div className="container">
          <div className="target-controls">
            <FilterInput value={filter} placeholder="Filter..." onChange={this.handleFilterChange.bind(this)} />
            {!this.isV2 && moreInvocationsButton}
          </div>
        </div>

        <div className="container tap-grid-container">
          {this.isV2 && (
            <InnerTopBar
              commits={this.state.commits || []}
              commitToMaxInvocationCreatedAtUsec={
                this.state.commitToMaxInvocationCreatedAtUsec || new Map<string, number>()
              }
              moreInvocationsButton={moreInvocationsButton}
            />
          )}
          {filteredTargets.slice(0, this.state.targetLimit).map((targetHistory) => {
            let targetParts = targetHistory.target?.label.split(":") || [];
            let targetPath = targetParts.length > 0 ? targetParts[0] : "";
            let targetName = targetParts.length > 1 ? targetParts[1] : "";
            let stats = this.state.stats.get(targetHistory.target?.label || "");
            return (
              <React.Fragment key={targetHistory.target?.label || ""}>
                <div title={targetHistory.target?.ruleType || ""} className="tap-target-label">
                  <span className="tap-target-path">{targetPath}</span>:
                  <span className="tap-target-name">{targetName}</span>
                  <span className="tap-target-stats">
                    ({format.formatWithCommas(stats?.count || 0)} invocations,{" "}
                    {format.percent((stats?.pass || 0) / (stats?.count || Number.MAX_VALUE))}% pass,{" "}
                    {format.percent((stats?.flake || 0) / (stats?.count || Number.MAX_VALUE))}% flaky,{" "}
                    {format.durationSec(stats?.avgDuration || 0)} avg, {format.durationSec(stats?.maxDuration || 0)}{" "}
                    max)
                  </span>
                </div>
                <div className="tap-row">
                  {this.getTargetStatuses(targetHistory)
                    .slice(0, this.state.invocationLimit)
                    .map((commitStatus: CommitStatus) => {
                      let statuses = commitStatus.statuses?.sort(
                        (a, b) => Number(b.invocationCreatedAtUsec) - Number(a.invocationCreatedAtUsec)
                      );
                      if (!statuses || statuses.length == 0) {
                        // For V2, null means the target was not run for this commit.
                        return (
                          <div className="tap-commit-container">
                            <div
                              className="tap-block no-status"
                              title={
                                commitStatus.commitSha
                                  ? `Target status not reported at commit ${commitStatus.commitSha}`
                                  : "No status reported"
                              }
                            />
                          </div>
                        );
                      }
                      return (
                        <div className="tap-commit-container">
                          {statuses.map((status) => {
                            let destinationUrl = `/invocation/${status.invocationId}?${new URLSearchParams({
                              target: targetHistory.target?.label || "",
                              targetStatus: String(status),
                            })}`;
                            let title =
                              this.getColorMode() == "timing"
                                ? `${this.durationToNum(status.timing?.duration || undefined).toFixed(2)}s`
                                : this.statusToString(status.status || Status.STATUS_UNSPECIFIED);
                            if (this.isV2 && commitStatus.commitSha) {
                              title += ` at commit ${commitStatus.commitSha}`;
                            }

                            return (
                              <a
                                key={targetHistory.target?.label || "" + status.invocationId}
                                href={destinationUrl}
                                onClick={this.navigateTo.bind(this, destinationUrl)}
                                title={title}
                                style={{
                                  opacity:
                                    this.getColorMode() == "timing"
                                      ? Math.max(
                                          MIN_OPACITY,
                                          (1.0 * this.durationToNum(status.timing?.duration || undefined)) /
                                            (stats?.maxDuration || 1)
                                        )
                                      : undefined,
                                }}
                                className={`tap-block ${
                                  this.getColorMode() == "status" ? `status-${status.status}` : "timing"
                                } clickable`}>
                                {this.statusToIcon(status.status || Status.STATUS_UNSPECIFIED)}
                              </a>
                            );
                          })}
                        </div>
                      );
                    })}
                </div>
              </React.Fragment>
            );
          })}
          {filteredTargets.length > this.state.targetLimit && (
            <FilledButton className="more-targets-button" onClick={this.loadMoreTargets.bind(this)}>
              Load more targets
            </FilledButton>
          )}
        </div>
      </>
    );
  }
}

interface InnerTopBarProps {
  commits: string[];
  commitToMaxInvocationCreatedAtUsec: Map<string, number>;
  moreInvocationsButton: JSX.Element;
}

interface InnerTopBarState {
  hoveredCommit: CommitTimestamp | null;
}

interface CommitTimestamp {
  commitSha: string;
  timestampUsec: number;
}

/**
 * Top bar that sticks to the top of the scrollable area, containing the commit
 * timeline.
 */
class InnerTopBar extends React.Component<InnerTopBarProps, InnerTopBarState> {
  state: InnerTopBarState = {
    hoveredCommit: null,
  };

  private commitTimeline = React.createRef<HTMLDivElement>();
  private hoveredCommitRow = React.createRef<HTMLDivElement>();
  private hoveredCommitInfo = React.createRef<HTMLDivElement>();
  private hoveredCommitPointer = React.createRef<HTMLDivElement>();
  // TODO(bduffany): Use a generic tooltip component.
  private tooltipPortal?: HTMLDivElement;

  componentWillMount() {
    const tooltipPortal = document.createElement("div");
    tooltipPortal.style.position = "fixed";
    tooltipPortal.style.zIndex = "1";
    tooltipPortal.style.opacity = "0";
    document.body.appendChild(tooltipPortal);
    this.tooltipPortal = tooltipPortal;
  }

  componentWillUnmount() {
    this.tooltipPortal!.remove();
  }

  onMouseLeaveCommitTimeline(event: React.MouseEvent) {
    this.setState({ hoveredCommit: null });
    this.tooltipPortal!.style.opacity = "0";
  }

  onMouseOverCommit(commitSha: string, event: React.MouseEvent) {
    this.setState({
      hoveredCommit: {
        commitSha,
        timestampUsec: this.props.commitToMaxInvocationCreatedAtUsec.get(commitSha) || 0,
      },
    });

    const hoveredElement = event.target as HTMLElement;
    setTimeout(() => this.centerHoveredCommitWith(hoveredElement));
  }

  onClickCommitLink(event: React.MouseEvent) {
    event.preventDefault();
    const link = event.target as HTMLAnchorElement;
    if (link.getAttribute("href")) {
      router.navigateTo(link.getAttribute("href")!);
    }
  }

  centerHoveredCommitWith(element: HTMLElement) {
    const hoveredCommit = this.hoveredCommitInfo.current;
    if (!hoveredCommit) return;

    const hoveredCommitPointer = this.hoveredCommitPointer.current!;
    const hoveredCommitRow = this.hoveredCommitRow.current!;

    const screenCenterX = element.getBoundingClientRect().left + element.clientWidth / 2;
    const maxScreenLeftX = window.innerWidth - hoveredCommit.clientWidth;
    const screenLeftX = clamp(screenCenterX - hoveredCommit.clientWidth / 2, 0, maxScreenLeftX);
    const screenTop = hoveredCommitRow.getBoundingClientRect().top;
    const screenBottom = screenTop + hoveredCommitRow.clientHeight;

    hoveredCommit.style.left = `${screenLeftX}px`;
    hoveredCommit.style.top = `${screenTop}px`;
    hoveredCommitPointer.style.left = `${screenCenterX}px`;
    hoveredCommitPointer.style.top = `${screenBottom}px`;
    this.tooltipPortal!.style.opacity = "1";
  }

  render() {
    return (
      <>
        <div className="inner-top-bar-underlay" />
        <div className="commit-timeline-label">Commits (most recent first)</div>
        <div className="inner-top-bar">
          <div className="hovered-commit-row" ref={this.hoveredCommitRow}>
            {this.state.hoveredCommit &&
              ReactDOM.createPortal(
                <>
                  <div className="tap-hovered-commit-pointer" ref={this.hoveredCommitPointer} />
                  <div className="tap-hovered-commit-info" ref={this.hoveredCommitInfo}>
                    {format.formatTimestampUsec(this.state.hoveredCommit.timestampUsec)}, commit{" "}
                    {this.state.hoveredCommit.commitSha.substring(0, 6)}
                  </div>
                </>,
                this.tooltipPortal!
              )}
          </div>
          <div
            className="commit-timeline"
            onMouseLeave={this.onMouseLeaveCommitTimeline.bind(this)}
            ref={this.commitTimeline}>
            <div className="commits-list">
              {this.props.commits.map((commitSha) => (
                <a
                  className="commit-link"
                  href={`/history/commit/${commitSha}`}
                  onClick={this.onClickCommitLink.bind(this)}
                  onMouseOver={this.onMouseOverCommit.bind(this, commitSha)}
                />
              ))}
            </div>
            {this.props.moreInvocationsButton}
          </div>
        </div>
      </>
    );
  }
}

/**
 * Merges two lists of target histories into a single list with the joined target histories.
 *
 * This operation may mutate h1, so callers shouldn't rely on the value of h1 after this is
 * called.
 */
function mergeHistories(h1: target.TargetHistory[], h2: target.TargetHistory[]): target.TargetHistory[] {
  const targetLabelToHistory = new Map<string, target.TargetHistory>();
  for (const history of h1) {
    targetLabelToHistory.set(history.target?.label || "undefined", history);
  }
  for (const history of h2) {
    const h1History = targetLabelToHistory.get(history.target?.label || "undefined");
    if (!h1History) {
      targetLabelToHistory.set(history.target?.label || "undefined", history);
      h1.push(history);
      continue;
    }
    h1History.targetStatus = h1History.targetStatus.concat(history.targetStatus);
  }
  return h1;
}
