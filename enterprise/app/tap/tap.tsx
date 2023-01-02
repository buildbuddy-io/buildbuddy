import React from "react";
import ReactDOM from "react-dom";
import Long from "long";
import moment from "moment";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import { api, target } from "../../../proto/target_ts_proto";
import { Subscription } from "rxjs";
import router from "../../../app/router/router";
import format from "../../../app/format/format";
import { clamp } from "../../../app/util/math";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import { ChevronsRight } from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import { normalizeRepoURL } from "../../../app/util/git";

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State extends CommitGrouping {
  repos: string[];
  targetHistory: target.ITargetHistory[];
  nextPageToken: string;
  loading: boolean;
  targetLimit: number;
  invocationLimit: number;
  stats: Map<string, Stat>;
  maxInvocations: number;
  maxDuration: number;
}

type ColorMode = "status" | "timing";

type SortMode = "target" | "count" | "pass" | "avgDuration" | "maxDuration";

type SortDirection = "asc" | "desc";

interface CommitGrouping {
  commits: string[] | null;
  commitToMaxInvocationCreatedAtUsec: Map<string, number> | null;
  commitToTargetIdToStatus: Map<string, Map<string, target.ITargetStatus>> | null;
}

interface CommitStatus {
  commitSha: string;
  status: target.ITargetStatus | null;
}

interface Stat {
  count: number;
  pass: number;
  totalDuration: number;
  maxDuration: number;
  avgDuration: number;
}

const Status = api.v1.Status;

const MIN_OPACITY = 0.1;
const DAYS_OF_DATA_TO_FETCH = 7;
const LAST_SELECTED_REPO_LOCALSTORAGE_KEY = "tests__last_selected_repo";
const SORT_MODE_PARAM = "sort";
const SORT_DIRECTION_PARAM = "direction";
const COLOR_MODE_PARAM = "color";

export default class TapComponent extends React.Component<Props, State> {
  state: State = {
    repos: [],
    targetHistory: [],
    nextPageToken: "",
    commits: null,
    commitToTargetIdToStatus: null,
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

  subscription: Subscription;
  targetsRPC?: CancelablePromise;

  componentWillMount() {
    document.title = `Tests | BuildBuddy`;

    this.fetchRepos().then(() => this.fetchTargets(/*initial=*/ true));

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetchTargets(/*initial=*/ true),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search.get("repo") !== prevProps.search.get("repo")) {
      // Repo-changed; re-fetch targets starting from scratch.
      this.fetchTargets(/*initial=*/ true);
    }
    localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY] = this.selectedRepo();
  }

  fetchRepos(): Promise<void> {
    if (!this.isV2) return Promise.resolve();

    // If we've already got a repo selected (from the last time we visited the page),
    // keep the repo selected and populate the full repo list in the background.
    const selectedRepo = this.selectedRepo();
    if (selectedRepo) this.setState({ repos: [selectedRepo] });

    const fetchPromise = rpcService.service
      .getInvocationStat({
        aggregationType: invocation.AggType.REPO_URL_AGGREGATION_TYPE,
      })
      .then((response) => {
        const repos = response.invocationStat.filter((stat) => stat.name).map((stat) => stat.name);
        if (selectedRepo && !repos.includes(selectedRepo)) {
          repos.push(selectedRepo);
        }
        this.setState({ repos: repos.sort() });
      })
      .catch((e) => errorService.handleError(e));

    return selectedRepo ? Promise.resolve() : fetchPromise;
  }

  selectedRepo(): string {
    const repo = this.props.search.get("repo");
    if (repo) return normalizeRepoURL(repo);

    const lastSelectedRepo = localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY];
    if (lastSelectedRepo) return normalizeRepoURL(lastSelectedRepo);

    return this.state?.repos[0] || "";
  }

  /**
   * Fetches targets. If `initial`, clear any existing target history and fetch
   * from scratch. Otherwise, append the fetched history to the existing
   * history.
   */
  fetchTargets(initial: boolean) {
    let request = new target.GetTargetRequest();

    request.startTimeUsec = Long.fromNumber(moment().subtract(DAYS_OF_DATA_TO_FETCH, "day").utc().valueOf() * 1000);
    request.endTimeUsec = Long.fromNumber(moment().utc().valueOf() * 1000);
    request.serverSidePagination = this.isV2;
    request.pageToken = initial ? "" : this.state.nextPageToken;
    if (this.isV2) {
      request.query = { repoUrl: this.selectedRepo() };
    }

    this.setState({ loading: true });
    this.targetsRPC?.cancel();
    this.targetsRPC = rpcService.service.getTarget(request).then((response: target.GetTargetResponse) => {
      console.log(response);
      this.updateState(response, initial);
    });
  }

  updateState(response: target.GetTargetResponse, initial: boolean) {
    this.state.stats.clear();

    let histories = response.invocationTargets;
    if (this.isV2 && !initial) {
      histories = mergeHistories(this.state.targetHistory, response.invocationTargets);
    }

    let maxInvocations = 0;
    let maxDuration = 1;
    for (let targetHistory of histories) {
      let stats: Stat = { count: 0, pass: 0, totalDuration: 0, maxDuration: 0, avgDuration: 0 };
      for (let status of targetHistory.targetStatus) {
        stats.count += 1;
        let duration = this.durationToNum(status.timing.duration);
        stats.totalDuration += duration;
        stats.maxDuration = Math.max(stats.maxDuration, this.durationToNum(status.timing.duration));
        if (status.status == Status.PASSED) {
          stats.pass += 1;
        }
      }
      stats.avgDuration = stats.totalDuration / stats.count;
      maxInvocations = Math.max(maxInvocations, stats.count);
      maxDuration = Math.max(maxDuration, stats.maxDuration);
      this.state.stats.set(targetHistory.target.id, stats);
    }

    this.setState({
      loading: false,
      targetHistory: histories,
      nextPageToken: response.nextPageToken,
      ...(this.isV2 && this.groupByCommit(histories)),
      stats: this.state.stats,
      maxInvocations: maxInvocations,
      maxDuration: maxDuration,
    });
  }

  groupByCommit(targetHistories: target.ITargetHistory[]): CommitGrouping {
    const commitToMaxInvocationCreatedAtUsec = new Map<string, number>();
    const commitToTargetIdToStatus = new Map<string, Map<string, target.ITargetStatus>>();

    for (const history of targetHistories) {
      for (const targetStatus of history.targetStatus) {
        const timestamp = Number(targetStatus.invocationCreatedAtUsec);
        const commitMaxTimestamp = commitToMaxInvocationCreatedAtUsec.get(targetStatus.commitSha) || 0;
        if (timestamp > commitMaxTimestamp) {
          commitToMaxInvocationCreatedAtUsec.set(targetStatus.commitSha, timestamp);
        }

        let targetIdToStatus = commitToTargetIdToStatus.get(targetStatus.commitSha);
        if (!targetIdToStatus) {
          targetIdToStatus = new Map<string, target.ITargetStatus>();
          commitToTargetIdToStatus.set(targetStatus.commitSha, targetIdToStatus);
        }

        // For a given commit, the representative target status that
        // we show in the UI is the one whose corresponding invocation
        // was created latest.
        //
        // TODO(bduffany): Keep track of per-target count by commit, in
        // case the same target was executed multiple times for a given
        // commit. Otherwise the count stat looks incorrect.
        const existing = targetIdToStatus.get(history.target.id);
        if (!existing || timestamp > Number(existing.invocationCreatedAtUsec)) {
          targetIdToStatus.set(history.target.id, targetStatus);
        }
      }
    }
    const commits = [...commitToMaxInvocationCreatedAtUsec.keys()].sort(
      (a, b) => commitToMaxInvocationCreatedAtUsec.get(b) - commitToMaxInvocationCreatedAtUsec.get(a)
    );

    return { commits, commitToMaxInvocationCreatedAtUsec, commitToTargetIdToStatus };
  }

  navigateTo(destination: string, event: React.MouseEvent) {
    event.preventDefault();
    router.navigateTo(destination);
  }

  handleFilterChange(event: React.ChangeEvent<HTMLInputElement>) {
    router.setQueryParam("filter", event.target.value);
  }

  durationToNum(duration: any) {
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

  handleRepoChange(event: React.ChangeEvent<HTMLSelectElement>) {
    const repo = event.target.value;
    router.replaceParams({ repo });
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

    let firstStats = this.state.stats.get(first?.target?.id);
    let secondStats = this.state.stats.get(second?.target?.id);

    switch (this.getSortMode()) {
      case "target":
        return first?.target?.label.localeCompare(second?.target?.label);
      case "count":
        return firstStats.count - secondStats.count;
      case "pass":
        return firstStats.pass / firstStats.count - secondStats.pass / secondStats.count;
      case "avgDuration":
        return firstStats.avgDuration - secondStats.avgDuration;
      case "maxDuration":
        return firstStats.maxDuration - secondStats.maxDuration;
    }
  }

  getTargetStatuses(history: target.ITargetHistory): CommitStatus[] {
    if (!this.isV2) {
      return history.targetStatus.map((status) => ({ commitSha: status.commitSha, status }));
    }
    // For test grid V2, ignore the incoming history (for now) and use the indexes we
    // built to order by commit.
    return this.state.commits.map((commitSha) => ({
      commitSha,
      status: this.state.commitToTargetIdToStatus.get(commitSha)?.get(history.target.id) || null,
    }));
  }

  render() {
    if (this.state.loading && !this.state.targetHistory?.length) {
      return <div className="loading"></div>;
    }

    const headerLeftSection = (
      <div className="tap-header-left-section">
        <div className="tap-title">Test grid</div>
        {this.isV2 && this.state.repos.length > 0 && (
          <Select onChange={this.handleRepoChange.bind(this)} value={this.selectedRepo()} className="repo-picker">
            {this.state.repos.map((repo) => (
              <Option key={repo} value={repo}>
                {format.formatGitUrl(repo)}
              </Option>
            ))}
          </Select>
        )}
      </div>
    );

    if (!this.state.targetHistory.length) {
      return (
        <div className="tap">
          <div className="tap-top-bar">
            <div className="container narrow">
              <div className="tap-header">{headerLeftSection}</div>
            </div>
          </div>
          <div className="container narrow">
            <div className="empty-state history">
              <h2>No CI tests found in the last week!</h2>
              {this.isV2 ? (
                <p>
                  To see a CI test grid, make sure your CI tests are configured as follows:
                  <ul>
                    <li>
                      Add <code className="inline-code">--build_metadata=ROLE=CI</code> to your CI bazel test command.
                    </li>
                    <li>
                      Provide a{" "}
                      <a target="_blank" href="https://www.buildbuddy.io/docs/guide-metadata/#commit-sha">
                        commit SHA
                      </a>{" "}
                      and{" "}
                      <a target="_blank" href="https://www.buildbuddy.io/docs/guide-metadata/#repository-url">
                        repository URL
                      </a>
                      .
                    </li>
                  </ul>
                </p>
              ) : (
                <p>
                  Seems like you haven't done any builds marked as CI. Add <b>--build_metadata=ROLE=CI</b> to your
                  builds to see a CI test grid. You'll likely also want to provide a commit SHA and optionally a git
                  repo url.
                </p>
              )}
              <p>Check out the Build Metadata Guide below for more information on configuring these.</p>
              <p>
                <a className="button" target="_blank" href="https://buildbuddy.io/docs/guide-metadata">
                  View the Build Metadata Guide
                </a>
              </p>
            </div>
          </div>
        </div>
      );
    }

    let filter = this.props.search.get("filter");

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

    return (
      <div className={`tap ${this.isV2 ? "v2" : ""}`}>
        <div className="tap-top-bar">
          <div className="container">
            <div className="tap-header">
              {headerLeftSection}

              <div className="controls">
                <div className="tap-sort-controls">
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Sort by</span>
                    <Select onChange={this.handleSortChange.bind(this)} value={this.getSortMode()}>
                      <Option value="target">Target name</Option>
                      <Option value="count">Invocation count</Option>
                      <Option value="pass">Pass percentage</Option>
                      <Option value="avgDuration">Average duration</Option>
                      <Option value="maxDuration">Max duration</Option>
                    </Select>
                  </div>
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Direction</span>
                    <Select onChange={this.handleDirectionChange.bind(this)} value={this.getSortDirection()}>
                      <Option value="asc">Asc</Option>
                      <Option value="desc">Desc</Option>
                    </Select>
                  </div>
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Color</span>
                    <Select onChange={this.handleColorChange.bind(this)} value={this.getColorMode()}>
                      <Option value="status">Status</Option>
                      <Option value="timing">Duration</Option>
                    </Select>
                  </div>
                </div>
              </div>
            </div>

            <div className="target-controls">
              <FilterInput value={filter} placeholder="Filter..." onChange={this.handleFilterChange.bind(this)} />
              {!this.isV2 && moreInvocationsButton}
            </div>
          </div>
        </div>

        <div className="container tap-grid-container">
          {this.isV2 && (
            <InnerTopBar
              commits={this.state.commits}
              commitToMaxInvocationCreatedAtUsec={this.state.commitToMaxInvocationCreatedAtUsec}
              moreInvocationsButton={moreInvocationsButton}
            />
          )}
          {this.state.targetHistory
            .filter((targetHistory) => (filter ? targetHistory.target.label.includes(filter) : true))
            .sort(this.sort.bind(this))
            .slice(0, this.state.targetLimit)
            .map((targetHistory) => {
              let targetParts = targetHistory.target.label.split(":");
              let targetPath = targetParts.length > 0 ? targetParts[0] : "";
              let targetName = targetParts.length > 1 ? targetParts[1] : "";
              let stats = this.state.stats.get(targetHistory.target.id);
              return (
                <React.Fragment key={targetHistory.target.id}>
                  <div title={targetHistory.target.ruleType} className="tap-target-label">
                    <span className="tap-target-path">{targetPath}</span>:
                    <span className="tap-target-name">{targetName}</span>
                    <span className="tap-target-stats">
                      ({format.formatWithCommas(stats.count)} invocations, {format.percent(stats.pass / stats.count)}%
                      pass, {format.durationSec(stats.avgDuration)} avg, {format.durationSec(stats.maxDuration)} max)
                    </span>
                  </div>
                  <div className="tap-row">
                    {this.getTargetStatuses(targetHistory)
                      .slice(0, this.state.invocationLimit)
                      .map((commitStatus: CommitStatus) => {
                        const { commitSha, status } = commitStatus;
                        if (status === null) {
                          // For V2, null means the target was not run for this commit.
                          return (
                            <div
                              className="tap-block no-status"
                              title={
                                commitSha ? `Target status not reported at commit ${commitSha}` : "No status reported"
                              }
                            />
                          );
                        }

                        let destinationUrl = `/invocation/${status.invocationId}?target=${encodeURIComponent(
                          targetHistory.target.label
                        )}`;
                        let title =
                          this.getColorMode() == "timing"
                            ? `${this.durationToNum(status.timing.duration).toFixed(2)}s`
                            : this.statusToString(status.status);
                        if (this.isV2 && commitSha) {
                          title += ` at commit ${commitSha}`;
                        }
                        return (
                          <a
                            key={targetHistory.target.id + status.invocationId}
                            href={destinationUrl}
                            onClick={this.navigateTo.bind(this, destinationUrl)}
                            title={title}
                            style={{
                              opacity:
                                this.getColorMode() == "timing"
                                  ? Math.max(
                                      MIN_OPACITY,
                                      (1.0 * this.durationToNum(status.timing.duration)) / stats.maxDuration
                                    )
                                  : undefined,
                            }}
                            className={`tap-block ${
                              this.getColorMode() == "status" ? `status-${status.status}` : "timing"
                            } clickable`}></a>
                        );
                      })}
                  </div>
                </React.Fragment>
              );
            })}
          {this.state.targetHistory.length > this.state.targetLimit && (
            <FilledButton className="more-targets-button" onClick={this.loadMoreTargets.bind(this)}>
              Load more targets
            </FilledButton>
          )}
        </div>
      </div>
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
  private tooltipPortal: HTMLDivElement;

  componentWillMount() {
    const tooltipPortal = document.createElement("div");
    tooltipPortal.style.position = "fixed";
    tooltipPortal.style.zIndex = "1";
    tooltipPortal.style.opacity = "0";
    document.body.appendChild(tooltipPortal);
    this.tooltipPortal = tooltipPortal;
  }

  componentWillUnmount() {
    this.tooltipPortal.remove();
  }

  onMouseLeaveCommitTimeline(event: React.MouseEvent) {
    this.setState({ hoveredCommit: null });
    this.tooltipPortal.style.opacity = "0";
  }

  onMouseOverCommit(commitSha: string, event: React.MouseEvent) {
    this.setState({
      hoveredCommit: {
        commitSha,
        timestampUsec: this.props.commitToMaxInvocationCreatedAtUsec.get(commitSha),
      },
    });

    const hoveredElement = event.target as HTMLElement;
    setTimeout(() => this.centerHoveredCommitWith(hoveredElement));
  }

  onClickCommitLink(event: React.MouseEvent) {
    event.preventDefault();
    const link = event.target as HTMLAnchorElement;
    router.navigateTo(link.getAttribute("href"));
  }

  centerHoveredCommitWith(element: HTMLElement) {
    const hoveredCommit = this.hoveredCommitInfo.current;
    if (!hoveredCommit) return;

    const hoveredCommitPointer = this.hoveredCommitPointer.current!;

    const screenCenterX = element.getBoundingClientRect().left + element.clientWidth / 2;
    const maxScreenLeftX = window.innerWidth - hoveredCommit.clientWidth;
    const screenLeftX = clamp(screenCenterX - hoveredCommit.clientWidth / 2, 0, maxScreenLeftX);
    const screenTop = this.hoveredCommitRow.current.getBoundingClientRect().top;
    const screenBottom = screenTop + this.hoveredCommitRow.current.clientHeight;

    hoveredCommit.style.left = `${screenLeftX}px`;
    hoveredCommit.style.top = `${screenTop}px`;
    hoveredCommitPointer.style.left = `${screenCenterX}px`;
    hoveredCommitPointer.style.top = `${screenBottom}px`;
    this.tooltipPortal.style.opacity = "1";
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
                this.tooltipPortal
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
function mergeHistories(h1: target.ITargetHistory[], h2: target.ITargetHistory[]): target.ITargetHistory[] {
  const targetIdToHistory = new Map<string, target.ITargetHistory>();
  for (const history of h1) {
    targetIdToHistory.set(history.target.id, history);
  }
  for (const history of h2) {
    const h1History = targetIdToHistory.get(history.target.id);
    if (!h1History) {
      targetIdToHistory.set(history.target.id, history);
      h1.push(history);
      continue;
    }
    h1History.targetStatus = h1History.targetStatus.concat(history.targetStatus);
  }
  return h1;
}
