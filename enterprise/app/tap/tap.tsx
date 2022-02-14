import React from "react";
import Long from "long";
import moment from "moment";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import { api, target } from "../../../proto/target_ts_proto";
import { Subscription } from "rxjs";
import router from "../../../app/router/router";
import format from "../../../app/format/format";
import Select, { Option } from "../../../app/components/select/select";
import { ChevronsRight, Filter } from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  repos: string[];
  targetHistory: target.ITargetHistory[];
  commits: string[] | null;
  commitToTargetIdToStatus: Map<string, Map<string, target.ITargetStatus>> | null;
  nextPageToken: string;
  loading: boolean;
  coloring: "status" | "timing";
  sort: "target" | "count" | "pass" | "avgDuration" | "maxDuration";
  direction: "asc" | "desc";
  targetLimit: number;
  invocationLimit: number;
  stats: Map<string, Stat>;
  maxInvocations: number;
  maxDuration: number;
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

export default class TapComponent extends React.Component<Props, State> {
  state: State = {
    repos: [],
    targetHistory: [],
    nextPageToken: "",
    commits: null,
    commitToTargetIdToStatus: null,
    loading: true,
    sort: "target",
    direction: "asc",
    coloring: "status",
    targetLimit: 100,
    invocationLimit: capabilities.config.testGridV2Enabled
      ? Number.MAX_SAFE_INTEGER
      : Math.min(100, Math.max(20, (window.innerWidth - 312) / 34)),
    stats: new Map<string, Stat>(),
    maxInvocations: 0,
    maxDuration: 1,
  };

  isV2 = capabilities.config.testGridV2Enabled;

  subscription: Subscription;

  componentWillMount() {
    document.title = `Tests | BuildBuddy`;
    this.updateColoring();

    this.fetchRepos().then(() => this.fetchTargets(/*initial=*/ true));

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetchTargets(/*initial=*/ true),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash) {
      this.updateColoring();
    }
    // Repo-changed; re-fetch targets starting from scratch.
    if (this.props.search !== prevProps.search) {
      this.fetchTargets(/*initial=*/ true);
    }
    localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY] = this.selectedRepo();
  }

  updateColoring() {
    this.setState({ coloring: this.props.hash == "#timing" ? "timing" : "status" });
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
    if (repo) return repo;

    const lastSelectedRepo = localStorage[LAST_SELECTED_REPO_LOCALSTORAGE_KEY];
    if (lastSelectedRepo) return lastSelectedRepo;

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
    rpcService.service.getTarget(request).then((response: target.GetTargetResponse) => {
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

  groupByCommit(targetHistories: target.ITargetHistory[]): Pick<State, "commits" | "commitToTargetIdToStatus"> {
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

    return { commits, commitToTargetIdToStatus };
  }

  navigateTo(event: any, destination: string) {
    router.navigateTo(destination);
    event.stopPropagation();
  }

  handleFilterChange(event: any) {
    router.replaceParams({ filter: event.target.value });
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

  handleSortChange(event: any) {
    this.setState({
      sort: event.target.value,
    });
  }

  handleDirectionChange(event: any) {
    this.setState({
      direction: event.target.value,
    });
  }

  handleColorChange(event: any) {
    window.location.hash = event?.target?.value;
  }

  sort(a: target.ITargetHistory, b: target.ITargetHistory) {
    let first = this.state.direction == "asc" ? a : b;
    let second = this.state.direction == "asc" ? b : a;

    let firstStats = this.state.stats.get(first?.target?.id);
    let secondStats = this.state.stats.get(second?.target?.id);

    switch (this.state.sort) {
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
        {this.isV2 && this.state.repos.length && (
          <Select onChange={this.handleRepoChange.bind(this)} value={this.selectedRepo()}>
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
              <p>
                Seems like you haven't done any builds marked as CI. Add <b>--build_metadata=ROLE=CI</b> to your builds
                to see a CI test grid. You'll likely also want to provide a commit SHA and optionally a git repo url.
                <br />
                <br />
                Check out the Build Metadata Guide below for more information on configuring these.
                <br />
                <br />
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

    const hasMoreInvocations = this.isV2
      ? Boolean(this.state.nextPageToken)
      : this.state.maxInvocations > this.state.invocationLimit;

    return (
      <div className="tap">
        <div className="tap-top-bar">
          <div className="container">
            <div className="tap-header">
              {headerLeftSection}

              <div className="controls">
                <div className="tap-sort-controls">
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Sort by</span>
                    <Select onChange={this.handleSortChange.bind(this)} value={this.state.sort}>
                      <Option value="target">Target name</Option>
                      <Option value="count">Invocation count</Option>
                      <Option value="pass">Pass percentage</Option>
                      <Option value="avgDuration">Average duration</Option>
                      <Option value="maxDuration">Max duration</Option>
                    </Select>
                  </div>
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Direction</span>
                    <Select onChange={this.handleDirectionChange.bind(this)} value={this.state.direction}>
                      <Option value="asc">Asc</Option>
                      <Option value="desc">Desc</Option>
                    </Select>
                  </div>
                  <div className="tap-sort-control">
                    <span className="tap-sort-title">Color</span>
                    <Select onChange={this.handleColorChange.bind(this)} value={this.state.coloring}>
                      <Option value="status">Status</Option>
                      <Option value="timing">Duration</Option>
                    </Select>
                  </div>
                </div>
              </div>
            </div>

            <div className="target-controls">
              <div className="filter">
                <Filter className="icon" />
                <input
                  value={filter}
                  className="filter-input"
                  placeholder="Filter..."
                  onChange={this.handleFilterChange.bind(this)}
                />
              </div>
              {hasMoreInvocations && (
                <FilledButton
                  className="more-invocations-button"
                  onClick={this.loadMoreInvocations.bind(this)}
                  disabled={this.state.loading}>
                  <span>Load more</span>
                  <ChevronsRight className="icon white" />
                </FilledButton>
              )}
            </div>
          </div>
        </div>

        <div className="container tap-grid-container">
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
                          this.state.coloring == "timing"
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
                                this.state.coloring == "timing"
                                  ? Math.max(
                                      MIN_OPACITY,
                                      (1.0 * this.durationToNum(status.timing.duration)) / stats.maxDuration
                                    )
                                  : undefined,
                            }}
                            className={`tap-block ${
                              this.state.coloring == "status" ? `status-${status.status}` : "timing"
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
