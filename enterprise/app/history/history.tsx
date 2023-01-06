import React from "react";
import { fromEvent, Subscription } from "rxjs";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import Button from "../../../app/components/button/button";
import LinkButton from "../../../app/components/button/link_button";
import { Tooltip } from "../../../app/components/tooltip/tooltip";
import Link from "../../../app/components/link/link";
import format from "../../../app/format/format";
import router, { ROLE_PARAM_NAME } from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import FilterComponent from "../filter/filter";
import OrgJoinRequestsComponent from "../org/org_join_requests";
import HistoryInvocationCardComponent from "./history_invocation_card";
import HistoryInvocationStatCardComponent from "./history_invocation_stat_card";
import { ProtoFilterParams, getProtoFilterParams } from "../filter/filter_util";
import Spinner from "../../../app/components/spinner/spinner";
import Long from "long";
import { BarChart2, CheckCircle, Clock, GitCommit, Github, Hash, Percent, XCircle } from "lucide-react";

interface State {
  /**
   * Invocations corresponding to individual invocation cards.
   * Not fetched for aggregate (sliced) views.
   */
  invocations?: invocation.IInvocation[];
  loadingInvocations?: boolean;
  /**
   * Stats summarizing the fetched invocations.
   * Not fetched for aggregate (sliced) views.
   */
  summaryStat?: invocation.IInvocationStat;
  loadingSummaryStat?: boolean;

  /**
   * Stats fetched for aggregate views.
   * Each stat corresponds to a card displaying the stats for a single repo (or user, etc.)
   */
  aggregateStats?: invocation.IInvocationStat[];
  loadingAggregateStats?: boolean;

  hoveredInvocationId?: string;
  pageToken?: string;
  invocationIdToCompare?: string;
}

interface Props {
  hostname?: string;
  username?: string;
  repo?: string;
  branch?: string;
  commit?: string;
  user?: User;
  search: URLSearchParams;
  hash: string;
}

export default class HistoryComponent extends React.Component<Props, State> {
  state: State = {
    invocationIdToCompare: localStorage["invocation_id_to_compare"],
  };

  refreshSubscription = new Subscription();

  invocationsRpc: CancelablePromise;
  summaryStatRpc: CancelablePromise;
  aggregateStatsRpc: CancelablePromise;

  hashToAggregationTypeMap = new Map<string, invocation.AggType>([
    ["#users", invocation.AggType.USER_AGGREGATION_TYPE],
    ["#hosts", invocation.AggType.HOSTNAME_AGGREGATION_TYPE],
    ["#repos", invocation.AggType.REPO_URL_AGGREGATION_TYPE],
    ["#branches", invocation.AggType.BRANCH_AGGREGATION_TYPE],
    ["#commits", invocation.AggType.COMMIT_SHA_AGGREGATION_TYPE],
  ]);

  private isFilteredToWorkflows() {
    return this.props.search?.get(ROLE_PARAM_NAME) === "CI_RUNNER";
  }

  private getSortField(filterParams: ProtoFilterParams) {
    if (filterParams.sortBy === "start-time") {
      return invocation.InvocationSort.SortField.CREATED_AT_USEC_SORT_FIELD;
    } else if (filterParams.sortBy === "end-time") {
      return invocation.InvocationSort.SortField.UPDATED_AT_USEC_SORT_FIELD;
    } else if (filterParams.sortBy === "duration") {
      return invocation.InvocationSort.SortField.DURATION_SORT_FIELD;
    } else if (filterParams.sortBy === "ac-hit-ratio") {
      return invocation.InvocationSort.SortField.ACTION_CACHE_HIT_RATIO_SORT_FIELD;
    } else if (filterParams.sortBy === "cas-hit-ratio") {
      return invocation.InvocationSort.SortField.CONTENT_ADDRESSABLE_STORE_CACHE_HIT_RATIO_SORT_FIELD;
    } else if (filterParams.sortBy === "cache-down") {
      return invocation.InvocationSort.SortField.CACHE_DOWNLOADED_SORT_FIELD;
    } else if (filterParams.sortBy === "cache-up") {
      return invocation.InvocationSort.SortField.CACHE_UPLOADED_SORT_FIELD;
    } else if (filterParams.sortBy === "cache-xfer") {
      return invocation.InvocationSort.SortField.CACHE_TRANSFERRED_SORT_FIELD;
    }
    return invocation.InvocationSort.SortField.UNKNOWN_SORT_FIELD;
  }

  getInvocations(nextPage?: boolean) {
    this.setState({
      loadingInvocations: true,
    });
    if (!nextPage) {
      this.setState({ invocations: undefined, pageToken: undefined });
    }

    const filterParams = getProtoFilterParams(this.props.search);
    let request = new invocation.SearchInvocationRequest({
      query: new invocation.InvocationQuery({
        host: this.props.hostname || filterParams.host,
        user: this.props.username || filterParams.user,
        repoUrl: this.props.repo || filterParams.repo,
        branchName: this.props.branch || filterParams.branch,
        commitSha: this.props.commit || filterParams.commit,
        command: filterParams.command,
        groupId: this.props.user?.selectedGroup?.id,
        minimumDuration: {
          seconds: new Long(parseInt(filterParams.minimumDuration || "0")),
        },
        maximumDuration: {
          seconds: new Long(parseInt(filterParams.maximumDuration || "0")),
        },
      }),
      sort: new invocation.InvocationSort({
        sortField: this.getSortField(filterParams),
        ascending: filterParams.sortOrder === "asc",
      }),
      pageToken: nextPage ? this.state.pageToken : "",
      // TODO(siggisim): This gives us 2 nice rows of 63 blocks each. Handle this better.
      count: 126,
    });
    if (capabilities.globalFilter) {
      request.query.role = filterParams.role;
      request.query.updatedAfter = filterParams.updatedAfter;
      request.query.updatedBefore = filterParams.updatedBefore;
      request.query.status = filterParams.status;
    }

    this.invocationsRpc = rpcService.service
      .searchInvocation(request)
      .then((response) => {
        console.log(response);
        this.setState({
          invocations: nextPage
            ? this.state.invocations.concat(response.invocation as invocation.Invocation[])
            : response.invocation,
          pageToken: response.nextPageToken,
        });
      })
      .finally(() => this.setState({ loadingInvocations: false }));
  }

  getAggregateStats() {
    this.setState({ aggregateStats: undefined, loadingAggregateStats: true });

    const aggregationType = this.hashToAggregationTypeMap.get(this.props.hash);
    const request = new invocation.GetInvocationStatRequest({ aggregationType });
    if (capabilities.globalFilter) {
      const filterParams = getProtoFilterParams(this.props.search);
      request.query = new invocation.InvocationStatQuery({
        host: this.props.hostname || filterParams.host,
        user: this.props.username || filterParams.user,
        repoUrl: this.props.repo || filterParams.repo,
        branchName: this.props.branch || filterParams.branch,
        commitSha: this.props.commit || filterParams.commit,
        command: filterParams.command,

        role: filterParams.role,
        updatedBefore: filterParams.updatedBefore,
        updatedAfter: filterParams.updatedAfter,
        status: filterParams.status,
      });
    }

    this.aggregateStatsRpc = rpcService.service
      .getInvocationStat(request)
      .then((response) => {
        console.log(response);
        this.setState({ aggregateStats: response.invocationStat.filter((stat) => stat.name) });
      })
      .finally(() => this.setState({ loadingAggregateStats: false }));
  }

  getSummaryStat() {
    this.setState({ summaryStat: undefined, loadingSummaryStat: true });

    const filterParams = getProtoFilterParams(this.props.search);
    const request = new invocation.GetInvocationStatRequest({
      aggregationType: invocation.AggType.GROUP_ID_AGGREGATION_TYPE,
    });
    if (capabilities.globalFilter) {
      request.query = new invocation.InvocationQuery({
        host: this.props.hostname || filterParams.host,
        user: this.props.username || filterParams.user,
        repoUrl: this.props.repo || filterParams.repo,
        branchName: this.props.branch || filterParams.branch,
        commitSha: this.props.commit || filterParams.commit,
        command: filterParams.command,

        role: filterParams.role,
        updatedAfter: filterParams.updatedAfter,
        updatedBefore: filterParams.updatedBefore,
        status: filterParams.status,
      });
    }

    this.summaryStatRpc = rpcService.service
      .getInvocationStat(request)
      .then((response) => this.setState({ summaryStat: response.invocationStat?.[0] }))
      .finally(() => this.setState({ loadingSummaryStat: false }));
  }

  componentDidMount() {
    document.title = `${
      this.props.username ||
      this.props.hostname ||
      format.formatGitUrl(this.props.repo) ||
      this.props.branch ||
      format.formatCommitHash(this.props.commit) ||
      this.props.user?.selectedGroupName()
    } Build History | BuildBuddy`;

    this.refreshSubscription.add(
      rpcService.events.subscribe({
        next: (name) => name == "refresh" && this.handleSidebarItemClicked(),
      })
    );
    this.refreshSubscription.add(fromEvent(window, "storage").subscribe(this.handleStorage.bind(this)));

    this.fetch();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash || this.props.search !== prevProps.search) {
      this.fetch();
    }
  }

  componentWillUnmount() {
    this.refreshSubscription.unsubscribe();
  }

  fetch() {
    // Cancel any in-flight RPC callbacks.
    this.invocationsRpc?.cancel();
    this.summaryStatRpc?.cancel();
    this.aggregateStatsRpc?.cancel();

    this.setState({
      invocations: undefined,
      summaryStat: undefined,
      aggregateStats: undefined,
      pageToken: undefined,
    });

    if (this.isAggregateView()) {
      this.getAggregateStats();
    } else {
      this.getSummaryStat();
      this.getInvocations();
    }
  }

  handleStorage() {
    this.setState({ invocationIdToCompare: localStorage["invocation_id_to_compare"] });
  }

  handleSidebarItemClicked() {
    if (this.props.username) {
      this.handleUsersClicked();
      return;
    }
    if (this.props.hostname) {
      this.handleHostsClicked();
      return;
    }
    if (this.props.commit) {
      this.handleCommitsClicked();
      return;
    }
    if (this.props.repo) {
      this.handleReposClicked();
      return;
    }

    this.fetch();
  }

  handleOrganizationClicked() {
    router.navigateHome();
  }

  handleUsersClicked() {
    router.navigateHome("#users");
  }

  handleHostsClicked() {
    router.navigateHome("#hosts");
  }

  handleReposClicked() {
    router.navigateHome("#repos");
  }

  handleBranchesClicked() {
    router.navigateHome("#branches");
  }

  handleCommitsClicked() {
    router.navigateHome("#commits");
  }

  handleClearFiltersClicked() {
    router.clearFilters();
  }

  handleMouseOver(invocation: invocation.Invocation) {
    this.setState({
      hoveredInvocationId: invocation.invocationId,
    });
  }

  handleMouseOut(invocation: invocation.IInvocation) {
    this.setState({ hoveredInvocationId: null });
  }

  handleLoadNextPageClicked() {
    this.getInvocations(true);
  }

  getInvocationStatusClass(selectedInvocation: invocation.IInvocation) {
    if (selectedInvocation.invocationStatus == invocation.Invocation.InvocationStatus.PARTIAL_INVOCATION_STATUS) {
      return "grid-block-in-progress";
    }
    if (selectedInvocation.invocationStatus == invocation.Invocation.InvocationStatus.DISCONNECTED_INVOCATION_STATUS) {
      return "grid-block-disconnected";
    }
    return selectedInvocation.success ? "grid-block-success" : "grid-block-failure";
  }

  getRepoUrl() {
    // TODO(siggisim): solve this for all future user-supplied hrefs by upgrading react once this warning
    // becomes enforced: https://github.com/facebook/react/pull/15047

    if (this.props.repo?.startsWith("http://") || this.props.repo?.startsWith("https://")) {
      return this.props.repo;
    }

    return undefined;
  }

  isAggregateView() {
    return Boolean(this.props.hash);
  }

  render() {
    let scope =
      this.props.username ||
      this.props.hostname ||
      format.formatCommitHash(this.props.commit) ||
      this.props.branch ||
      format.formatGitUrl(this.props.repo);
    let viewType = "build history";
    if (this.props.hash == "#users") viewType = "users";
    if (this.props.hash == "#repos") viewType = "repos";
    if (this.props.hash == "#branches") viewType = "branches";
    if (this.props.hash == "#commits") viewType = "commits";
    if (this.props.hash == "#hosts") viewType = "hosts";

    // Note: we don't show summary stats for scoped views because the summary stats
    // don't currently get filtered by the scope as well.
    // TODO(bduffany): Make sure scope-filtered queries are optimized and remove this limitation.
    const hideSummaryStats = Boolean(scope);

    return (
      <div className="history">
        <div className="shelf">
          <div className="container">
            <div className="top-bar">
              <div className="breadcrumbs">
                {this.props.user && this.props.user?.selectedGroupName() && (
                  <span onClick={this.handleOrganizationClicked.bind(this)} className="clickable">
                    {this.props.user?.selectedGroupName()}
                  </span>
                )}
                {(this.props.username || this.props.hash == "#users") && (
                  <span onClick={this.handleUsersClicked.bind(this)} className="clickable">
                    Users
                  </span>
                )}
                {(this.props.hostname || this.props.hash == "#hosts") && (
                  <span onClick={this.handleHostsClicked.bind(this)} className="clickable">
                    Hosts
                  </span>
                )}
                {(this.props.repo || this.props.hash == "#repos") && (
                  <span onClick={this.handleReposClicked.bind(this)} className="clickable">
                    Repos
                  </span>
                )}
                {(this.props.branch || this.props.hash == "#branches") && (
                  <span onClick={this.handleBranchesClicked.bind(this)} className="clickable">
                    Branches
                  </span>
                )}
                {(this.props.commit || this.props.hash == "#commits") && (
                  <span onClick={this.handleCommitsClicked.bind(this)} className="clickable">
                    Commits
                  </span>
                )}
                {scope && <span>{scope}</span>}
                {!this.props.username && !this.props.hostname && this.props.hash == "" && (
                  <>{this.isFilteredToWorkflows() ? <span>Workflow runs</span> : <span>Builds</span>}</>
                )}
              </div>
              {capabilities.globalFilter && <FilterComponent search={this.props.search} />}
            </div>
            <div className="titles">
              <div className="title">
                {this.props.username && (
                  <span>
                    <span className="history-title">{this.props.username}'s builds</span>
                    <a className="history-button" href={`/trends/?user=${this.props.username}`}>
                      <BarChart2 /> Trends
                    </a>
                  </span>
                )}
                {this.props.hostname && (
                  <span>
                    <span className="history-title">Builds on {this.props.hostname}</span>
                    <a className="history-button" href={`/trends/?host=${this.props.hostname}`}>
                      <BarChart2 /> Trends
                    </a>
                  </span>
                )}
                {this.props.repo && !this.isFilteredToWorkflows() && (
                  <>
                    <span className="history-title">Builds of {format.formatGitUrl(this.props.repo)}</span>
                    {this.getRepoUrl() && (
                      <a className="history-button" target="_blank" href={this.getRepoUrl()}>
                        <Github /> Repo
                      </a>
                    )}
                    <a className="history-button" href={`/trends/?repo=${this.props.repo}`}>
                      <BarChart2 /> Trends
                    </a>
                  </>
                )}
                {this.props.repo && this.isFilteredToWorkflows() && (
                  <>
                    <span className="history-title">Workflow runs of {format.formatGitUrl(this.props.repo)}</span>
                    {this.getRepoUrl() && (
                      <a className="history-button" target="_blank" href={this.getRepoUrl()}>
                        <Github /> Repo
                      </a>
                    )}
                  </>
                )}
                {this.props.branch && (
                  <>
                    <span className="history-title">Builds from branch {this.props.branch}</span>
                    <a className="history-button" href={`/trends/?branch=${this.props.branch}`}>
                      <BarChart2 /> Trends
                    </a>
                  </>
                )}
                {this.props.commit && (
                  <span>
                    <span className="history-title">
                      Builds from commit {format.formatCommitHash(this.props.commit)}
                    </span>
                    <a
                      className="history-button"
                      target="_blank"
                      href={`https://github.com/search?q=hash%3A${this.props.commit}`}>
                      <GitCommit /> Commit
                    </a>
                    <a className="history-button" href={`/trends/?commit=${this.props.commit}`}>
                      <BarChart2 /> Trends
                    </a>
                  </span>
                )}
                {!this.props.hostname &&
                  !this.props.username &&
                  !this.props.repo &&
                  !this.props.branch &&
                  !this.props.commit &&
                  `${this.props.user?.selectedGroupName() || "User"}'s ${viewType}`}
              </div>
            </div>
            {this.state.loadingSummaryStat && !hideSummaryStats && (
              <div className="details loading-details">
                <Spinner />
                <div>Loading stats...</div>
              </div>
            )}
            {this.state.summaryStat && !hideSummaryStats && (
              <div className="details">
                <div className="detail">
                  <Hash className="icon gray" />
                  {format.formatWithCommas(this.state.summaryStat.totalNumBuilds)} builds
                </div>
                <div className="detail">
                  <CheckCircle className="icon green" />
                  {format.formatWithCommas(this.state.summaryStat.totalNumSucessfulBuilds)} passed
                </div>
                <div className="detail">
                  <XCircle className="icon red" />
                  {format.formatWithCommas(this.state.summaryStat.totalNumFailingBuilds)} failed
                </div>
                <div className="detail">
                  <Percent className="icon grey" />
                  {format.percent(
                    Number(this.state.summaryStat.totalNumSucessfulBuilds) /
                      (Number(this.state.summaryStat.totalNumSucessfulBuilds) +
                        Number(this.state.summaryStat.totalNumFailingBuilds))
                  )}{" "}
                  passed
                </div>
                <div className="detail">
                  <Clock className="icon" />
                  {format.durationUsec(this.state.summaryStat.totalBuildTimeUsec)} total
                </div>
                <div className="detail">
                  <Clock className="icon" />
                  {format.durationUsec(
                    Number(this.state.summaryStat.totalBuildTimeUsec) / Number(this.state.summaryStat.totalNumBuilds)
                  )}{" "}
                  avg.
                </div>
              </div>
            )}
          </div>
          {Boolean(this.state.invocations?.length) && (
            <div className="container nopadding-dense">
              <div className={`grid ${this.state.invocations.length < 20 ? "grid-grow" : ""}`}>
                {this.state.invocations.map((invocation) => (
                  <Link key={invocation.invocationId} href={`/invocation/${invocation.invocationId}`}>
                    <Tooltip
                      renderContent={() => (
                        // this renders in a portal, so wrap with .history to simplify styling
                        <div className="history">
                          <HistoryInvocationCardComponent hover={true} invocation={invocation} />
                        </div>
                      )}
                      className={`clickable grid-block ${this.getInvocationStatusClass(invocation)}`}
                    />
                  </Link>
                ))}
              </div>
            </div>
          )}
        </div>
        {this.props.hash === "#users" && this.props.user.canCall("getGroupUsers") && (
          <OrgJoinRequestsComponent user={this.props.user} />
        )}
        {Boolean(this.state.invocations?.length || this.state.aggregateStats?.length) && (
          <div className="container nopadding-dense">
            {this.state.invocations?.map((invocation) => (
              <a href={`/invocation/${invocation.invocationId}`} onClick={(e) => e.preventDefault()}>
                <HistoryInvocationCardComponent
                  className={this.state.hoveredInvocationId == invocation.invocationId ? "card-hovered" : ""}
                  onMouseOver={this.handleMouseOver.bind(this, invocation)}
                  onMouseOut={this.handleMouseOut.bind(this, invocation)}
                  invocation={invocation}
                  isSelectedForCompare={invocation.invocationId === this.state.invocationIdToCompare}
                />
              </a>
            ))}
            {this.state.pageToken && (
              <button
                className="load-more"
                disabled={this.state.loadingInvocations}
                onClick={this.handleLoadNextPageClicked.bind(this)}>
                {this.state.loadingInvocations ? "Loading..." : "Load more"}
              </button>
            )}
            {this.state.aggregateStats?.map((invocationStat) => (
              <HistoryInvocationStatCardComponent
                type={this.hashToAggregationTypeMap.get(this.props.hash)}
                invocationStat={invocationStat}
              />
            ))}
          </div>
        )}
        {((this.state.loadingInvocations && !this.state.invocations?.length) || this.state.loadingAggregateStats) && (
          <div className="loading"></div>
        )}
        {router.isFiltering() &&
          !this.state.loadingInvocations &&
          !this.state.invocations?.length &&
          !this.state.loadingAggregateStats &&
          !this.state.aggregateStats?.length && (
            <div className="container narrow">
              <div className="empty-state history">
                <h2>No matching builds</h2>
                <p>No builds matched the current filters or selected dates.</p>
                <div>
                  <Button onClick={this.handleClearFiltersClicked.bind(this)}>Clear filters</Button>
                </div>
              </div>
            </div>
          )}
        {!router.isFiltering() &&
          !this.isAggregateView() &&
          !this.state.loadingInvocations &&
          !this.state.invocations?.length &&
          this.isFilteredToWorkflows() && (
            <div className="container narrow">
              <div className="empty-state history">
                <h2>No workflow runs yet!</h2>
                <p>
                  Push commits or send pull requests to{" "}
                  <a href={this.props.repo} target="_new" className="text-link">
                    {format.formatGitUrl(this.props.repo)}
                  </a>{" "}
                  to trigger BuildBuddy workflows.
                </p>
                <p>
                  By default, BuildBuddy will run <code className="inline-code">bazel test //...</code> on pushes to
                  your main branch and on pull request branches.
                </p>
                <div>
                  <LinkButton href="https://docs.buildbuddy.io/docs/workflows-config" target="_new">
                    Learn more
                  </LinkButton>
                </div>
              </div>
            </div>
          )}
        {!router.isFiltering() &&
          !this.isAggregateView() &&
          !this.state.loadingInvocations &&
          !this.state.invocations?.length &&
          !this.isFilteredToWorkflows() && (
            <div className="container narrow">
              <div className="empty-state history">
                <h2>No builds found!</h2>
                <p>
                  Seems like you haven't connected Bazel to your BuildBuddy account yet.
                  <br />
                  <br />
                  <a className="button" href="/docs/setup">
                    Click here to get started
                  </a>
                </p>
              </div>
            </div>
          )}
        {!router.isFiltering() &&
          this.isAggregateView() &&
          !this.state.loadingAggregateStats &&
          !this.state.aggregateStats?.length && (
            <div className="container narrow">
              <div className="empty-state history">
                <h2>No {viewType} found!</h2>
                <p>
                  You can associate builds with {viewType} using build metadata.
                  <br />
                  <br />
                  <a className="button" href="https://www.buildbuddy.io/docs/guide-metadata" target="_blank">
                    View build metadata guide
                  </a>
                </p>
              </div>
            </div>
          )}
      </div>
    );
  }
}
