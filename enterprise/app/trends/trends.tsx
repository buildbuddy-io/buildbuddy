import React from "react";
import moment from "moment";
import * as format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { stats } from "../../../proto/stats_ts_proto";
import TrendsChartComponent from "./trends_chart";
import CacheChartComponent from "./cache_chart";
import PercentilesChartComponent from "./percentile_chart";
import { Subscription } from "rxjs";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import FilterComponent from "../filter/filter";
import capabilities from "../../../app/capabilities/capabilities";
import { getProtoFilterParams } from "../filter/filter_util";
import router from "../../../app/router/router";
import * as proto from "../../../app/util/proto";
import DrilldownPageComponent from "./drilldown_page";

const BITS_PER_BYTE = 8;

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  stats: stats.ITrendStat[];
  loading: boolean;
  dateToStatMap: Map<string, stats.ITrendStat>;
  dateToExecutionStatMap: Map<string, stats.IExecutionStat>;
  enableInvocationPercentileCharts: boolean;
  dates: string[];

  loadingTrendSummary: boolean;
  trendSummary: stats.ITrendSummary;
  summaryModalPage: number;
}

const SECONDS_PER_MICROSECOND = 1e-6;

export default class TrendsComponent extends React.Component<Props, State> {
  state: State = {
    stats: [],
    loading: true,
    dateToStatMap: new Map<string, stats.ITrendStat>(),
    dateToExecutionStatMap: new Map<string, stats.IExecutionStat>(),
    enableInvocationPercentileCharts: false,
    dates: [],

    loadingTrendSummary: false,
    trendSummary: null,
    summaryModalPage: 1,
  };

  subscription?: Subscription;

  componentWillMount() {
    document.title = `Trends | BuildBuddy`;
    this.fetchStats();

    this.subscription = rpcService.events.subscribe({
      next: (name) => name == "refresh" && this.fetchStats(),
    });
  }

  componentWillUnmount() {
    this.subscription?.unsubscribe();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.hash !== prevProps.hash || this.props.search != prevProps.search) {
      this.fetchStats();
    }
  }

  updateSelectedTab(tab: "charts" | "drilldown") {
    window.location.hash = "#" + tab;
  }

  getSelectedTab(): "charts" | "drilldown" {
    if (this.props.hash.replace("#", "") === "drilldown") {
      return "drilldown";
    }
    return "charts";
  }

  fetchStats() {
    // TODO(bduffany): Cancel in-progress request

    let request = new stats.GetTrendRequest();
    request.query = new stats.TrendQuery();

    const filterParams = getProtoFilterParams(this.props.search);
    if (filterParams.role) {
      request.query.role = filterParams.role;
    } else {
      // Note: Technically we're filtering out workflows and unknown roles,
      // even though the user has selected "All roles". But we do this to
      // avoid double-counting build times for workflows and their nested CI runs.
      request.query.role = ["", "CI"];
    }

    if (filterParams.host) request.query.host = filterParams.host;
    if (filterParams.user) request.query.user = filterParams.user;
    if (filterParams.repo) request.query.repoUrl = filterParams.repo;
    if (filterParams.branch) request.query.branchName = filterParams.branch;
    if (filterParams.commit) request.query.commitSha = filterParams.commit;
    if (filterParams.command) request.query.command = filterParams.command;
    if (filterParams.status) request.query.status = filterParams.status;

    request.query.updatedBefore = filterParams.updatedBefore;
    request.query.updatedAfter = filterParams.updatedAfter;

    const user = this.props.search.get("user");
    if (user) {
      request.query.user = user;
    }

    const host = this.props.search.get("host");
    if (host) {
      request.query.host = host;
    }

    const commit = this.props.search.get("commit");
    if (commit) {
      request.query.commitSha = commit;
    }

    const branch = this.props.search.get("branch");
    if (branch) {
      request.query.branchName = branch;
    }

    const repo = this.props.search.get("repo");
    if (repo) {
      request.query.repoUrl = repo;
    }

    const command = this.props.search.get("command");
    if (command) {
      request.query.command = command;
    }

    this.setState({ ...this.state, loading: true });
    rpcService.service.getTrend(request).then((response) => {
      console.log(response);
      const dateToStatMap = new Map<string, stats.ITrendStat>();
      for (let stat of response.trendStat) {
        dateToStatMap.set(stat.name ?? "", stat);
      }
      const dateToExecutionStatMap = new Map<string, stats.IExecutionStat>();
      for (let stat of response.executionStat) {
        dateToExecutionStatMap.set(stat.name ?? "", stat);
      }
      this.setState({
        ...this.state,
        stats: response.trendStat,
        dates: getDatesBetween(
          // Start date should always be defined.
          proto.timestampToDate(request.query!.updatedAfter || {}),
          // End date may not be defined -- default to today.
          request.query!.updatedBefore ? proto.timestampToDate(request.query!.updatedBefore) : new Date()
        ),
        dateToStatMap,
        dateToExecutionStatMap,
        enableInvocationPercentileCharts: response.hasInvocationStatPercentiles,
        loading: false,
      });
    });
  }

  fetchTrendsSummary(username: string) {
    let request = new stats.GetTrendSummaryRequest();
    request.query = new stats.TrendQuery();
    // TODO: Input a user
    request.user = username;

    const filterParams = getProtoFilterParams(this.props.search);
    if (filterParams.role) {
      request.query.role = filterParams.role;
    } else {
      // Note: Technically we're filtering out workflows and unknown roles,
      // even though the user has selected "All roles". But we do this to
      // avoid double-counting build times for workflows and their nested CI runs.
      request.query.role = ["", "CI"];
    }

    request.query.updatedBefore = filterParams.updatedBefore;
    request.query.updatedAfter = filterParams.updatedAfter;

    const user = this.props.search.get("user");
    if (user) {
      request.query.user = user;
    }

    const host = this.props.search.get("host");
    if (host) {
      request.query.host = host;
    }

    const commit = this.props.search.get("commit");
    if (commit) {
      request.query.commitSha = commit;
    }

    const branch = this.props.search.get("branch");
    if (branch) {
      request.query.branchName = branch;
    }

    const repo = this.props.search.get("repo");
    if (repo) {
      request.query.repoUrl = repo;
    }

    const command = this.props.search.get("command");
    if (command) {
      request.query.command = command;
    }

    this.setState({ ...this.state, loadingTrendSummary: true });
    rpcService.service.getTrendSummary(request).then((response) => {
      console.log(response);
      this.setState({
        ...this.state,
        loadingTrendSummary:false,
        trendSummary: response,
      });
    });

    // TODO: Need to subtract time spent uploading/downloading to remote cache
  }

  getStat(date: string): stats.ITrendStat {
    return this.state.dateToStatMap.get(date) || {};
  }

  getExecutionStat(date: string): stats.IExecutionStat {
    return this.state.dateToExecutionStatMap.get(date) || {};
  }

  formatLongDate(date: any) {
    return moment(date).format("dddd, MMMM Do YYYY");
  }

  formatShortDate(date: any) {
    return moment(date).format("MMM D");
  }

  onBarClicked(hash: string, sortBy: string, date: string) {
    router.navigateTo("/?start=" + date + "&end=" + date + "&sort-by=" + sortBy + hash);
  }

  showingDrilldown(): boolean {
    return (capabilities.config.trendsHeatmapEnabled || false) && this.props.hash === "#drilldown";
  }

  onModalNextClick() {
    this.setState(prevState => {
      return {summaryModalPage: prevState.summaryModalPage + 1}
    })
  }

  trendSummaryIntro() {
    let textInput = React.createRef();
    return (
        <div>
          {this.state.summaryModalPage == 1 &&
              <div className="trend-chart-hover trends-summary trends-summary-intro">
                <div className="trends-summary-title title-white">
                  <img src="/image/bb_wrapped.png" className="trends-logo" />
                  BuildBuddy Wrapped
                </div>
                <div className="trends-summary-title title-white">
                  What's your username for builds?
                </div>

                <div className="trends-summary-username-input">
                  <input ref={textInput} placeholder="Username (e.g. tylerw)" />
                </div>
                <button
                    className="trends-summary-button"
                    onClick={() => {
                      var username = textInput.current.value;
                      this.fetchTrendsSummary(username);
                      this.onModalNextClick()
                    }}
                >
                  {">"}
                </button>
              </div>
          }
        </div>
    )
  }

  trendSummaryModal() {
    if (!this.state.trendSummary) {
      return null;
    }

    const avgHourlyDeveloperSalary = 60;
    const avatarSizeGB = 17.28;

    var trendSummary = this.state.trendSummary;
    // var userSecSavedCache = this.secondsSavedWithCacheUsingAvgExecutionTime(trendSummary.userTotalCacheHits);
    // var groupSecSavedCache = this.secondsSavedWithCacheUsingAvgExecutionTime(trendSummary.groupTotalCacheHits)
    var userCacheHitRate = trendSummary.userTotalCacheHits / trendSummary.userTotalCacheRequests * 100;
    var groupCacheHitRate = trendSummary.groupTotalCacheHits / trendSummary.groupTotalCacheRequests * 100;
    var userSecSavedCache = this.secondsSavedWithCacheUsingMedianInvocationLength(
        trendSummary.medianCachedInvocationTimeUsec,
        trendSummary.medianNotCachedInvocationTimeUsec,
        userCacheHitRate / 100,
        trendSummary.userTotalBuilds,
    );
    var groupSecSavedCache = this.secondsSavedWithCacheUsingMedianInvocationLength(
        trendSummary.medianCachedInvocationTimeUsec,
        trendSummary.medianNotCachedInvocationTimeUsec,
        groupCacheHitRate / 100,
        trendSummary.groupTotalBuilds,
    );

    return (
        <div>
          {this.state.summaryModalPage == 2 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail2">
                <div className="trends-summary-title">
                  Congratulations! Since you signed up with BuildBuddy, you've been busy!
                </div>
                <div className="trends-summary-title">
                  Let's see what you've been up to...
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 3 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  You've had {trendSummary.userTotalBuilds} builds.
                </div>
                <div className="trends-summary-title">
                  That's #{trendSummary.userBuildRank} in your org!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 4 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  These builds spanned {trendSummary.userRepos} repo(s) across {trendSummary.userCommits} commits.
                </div>
                <div className="trends-summary-title">
                  You are a real do-er!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 5 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  These builds spanned {trendSummary.userRepos} repo(s) across {trendSummary.userCommits} commits.
                </div>
                <div className="trends-summary-title">
                  <del>You are a real do-er!</del>
                  <div>
                    <div className="title-red">
                      You are a plain old gold digger.
                    </div>
                    <img src="/image/saboteur.jpeg" className="trends-saboteur" />
                  </div>
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 6 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Across your org, there were {this.formatNumber(trendSummary.groupTotalBuilds)} builds.
                </div>
                <div className="trends-summary-title">
                  That's #{trendSummary.groupBuildRank} of all orgs using BuildBuddy!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 7 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  These builds spanned {trendSummary.groupRepos} repo(s) across {this.formatNumber(trendSummary.groupCommits)} commits.
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 8 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  You're moving a lot of data!
                </div>
                <div className="trends-summary-title">
                  You uploaded {this.formatNumber(trendSummary.userBytesUploaded / 1e9)}GB and
                  downloaded {this.formatNumber(trendSummary.userBytesDownloaded / 1e9)}GB from the cache.
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 9 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Your org uploaded {this.formatNumber(trendSummary.groupBytesUploaded / 1e9)}GB and
                  downloaded {this.formatNumber(trendSummary.groupBytesDownloaded / 1e9)}GB from the cache.
                </div>
                <div className="trends-summary-title">
                  Downloading that much data is equivalent to streaming
                  Avatar {this.formatNumber(trendSummary.groupBytesDownloaded / 1e9 / avatarSizeGB)} times!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 10 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Your builds made {this.formatNumber(trendSummary.userTotalCacheRequests)} cache requests.
                </div>
                <div className="trends-summary-title">
                  With {this.formatNumber(trendSummary.userTotalCacheHits)} cache hits, that's a {this.formatNumber(userCacheHitRate)}% cache hit rate!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 11 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  For builds that were {">"}95% cached, your median build time
                  was {this.formatNumber(trendSummary.medianCachedInvocationTimeUsec /1e6 / 60)} minutes.
                </div>
                <div className="trends-summary-title">
                  For builds that were {"<"}10% cached, your median build time
                  was {this.formatNumber(trendSummary.medianNotCachedInvocationTimeUsec /1e6 / 60)} minutes.
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }
          {this.state.summaryModalPage == 12 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  That's
                  roughly {this.formatNumber((trendSummary.medianNotCachedInvocationTimeUsec - trendSummary.medianCachedInvocationTimeUsec) / 1e6 / 60)} minutes
                  saved for fully cached builds!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 13 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Based on those median build times and your cache hit rate,
                  that's approximately {this.formatNumber(userSecSavedCache / 3600)} hours you've saved waiting for slow builds!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 14 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  In other words...
                </div>
                <div className="trends-summary-title">
                  That's {this.formatNumber(userSecSavedCache / 3600 / 24)} days not
                  doom-scrolling Twitter while waiting for slow builds!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 15 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Across your org, there were {this.formatNumber(trendSummary.groupTotalCacheHits)} cache hits
                  out of {this.formatNumber(trendSummary.groupTotalCacheRequests)} total cache requests.
                </div>
                <div className="trends-summary-title">
                  That's a {this.formatNumber(groupCacheHitRate)}% cache hit rate!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 16 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Based on your median build times and your org's cache hit rate,
                  that's roughly {this.formatNumber(groupSecSavedCache / 3600)} developer hours saved.
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 17 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  That's equivalent to...
                </div>
                <div className="trends-summary-title">
                  {this.formatNumber(groupSecSavedCache / 3600 / 8)} work days...
                </div>
                <div className="trends-summary-title">
                  {this.formatNumber(groupSecSavedCache / 3600 / 8 / 5)} weeks...
                </div>
                <div className="trends-summary-title">
                  Or {this.formatNumber(groupSecSavedCache / 3600 / 8 / 5 / 52)} years!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 18 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Based on average developer salary, that's ${this.formatNumber(groupSecSavedCache / 3600 * avgHourlyDeveloperSalary)}!
                </div>
                <div className="trends-summary-title">
                  Cache-ching! $$$
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }

          {this.state.summaryModalPage == 19 &&
              <div className="trend-chart-hover trends-summary trends-summary-detail">
                <div className="trends-summary-title">
                  Thanks for spending the past year with us.
                </div>
                <div className="trends-summary-title">
                  Happy building!
                </div>
                <button className="trends-summary-button" onClick={this.onModalNextClick.bind(this)}> {">"} </button>
              </div>
          }
      </div>
    )
  }

  secondsSavedWithCacheUsingAvgExecutionTime(cacheHits: number): number {
    const avgCoresLaptop = 8;
    const usecInSec = 1e6;
    var secondsSaved = cacheHits * this.state.trendSummary.avgActionExecutionTimeUsec / usecInSec / avgCoresLaptop;
    return secondsSaved;
  }

  secondsSavedWithCacheUsingMedianInvocationLength(
      highlyCachedInvocationTime: number,
      notCachedInvocationTime: number,
      cachedHitRate: number,
      numBuilds: number,
    ): number {
    const cachedInvocationTimeDiffSec = (notCachedInvocationTime - highlyCachedInvocationTime) / 1e6;
    return cachedInvocationTimeDiffSec * numBuilds * cachedHitRate;
  }

  formatNumber(n: number): string {
    var truncated = +n.toFixed(2);
    return truncated.toLocaleString();
  }

  render() {
    return (
        <div className="trends">
          <div className="container">
            <div className="trends-header">
              <div className="trends-title">Trends</div>
              <FilterComponent search={this.props.search} />
            </div>
            {capabilities.config.trendsHeatmapEnabled && (
                <div className="tabs">
                  <div
                      onClick={() => this.updateSelectedTab("charts")}
                      className={`tab ${this.getSelectedTab() == "charts" ? "selected" : ""}`}>
                    Charts
                  </div>
                  <div
                      onClick={() => this.updateSelectedTab("drilldown")}
                      className={`tab ${this.getSelectedTab() == "drilldown" ? "selected" : ""}`}>
                    Drilldown
                  </div>
                </div>
          )}
          {this.showingDrilldown() && (
            <DrilldownPageComponent user={this.props.user} search={this.props.search}></DrilldownPageComponent>
          )}
          {!this.showingDrilldown() && this.state.loading && <div className="loading"></div>}
          {!this.showingDrilldown() && !this.state.loading && (
            <>
              {this.trendSummaryIntro()}
              {this.trendSummaryModal()}

              <TrendsChartComponent
                title="Builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).totalNumBuilds ?? 0)}
                extractSecondaryValue={(date) => {
                  let stat = this.getStat(date);
                  return (
                    (+(stat.totalBuildTimeUsec ?? 0) * SECONDS_PER_MICROSECOND) / +(stat.completedInvocationCount ?? 0)
                  );
                }}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " builds"}
                formatSecondaryHoverValue={(value) => `${format.durationSec(value)} average`}
                formatSecondaryTickValue={format.durationSec}
                name="builds"
                secondaryName="average build duration"
                secondaryLine={true}
                separateAxis={true}
                onBarClicked={this.onBarClicked.bind(this, "", "")}
              />
              {this.state.enableInvocationPercentileCharts && (
                <PercentilesChartComponent
                  title="Build duration"
                  data={this.state.dates}
                  extractLabel={this.formatShortDate}
                  formatHoverLabel={this.formatLongDate}
                  extractP50={(date) => +(this.getStat(date).buildTimeUsecP50 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP75={(date) => +(this.getStat(date).buildTimeUsecP75 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP90={(date) => +(this.getStat(date).buildTimeUsecP90 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP95={(date) => +(this.getStat(date).buildTimeUsecP95 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP99={(date) => +(this.getStat(date).buildTimeUsecP99 ?? 0) * SECONDS_PER_MICROSECOND}
                  onColumnClicked={this.onBarClicked.bind(this, "", "duration")}
                />
              )}
              {!this.state.enableInvocationPercentileCharts && (
                <TrendsChartComponent
                  title="Build duration"
                  data={this.state.dates}
                  extractValue={(date) => {
                    let stat = this.getStat(date);
                    return +(stat.totalBuildTimeUsec ?? 0) / +(stat.completedInvocationCount ?? 0) / 1000000;
                  }}
                  extractSecondaryValue={(date) => +(this.getStat(date).maxDurationUsec ?? 0) / 1000000}
                  extractLabel={this.formatShortDate}
                  formatTickValue={format.durationSec}
                  formatHoverLabel={this.formatLongDate}
                  formatHoverValue={(value) => `${format.durationSec(value || 0)} average`}
                  formatSecondaryHoverValue={(value) => `${format.durationSec(value || 0)} slowest`}
                  name="average build duration"
                  secondaryName="slowest build duration"
                  onBarClicked={this.onBarClicked.bind(this, "", "")}
                  onSecondaryBarClicked={this.onBarClicked.bind(this, "", "duration")}
                />
              )}

              <CacheChartComponent
                title="Action Cache"
                data={this.state.dates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +(this.getStat(date).actionCacheHits ?? 0)}
                secondaryBarName="misses"
                extractSecondary={(date) => +(this.getStat(date).actionCacheMisses ?? 0)}
              />
              <CacheChartComponent
                title="Content Addressable Store"
                data={this.state.dates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +(this.getStat(date).casCacheHits ?? 0)}
                secondaryBarName="writes"
                extractSecondary={(date) => +(this.getStat(date).casCacheUploads ?? 0)}
              />

              <TrendsChartComponent
                title="Cache read throughput"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).totalDownloadSizeBytes ?? 0)}
                extractSecondaryValue={(date) =>
                  (+(this.getStat(date).totalDownloadSizeBytes ?? 0) * BITS_PER_BYTE) /
                  (+(this.getStat(date).totalDownloadUsec ?? 0) * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate}
                formatTickValue={format.bytes}
                allowDecimals={false}
                formatSecondaryTickValue={format.bitsPerSecond}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => `${format.bytes(value || 0)} downloaded`}
                formatSecondaryHoverValue={(value) => format.bitsPerSecond(value || 0)}
                name="total download size"
                secondaryName="download rate"
                secondaryLine={true}
                separateAxis={true}
              />

              <TrendsChartComponent
                title="Cache write throughput"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).totalUploadSizeBytes ?? 0)}
                extractSecondaryValue={(date) =>
                  (+(this.getStat(date).totalUploadSizeBytes ?? 0) * BITS_PER_BYTE) /
                  (+(this.getStat(date).totalUploadUsec ?? 0) * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate}
                formatTickValue={format.bytes}
                formatSecondaryTickValue={format.bitsPerSecond}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => `${format.bytes(value || 0)} uploaded`}
                formatSecondaryHoverValue={(value) => format.bitsPerSecond(value || 0)}
                name="total upload size"
                secondaryName="upload rate"
                secondaryLine={true}
                separateAxis={true}
              />

              <TrendsChartComponent
                title="Users with builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).userCount ?? 0)}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " users"}
                name="users with builds"
                onBarClicked={this.onBarClicked.bind(this, "#users", "")}
              />
              <TrendsChartComponent
                title="Commits with builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).commitCount ?? 0)}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " commits"}
                name="commits with builds"
                onBarClicked={this.onBarClicked.bind(this, "#commits", "")}
              />
              <TrendsChartComponent
                title="Branches with builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).branchCount ?? 0)}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " branches"}
                name="branches with builds"
              />
              <TrendsChartComponent
                title="Hosts with builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).hostCount ?? 0)}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " hosts"}
                name="hosts with builds"
                onBarClicked={this.onBarClicked.bind(this, "#hosts", "")}
              />
              <TrendsChartComponent
                title="Repos with builds"
                data={this.state.dates}
                extractValue={(date) => +(this.getStat(date).repoCount ?? 0)}
                extractLabel={this.formatShortDate}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " repos"}
                name="repos with builds"
                onBarClicked={this.onBarClicked.bind(this, "#repos", "")}
              />
              {this.state.dateToExecutionStatMap.size > 0 && (
                <PercentilesChartComponent
                  title="Remote Execution Queue Duration"
                  data={this.state.dates}
                  extractLabel={this.formatShortDate}
                  formatHoverLabel={this.formatLongDate}
                  extractP50={(date) =>
                    +(this.getExecutionStat(date).queueDurationUsecP50 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP75={(date) =>
                    +(this.getExecutionStat(date).queueDurationUsecP75 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP90={(date) =>
                    +(this.getExecutionStat(date).queueDurationUsecP90 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP95={(date) =>
                    +(this.getExecutionStat(date).queueDurationUsecP95 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP99={(date) =>
                    +(this.getExecutionStat(date).queueDurationUsecP99 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                />
              )}
            </>
          )}
        </div>
      </div>
    );
  }
}

function getDatesBetween(start: Date, end: Date): string[] {
  const endMoment = moment(end);
  const formattedDates: string[] = [];
  for (let date = moment(start); date.isBefore(endMoment); date = date.add(1, "days")) {
    formattedDates.push(date.format("YYYY-MM-DD"));
  }
  return formattedDates;
}
