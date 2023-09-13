import React from "react";
import moment from "moment";
import * as format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { stats } from "../../../proto/stats_ts_proto";
import TrendsChartComponent from "./trends_chart";
import CacheChartComponent from "./cache_chart";
import PercentilesChartComponent from "./percentile_chart";
import TrendsSummaryCard from "./summary_card";
import { Subscription } from "rxjs";
import FilterComponent from "../filter/filter";
import capabilities from "../../../app/capabilities/capabilities";
import { getProtoFilterParams } from "../filter/filter_util";
import router from "../../../app/router/router";
import * as proto from "../../../app/util/proto";
import DrilldownPageComponent from "./drilldown_page";
import { timeDay } from "d3-time";

const BITS_PER_BYTE = 8;

interface Props {
  user: User;
  tab: string;
  search: URLSearchParams;
}

interface State {
  stats: stats.ITrendStat[];
  loading: boolean;
  timeToStatMap: Map<number, stats.ITrendStat>;
  timeToExecutionStatMap: Map<number, stats.IExecutionStat>;
  enableInvocationPercentileCharts: boolean;
  currentSummary?: stats.Summary;
  previousSummary?: stats.Summary;
  timeKeys: number[];
}

const SECONDS_PER_MICROSECOND = 1e-6;

export default class TrendsComponent extends React.Component<Props, State> {
  state: State = {
    stats: [],
    loading: true,
    timeToStatMap: new Map<number, stats.ITrendStat>(),
    timeToExecutionStatMap: new Map<number, stats.IExecutionStat>(),
    enableInvocationPercentileCharts: false,
    timeKeys: [],
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
    if (
      this.showingDrilldown(this.props.tab) !== this.showingDrilldown(prevProps.tab) ||
      this.props.search.toString() != prevProps.search.toString()
    ) {
      this.fetchStats();
    }
  }

  updateSelectedTab(tab: "charts" | "drilldown") {
    window.location.hash = "#" + tab;
  }

  getSelectedTab(): "charts" | "drilldown" {
    if (this.props.tab.replace("#", "") === "drilldown") {
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
    if (filterParams.pattern) request.query.pattern = filterParams.pattern;
    if (filterParams.tags) request.query.tags = filterParams.tags;
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

    const pattern = capabilities.config.patternFilterEnabled && this.props.search.get("pattern");
    if (pattern) {
      request.query.pattern = pattern;
    }

    this.setState({ loading: true });
    rpcService.service.getTrend(request).then((response) => {
      console.log(response);
      const timeToStatMap = new Map<number, stats.ITrendStat>();
      for (let stat of response.trendStat) {
        const time = new Date(stat.name + " 00:00").getTime();
        timeToStatMap.set(time, stat);
      }
      const timeToExecutionStatMap = new Map<number, stats.IExecutionStat>();
      for (let stat of response.executionStat) {
        const time = new Date(stat.name + " 00:00").getTime();
        timeToExecutionStatMap.set(time, stat);
      }
      const domain: [Date, Date] = [
        proto.timestampToDate(request.query!.updatedAfter!),
        request.query!.updatedBefore ? proto.timestampToDate(request.query!.updatedBefore) : new Date(),
      ];

      this.setState({
        stats: response.trendStat,
        timeKeys: computeTimeKeys(domain),
        currentSummary: response.currentSummary || undefined,
        previousSummary: response.previousSummary || undefined,
        timeToStatMap,
        timeToExecutionStatMap,
        enableInvocationPercentileCharts: response.hasInvocationStatPercentiles,
        loading: false,
      });
    });
  }

  getStat(timestampMillis: number): stats.ITrendStat {
    return this.state.timeToStatMap.get(timestampMillis) || {};
  }

  getExecutionStat(timestampMillis: number): stats.IExecutionStat {
    return this.state.timeToExecutionStatMap.get(timestampMillis) || {};
  }

  formatLongDate(timestampMillis: number) {
    return moment(timestampMillis).format("dddd, MMMM Do YYYY");
  }

  formatShortDate(timestampMillis: number) {
    const time = moment(timestampMillis);

    if (time.hour() === 0) {
      return time.format("MMM D");
    }

    return time.format("HH:mm");
  }

  onBarClicked(hash: string, sortBy: string, tsMillis: number) {
    const date = new Date(tsMillis).toISOString().split("T")[0];
    router.navigateTo("/?start=" + date + "&end=" + date + "&sort-by=" + sortBy + hash);
  }

  showingDrilldown(tab: string): boolean {
    return (capabilities.config.trendsHeatmapEnabled || false) && tab === "#drilldown";
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
          {this.showingDrilldown(this.props.tab) && (
            <DrilldownPageComponent user={this.props.user} search={this.props.search}></DrilldownPageComponent>
          )}
          {!this.showingDrilldown(this.props.tab) && this.state.loading && <div className="loading"></div>}
          {!this.showingDrilldown(this.props.tab) && !this.state.loading && (
            <>
              {capabilities.config.trendsSummaryEnabled && this.state.currentSummary && (
                <TrendsSummaryCard
                  search={this.props.search}
                  currentPeriod={this.state.currentSummary}
                  previousPeriod={this.state.previousSummary}></TrendsSummaryCard>
              )}
              <TrendsChartComponent
                title="Builds"
                id="builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).totalNumBuilds ?? 0)}
                extractSecondaryValue={(tsMillis) => {
                  let stat = this.getStat(tsMillis);
                  return (
                    (+(stat.totalBuildTimeUsec ?? 0) * SECONDS_PER_MICROSECOND) / +(stat.completedInvocationCount ?? 0)
                  );
                }}
                extractLabel={this.formatShortDate.bind(this)}
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
                  id="duration"
                  data={this.state.timeKeys}
                  extractLabel={this.formatShortDate.bind(this)}
                  formatHoverLabel={this.formatLongDate}
                  extractP50={(tsMillis) => +(this.getStat(tsMillis).buildTimeUsecP50 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP75={(tsMillis) => +(this.getStat(tsMillis).buildTimeUsecP75 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP90={(tsMillis) => +(this.getStat(tsMillis).buildTimeUsecP90 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP95={(tsMillis) => +(this.getStat(tsMillis).buildTimeUsecP95 ?? 0) * SECONDS_PER_MICROSECOND}
                  extractP99={(tsMillis) => +(this.getStat(tsMillis).buildTimeUsecP99 ?? 0) * SECONDS_PER_MICROSECOND}
                  onColumnClicked={this.onBarClicked.bind(this, "", "duration")}
                />
              )}
              {!this.state.enableInvocationPercentileCharts && (
                <TrendsChartComponent
                  title="Build duration"
                  id="duration"
                  data={this.state.timeKeys}
                  extractValue={(tsMillis) => {
                    let stat = this.getStat(tsMillis);
                    return +(stat.totalBuildTimeUsec ?? 0) / +(stat.completedInvocationCount ?? 0) / 1000000;
                  }}
                  extractSecondaryValue={(tsMillis) => +(this.getStat(tsMillis).maxDurationUsec ?? 0) / 1000000}
                  extractLabel={this.formatShortDate.bind(this)}
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
                id="cache"
                data={this.state.timeKeys}
                extractLabel={this.formatShortDate.bind(this)}
                formatHoverLabel={this.formatLongDate}
                extractHits={(tsMillis) => +(this.getStat(tsMillis).actionCacheHits ?? 0)}
                secondaryBarName="misses"
                extractSecondary={(tsMillis) => +(this.getStat(tsMillis).actionCacheMisses ?? 0)}
              />
              <CacheChartComponent
                title="Content Addressable Store"
                data={this.state.timeKeys}
                extractLabel={this.formatShortDate.bind(this)}
                formatHoverLabel={this.formatLongDate}
                extractHits={(tsMillis) => +(this.getStat(tsMillis).casCacheHits ?? 0)}
                secondaryBarName="writes"
                extractSecondary={(tsMillis) => +(this.getStat(tsMillis).casCacheUploads ?? 0)}
              />

              <TrendsChartComponent
                title="Cache read throughput"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).totalDownloadSizeBytes ?? 0)}
                extractSecondaryValue={(tsMillis) =>
                  (+(this.getStat(tsMillis).totalDownloadSizeBytes ?? 0) * BITS_PER_BYTE) /
                  (+(this.getStat(tsMillis).totalDownloadUsec ?? 0) * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate.bind(this)}
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
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).totalUploadSizeBytes ?? 0)}
                extractSecondaryValue={(tsMillis) =>
                  (+(this.getStat(tsMillis).totalUploadSizeBytes ?? 0) * BITS_PER_BYTE) /
                  (+(this.getStat(tsMillis).totalUploadUsec ?? 0) * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate.bind(this)}
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

              {capabilities.config.trendsSummaryEnabled && (
                <TrendsChartComponent
                  title="Saved CPU Time"
                  id="savings"
                  data={this.state.timeKeys}
                  extractValue={(tsMillis) =>
                    +(this.getStat(tsMillis).totalCpuMicrosSaved ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractLabel={this.formatShortDate.bind(this)}
                  formatTickValue={format.durationSec}
                  allowDecimals={false}
                  formatHoverLabel={this.formatLongDate}
                  formatHoverValue={(value) => `${format.durationSec(value || 0)} CPU time saved`}
                  name="saved cpu time"
                />
              )}

              <TrendsChartComponent
                title="Users with builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).userCount ?? 0)}
                extractLabel={this.formatShortDate.bind(this)}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " users"}
                name="users with builds"
                onBarClicked={this.onBarClicked.bind(this, "#users", "")}
              />
              <TrendsChartComponent
                title="Commits with builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).commitCount ?? 0)}
                extractLabel={this.formatShortDate.bind(this)}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " commits"}
                name="commits with builds"
                onBarClicked={this.onBarClicked.bind(this, "#commits", "")}
              />
              <TrendsChartComponent
                title="Branches with builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).branchCount ?? 0)}
                extractLabel={this.formatShortDate.bind(this)}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " branches"}
                name="branches with builds"
              />
              <TrendsChartComponent
                title="Hosts with builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).hostCount ?? 0)}
                extractLabel={this.formatShortDate.bind(this)}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " hosts"}
                name="hosts with builds"
                onBarClicked={this.onBarClicked.bind(this, "#hosts", "")}
              />
              <TrendsChartComponent
                title="Repos with builds"
                data={this.state.timeKeys}
                extractValue={(tsMillis) => +(this.getStat(tsMillis).repoCount ?? 0)}
                extractLabel={this.formatShortDate.bind(this)}
                formatTickValue={format.count}
                allowDecimals={false}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " repos"}
                name="repos with builds"
                onBarClicked={this.onBarClicked.bind(this, "#repos", "")}
              />
              {this.state.timeToExecutionStatMap.size > 0 && (
                <PercentilesChartComponent
                  title="Remote Execution Queue Duration"
                  data={this.state.timeKeys}
                  extractLabel={this.formatShortDate.bind(this)}
                  formatHoverLabel={this.formatLongDate}
                  extractP50={(tsMillis) =>
                    +(this.getExecutionStat(tsMillis).queueDurationUsecP50 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP75={(tsMillis) =>
                    +(this.getExecutionStat(tsMillis).queueDurationUsecP75 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP90={(tsMillis) =>
                    +(this.getExecutionStat(tsMillis).queueDurationUsecP90 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP95={(tsMillis) =>
                    +(this.getExecutionStat(tsMillis).queueDurationUsecP95 ?? 0) * SECONDS_PER_MICROSECOND
                  }
                  extractP99={(tsMillis) =>
                    +(this.getExecutionStat(tsMillis).queueDurationUsecP99 ?? 0) * SECONDS_PER_MICROSECOND
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

// TODO(jdhollen): support smaller time ranges.
function computeTimeKeys(domain: [Date, Date]): number[] {
  return timeDay.range(timeDay.floor(domain[0]), domain[1]).map((v) => v.getTime());
}
