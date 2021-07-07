import React from "react";
import moment from "moment";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import TrendsChartComponent from "./trends_chart";
import CacheChartComponent from "./cache_chart";
import { Subscription } from "rxjs";
import CheckboxButton from "../../../app/components/button/checkbox_button";

const BITS_PER_BYTE = 8;

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  stats: invocation.TrendStat[];
  loading: boolean;
  dateToStatMap: Map<string, invocation.TrendStat>;
  lastNDates: string[];
  filterOnlyCI: boolean;
}

export default class TrendsComponent extends React.Component<Props> {
  props: Props;

  state: State = {
    stats: [],
    loading: true,
    dateToStatMap: new Map<string, invocation.TrendStat>(),
    lastNDates: [],
    filterOnlyCI: false,
  };

  subscription: Subscription;

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

  getLimit() {
    return parseInt(this.props.hash.replace("#", "")) || 30;
  }

  updateLimit(limit: number) {
    window.location.hash = "#" + limit;
  }

  fetchStats() {
    let request = new invocation.GetTrendRequest();
    request.lookbackWindowDays = this.getLimit();
    request.query = new invocation.TrendQuery();

    if (this.state.filterOnlyCI) {
      request.query.role = ["CI"];
    } else {
      request.query.role = ["", "CI"];
    }

    if (this.props.search.get("user")) {
      request.query.user = this.props.search.get("user");
    }

    if (this.props.search.get("host")) {
      request.query.host = this.props.search.get("host");
    }

    if (this.props.search.get("commit")) {
      request.query.commitSha = this.props.search.get("commit");
    }

    if (this.props.search.get("repo")) {
      request.query.repoUrl = this.props.search.get("repo");
    }

    this.setState({ ...this.state, loading: true });
    rpcService.service.getTrend(request).then((response) => {
      console.log(response);
      const lastNDates = this.getLastNDates(request.lookbackWindowDays);
      let dateToStatMap = new Map<string, invocation.TrendStat>();
      for (let stat of response.trendStat) {
        dateToStatMap.set(stat.name, stat as invocation.TrendStat);
      }
      this.setState({
        ...this.state,
        stats: response.trendStat,
        lastNDates: lastNDates,
        dateToStatMap: dateToStatMap,
        loading: false,
      });
    });
  }

  getLastNDates(n: number) {
    return [...new Array(n)]
      .map((i, index) => moment().startOf("day").subtract(index, "days").format("YYYY-MM-DD"))
      .reverse();
  }

  formatLongDate(date: any) {
    return moment(date).format("dddd, MMMM Do YYYY");
  }

  formatShortDate(date: any) {
    return moment(date).format("MMM D");
  }

  handleCheckboxChange(event: any) {
    this.setState({ ...this.state, filterOnlyCI: event.target.checked }, () => {
      this.fetchStats();
    });
  }

  render() {
    return (
      <div className="trends">
        <div className="container">
          <div className="trends-header">
            <div className="trends-title">Trends</div>
            <div>
              <CheckboxButton
                className="show-changes-only-button"
                onChange={this.handleCheckboxChange.bind(this)}
                checked={this.state.filterOnlyCI}>
                Only show CI builds
              </CheckboxButton>
            </div>
          </div>
          <div className="tabs">
            <div onClick={() => this.updateLimit(7)} className={`tab ${this.getLimit() == 7 ? "selected" : ""}`}>
              7 days
            </div>
            <div onClick={() => this.updateLimit(30)} className={`tab ${this.getLimit() == 30 ? "selected" : ""}`}>
              30 days
            </div>
            <div onClick={() => this.updateLimit(90)} className={`tab ${this.getLimit() == 90 ? "selected" : ""}`}>
              90 days
            </div>
            <div onClick={() => this.updateLimit(180)} className={`tab ${this.getLimit() == 180 ? "selected" : ""}`}>
              180 days
            </div>
            <div onClick={() => this.updateLimit(365)} className={`tab ${this.getLimit() == 365 ? "selected" : ""}`}>
              365 days
            </div>
          </div>
          {this.state.loading && <div className="loading"></div>}
          {!this.state.loading && (
            <>
              <TrendsChartComponent
                title="Builds"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalNumBuilds}
                extractSecondaryValue={(date) => {
                  let stat = this.state.dateToStatMap.get(date);
                  return +stat?.totalBuildTimeUsec / +stat?.completedInvocationCount / 1000000;
                }}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " builds"}
                formatSecondaryHoverValue={(value) => (value || 0).toFixed() + " seconds average"}
                name="number of builds"
                secondaryName="average build time seconds"
                secondaryLine={true}
                separateAxis={true}
              />
              <TrendsChartComponent
                title="Build duration"
                data={this.state.lastNDates}
                extractValue={(date) => {
                  let stat = this.state.dateToStatMap.get(date);
                  return +stat?.totalBuildTimeUsec / +stat?.completedInvocationCount / 1000000;
                }}
                extractSecondaryValue={(date) => +this.state.dateToStatMap.get(date)?.maxDurationUsec / 1000000}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0).toFixed() + " seconds average"}
                formatSecondaryHoverValue={(value) => (value || 0).toFixed() + " seconds slowest"}
                name="average build time seconds"
                secondaryName="slowest build time seconds"
              />

              <CacheChartComponent
                title="Action Cache"
                data={this.state.lastNDates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +this.state.dateToStatMap.get(date)?.actionCacheHits}
                extractMisses={(date) => +this.state.dateToStatMap.get(date)?.actionCacheMisses}
              />
              <CacheChartComponent
                title="Content Adressable Store"
                data={this.state.lastNDates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +this.state.dateToStatMap.get(date)?.casCacheHits}
                extractWrites={(date) => +this.state.dateToStatMap.get(date)?.casCacheUploads}
              />

              <TrendsChartComponent
                title="Cache read throughput"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalDownloadSizeBytes / 1000000}
                extractSecondaryValue={(date) =>
                  BITS_PER_BYTE *
                  (+this.state.dateToStatMap.get(date)?.totalDownloadSizeBytes /
                    +this.state.dateToStatMap.get(date)?.totalDownloadUsec)
                }
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0).toFixed(2) + " MB downloaded"}
                formatSecondaryHoverValue={(value) => (value || 0).toFixed(2) + " Mbps"}
                name="MB downloaded"
                secondaryName="Mbps download throughput"
                secondaryLine={true}
                separateAxis={true}
              />

              <TrendsChartComponent
                title="Cache write throughput"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalUploadSizeBytes / 1000000}
                extractSecondaryValue={(date) =>
                  BITS_PER_BYTE *
                  (+this.state.dateToStatMap.get(date)?.totalUploadSizeBytes /
                    +this.state.dateToStatMap.get(date)?.totalUploadUsec)
                }
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0).toFixed(2) + " MB uploaded"}
                formatSecondaryHoverValue={(value) => (value || 0).toFixed(2) + " Mbps"}
                name="MB uploaded"
                secondaryName="Mbps upload throughput"
                secondaryLine={true}
                separateAxis={true}
              />

              <TrendsChartComponent
                title="Users with builds"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.userCount}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " users"}
                name="users with builds"
              />
              <TrendsChartComponent
                title="Commits with builds"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.commitCount}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " commits"}
                name="commits with builds"
              />
              <TrendsChartComponent
                title="Hosts with builds"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.hostCount}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " hosts"}
                name="hosts with builds"
              />
              <TrendsChartComponent
                title="Repos with builds"
                data={this.state.lastNDates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.repoCount}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " repos"}
                name="repos with builds"
              />
            </>
          )}
        </div>
      </div>
    );
  }
}
