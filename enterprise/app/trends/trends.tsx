import React from "react";
import moment from "moment";
import * as format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import { User } from "../../../app/auth/auth_service";
import { invocation } from "../../../proto/invocation_ts_proto";
import TrendsChartComponent from "./trends_chart";
import CacheChartComponent from "./cache_chart";
import { Subscription } from "rxjs";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import FilterComponent from "../filter/filter";
import capabilities from "../../../app/capabilities/capabilities";
import { getProtoFilterParams } from "../filter/filter_util";
import router from "../../../app/router/router";
import * as proto from "../../../app/util/proto";

const BITS_PER_BYTE = 8;

interface Props {
  user: User;
  hash: string;
  search: URLSearchParams;
}

interface State {
  stats: invocation.ITrendStat[];
  loading: boolean;
  dateToStatMap: Map<string, invocation.ITrendStat>;
  dates: string[];
  filterOnlyCI: boolean;
}

const SECONDS_PER_MICROSECOND = 1e-6;

const countTickFormatter = (value: number) => {
  // Don't show fractional counts ("1.5 builds", "0.5 users" etc.)
  // This case will only ever be reached for very small numbers, because
  // tick values should be large, whole numbers when counts are high.
  if (Math.floor(Number(value)) !== Number(value)) return "";
  return format.count(value, /*fractionDigits=*/ 0);
};
const bytesTickFormatter = (value: number) => format.bytes(value, /*fractionDigits=*/ 0);
const bitsPerSecondTickFormatter = (value: number) => format.bitsPerSecond(value, /*=fractionDigits=*/ 0);

export default class TrendsComponent extends React.Component<Props, State> {
  props: Props;

  state: State = {
    stats: [],
    loading: true,
    dateToStatMap: new Map<string, invocation.ITrendStat>(),
    dates: [],
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
    // TODO(bduffany): Cancel in-progress request

    let request = new invocation.GetTrendRequest();
    request.query = new invocation.TrendQuery();

    if (capabilities.globalFilter) {
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
      request.query.status = filterParams.status;
    } else {
      // TODO(bduffany): Clean up this branch once the global filter is switched on
      if (this.state.filterOnlyCI) {
        request.query.role = ["CI"];
      } else {
        request.query.role = ["", "CI"];
      }
      request.lookbackWindowDays = this.getLimit();
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

    if (this.props.search.get("branch")) {
      request.query.branchName = this.props.search.get("branch");
    }

    if (this.props.search.get("repo")) {
      request.query.repoUrl = this.props.search.get("repo");
    }

    this.setState({ ...this.state, loading: true });
    rpcService.service.getTrend(request).then((response) => {
      console.log(response);
      const dateToStatMap = new Map<string, invocation.ITrendStat>();
      for (let stat of response.trendStat) {
        dateToStatMap.set(stat.name, stat);
      }
      this.setState({
        ...this.state,
        stats: response.trendStat,
        dates: capabilities.globalFilter
          ? getDatesBetween(
              // Start date should always be defined.
              proto.timestampToDate(request.query.updatedAfter),
              // End date may not be defined -- default to today.
              request.query.updatedBefore ? proto.timestampToDate(request.query.updatedBefore) : new Date()
            )
          : this.getLastNDates(request.lookbackWindowDays),
        dateToStatMap,
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

  onBarClicked(hash: string, date: string) {
    router.navigateTo("/?start=" + date + "&end=" + date + hash);
  }

  render() {
    return (
      <div className="trends">
        <div className="container">
          <div className="trends-header">
            <div className="trends-title">Trends</div>
            {capabilities.globalFilter ? (
              <FilterComponent search={this.props.search} />
            ) : (
              <div>
                <CheckboxButton
                  className="show-changes-only-button"
                  onChange={this.handleCheckboxChange.bind(this)}
                  checked={this.state.filterOnlyCI}>
                  Only show CI builds
                </CheckboxButton>
              </div>
            )}
          </div>
          {!capabilities.globalFilter && (
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
          )}
          {this.state.loading && <div className="loading"></div>}
          {!this.state.loading && (
            <>
              <TrendsChartComponent
                title="Builds"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalNumBuilds}
                extractSecondaryValue={(date) => {
                  let stat = this.state.dateToStatMap.get(date);
                  return (+stat?.totalBuildTimeUsec * SECONDS_PER_MICROSECOND) / +stat?.completedInvocationCount;
                }}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " builds"}
                formatSecondaryHoverValue={(value) => `${format.durationSec(value)} average`}
                formatSecondaryTickValue={format.compactDurationSec}
                name="builds"
                secondaryName="average build duration"
                secondaryLine={true}
                separateAxis={true}
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "") : null}
              />
              <TrendsChartComponent
                title="Build duration"
                data={this.state.dates}
                extractValue={(date) => {
                  let stat = this.state.dateToStatMap.get(date);
                  return +stat?.totalBuildTimeUsec / +stat?.completedInvocationCount / 1000000;
                }}
                extractSecondaryValue={(date) => +this.state.dateToStatMap.get(date)?.maxDurationUsec / 1000000}
                extractLabel={this.formatShortDate}
                formatTickValue={format.compactDurationSec}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => `${format.durationSec(value || 0)} average`}
                formatSecondaryHoverValue={(value) => `${format.durationSec(value || 0)} slowest`}
                name="average build duration"
                secondaryName="slowest build duration"
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "") : null}
              />

              <CacheChartComponent
                title="Action Cache"
                data={this.state.dates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +this.state.dateToStatMap.get(date)?.actionCacheHits}
                extractMisses={(date) => +this.state.dateToStatMap.get(date)?.actionCacheMisses}
              />
              <CacheChartComponent
                title="Content Addressable Store"
                data={this.state.dates}
                extractLabel={this.formatShortDate}
                formatHoverLabel={this.formatLongDate}
                extractHits={(date) => +this.state.dateToStatMap.get(date)?.casCacheHits}
                extractWrites={(date) => +this.state.dateToStatMap.get(date)?.casCacheUploads}
              />

              <TrendsChartComponent
                title="Cache read throughput"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalDownloadSizeBytes}
                extractSecondaryValue={(date) =>
                  (+this.state.dateToStatMap.get(date)?.totalDownloadSizeBytes * BITS_PER_BYTE) /
                  (+this.state.dateToStatMap.get(date)?.totalDownloadUsec * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate}
                formatTickValue={bytesTickFormatter}
                formatSecondaryTickValue={bitsPerSecondTickFormatter}
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
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.totalUploadSizeBytes}
                extractSecondaryValue={(date) =>
                  (+this.state.dateToStatMap.get(date)?.totalUploadSizeBytes * BITS_PER_BYTE) /
                  (+this.state.dateToStatMap.get(date)?.totalUploadUsec * SECONDS_PER_MICROSECOND)
                }
                extractLabel={this.formatShortDate}
                formatTickValue={bytesTickFormatter}
                formatSecondaryTickValue={bitsPerSecondTickFormatter}
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
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.userCount}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " users"}
                name="users with builds"
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "#users") : null}
              />
              <TrendsChartComponent
                title="Commits with builds"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.commitCount}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " commits"}
                name="commits with builds"
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "#commits") : null}
              />
              <TrendsChartComponent
                title="Branches with builds"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.branchCount}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " branches"}
                name="branches with builds"
              />
              <TrendsChartComponent
                title="Hosts with builds"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.hostCount}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " hosts"}
                name="hosts with builds"
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "#hosts") : null}
              />
              <TrendsChartComponent
                title="Repos with builds"
                data={this.state.dates}
                extractValue={(date) => +this.state.dateToStatMap.get(date)?.repoCount}
                extractLabel={this.formatShortDate}
                formatTickValue={countTickFormatter}
                formatHoverLabel={this.formatLongDate}
                formatHoverValue={(value) => (value || 0) + " repos"}
                name="repos with builds"
                onBarClicked={capabilities.globalFilter ? this.onBarClicked.bind(this, "#repos") : null}
              />
            </>
          )}
        </div>
      </div>
    );
  }
}

function getDatesBetween(start: Date, end: Date): string[] {
  const endMoment = moment(end);
  const formattedDates = [];
  for (let date = moment(start); date.isBefore(endMoment); date = date.add(1, "days")) {
    formattedDates.push(date.format("YYYY-MM-DD"));
  }
  return formattedDates;
}
