import React, { ReactNode } from "react";
import moment from "moment";

import TrendsModel from "./trends_model";
import TrendsChartComponent from "./trends_chart";
import router from "../../../app/router/router";
import format from "../../../app/format/format";
import PercentilesChartComponent from "./percentile_chart";
import { CancelablePromise } from "../../../app/util/async";
import TrendsTab from "./common";
import TrendsSummaryCard from "./summary_card";
import capabilities from "../../../app/capabilities/capabilities";
import CacheChartComponent from "./cache_chart";
import { fetchTrends, TrendsRpcCache } from "./trends_requests";

interface Props {
  search: URLSearchParams;
  cache: TrendsRpcCache;
  tab: TrendsTab;
}

interface State {
  trendsModel?: TrendsModel;
}

const SECONDS_PER_MICROSECOND = 1e-6;
const BITS_PER_BYTE = 8;

export default class SimpleTrendsTabComponent extends React.Component<Props, State> {
  private pendingTrendsRequest?: CancelablePromise<any>;

  componentWillMount() {
    this.fetch();
  }

  componentWillUnmount() {
    this.pendingTrendsRequest?.cancel();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search.toString() != prevProps.search.toString()) {
      this.fetch();
    }
  }

  fetch() {
    if (this.pendingTrendsRequest) {
      this.pendingTrendsRequest.cancel();
      this.pendingTrendsRequest = undefined;
    }
    this.pendingTrendsRequest = fetchTrends(
      this.props.search,
      (m) => {
        this.setState({ trendsModel: m });
      },
      this.props.cache
    );
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

  renderOverview(model: TrendsModel) {
    const currentSummary = model.getCurrentSummary();
    const previousSummary = model.getPreviousSummary();
    if (currentSummary && previousSummary) {
      return (
        <TrendsSummaryCard
          search={this.props.search}
          currentPeriod={currentSummary}
          previousPeriod={previousSummary}></TrendsSummaryCard>
      );
    }
    return <div>Overview...</div>;
  }

  renderBuilds(model: TrendsModel) {
    return (
      <>
        <TrendsChartComponent
          title="Builds"
          id="builds"
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).totalNumBuilds ?? 0)}
          extractSecondaryValue={(date) => {
            let stat = model.getStat(date);
            return (+(stat.totalBuildTimeUsec ?? 0) * SECONDS_PER_MICROSECOND) / +(stat.completedInvocationCount ?? 0);
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
        {model.hasInvocationStatPercentiles() && (
          <PercentilesChartComponent
            title="Build duration"
            id="duration"
            data={model.getDates()}
            extractLabel={this.formatShortDate}
            formatHoverLabel={this.formatLongDate}
            extractP50={(date) => +(model.getStat(date).buildTimeUsecP50 ?? 0) * SECONDS_PER_MICROSECOND}
            extractP75={(date) => +(model.getStat(date).buildTimeUsecP75 ?? 0) * SECONDS_PER_MICROSECOND}
            extractP90={(date) => +(model.getStat(date).buildTimeUsecP90 ?? 0) * SECONDS_PER_MICROSECOND}
            extractP95={(date) => +(model.getStat(date).buildTimeUsecP95 ?? 0) * SECONDS_PER_MICROSECOND}
            extractP99={(date) => +(model.getStat(date).buildTimeUsecP99 ?? 0) * SECONDS_PER_MICROSECOND}
            onColumnClicked={this.onBarClicked.bind(this, "", "duration")}
          />
        )}
        {!model.hasInvocationStatPercentiles() && (
          <TrendsChartComponent
            title="Build duration"
            id="duration"
            data={model.getDates()}
            extractValue={(date) => {
              let stat = model.getStat(date);
              return +(stat.totalBuildTimeUsec ?? 0) / +(stat.completedInvocationCount ?? 0) / 1000000;
            }}
            extractSecondaryValue={(date) => +(model.getStat(date).maxDurationUsec ?? 0) / 1000000}
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

        <TrendsChartComponent
          title="Users with builds"
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).userCount ?? 0)}
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
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).commitCount ?? 0)}
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
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).branchCount ?? 0)}
          extractLabel={this.formatShortDate}
          formatTickValue={format.count}
          allowDecimals={false}
          formatHoverLabel={this.formatLongDate}
          formatHoverValue={(value) => (value || 0) + " branches"}
          name="branches with builds"
        />
        <TrendsChartComponent
          title="Hosts with builds"
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).hostCount ?? 0)}
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
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).repoCount ?? 0)}
          extractLabel={this.formatShortDate}
          formatTickValue={format.count}
          allowDecimals={false}
          formatHoverLabel={this.formatLongDate}
          formatHoverValue={(value) => (value || 0) + " repos"}
          name="repos with builds"
          onBarClicked={this.onBarClicked.bind(this, "#repos", "")}
        />
      </>
    );
  }

  renderCache(model: TrendsModel) {
    return (
      <>
        <CacheChartComponent
          title="Action Cache"
          id="cache"
          data={model.getDates()}
          extractLabel={this.formatShortDate}
          formatHoverLabel={this.formatLongDate}
          extractHits={(date) => +(model.getStat(date).actionCacheHits ?? 0)}
          secondaryBarName="misses"
          extractSecondary={(date) => +(model.getStat(date).actionCacheMisses ?? 0)}
        />
        <CacheChartComponent
          title="Content Addressable Store"
          data={model.getDates()}
          extractLabel={this.formatShortDate}
          formatHoverLabel={this.formatLongDate}
          extractHits={(date) => +(model.getStat(date).casCacheHits ?? 0)}
          secondaryBarName="writes"
          extractSecondary={(date) => +(model.getStat(date).casCacheUploads ?? 0)}
        />
        <TrendsChartComponent
          title="Cache read throughput"
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).totalDownloadSizeBytes ?? 0)}
          extractSecondaryValue={(date) =>
            (+(model.getStat(date).totalDownloadSizeBytes ?? 0) * BITS_PER_BYTE) /
            (+(model.getStat(date).totalDownloadUsec ?? 0) * SECONDS_PER_MICROSECOND)
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
          data={model.getDates()}
          extractValue={(date) => +(model.getStat(date).totalUploadSizeBytes ?? 0)}
          extractSecondaryValue={(date) =>
            (+(model.getStat(date).totalUploadSizeBytes ?? 0) * BITS_PER_BYTE) /
            (+(model.getStat(date).totalUploadUsec ?? 0) * SECONDS_PER_MICROSECOND)
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

        {capabilities.config.trendsSummaryEnabled && (
          <TrendsChartComponent
            title="Saved CPU Time"
            id="savings"
            data={model.getDates()}
            extractValue={(date) => +(model.getStat(date).totalCpuMicrosSaved ?? 0) * SECONDS_PER_MICROSECOND}
            extractLabel={this.formatShortDate}
            formatTickValue={format.durationSec}
            allowDecimals={false}
            formatHoverLabel={this.formatLongDate}
            formatHoverValue={(value) => `${format.durationSec(value || 0)} CPU time saved`}
            name="saved cpu time"
          />
        )}
      </>
    );
  }

  renderExecutions(model: TrendsModel) {
    return (
      model.hasExecutionStats() && (
        <PercentilesChartComponent
          title="Remote Execution Queue Duration"
          data={model.getDates()}
          extractLabel={this.formatShortDate}
          formatHoverLabel={this.formatLongDate}
          extractP50={(date) => +(model.getExecutionStat(date).queueDurationUsecP50 ?? 0) * SECONDS_PER_MICROSECOND}
          extractP75={(date) => +(model.getExecutionStat(date).queueDurationUsecP75 ?? 0) * SECONDS_PER_MICROSECOND}
          extractP90={(date) => +(model.getExecutionStat(date).queueDurationUsecP90 ?? 0) * SECONDS_PER_MICROSECOND}
          extractP95={(date) => +(model.getExecutionStat(date).queueDurationUsecP95 ?? 0) * SECONDS_PER_MICROSECOND}
          extractP99={(date) => +(model.getExecutionStat(date).queueDurationUsecP99 ?? 0) * SECONDS_PER_MICROSECOND}
        />
      )
    );
  }

  renderError(error?: string) {
    if (error) {
      error = `There was a problem loading trends data: ${error}`;
    } else {
      error = `There was an unknown error while loading trends data.`;
    }
    return (
      <div className="error">
        <div>{error}</div>
        <div>Please try reloading this page in a few minutes.</div>
      </div>
    );
  }

  render() {
    if (!this.state.trendsModel || this.state.trendsModel.isLoading()) {
      return <div className="loading"></div>;
    }
    if (this.state.trendsModel.isError()) {
      return this.renderError(this.state.trendsModel.getError());
    }
    switch (this.props.tab) {
      case TrendsTab.OVERVIEW:
        return this.renderOverview(this.state.trendsModel);
      case TrendsTab.BUILDS:
        return this.renderBuilds(this.state.trendsModel);
      case TrendsTab.CACHE:
        return this.renderCache(this.state.trendsModel);
      case TrendsTab.EXECUTIONS:
        return this.renderExecutions(this.state.trendsModel);
      default:
        console.error(`Unsupported trends tab type: ${this.props.tab}`);
        return null;
    }
  }
}
