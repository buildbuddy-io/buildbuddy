import React from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  TooltipContentProps,
  XAxis,
  YAxis,
} from "recharts";
import { User } from "../../../app/auth/user";
import capabilities from "../../../app/capabilities/capabilities";
import Button from "../../../app/components/button/button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpc_service, { CancelablePromise } from "../../../app/service/rpc_service";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import FilterComponent from "../filter/filter";
import { getProtoFilterParams } from "../filter/filter_util";
import {
  decodeMetricUrlParam,
  encodeMetricUrlParam,
  encodeTargetLabelUrlParam,
  renderMetricValue,
} from "../trends/common";
import { ChartColor } from "../trends/trends_chart";

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  // True while the first page of results is being (re)loaded.
  loading: boolean;
  // True while an additional page of results is being loaded.
  loadingMore: boolean;
  failed: boolean;
  // All target stats loaded so far, across every page fetched for the current
  // query. Undefined until the first page has loaded.
  targetStats?: stats.TargetStats[];
  // Token for the next page of results, or empty if there are no more pages.
  nextPageToken: string;
  // The current contents of the filter input. This may be ahead of the filter
  // in the URL (and therefore the loaded data) while a debounced update is
  // pending.
  filterText: string;
}

interface TargetChartData {
  target: string;
  value: number;
}

interface MetricOption {
  name: string;
  metric: stat_filter.Metric;
}

interface AggOption {
  name: string;
  agg: stats.TargetAggregation;
}

const TARGET_SELECTED_METRIC_URL_PARAM: string = "targetsMetric";
const TARGET_SELECTED_AGG_URL_PARAM: string = "targetsAgg";
const TARGET_LABEL_FILTER_URL_PARAM: string = "targetsFilter";

// How long to wait after the user stops typing before applying the label
// filter, so that we don't issue a query on every keystroke.
const FILTER_DEBOUNCE_INTERVAL_MS = 300;
// The number of top targets shown in the bar chart.
const CHART_TARGET_COUNT = 20;

const METRIC_OPTIONS: MetricOption[] = [
  {
    name: "CPU time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.EXECUTION_CPU_NANOS_EXECUTION_METRIC }),
  },
  {
    name: "Wall time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.EXECUTION_WALL_TIME_EXECUTION_METRIC }),
  },
  {
    name: "Input download size",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.INPUT_DOWNLOAD_SIZE_EXECUTION_METRIC }),
  },
  {
    name: "Output upload size",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.OUTPUT_UPLOAD_SIZE_EXECUTION_METRIC }),
  },
  {
    name: "Queue time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.QUEUE_TIME_USEC_EXECUTION_METRIC }),
  },
  {
    name: "Input download time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.INPUT_DOWNLOAD_TIME_EXECUTION_METRIC }),
  },
  {
    name: "Action execution time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.REAL_EXECUTION_TIME_EXECUTION_METRIC }),
  },
  {
    name: "Output upload time",
    metric: new stat_filter.Metric({ execution: stat_filter.ExecutionMetricType.OUTPUT_UPLOAD_TIME_EXECUTION_METRIC }),
  },
];

const AGG_OPTIONS: AggOption[] = [
  {
    name: "Total",
    agg: stats.TargetAggregation.SUM_TARGET_AGGREGATION,
  },
  {
    name: "Avg",
    agg: stats.TargetAggregation.AVG_TARGET_AGGREGATION,
  },
  {
    name: "Max",
    agg: stats.TargetAggregation.MAX_TARGET_AGGREGATION,
  },
  {
    name: "Min",
    agg: stats.TargetAggregation.MIN_TARGET_AGGREGATION,
  },
  {
    name: "P50",
    agg: stats.TargetAggregation.P50_TARGET_AGGREGATION,
  },
  {
    name: "P90",
    agg: stats.TargetAggregation.P90_TARGET_AGGREGATION,
  },
  {
    name: "P99",
    agg: stats.TargetAggregation.P99_TARGET_AGGREGATION,
  },
];

function convertMetricUrlParam(param: string): MetricOption | undefined {
  const metric = decodeMetricUrlParam(param);
  if (metric?.execution) {
    return METRIC_OPTIONS.find((v) => metric.execution === v.metric.execution) || undefined;
  }
  return undefined;
}

function convertAggUrlParam(param: string): AggOption {
  const val = Number.parseInt(param);
  if (val) {
    return AGG_OPTIONS.find((v) => v.agg == val) ?? AGG_OPTIONS[5];
  }
  return AGG_OPTIONS[5];
}

function getFilterUrlParam(search: URLSearchParams): string {
  return search.get(TARGET_LABEL_FILTER_URL_PARAM) ?? "";
}

export default class TrendsComponent extends React.Component<Props, State> {
  selectedMetric: MetricOption = METRIC_OPTIONS[6];
  selectedAgg: AggOption = AGG_OPTIONS[5];

  state: State = {
    loading: false,
    loadingMore: false,
    failed: false,
    nextPageToken: "",
    filterText: "",
  };

  private filterTimeout = 0;
  private pendingRpc?: CancelablePromise;

  private getPageTitle() {
    return "Target Performance";
  }

  private updateDocumentTitle() {
    document.title = `${this.getPageTitle()} | BuildBuddy`;
  }

  componentDidMount(): void {
    this.updateDocumentTitle();
    this.selectedMetric =
      convertMetricUrlParam(this.props.search.get(TARGET_SELECTED_METRIC_URL_PARAM) || "") || METRIC_OPTIONS[6];
    this.selectedAgg = convertAggUrlParam(this.props.search.get(TARGET_SELECTED_AGG_URL_PARAM) || "");
    this.setState({ filterText: getFilterUrlParam(this.props.search) });
    this.fetchTargetTrends();
  }

  componentDidUpdate(prevProps: Props, prevState: State): void {
    this.updateDocumentTitle();
    if (this.props.search !== prevProps.search) {
      this.selectedMetric =
        convertMetricUrlParam(this.props.search.get(TARGET_SELECTED_METRIC_URL_PARAM) || "") || METRIC_OPTIONS[6];
      this.selectedAgg = convertAggUrlParam(this.props.search.get(TARGET_SELECTED_AGG_URL_PARAM) || "");
      // Only sync the input from the URL when the URL's filter actually
      // changed, so that an unrelated URL change (e.g. picking a new metric)
      // doesn't clobber text the user is still typing.
      const filterParam = getFilterUrlParam(this.props.search);
      if (filterParam !== getFilterUrlParam(prevProps.search)) {
        // Something other than user input did this (manually edited URL?)
        window.clearTimeout(this.filterTimeout);
        if (filterParam !== this.state.filterText) {
          this.setState({ filterText: filterParam });
        }
      }
      this.fetchTargetTrends();
    }
  }

  componentWillUnmount(): void {
    window.clearTimeout(this.filterTimeout);
    this.pendingRpc?.cancel();
  }

  /**
   * Fetches a page of target trends for the current URL state. With no page
   * token, this replaces the loaded results with the first page; with a page
   * token, the fetched page is appended to the results loaded so far.
   */
  fetchTargetTrends(pageToken: string = "") {
    this.pendingRpc?.cancel();
    const isFirstPage = !pageToken;
    if (isFirstPage) {
      this.setState({ loading: true, loadingMore: false, failed: false });
    } else {
      this.setState({ loadingMore: true });
    }
    const filterParams = getProtoFilterParams(this.props.search);
    const request = stats.GetTargetTrendsRequest.create({
      metric: this.selectedMetric.metric.execution!,
      agg: this.selectedAgg.agg,
      targetLabelFilter: getFilterUrlParam(this.props.search).trim(),
      pageToken: pageToken,
    });
    request.query = new stats.TrendQuery({
      host: filterParams.host,
      user: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,
      pattern: filterParams.pattern,
      tags: filterParams.tags,
      role: filterParams.role,
      updatedBefore: filterParams.updatedBefore,
      updatedAfter: filterParams.updatedAfter,
      status: filterParams.status,
      dimensionFilter: filterParams.dimensionFilters,
      genericFilters: filterParams.genericFilters,
    });

    this.pendingRpc = rpc_service.service
      .getTargetTrends(request)
      .then((response) => {
        this.setState({
          targetStats: isFirstPage
            ? response.targetStats
            : [...(this.state.targetStats ?? []), ...response.targetStats],
          nextPageToken: response.nextPageToken,
          loading: false,
          loadingMore: false,
        });
      })
      .catch((e) => {
        if (isFirstPage) {
          console.log(e);
          this.setState({ failed: true, targetStats: undefined, nextPageToken: "", loading: false });
        } else {
          // Keep the results loaded so far and let the user retry.
          errorService.handleError(e);
          this.setState({ loadingMore: false });
        }
      });
  }

  handleMetricChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newMetric = e.target.value;

    if (!newMetric || this.selectedMetric.name === newMetric) {
      return;
    }
    const option = METRIC_OPTIONS.find((v) => v.name === newMetric) || METRIC_OPTIONS[0];
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [TARGET_SELECTED_METRIC_URL_PARAM]: encodeMetricUrlParam(option.metric),
    });
  };

  handleAggChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newAgg = e.target.value;

    if (!newAgg || this.selectedAgg.name === newAgg) {
      return;
    }
    const option = AGG_OPTIONS.find((v) => v.name === newAgg) || AGG_OPTIONS[0];
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [TARGET_SELECTED_AGG_URL_PARAM]: option.agg.toString(),
    });
  };

  handleFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const filterText = e.target.value;
    // Update the input immediately, but debounce the URL update (which
    // triggers the fetch) so we don't query on every keystroke.
    this.setState({ filterText });
    window.clearTimeout(this.filterTimeout);
    this.filterTimeout = window.setTimeout(() => this.applyFilter(filterText), FILTER_DEBOUNCE_INTERVAL_MS);
  };

  handleFilterKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      window.clearTimeout(this.filterTimeout);
      this.applyFilter(this.state.filterText);
    }
  };

  applyFilter(filterText: string) {
    if (filterText === getFilterUrlParam(this.props.search)) {
      return;
    }
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [TARGET_LABEL_FILTER_URL_PARAM]: filterText,
    });
  }

  handleShowMore = () => {
    if (!this.state.nextPageToken || this.state.loadingMore) {
      return;
    }
    this.fetchTargetTrends(this.state.nextPageToken);
  };

  getChartData = (): TargetChartData[] => {
    return (this.state.targetStats ?? []).slice(0, CHART_TARGET_COUNT).map((target) => ({
      target: target.target || "Unknown",
      value: +(target.value || 0),
    }));
  };

  handleBarClick = (data: TargetChartData) => {
    if (capabilities.config.singleTargetStatsEnabled) {
      router.navigateToSingleTarget(data.target);
    } else {
      this.navigateToTargetDrilldown(data.target);
    }
  };

  handleTableRowClick = (target: string) => {
    if (capabilities.config.singleTargetStatsEnabled) {
      router.navigateToSingleTarget(target);
    } else {
      this.navigateToTargetDrilldown(target);
    }
  };

  navigateToTargetDrilldown = (target: string) => {
    const currentParams = Object.fromEntries(this.props.search.entries());
    const dimensionParam = encodeURIComponent(encodeTargetLabelUrlParam(target));
    const dimensions = currentParams.d ? currentParams.d + "|" + dimensionParam : dimensionParam;

    // Set the metric to CPU nanos or execution time based on current selection
    const metricParam = encodeMetricUrlParam(this.selectedMetric.metric);

    router.navigateTo(`/trends/?ddMetric=${metricParam}&d=${dimensions}#drilldown`, false);
  };

  renderCustomTooltip = (p: TooltipContentProps<any, any>) => {
    if (p.active && p.payload && p.payload.length > 0) {
      const data = p.payload[0].payload as TargetChartData;
      return (
        <div className="trend-chart-hover">
          <div>
            <strong>{data.target}</strong>
          </div>
          <div>
            {this.selectedAgg.name} {this.selectedMetric.name}:{" "}
            {renderMetricValue(this.selectedMetric.metric, data.value)}
          </div>
        </div>
      );
    }
    return null;
  };

  renderMatchCount(): string | null {
    // We only know a count once a filter has been applied and loaded; the
    // count is a lower bound if there are more pages to load.
    if (!getFilterUrlParam(this.props.search) || this.state.loading || !this.state.targetStats) {
      return null;
    }
    return `${this.state.targetStats.length}${this.state.nextPageToken ? "+" : ""} matches`;
  }

  render(): React.ReactNode {
    const pageTitle = this.getPageTitle();
    const chartData = this.getChartData();
    const tableData = this.state.targetStats ?? [];
    const isFiltering = Boolean(getFilterUrlParam(this.props.search));

    return (
      <div className="targets">
        <div className="container">
          <div className="targets-header">
            <div className="targets-title">{pageTitle}</div>
            <FilterComponent search={this.props.search} />
          </div>

          <div className="targets-controls">
            <div className="controls row">
              <Select
                className="targets-metric-select"
                onChange={this.handleMetricChange.bind(this)}
                value={this.selectedMetric.name}>
                {METRIC_OPTIONS.map(
                  (o) =>
                    o.name && (
                      <Option key={o.name} value={o.name}>
                        {o.name}
                      </Option>
                    )
                )}
              </Select>
              <Select
                className="targets-agg-select"
                onChange={this.handleAggChange.bind(this)}
                value={this.selectedAgg.name}>
                {AGG_OPTIONS.map(
                  (o) =>
                    o.name && (
                      <Option key={o.name} value={o.name}>
                        {o.name}
                      </Option>
                    )
                )}
              </Select>
              <FilterInput
                placeholder="Filter targets..."
                value={this.state.filterText}
                onChange={this.handleFilterChange}
                onKeyDown={this.handleFilterKeyDown}
                rightElement={this.renderMatchCount()}
              />
            </div>
          </div>

          {this.state.loading && (
            <div className="loading-section">
              <Spinner />
            </div>
          )}

          {this.state.failed && (
            <div className="error-section">
              <div className="error-message">Failed to load target trends</div>
            </div>
          )}

          {!this.state.loading && !this.state.failed && this.state.targetStats && (
            <>
              {tableData.length > 0 ? (
                <>
                  <div className="targets-chart-section">
                    <div className="targets-section-title">
                      Top targets by {this.selectedAgg.name} {this.selectedMetric.name}
                    </div>
                    <div className="targets-chart-container">
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart accessibilityLayer={false} data={chartData}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis tick={false} tickFormatter={() => ""}></XAxis>
                          <YAxis width={120} tickFormatter={(v) => renderMetricValue(this.selectedMetric.metric, v)} />
                          <Tooltip content={this.renderCustomTooltip} wrapperStyle={{ zIndex: 1 }} />
                          <Bar
                            dataKey="value"
                            fill={ChartColor.GREEN}
                            cursor="pointer"
                            onClick={(bar) => {
                              const clickedData = bar.payload as TargetChartData | undefined;
                              if (clickedData) {
                                this.handleBarClick(clickedData);
                              }
                            }}
                          />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  <div className="targets-table-section">
                    <div className="targets-table-container">
                      <div className="results-table">
                        <div className="row column-headers">
                          <div className="name-column">Target</div>
                          <div className="value-column">
                            {this.selectedAgg.name} {this.selectedMetric.name}
                          </div>
                        </div>
                        <div className="results-list column">
                          {tableData.map((target, index) => (
                            <div
                              key={target.target || index}
                              className="row result-row clickable"
                              onClick={() => this.handleTableRowClick(target.target || "")}>
                              <div className="name-column targets-table-target">{target.target}</div>
                              <div className="value-column targets-table-value">
                                {renderMetricValue(this.selectedMetric.metric, +(target.value || 0))}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                      {this.state.nextPageToken && (
                        <div className="table-footer-controls">
                          <Button
                            className="load-more-button"
                            onClick={this.handleShowMore}
                            disabled={this.state.loadingMore}>
                            <span>Show more</span>
                            {this.state.loadingMore && <Spinner className="white" />}
                          </Button>
                        </div>
                      )}
                    </div>
                  </div>
                </>
              ) : isFiltering ? (
                <div className="targets-empty">
                  <div className="empty-message">No targets match the current filter.</div>
                </div>
              ) : (
                <div className="targets-empty">
                  <div className="empty-message">
                    No target data available for the selected filters and time range. This dashboard currently only
                    shows target data for targets that run on BuildBuddy's{" "}
                    <a href="https://www.buildbuddy.io/docs/remote-build-execution/">remote build execution service</a>.
                    If you're interested in trying it out, you can follow our{" "}
                    <a href="https://www.buildbuddy.io/docs/rbe-setup">setup instructions</a>.
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    );
  }
}
