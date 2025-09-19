import React from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, TooltipProps, XAxis, YAxis } from "recharts";
import { User } from "../../../app/auth/user";
import Button from "../../../app/components/button/button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import router from "../../../app/router/router";
import rpc_service from "../../../app/service/rpc_service";
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
  loading: boolean;
  failed: boolean;
  data?: stats.GetTargetTrendsResponse;
  displayCount: number;
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
    const opt = AGG_OPTIONS.find((v) => v.agg == val);
    return opt ?? AGG_OPTIONS[0];
  }
  return AGG_OPTIONS[0];
}

export default class TrendsComponent extends React.Component<Props, State> {
  selectedMetric: MetricOption = METRIC_OPTIONS[0];
  selectedAgg: AggOption = AGG_OPTIONS[0];

  state: State = {
    loading: false,
    failed: false,
    displayCount: 50,
    filterText: "",
  };
  componentDidMount(): void {
    this.selectedMetric =
      convertMetricUrlParam(this.props.search.get(TARGET_SELECTED_METRIC_URL_PARAM) || "") || METRIC_OPTIONS[0];
    this.selectedAgg = convertAggUrlParam(this.props.search.get(TARGET_SELECTED_AGG_URL_PARAM) || "");
    this.fetchTargetTrends();
  }

  componentDidUpdate(prevProps: Props, prevState: State): void {
    if (this.props.search !== prevProps.search) {
      this.selectedMetric =
        convertMetricUrlParam(this.props.search.get(TARGET_SELECTED_METRIC_URL_PARAM) || "") || METRIC_OPTIONS[0];
      this.selectedAgg = convertAggUrlParam(this.props.search.get(TARGET_SELECTED_AGG_URL_PARAM) || "");
      this.fetchTargetTrends();
    }
  }

  fetchTargetTrends() {
    this.setState({ loading: true, failed: false });
    const filterParams = getProtoFilterParams(this.props.search);
    const request = stats.GetTargetTrendsRequest.create({
      metric: this.selectedMetric.metric.execution!,
      agg: this.selectedAgg.agg,
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

    rpc_service.service
      .getTargetTrends(request)
      .then((response) => {
        this.setState({ data: response });
      })
      .catch((e) => {
        console.log(e);
        this.setState({ failed: true, data: undefined });
      })
      .finally(() => this.setState({ loading: false }));
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
    this.setState({ displayCount: 50 }); // Reset pagination when changing metrics
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
    this.setState({ displayCount: 50 }); // Reset pagination when changing aggregation.
  };

  handleFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({ filterText: e.target.value, displayCount: 50 }); // Reset pagination when filtering
  };

  handleShowMore = () => {
    this.setState({ displayCount: this.state.displayCount + 50 });
  };

  getChartData = (): TargetChartData[] => {
    const filteredData = this.getFilteredData();
    return filteredData.slice(0, 20).map((target) => ({
      target: target.target || "Unknown",
      value: +(target.value || 0),
    }));
  };

  getFilteredData = (): stats.TargetStats[] => {
    if (!this.state.data) return [];

    // Take up to 1000 results for client-side filtering
    const maxData = this.state.data.targetStats.slice(0, 1000);

    if (!this.state.filterText) {
      return maxData;
    }

    // Filter by target name
    const filterLower = this.state.filterText.toLowerCase();
    return maxData.filter((target) => (target.target || "").toLowerCase().includes(filterLower));
  };

  getTableData = (): stats.TargetStats[] => {
    const filteredData = this.getFilteredData();
    return filteredData.slice(0, this.state.displayCount);
  };

  handleBarClick = (data: TargetChartData) => {
    this.navigateToTargetDrilldown(data.target);
  };

  handleTableRowClick = (target: string) => {
    this.navigateToTargetDrilldown(target);
  };

  navigateToTargetDrilldown = (target: string) => {
    const currentParams = Object.fromEntries(this.props.search.entries());
    const dimensionParam = encodeTargetLabelUrlParam(target);
    const dimensions = currentParams.d ? currentParams.d + "|" + dimensionParam : dimensionParam;

    // Set the metric to CPU nanos or execution time based on current selection
    const metricParam = encodeMetricUrlParam(this.selectedMetric.metric);

    router.navigateTo(`/trends/?ddMetric=${metricParam}&d=${dimensions}#drilldown`, false);
  };

  renderCustomTooltip = (p: TooltipProps<any, any>) => {
    if (p.active && p.payload && p.payload.length > 0) {
      const data = p.payload[0].payload as TargetChartData;
      return (
        <div className="trend-chart-hover">
          <div>
            <strong>{data.target}</strong>
          </div>
          <div>
            {this.selectedMetric.name}: {renderMetricValue(this.selectedMetric.metric, data.value)}
          </div>
        </div>
      );
    }
    return null;
  };

  render(): React.ReactNode {
    const chartData = this.getChartData();
    const tableData = this.getTableData();

    return (
      <div className="targets">
        <div className="container">
          <div className="targets-header">
            <div className="targets-title">Target Performance</div>
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
                rightElement={
                  this.getFilteredData().length !== (this.state.data?.targetStats.length ?? 0)
                    ? `${this.getFilteredData().length} matches`
                    : null
                }
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

          {!this.state.loading && !this.state.failed && this.state.data && (
            <>
              {chartData.length > 0 ? (
                <>
                  <div className="targets-chart-section">
                    <div className="targets-section-title">
                      Top targets by {this.selectedMetric.name} ({this.selectedAgg.name})
                    </div>
                    <div className="targets-chart-container">
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={chartData}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis tick={false} tickFormatter={() => ""}></XAxis>
                          <YAxis width={120} tickFormatter={(v) => renderMetricValue(this.selectedMetric.metric, v)} />
                          <Tooltip content={this.renderCustomTooltip} wrapperStyle={{ zIndex: 1 }} />
                          <Bar
                            dataKey="value"
                            fill={ChartColor.GREEN}
                            cursor="pointer"
                            onClick={(e: any) => {
                              const clickedData = chartData.find((d) => {
                                return d.target === e.target;
                              });
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
                      {this.getFilteredData().length > this.state.displayCount && (
                        <div className="table-footer-controls">
                          <Button
                            className="load-more-button"
                            onClick={this.handleShowMore}
                            disabled={this.state.loading}>
                            <span>Show more</span>
                            {this.state.loading && <Spinner className="white" />}
                          </Button>
                        </div>
                      )}
                    </div>
                  </div>
                </>
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
