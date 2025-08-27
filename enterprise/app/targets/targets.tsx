import React from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, TooltipProps, XAxis, YAxis } from "recharts";
import { User } from "../../../app/auth/user";
import Button from "../../../app/components/button/button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import * as format from "../../../app/format/format";
import router from "../../../app/router/router";
import rpc_service from "../../../app/service/rpc_service";
import { stats } from "../../../proto/stats_ts_proto";
import FilterComponent from "../filter/filter";
import { getProtoFilterParams } from "../filter/filter_util";
import { encodeTargetLabelUrlParam } from "../trends/common";
import { ChartColor } from "../trends/trends_chart";

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  failed: boolean;
  data?: stats.GetTargetTrendsResponse;
  selectedMetric: "cpu" | "time";
  displayCount: number;
  filterText: string;
}

interface TargetChartData {
  target: string;
  value: number;
}

export default class TrendsComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    failed: false,
    selectedMetric: "cpu",
    displayCount: 50,
    filterText: "",
  };
  componentDidMount(): void {
    this.fetchTargetTrends();
  }

  componentDidUpdate(prevProps: Props, prevState: State): void {
    if (this.props.search !== prevProps.search || this.state.selectedMetric !== prevState.selectedMetric) {
      this.fetchTargetTrends();
    }
  }

  fetchTargetTrends() {
    this.setState({ loading: true, failed: false });
    const filterParams = getProtoFilterParams(this.props.search);
    const request = stats.GetTargetTrendsRequest.create({});
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
    request.filter = filterParams.statFilters;
    request.dimensionFilter = filterParams.dimensionFilters;

    rpc_service.service
      .getTargetTrends(request)
      .then((response) => {
        this.setState({ data: response });
      })
      .catch(() => this.setState({ failed: true, data: undefined }))
      .finally(() => this.setState({ loading: false }));
  }

  handleMetricChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selectedMetric = e.target.value as "cpu" | "time";
    this.setState({ selectedMetric, displayCount: 50 }); // Reset pagination when changing metrics
  };

  handleFilterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    this.setState({ filterText: e.target.value, displayCount: 50 }); // Reset pagination when filtering
  };

  handleShowMore = () => {
    this.setState({ displayCount: this.state.displayCount + 50 });
  };

  formatValue = (value: number): string => {
    if (this.state.selectedMetric === "cpu") {
      // CPU nanos -> seconds
      return format.durationSec(value / 1e9);
    } else {
      // Execution time in microseconds -> seconds
      return format.durationUsec(value);
    }
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

    const targets =
      this.state.selectedMetric === "cpu"
        ? this.state.data.targetsByCpuNanos || []
        : this.state.data.targetsByExecutionTime || [];

    // Take up to 1000 results for client-side filtering
    const maxData = targets.slice(0, 1000);

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
    const metricParam = this.state.selectedMetric === "cpu" ? "e10" : "e4"; // e10 = cpu nanos, e4 = wall time

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
            {this.state.selectedMetric === "cpu" ? "Total CPU time" : "Total wall time"}: {this.formatValue(data.value)}
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
                value={this.state.selectedMetric}
                onChange={this.handleMetricChange}>
                <Option value="cpu">CPU time</Option>
                <Option value="time">Wall time</Option>
              </Select>
              <FilterInput
                placeholder="Filter targets..."
                value={this.state.filterText}
                onChange={this.handleFilterChange}
                rightElement={
                  this.getFilteredData().length !==
                  (this.state.selectedMetric === "cpu"
                    ? this.state.data?.targetsByCpuNanos?.length || 0
                    : this.state.data?.targetsByExecutionTime?.length || 0)
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
                      Top targets by {this.state.selectedMetric === "cpu" ? "CPU Time" : "Wall Time"}
                    </div>
                    <div className="targets-chart-container">
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={chartData}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis tick={false} tickFormatter={() => ""}></XAxis>
                          <YAxis tickFormatter={this.formatValue} />
                          <Tooltip content={this.renderCustomTooltip} wrapperStyle={{ zIndex: 1 }} />
                          <Bar
                            dataKey="value"
                            fill={ChartColor.GREEN}
                            cursor="pointer"
                            onClick={(e: any) => {
                              console.log("event");
                              console.log(e);
                              const clickedData = chartData.find((d) => {
                                return d.target === e.target;
                              });
                              console.log("data");
                              console.log(clickedData);
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
                            Total {this.state.selectedMetric === "cpu" ? "CPU Time" : "Wall Time"}
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
                                {this.formatValue(+(target.value || 0))}
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
                  <div className="empty-message">No target data available for the selected filters and time range.</div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    );
  }
}
