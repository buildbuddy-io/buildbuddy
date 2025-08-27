import React from "react";
import { Bar, BarChart, CartesianGrid, Tooltip, TooltipProps, XAxis, YAxis } from "recharts";
import { CategoricalChartState } from "recharts/types/chart/types";
import { User } from "../../../app/auth/user";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import * as format from "../../../app/format/format";
import router from "../../../app/router/router";
import rpc_service from "../../../app/service/rpc_service";
import { stats } from "../../../proto/stats_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import FilterComponent from "../filter/filter";
import { getProtoFilterParams } from "../filter/filter_util";
import { encodeTargetLabelUrlParam } from "../trends/common";

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  failed: boolean;
  data?: stats.GetTargetTrendsResponse;
  selectedMetric: "cpu" | "time";
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
    this.setState({ selectedMetric });
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
    if (!this.state.data) return [];

    const targets = this.state.selectedMetric === "cpu" 
      ? this.state.data.targetsByCpuNanos || []
      : this.state.data.targetsByExecutionTime || [];

    return targets.slice(0, 20).map(target => ({
      target: target.target || "Unknown",
      value: +(target.value || 0)
    }));
  };

  getTableData = (): stats.TargetStats[] => {
    if (!this.state.data) return [];

    const targets = this.state.selectedMetric === "cpu" 
      ? this.state.data.targetsByCpuNanos || []
      : this.state.data.targetsByExecutionTime || [];

    return targets.slice(0, 50);
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
    const existingDimensions = currentParams.d ? currentParams.d + "|" + dimensionParam : dimensionParam;

    // Set the metric to CPU nanos or execution time based on current selection
    const metricParam = this.state.selectedMetric === "cpu" ? "e7" : "e5"; // e7 = CPU_NANOS, e5 = REAL_EXECUTION_TIME

    router.navigateTo("/trends/drilldowns", {
      ...currentParams,
      d: existingDimensions,
      ddMetric: metricParam
    });
  };

  renderCustomTooltip = (p: TooltipProps<any, any>) => {
    if (p.active && p.payload && p.payload.length > 0) {
      const data = p.payload[0].payload as TargetChartData;
      return (
        <div className="trend-chart-hover">
          <div><strong>{data.target}</strong></div>
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
            <Select
              className="targets-metric-select"
              value={this.state.selectedMetric === "cpu" ? "CPU time" : "Wall time"}
              onChange={this.handleMetricChange}
            >
              <Option value="cpu">CPU time</Option>
              <Option value="time">Wall time</Option>
            </Select>
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
                      Top 20 Targets by {this.state.selectedMetric === "cpu" ? "CPU Time" : "Wall Time"}
                    </div>
                    <div className="targets-chart-container">
                      <BarChart
                        width={1000}
                        height={400}
                        data={chartData}
                        onClick={(e: any) => {
                          if (e?.activeLabel) {
                            const clickedData = chartData.find(d => d.target === e.activeLabel);
                            if (clickedData) {
                              this.handleBarClick(clickedData);
                            }
                          }
                        }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="target" 
                          angle={-45}
                          textAnchor="end"
                          height={120}
                          interval={0}
                        />
                        <YAxis 
                          tickFormatter={this.formatValue}
                        />
                        <Tooltip 
                          content={this.renderCustomTooltip}
                          allowEscapeViewBox={{ x: true, y: true }}
                          wrapperStyle={{ zIndex: 1 }}
                        />
                        <Bar 
                          dataKey="value" 
                          fill="#4daf62"
                          cursor="pointer"
                        />
                      </BarChart>
                    </div>
                  </div>

                  <div className="targets-table-section">
                    <div className="targets-section-title">
                      Top 50 Targets by {this.state.selectedMetric === "cpu" ? "CPU Time" : "Wall Time"}
                    </div>
                    <div className="targets-table-container">
                      <table className="targets-table">
                        <thead>
                          <tr>
                            <th>Target</th>
                            <th>Total {this.state.selectedMetric === "cpu" ? "CPU Time" : "Wall Time"}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {tableData.map((target, index) => (
                            <tr 
                              key={target.target || index} 
                              className="targets-table-row clickable"
                              onClick={() => this.handleTableRowClick(target.target || "")}
                            >
                              <td className="targets-table-target">{target.target}</td>
                              <td className="targets-table-value">{this.formatValue(+(target.value || 0))}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              ) : (
                <div className="targets-empty">
                  <div className="empty-message">
                    No target data available for the selected filters and time range.
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
