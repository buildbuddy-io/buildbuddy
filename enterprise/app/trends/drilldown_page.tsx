import React from "react";
import Long from "long";

import rpcService from "../../../app/service/rpc_service";
import capabilities from "../../../app/capabilities/capabilities";
import Spinner from "../../../app/components/spinner/spinner";
import HistoryInvocationCardComponent from "../../app/history/history_invocation_card";
import { invocation } from "../../../proto/invocation_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import { getProtoFilterParams, isExecutionMetric } from "../filter/filter_util";
import { HeatmapComponent, HeatmapSelection } from "./heatmap";
import { BarChart, Bar, XAxis, Tooltip, CartesianGrid, TooltipProps } from "recharts";
import { User } from "../../../app/auth/user";
import Select, { Option } from "../../../app/components/select/select";
import router from "../../../app/router/router";
import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";

interface Props {
  user?: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  loadingDrilldowns: boolean;
  drilldownsFailed: boolean;
  loadingInvocations: boolean;
  invocationsFailed: boolean;
  heatmapData?: stats.GetStatHeatmapResponse;
  drilldownData?: stats.GetStatDrilldownResponse;
  invocationsData?: invocation.Invocation[];
}

interface MetricOption {
  name: string;
  metric: stat_filter.Metric;
}

const METRIC_OPTIONS: MetricOption[] = [
  {
    name: "Build duration",
    metric: stat_filter.Metric.create({ invocation: stat_filter.InvocationMetricType.DURATION_USEC_INVOCATION_METRIC }),
  },
  {
    name: "CAS cache misses",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_MISSES_INVOCATION_METRIC,
    }),
  },
  {
    name: "Queue time",
    metric: stat_filter.Metric.create({ execution: stat_filter.ExecutionMetricType.QUEUE_TIME_USEC_EXECUTION_METRIC }),
  },
];

export default class DrilldownPageComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    loadingDrilldowns: false,
    drilldownsFailed: false,
    loadingInvocations: false,
    invocationsFailed: false,
    heatmapData: undefined,
    drilldownData: undefined,
    invocationsData: undefined,
  };

  selectedMetric: MetricOption = METRIC_OPTIONS[0];

  currentHeatmapSelection?: HeatmapSelection;

  renderBucketValue(v: number) {
    if (isExecutionMetric(this.selectedMetric.metric)) {
      return `${v} execution${v === 1 ? "" : "s"}`;
    } else {
      return `${v} invocation${v === 1 ? "" : "s"}`;
    }
  }

  renderYBucketValue(v: number): string {
    if (isExecutionMetric(this.selectedMetric.metric)) {
      switch (this.selectedMetric.metric.execution) {
        case stat_filter.ExecutionMetricType.QUEUE_TIME_USEC_EXECUTION_METRIC:
          return (v / 1000000).toFixed(2) + "s";
        default:
          return v.toString();
      }
    } else {
      switch (this.selectedMetric.metric.invocation) {
        case stat_filter.InvocationMetricType.DURATION_USEC_INVOCATION_METRIC:
          return (v / 1000000).toFixed(2) + "s";
        case stat_filter.InvocationMetricType.CAS_CACHE_MISSES_INVOCATION_METRIC:
        default:
          return v.toString();
      }
    }
  }

  toStatFilterList(s: HeatmapSelection): stat_filter.StatFilter[] {
    const updatedAtUsecMetric = isExecutionMetric(this.selectedMetric.metric)
      ? stat_filter.Metric.create({ execution: stat_filter.ExecutionMetricType.UPDATED_AT_USEC_EXECUTION_METRIC })
      : stat_filter.Metric.create({ invocation: stat_filter.InvocationMetricType.UPDATED_AT_USEC_INVOCATION_METRIC });
    return [
      stat_filter.StatFilter.create({
        metric: updatedAtUsecMetric,
        min: Long.fromNumber(s.dateRangeMicros.startInclusive),
        max: Long.fromNumber(s.dateRangeMicros.endExclusive - 1),
      }),
      stat_filter.StatFilter.create({
        metric: this.selectedMetric.metric,
        min: Long.fromNumber(s.bucketRange.startInclusive),
        max: Long.fromNumber(s.bucketRange.endExclusive - 1),
      }),
    ];
  }

  fetchDrilldowns() {
    if (!this.currentHeatmapSelection) {
      this.setState({ drilldownData: undefined });
      return;
    }
    this.setState({ loadingDrilldowns: true, drilldownsFailed: false });
    const filterParams = getProtoFilterParams(this.props.search);
    const drilldownRequest = stats.GetStatDrilldownRequest.create({});
    drilldownRequest.query = new stats.TrendQuery({
      host: filterParams.host,
      user: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,

      role: filterParams.role,
      updatedBefore: filterParams.updatedBefore,
      updatedAfter: filterParams.updatedAfter,
      status: filterParams.status,
    });
    drilldownRequest.filter = this.toStatFilterList(this.currentHeatmapSelection);
    drilldownRequest.drilldownMetric = this.selectedMetric.metric;
    rpcService.service
      .getStatDrilldown(drilldownRequest)
      .then((response) => {
        this.setState({ drilldownData: response });
      })
      .catch(() => this.setState({ drilldownsFailed: true, drilldownData: undefined }))
      .finally(() => this.setState({ loadingDrilldowns: false }));
  }

  fetchInvocationList() {
    // TODO(jdhollen): Support fetching invocations based on executions data.
    if (!this.props.user?.selectedGroup || isExecutionMetric(this.selectedMetric.metric)) {
      return;
    }
    this.setState({ loadingInvocations: true, invocationsFailed: false });
    const filterParams = getProtoFilterParams(this.props.search);
    let request = new invocation.SearchInvocationRequest({
      query: new invocation.InvocationQuery({
        host: filterParams.host,
        user: filterParams.user,
        repoUrl: filterParams.repo,
        branchName: filterParams.branch,
        commitSha: filterParams.commit,
        command: filterParams.command,
        minimumDuration: filterParams.minimumDuration,
        maximumDuration: filterParams.maximumDuration,
        groupId: this.props.user.selectedGroup.id,
      }),
      pageToken: "",
      count: 25,
    });
    request.query!.role = filterParams.role || [];
    request.query!.updatedAfter = filterParams.updatedAfter;
    request.query!.updatedBefore = filterParams.updatedBefore;
    request.query!.status = filterParams.status || [];
    if (this.currentHeatmapSelection) {
      request.query!.filter = this.toStatFilterList(this.currentHeatmapSelection);
    }

    rpcService.service
      .searchInvocation(request)
      .then((response) => {
        this.setState({
          invocationsData: response.invocation,
        });
      })
      .catch(() => this.setState({ invocationsFailed: true, invocationsData: undefined }))
      .finally(() => this.setState({ loadingInvocations: false }));
  }

  fetch() {
    const filterParams = getProtoFilterParams(this.props.search);
    this.setState({ loading: true, heatmapData: undefined, drilldownData: undefined, invocationsData: undefined });

    // Build request...
    const heatmapRequest = stats.GetStatHeatmapRequest.create({});
    heatmapRequest.metric = this.selectedMetric.metric;

    heatmapRequest.query = new stats.TrendQuery({
      host: filterParams.host,
      user: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,
      role: filterParams.role,
      updatedBefore: filterParams.updatedBefore,
      updatedAfter: filterParams.updatedAfter,
      minimumDuration: filterParams.minimumDuration,
      maximumDuration: filterParams.maximumDuration,
      status: filterParams.status,
    });

    rpcService.service
      .getStatHeatmap(heatmapRequest)
      .then((response) => {
        this.setState({
          heatmapData: response,
        });
      })
      .finally(() => this.setState({ loading: false }));
  }

  componentWillMount() {
    this.fetch();
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.search != prevProps.search) {
      this.fetch();
    }
  }

  handleMetricChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const newMetric = e.target.value;

    if (!newMetric || this.selectedMetric.name === newMetric) {
      return;
    }
    this.selectedMetric = METRIC_OPTIONS.find((v) => v.name === newMetric) || METRIC_OPTIONS[0];
    this.fetch();
  }

  handleHeatmapSelection(s?: HeatmapSelection) {
    this.currentHeatmapSelection = s;
    this.fetchDrilldowns();
    this.fetchInvocationList();
  }

  handleBarClick(a: invocation.AggType, e?: CategoricalChartState) {
    if (!e || !e.activeLabel) {
      return;
    }
    switch (a) {
      case invocation.AggType.USER_AGGREGATION_TYPE:
        router.setQueryParam("user", e.activeLabel);
        return;
      case invocation.AggType.HOSTNAME_AGGREGATION_TYPE:
        router.setQueryParam("host", e.activeLabel);
        return;
      case invocation.AggType.REPO_URL_AGGREGATION_TYPE:
        router.setQueryParam("repo", e.activeLabel);
        return;
      case invocation.AggType.COMMIT_SHA_AGGREGATION_TYPE:
        router.setQueryParam("commit", e.activeLabel);
        return;
      case invocation.AggType.BRANCH_AGGREGATION_TYPE:
        router.setQueryParam("branch", e.activeLabel);
        return;
      case invocation.AggType.GROUP_ID_AGGREGATION_TYPE:
      case invocation.AggType.DATE_AGGREGATION_TYPE:
      default:
        return;
    }
  }

  formatAggType(a: invocation.AggType) {
    switch (a) {
      case invocation.AggType.USER_AGGREGATION_TYPE:
        return "user";
      case invocation.AggType.HOSTNAME_AGGREGATION_TYPE:
        return "host";
      case invocation.AggType.GROUP_ID_AGGREGATION_TYPE:
        return "group_id";
      case invocation.AggType.REPO_URL_AGGREGATION_TYPE:
        return "repo_url";
      case invocation.AggType.COMMIT_SHA_AGGREGATION_TYPE:
        return "commit_sha";
      case invocation.AggType.BRANCH_AGGREGATION_TYPE:
        return "branch_name";
      case invocation.AggType.PATTERN_AGGREGATION_TYPE:
        return "pattern";
      default:
        return "???";
    }
  }

  renderCustomTooltip(aggType: string, p: TooltipProps<any, any>) {
    if (!this.state.drilldownData) {
      return null;
    }
    if (p.active && p.payload && p.payload.length > 0) {
      return (
        <div className="trend-chart-hover">
          <div>
            {aggType}: {p.label}
          </div>
          <div>
            Base:{" "}
            <span className="drilldown-page-tooltip-base">
              {((p.payload[0].payload.baseValue / +this.state.drilldownData.totalInBase) * 100).toFixed(1)}%
            </span>
          </div>
          <div>
            Selection:{" "}
            <span className="drilldown-page-tooltip-selected">
              {((p.payload[0].payload.selectionValue / +this.state.drilldownData.totalInSelection) * 100).toFixed(1)}%
            </span>
          </div>
        </div>
      );
    }

    return null;
  }

  getColumnBucketIndex(timestamp: string) {
    return this.state.heatmapData?.timestampBracket.indexOf(Long.fromString(timestamp));
  }

  getMetricBucketIndex(metric: Long) {
    return this.state.heatmapData?.bucketBracket.indexOf(metric);
  }

  getInvocationsTitleString(): string {
    if (this.state.loadingInvocations) {
      return "";
    } else if (this.state.invocationsData) {
      if (this.state.invocationsData.length < (this.currentHeatmapSelection?.invocationsSelected || 0)) {
        if (isExecutionMetric(this.selectedMetric.metric)) {
          return `Selected Invocations (showing ${this.state.invocationsData.length} from ${this.currentHeatmapSelection?.invocationsSelected} executions)`;
        }
        return `Selected Invocations (showing ${this.state.invocationsData.length} of ${this.currentHeatmapSelection?.invocationsSelected})`;
      } else {
        return `Selected invocations (${this.state.invocationsData.length})`;
      }
    } else if (this.state.invocationsFailed) {
      return "Failed to load invocations.";
    }
    return "";
  }

  getInvocationsTitle(): React.ReactElement {
    const content = this.state.loadingInvocations ? <Spinner></Spinner> : this.getInvocationsTitleString();
    return <div className="trend-chart-title">{content}</div>;
  }

  getDrilldownChartsTitle(): string {
    if (this.state.loadingDrilldowns) {
      return "Loading drilldown dimensions";
    } else if (this.state.drilldownData) {
      return "Drilldown dimensions";
    } else if (this.state.drilldownsFailed) {
      return "Failed to load drilldown dimensions.";
    }
    return "To see drilldown charts and invocations, click and drag to select a region in the chart above";
  }

  render() {
    return (
      <div className="trend-chart">
        <div className="trend-chart-title">
          Drilldown by
          <Select
            className="drilldown-page-select"
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
        </div>
        {this.state.loading && <div className="loading"></div>}
        {!this.state.loading && (
          <>
            {this.state.heatmapData && (
              <>
                <HeatmapComponent
                  heatmapData={this.state.heatmapData || stats.GetStatHeatmapResponse.create({})}
                  metricBucketFormatter={(v) => this.renderYBucketValue(v)}
                  metricBucketName={this.selectedMetric.name}
                  valueFormatter={(v) => this.renderBucketValue(v)}
                  selectionCallback={(s) => this.handleHeatmapSelection(s)}></HeatmapComponent>
                <div className="trend-chart">
                  <div className="trend-chart-title">{this.getDrilldownChartsTitle()}</div>
                  {this.state.loadingDrilldowns && <div className="loading"></div>}
                  {!this.state.loadingDrilldowns && this.state.drilldownData && (
                    <div className="container nopadding-dense">
                      {!this.state.loadingDrilldowns &&
                        this.state.drilldownData &&
                        this.state.drilldownData.chart.map((chart) => (
                          <div className="drilldown-page-dd-chart">
                            <div className="drilldown-page-dd-chart-title">
                              {this.formatAggType(chart.drilldownDimension)}
                            </div>
                            <BarChart
                              width={300}
                              height={200}
                              data={chart.entry}
                              onClick={this.handleBarClick.bind(this, chart.drilldownDimension)}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis interval="preserveStart" dataKey={(entry: stats.DrilldownEntry) => entry.label} />
                              <Tooltip
                                content={this.renderCustomTooltip.bind(
                                  this,
                                  this.formatAggType(chart.drilldownDimension)
                                )}
                              />
                              <Bar
                                dataKey={(entry: stats.DrilldownEntry) =>
                                  +entry.baseValue / +(this.state.drilldownData?.totalInBase || 1)
                                }
                                fill="#8884d8"
                              />
                              <Bar
                                dataKey={(entry: stats.DrilldownEntry) =>
                                  +entry.selectionValue / +(this.state.drilldownData?.totalInSelection || 1)
                                }
                                fill="#82ca9d"
                              />
                            </BarChart>
                          </div>
                        ))}
                    </div>
                  )}
                </div>
                <div className="trend-chart">
                  {this.getInvocationsTitle()}
                  {this.state.invocationsData && (
                    <div className="history">
                      <div className="container nopadding-dense">
                        {this.state.invocationsData?.map((invocation) => (
                          <a href={`/invocation/${invocation.invocationId}`} onClick={(e) => e.preventDefault()}>
                            <HistoryInvocationCardComponent invocation={invocation} />
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </>
            )}
          </>
        )}
      </div>
    );
  }
}
