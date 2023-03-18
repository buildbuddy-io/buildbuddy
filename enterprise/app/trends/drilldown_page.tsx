import React from "react";
import Long from "long";
import moment from "moment";
import { X, ZoomIn } from "lucide-react";

import format from "../../../app/format/format";
import rpcService from "../../../app/service/rpc_service";
import capabilities from "../../../app/capabilities/capabilities";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import HistoryInvocationCardComponent from "../../app/history/history_invocation_card";
import InvocationExecutionTable from "../../../app/invocation/invocation_execution_table";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { invocation } from "../../../proto/invocation_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";
import { usecToTimestamp } from "../../../app/util/proto";
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

type EventData = {
  invocations?: invocation.Invocation[];
  executions?: execution_stats.ExecutionWithInvocationMetadata[];
};

interface State {
  loading: boolean;
  loadingDrilldowns: boolean;
  drilldownsFailed: boolean;
  loadingEvents: boolean;
  eventsFailed: boolean;
  heatmapData?: stats.GetStatHeatmapResponse;
  drilldownData?: stats.GetStatDrilldownResponse;
  eventData?: EventData;
}

interface MetricOption {
  name: string;
  metric: stat_filter.Metric;
}

// A little bit of structural typing for TrendQuery and InvocationQuery
type CommonQueryFields = {
  updatedBefore?: google_timestamp.protobuf.Timestamp | null;
  updatedAfter?: google_timestamp.protobuf.Timestamp | null;
  filter?: stat_filter.StatFilter[];
};

const METRIC_OPTIONS: MetricOption[] = [
  {
    name: "Build duration",
    metric: stat_filter.Metric.create({ invocation: stat_filter.InvocationMetricType.DURATION_USEC_INVOCATION_METRIC }),
  },
  {
    name: "Cache download size",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_DOWNLOAD_SIZE_INVOCATION_METRIC,
    }),
  },
  {
    name: "Cache download speed",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_DOWNLOAD_SPEED_INVOCATION_METRIC,
    }),
  },
  {
    name: "Cache upload size",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_UPLOAD_SIZE_INVOCATION_METRIC,
    }),
  },
  {
    name: "Cache upload speed",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_UPLOAD_SPEED_INVOCATION_METRIC,
    }),
  },
  {
    name: "CAS cache misses",
    metric: stat_filter.Metric.create({
      invocation: stat_filter.InvocationMetricType.CAS_CACHE_MISSES_INVOCATION_METRIC,
    }),
  },
  {
    name: "Execution queue time",
    metric: stat_filter.Metric.create({ execution: stat_filter.ExecutionMetricType.QUEUE_TIME_USEC_EXECUTION_METRIC }),
  },
  {
    name: "Execution input download time",
    metric: stat_filter.Metric.create({
      execution: stat_filter.ExecutionMetricType.INPUT_DOWNLOAD_TIME_EXECUTION_METRIC,
    }),
  },
  {
    name: "Execution action execution time",
    metric: stat_filter.Metric.create({
      execution: stat_filter.ExecutionMetricType.REAL_EXECUTION_TIME_EXECUTION_METRIC,
    }),
  },
  {
    name: "Execution output upload time",
    metric: stat_filter.Metric.create({
      execution: stat_filter.ExecutionMetricType.OUTPUT_UPLOAD_TIME_EXECUTION_METRIC,
    }),
  },
  {
    name: "Executor peak memory usage",
    metric: stat_filter.Metric.create({ execution: stat_filter.ExecutionMetricType.PEAK_MEMORY_EXECUTION_METRIC }),
  },
];

export default class DrilldownPageComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    loadingDrilldowns: false,
    drilldownsFailed: false,
    loadingEvents: false,
    eventsFailed: false,
    heatmapData: undefined,
    drilldownData: undefined,
    eventData: undefined,
  };

  selectedMetric: MetricOption = METRIC_OPTIONS[0];

  currentHeatmapSelection?: HeatmapSelection;
  currentZoomFilters?: HeatmapSelection;

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
        case stat_filter.ExecutionMetricType.INPUT_DOWNLOAD_TIME_EXECUTION_METRIC:
        case stat_filter.ExecutionMetricType.REAL_EXECUTION_TIME_EXECUTION_METRIC:
        case stat_filter.ExecutionMetricType.OUTPUT_UPLOAD_TIME_EXECUTION_METRIC:
          return (v / 1000000).toFixed(2) + "s";
        case stat_filter.ExecutionMetricType.PEAK_MEMORY_EXECUTION_METRIC:
          return format.bytes(v);
        default:
          return v.toString();
      }
    } else {
      switch (this.selectedMetric.metric.invocation) {
        case stat_filter.InvocationMetricType.DURATION_USEC_INVOCATION_METRIC:
          return (v / 1000000).toFixed(2) + "s";
        case stat_filter.InvocationMetricType.CAS_CACHE_DOWNLOAD_SPEED_INVOCATION_METRIC:
        case stat_filter.InvocationMetricType.CAS_CACHE_UPLOAD_SPEED_INVOCATION_METRIC:
          return format.bitsPerSecond(8 * v);
        case stat_filter.InvocationMetricType.CAS_CACHE_DOWNLOAD_SIZE_INVOCATION_METRIC:
        case stat_filter.InvocationMetricType.CAS_CACHE_UPLOAD_SIZE_INVOCATION_METRIC:
          return format.bytes(v);
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
      pattern: filterParams.pattern,
      role: filterParams.role,
      updatedBefore: filterParams.updatedBefore,
      updatedAfter: filterParams.updatedAfter,
      status: filterParams.status,
    });
    this.addZoomFiltersToQuery(drilldownRequest.query);
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

  fetchExecutionList(heatmapSelection: HeatmapSelection) {
    if (!capabilities.config.executionSearchEnabled) {
      return;
    }
    this.setState({
      loadingEvents: true,
      eventsFailed: false,
      eventData: undefined,
    });
    const filterParams = getProtoFilterParams(this.props.search);
    let request = new execution_stats.SearchExecutionRequest({
      query: new execution_stats.ExecutionQuery({
        invocationHost: filterParams.host,
        invocationUser: filterParams.user,
        repoUrl: filterParams.repo,
        branchName: filterParams.branch,
        commitSha: filterParams.commit,
        command: filterParams.command,
        pattern: filterParams.pattern,
        role: filterParams.role || [],
        updatedAfter: filterParams.updatedAfter,
        updatedBefore: filterParams.updatedBefore,
        invocationStatus: filterParams.status || [],
        filter: this.toStatFilterList(heatmapSelection),
      }),
      pageToken: "",
      count: 25,
    });
    this.addZoomFiltersToQuery(request.query!);

    rpcService.service
      .searchExecution(request)
      .then((response) => {
        console.log(response);
        this.setState({
          eventData: { executions: response.execution },
        });
      })
      .catch((e) => {
        errorService.handleError(e);
        this.setState({ eventsFailed: true, eventData: undefined });
      })
      .finally(() => this.setState({ loadingEvents: false }));
  }

  fetchInvocationList(groupId: string, heatmapSelection: HeatmapSelection) {
    this.setState({
      loadingEvents: true,
      eventsFailed: false,
      eventData: undefined,
    });
    const filterParams = getProtoFilterParams(this.props.search);
    let request = new invocation.SearchInvocationRequest({
      query: new invocation.InvocationQuery({
        host: filterParams.host,
        user: filterParams.user,
        repoUrl: filterParams.repo,
        branchName: filterParams.branch,
        commitSha: filterParams.commit,
        command: filterParams.command,
        pattern: filterParams.pattern,
        minimumDuration: filterParams.minimumDuration,
        maximumDuration: filterParams.maximumDuration,
        groupId: groupId,
        role: filterParams.role || [],
        updatedAfter: filterParams.updatedAfter,
        updatedBefore: filterParams.updatedBefore,
        status: filterParams.status || [],
        filter: this.toStatFilterList(heatmapSelection),
      }),
      pageToken: "",
      count: 25,
    });
    this.addZoomFiltersToQuery(request.query!);

    rpcService.service
      .searchInvocation(request)
      .then((response) => {
        this.setState({
          eventData: { invocations: response.invocation },
        });
      })
      .catch(() => this.setState({ eventsFailed: true, eventData: undefined }))
      .finally(() => this.setState({ loadingEvents: false }));
  }

  fetchEventList() {
    if (!this.props.user?.selectedGroup || !this.currentHeatmapSelection) {
      return;
    }
    if (isExecutionMetric(this.selectedMetric.metric)) {
      this.fetchExecutionList(this.currentHeatmapSelection);
    } else {
      this.fetchInvocationList(this.props.user.selectedGroup.id, this.currentHeatmapSelection);
    }
  }

  fetch() {
    const filterParams = getProtoFilterParams(this.props.search);
    this.setState({ loading: true, heatmapData: undefined, drilldownData: undefined, eventData: undefined });

    // Build request...
    const heatmapRequest = stats.GetStatHeatmapRequest.create({});
    heatmapRequest.metric = this.selectedMetric.metric;
    const isExecution = isExecutionMetric(heatmapRequest.metric);

    heatmapRequest.query = new stats.TrendQuery({
      host: filterParams.host,
      user: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,
      pattern: filterParams.pattern,
      role: filterParams.role,
      updatedBefore: filterParams.updatedBefore,
      updatedAfter: filterParams.updatedAfter,
      minimumDuration: isExecution ? undefined : filterParams.minimumDuration,
      maximumDuration: isExecution ? undefined : filterParams.maximumDuration,
      status: filterParams.status,
    });
    this.addZoomFiltersToQuery(heatmapRequest.query);

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
    this.currentHeatmapSelection = undefined;
    this.currentZoomFilters = undefined;
    this.fetch();
  }

  handleHeatmapSelection(s?: HeatmapSelection) {
    this.currentHeatmapSelection = s;
    this.fetchDrilldowns();
    this.fetchEventList();
  }

  handleHeatmapZoom(s?: HeatmapSelection) {
    this.currentHeatmapSelection = undefined;
    this.currentZoomFilters = s;
    this.fetch();
  }

  handleClearZoom() {
    this.currentHeatmapSelection = undefined;
    this.currentZoomFilters = undefined;
    this.fetch();
  }

  addZoomFiltersToQuery(query: CommonQueryFields) {
    if (!this.currentZoomFilters) {
      return;
    }

    query.updatedAfter = usecToTimestamp(this.currentZoomFilters.dateRangeMicros.startInclusive);
    query.updatedBefore = usecToTimestamp(this.currentZoomFilters.dateRangeMicros.endExclusive);
    if (!query.filter) {
      query.filter = [];
    }
    query.filter.push(
      stat_filter.StatFilter.create({
        metric: this.selectedMetric.metric,
        min: Long.fromNumber(this.currentZoomFilters.bucketRange.startInclusive),
        max: Long.fromNumber(this.currentZoomFilters.bucketRange.endExclusive - 1),
      })
    );
  }

  handleBarClick(d: stats.DrilldownType, e?: CategoricalChartState) {
    if (!e || !e.activeLabel) {
      return;
    }
    switch (d) {
      case stats.DrilldownType.USER_DRILLDOWN_TYPE:
        router.setQueryParam("user", e.activeLabel);
        return;
      case stats.DrilldownType.HOSTNAME_DRILLDOWN_TYPE:
        router.setQueryParam("host", e.activeLabel);
        return;
      case stats.DrilldownType.REPO_URL_DRILLDOWN_TYPE:
        router.setQueryParam("repo", e.activeLabel);
        return;
      case stats.DrilldownType.COMMIT_SHA_DRILLDOWN_TYPE:
        router.setQueryParam("commit", e.activeLabel);
        return;
      case stats.DrilldownType.BRANCH_DRILLDOWN_TYPE:
        router.setQueryParam("branch", e.activeLabel);
        return;
      case stats.DrilldownType.PATTERN_DRILLDOWN_TYPE:
        if (capabilities.config.patternFilterEnabled) {
          router.setQueryParam("pattern", e.activeLabel);
        }
        return;
      case stats.DrilldownType.GROUP_ID_DRILLDOWN_TYPE:
      case stats.DrilldownType.DATE_DRILLDOWN_TYPE:
      default:
        return;
    }
  }

  formatDrilldownType(d: stats.DrilldownType) {
    switch (d) {
      case stats.DrilldownType.USER_DRILLDOWN_TYPE:
        return "user";
      case stats.DrilldownType.HOSTNAME_DRILLDOWN_TYPE:
        return "host";
      case stats.DrilldownType.GROUP_ID_DRILLDOWN_TYPE:
        return "group_id";
      case stats.DrilldownType.REPO_URL_DRILLDOWN_TYPE:
        return "repo_url";
      case stats.DrilldownType.COMMIT_SHA_DRILLDOWN_TYPE:
        return "commit_sha";
      case stats.DrilldownType.BRANCH_DRILLDOWN_TYPE:
        return "branch_name";
      case stats.DrilldownType.PATTERN_DRILLDOWN_TYPE:
        return "pattern";
      case stats.DrilldownType.WORKER_DRILLDOWN_TYPE:
        return "worker (execution)";
      default:
        return "???";
    }
  }

  renderCustomTooltip(drilldownType: string, p: TooltipProps<any, any>) {
    if (!this.state.drilldownData) {
      return null;
    }
    if (p.active && p.payload && p.payload.length > 0) {
      return (
        <div className="trend-chart-hover">
          <div>
            {drilldownType}: {p.label}
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

  getEventListTitleString(): string {
    if (this.state.loadingEvents) {
      return "";
    } else if (this.state.eventData?.invocations) {
      const invocationCount = this.state.eventData.invocations.length;
      if (invocationCount < (this.currentHeatmapSelection?.eventsSelected || 0)) {
        return `Selected invocations (showing ${invocationCount} of ${this.currentHeatmapSelection?.eventsSelected})`;
      } else {
        return `Selected invocations (${invocationCount})`;
      }
    } else if (this.state.eventData?.executions) {
      const executionCount = this.state.eventData.executions.length;
      if (executionCount < (this.currentHeatmapSelection?.eventsSelected || 0)) {
        return `Selected executions (showing ${executionCount} of ${this.currentHeatmapSelection?.eventsSelected})`;
      } else {
        return `Selected executions (${executionCount})`;
      }
    } else if (this.state.eventsFailed) {
      return "Failed to load events.";
    }
    return "";
  }

  getEventsListTitle(): React.ReactElement {
    const content = this.state.loadingEvents ? <Spinner></Spinner> : this.getEventListTitleString();
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
    return "To see drilldown charts and individual events, click and drag to select a region in the chart above";
  }

  renderZoomChip(): React.ReactElement | null {
    if (!this.currentZoomFilters) {
      return null;
    }

    const startDate = moment(this.currentZoomFilters.dateRangeMicros.startInclusive / 1000).format("YYYY-MM-DD");
    const endDate = moment((this.currentZoomFilters.dateRangeMicros.endExclusive - 1) / 1000).format("YYYY-MM-DD");
    const startValue = this.renderYBucketValue(this.currentZoomFilters.bucketRange.startInclusive);
    const endValue = this.renderYBucketValue(this.currentZoomFilters.bucketRange.endExclusive);

    return (
      <div className="drilldown-page-zoom-summary zoomed">
        <ZoomIn className="icon"></ZoomIn>
        {this.currentZoomFilters && (
          <div className="drilldown-page-zoom-filters">
            <div className="drilldown-page-zoom-filter-attr">
              Date: {startDate} - {endDate}
            </div>
            <div className="drilldown-page-zoom-filter-attr">
              Value: {startValue} - {endValue}
            </div>
          </div>
        )}
        <FilledButton
          className="square drilldown-page-zoom-button"
          title={"Clear zoom"}
          onClick={() => this.handleClearZoom()}>
          <X className="icon white" />
        </FilledButton>
      </div>
    );
  }

  getInvocationIdForExecution(target: execution_stats.IExecution): string {
    if (!this.state.eventData?.executions) {
      return "";
    }
    const found = this.state.eventData.executions.find((e) => e.execution === target);
    return found?.invocationMetadata?.id || "";
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
          {this.renderZoomChip()}
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
                  selectionCallback={(s) => this.handleHeatmapSelection(s)}
                  zoomCallback={(s) => this.handleHeatmapZoom(s)}></HeatmapComponent>
                <div className="trend-chart">
                  <div className="trend-chart-title">{this.getDrilldownChartsTitle()}</div>
                  {this.state.loadingDrilldowns && <div className="loading"></div>}
                  {!this.state.loadingDrilldowns && this.state.drilldownData && (
                    <div className="container nopadding-dense">
                      {!this.state.loadingDrilldowns &&
                        this.state.drilldownData &&
                        this.state.drilldownData.chart.map(
                          (chart) =>
                            chart.entry.length > 1 && (
                              <div className="drilldown-page-dd-chart">
                                <div className="drilldown-page-dd-chart-title">
                                  {this.formatDrilldownType(chart.drilldownType)}
                                </div>
                                <BarChart
                                  width={300}
                                  height={200}
                                  data={chart.entry}
                                  onClick={this.handleBarClick.bind(this, chart.drilldownType)}>
                                  <CartesianGrid strokeDasharray="3 3" />
                                  <XAxis
                                    interval="preserveStart"
                                    dataKey={(entry: stats.DrilldownEntry) => entry.label}
                                  />
                                  <Tooltip
                                    content={this.renderCustomTooltip.bind(
                                      this,
                                      this.formatDrilldownType(chart.drilldownType)
                                    )}
                                  />
                                  <Bar
                                    cursor="pointer"
                                    dataKey={(entry: stats.DrilldownEntry) =>
                                      +entry.baseValue / +(this.state.drilldownData?.totalInBase || 1)
                                    }
                                    fill="#8884d8"
                                  />
                                  <Bar
                                    cursor="pointer"
                                    dataKey={(entry: stats.DrilldownEntry) =>
                                      +entry.selectionValue / +(this.state.drilldownData?.totalInSelection || 1)
                                    }
                                    fill="#82ca9d"
                                  />
                                </BarChart>
                              </div>
                            )
                        )}
                    </div>
                  )}
                </div>
                <div className="trend-chart">
                  {this.getEventsListTitle()}
                  {this.state.eventData?.invocations && (
                    <div className="history">
                      <div className="container nopadding-dense">
                        {this.state.eventData.invocations.map((invocation) => (
                          <a href={`/invocation/${invocation.invocationId}`} onClick={(e) => e.preventDefault()}>
                            <HistoryInvocationCardComponent invocation={invocation} />
                          </a>
                        ))}
                      </div>
                    </div>
                  )}
                  {this.state.eventData?.executions && (
                    <InvocationExecutionTable
                      executions={this.state.eventData.executions.map((e) => e.execution as execution_stats.IExecution)}
                      invocationIdProvider={(e) => this.getInvocationIdForExecution(e)}></InvocationExecutionTable>
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
