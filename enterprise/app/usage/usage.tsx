import Long from "long";
import { Download } from "lucide-react";
import moment from "moment";
import React from "react";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import HelpTooltip from "../../../app/components/tooltip/help_tooltip";
import errorService from "../../../app/errors/error_service";
import { bytes, count, formatWithCommas } from "../../../app/format/format";
import router, { Path, TrendsChartId } from "../../../app/router/router";
import { END_DATE_PARAM_NAME, LAST_N_DAYS_PARAM_NAME, START_DATE_PARAM_NAME } from "../../../app/router/router_params";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";
import DateRangePickerButton from "../filter/date_range_picker_button";
import { getDateRangeForPicker } from "../filter/filter_util";
import TrendsChartComponent, { ChartColor, SeriesType } from "../trends/trends_chart";
import UsageAlertsComponent from "./usage_alerts";
import UsageBillCard from "./usage_bill";

export interface UsageProps {
  user?: User;
  path: string;
  search: URLSearchParams;
}

type UsageTab = "report" | "alerting";

interface UsageReportProps {
  user?: User;
  search: URLSearchParams;
}

interface State {
  response?: usage.GetUsageResponse;
  loading?: boolean;
  /** Undefined until the bill request settles, null if the group has no bill. */
  bill?: usage.IBill | null;
}

// This is the first month with usage numbers broken down by internal/external,
// workflows, etc.  Prior months will still show the "old" charts.
const FIRST_DETAILED_MONTH = "2025-08";
const OLAP_QUERY_PARAM = "olap";

function shouldShowDetailedView(periodStart: string): boolean {
  return new Date(periodStart) >= new Date(FIRST_DETAILED_MONTH);
}

function useOLAPFromURL(): boolean {
  return new URLSearchParams(window.location.search).get(OLAP_QUERY_PARAM) === "1";
}

/** UsageComponent renders the Usage page shell and active tab. */
export default class UsageComponent extends React.Component<UsageProps> {
  componentDidMount() {
    document.title = "Usage | BuildBuddy";
  }

  private usageAlertsEnabled() {
    return router.canAccessUsageAlertingPage(this.props.user);
  }

  private activeTab(): UsageTab {
    if (this.usageAlertsEnabled() && this.isAlertingPath()) {
      return "alerting";
    }
    return "report";
  }

  private isAlertingPath() {
    return this.props.path.replace(/\/$/, "") === Path.usageAlertingPath;
  }

  private onClickTab(selectedTab: UsageTab) {
    if (selectedTab === "alerting" && !this.usageAlertsEnabled()) {
      return;
    }
    router.navigateTo(selectedTab === "alerting" ? Path.usageAlertingPath : Path.usagePath);
  }

  private renderTabs() {
    if (!this.usageAlertsEnabled()) {
      return null;
    }
    const activeTab = this.activeTab();
    return (
      <div className="tabs usage-tabs">
        <button
          type="button"
          className={`tab ${activeTab === "report" ? "selected" : ""}`}
          onClick={this.onClickTab.bind(this, "report")}>
          Report
        </button>
        <button
          type="button"
          className={`tab ${activeTab === "alerting" ? "selected" : ""}`}
          onClick={this.onClickTab.bind(this, "alerting")}>
          Alerts
        </button>
      </div>
    );
  }

  private renderHeader() {
    return (
      <>
        <div className="usage-header">
          <div className="usage-title">Usage</div>
          {this.activeTab() === "report" && <DateRangePickerButton search={usageDateRange(this.props.search).search} />}
        </div>
        {this.renderTabs()}
      </>
    );
  }

  render() {
    const activeTab = this.activeTab();

    return (
      <div className="usage-page">
        <div className="container">{this.renderHeader()}</div>
        <div className="container usage-page-container">
          {activeTab === "report" && <UsageReport user={this.props.user} search={this.props.search} />}
          {activeTab === "alerting" && <UsageAlertsComponent />}
        </div>
      </div>
    );
  }
}

/** UsageReport renders the usage report tab contents. */
class UsageReport extends React.Component<UsageReportProps, State> {
  state: State = { loading: true };
  pendingRequest?: CancelablePromise<any>;

  componentDidMount() {
    document.title = "Usage | BuildBuddy";
    this.fetchUsage();
    if (capabilities.config.usageBillEnabled) {
      rpcService.service
        .getCurrentBill(new usage.GetCurrentBillRequest())
        .then((response) => this.setState({ bill: response.bill ?? null }))
        .catch((e) => {
          errorService.handleError(e);
          this.setState({ bill: null });
        });
    }
  }

  componentDidUpdate(prevProps: UsageReportProps) {
    const prev = usageDateRange(prevProps.search);
    const next = usageDateRange(this.props.search);
    if (prev.start !== next.start || prev.end !== next.end) {
      this.fetchUsage();
    }
  }

  private fetchUsage() {
    this.pendingRequest?.cancel();
    this.setState({ loading: true });

    const { start, end } = usageDateRange(this.props.search);
    rpcService.service
      .getUsage(new usage.GetUsageRequest({ startDate: start, endDate: end, useOlap: useOLAPFromURL() }))
      .then((response) => {
        console.log(response);
        if (!response.usage) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response });
      })
      .catch((e) => {
        errorService.handleError(e);
        // Don't leave the previous range's data on screen.
        this.setState({ response: undefined });
      })
      .finally(() => this.setState({ loading: false }));
  }

  getUsage(start: number) {
    const date = moment.unix(start).format("YYYY-MM-DD");
    return this.state.response?.dailyUsage.find((v) => v.period === date) ?? new usage.Usage();
  }

  onBarClicked(chartId: TrendsChartId, ts: number) {
    const date = moment.unix(ts).format("YYYY-MM-DD");
    router.navigateTo(`/trends?start=${date}&end=${date}#${chartId}`, true);
  }

  renderCharts(detailed: boolean) {
    if (this.state.loading || !this.state.response?.dailyUsage) {
      return undefined;
    }
    const { start, end } = usageDateRange(this.props.search);
    const dates: number[] = [];
    for (const day = moment(start, "YYYY-MM-DD"); day.isSameOrBefore(moment(end, "YYYY-MM-DD")); day.add(1, "day")) {
      dates.push(day.unix());
    }
    return (
      <>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Invocations"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  type: SeriesType.BAR,
                  name: "invocations",
                  extractValue: (ts) => +(this.getUsage(ts).invocations ?? 0),
                  formatHoverValue: (value) => (value || 0) + " invocations",
                  onClick: this.onBarClicked.bind(this, "builds"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Action cache hits"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  type: SeriesType.BAR,
                  name: "action cache hits",
                  extractValue: (ts) => +(this.getUsage(ts).actionCacheHits ?? 0),
                  formatHoverValue: (value) => (value || 0) + " action cache hits",
                  onClick: this.onBarClicked.bind(this, "cache"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Cached build minutes"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  type: SeriesType.BAR,
                  name: "cached build minutes",
                  extractValue: (ts) => +(this.getUsage(ts).totalCachedActionExecUsec ?? 0),
                  formatHoverValue: (value) => formatMinutes(value || 0),
                  onClick: this.onBarClicked.bind(this, "savings"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            <TrendsChartComponent
              title="Content addressable storage cache hits"
              standaloneChart={true}
              data={dates}
              dataSeries={[
                {
                  type: SeriesType.BAR,
                  name: "content addressable storage cache hits",
                  extractValue: (ts) => +(this.getUsage(ts).casCacheHits ?? 0),
                  formatHoverValue: (value) => (value || 0) + " CAS cache hits",
                  onClick: this.onBarClicked.bind(this, "cas"),
                  color: ChartColor.BLUE,
                },
              ]}
              primaryYAxis={{
                formatTickValue: count,
                allowDecimals: false,
              }}
              formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
              formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
              ticks={[]}
            />
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            {detailed && (
              <TrendsChartComponent
                title="Total cache download (bytes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "internal",
                    extractValue: (ts) => +(this.getUsage(ts).totalInternalDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " internal downloads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "dl",
                    color: ChartColor.GREY,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).totalWorkflowDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " workflows downloads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "dl",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "external",
                    extractValue: (ts) => +(this.getUsage(ts).totalExternalDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " external downloads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "dl",
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: bytes,
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
            {!detailed && (
              <TrendsChartComponent
                title="Total cache download (bytes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "total cache download (bytes)",
                    extractValue: (ts) => +(this.getUsage(ts).totalDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " downloaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: bytes,
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            {detailed && (
              <TrendsChartComponent
                title="Total cache upload (bytes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "internal",
                    extractValue: (ts) => +(this.getUsage(ts).totalInternalUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " internal uploads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "ul",
                    color: ChartColor.GREY,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).totalWorkflowUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " workflows uploads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "ul",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "external",
                    extractValue: (ts) => +(this.getUsage(ts).totalExternalUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " external uploads",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "ul",
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: bytes,
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
            {!detailed && (
              <TrendsChartComponent
                title="Total cache upload (bytes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "total cache upload (bytes)",
                    extractValue: (ts) => +(this.getUsage(ts).totalUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " uploaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: bytes,
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            {detailed && (
              <TrendsChartComponent
                title="Linux remote execution duration (minutes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).cloudWorkflowLinuxExecutionDurationUsec ?? 0),
                    formatHoverValue: (value) => formatMinutes(value || 0, "workflow"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "bt",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "rbe",
                    extractValue: (ts) => +(this.getUsage(ts).cloudRbeLinuxExecutionDurationUsec ?? 0),
                    formatHoverValue: (value) => formatMinutes(value || 0, "rbe"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "bt",
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
            {!detailed && (
              <TrendsChartComponent
                title="Linux remote execution duration (minutes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "linux remote execution duration (minutes)",
                    extractValue: (ts) => +(this.getUsage(ts).linuxExecutionDurationUsec ?? 0),
                    formatHoverValue: (value) => formatMinutes(value || 0),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
          </div>
        </div>
        <div className="card usage-card">
          <div className="content">
            {detailed && (
              <TrendsChartComponent
                title="Linux remote execution cpu time (minutes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).cloudWorkflowCpuNanos ?? 0) / 1000,
                    formatHoverValue: (value) => formatMinutes(value || 0, "workflow"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "cpu",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    type: SeriesType.BAR,
                    name: "rbe",
                    extractValue: (ts) => +(this.getUsage(ts).cloudRbeCpuNanos ?? 0) / 1000,
                    formatHoverValue: (value) => formatMinutes(value || 0, "rbe"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "cpu",
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
            {!detailed && (
              <TrendsChartComponent
                title="Linux remote execution cpu time (minutes)"
                standaloneChart={true}
                data={dates}
                dataSeries={[
                  {
                    type: SeriesType.BAR,
                    name: "linux remote execution cpu time",
                    extractValue: (ts) => +(this.getUsage(ts).cloudCpuNanos ?? 0) / 1000,
                    formatHoverValue: (value) => formatMinutes(value || 0),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    color: ChartColor.BLUE,
                  },
                ]}
                primaryYAxis={{
                  formatTickValue: (v) => formatWithCommas(Math.floor(v / 60e6)),
                  allowDecimals: false,
                }}
                formatXAxisLabel={(ts) => moment.unix(ts).format("MMM D")}
                formatHoverXAxisLabel={(ts) => moment.unix(ts).format("dddd, MMMM Do YYYY")}
                ticks={[]}
              />
            )}
          </div>
        </div>
      </>
    );
  }

  renderSnapshotUsage(selection: usage.Usage) {
    const rows = [
      ...selection.remoteSnapshotSavedBytes.map((row) => executionUsageRow("Remote snapshots", row, false)),
      ...selection.localSnapshotSavedBytes.map((row) => executionUsageRow("Local snapshots", row, false)),
    ];
    // Don't show the section to orgs that don't use snapshots.
    if (!rows.length) return null;
    const totalBytes = rows.reduce((sum, row) => sum + row.value, 0);
    return (
      <>
        <div className="usage-resource-name">Snapshot bytes saved</div>
        <div className="usage-value" title={formatWithCommas(totalBytes)}>
          {formatBytes(totalBytes, totalBytes)}
        </div>
        {renderBreakdownTable(
          breakdownTableRows(rows, SNAPSHOT_USAGE_LEVELS),
          SNAPSHOT_USAGE_LEVELS.length,
          (bytes) => formatBytes(bytes, totalBytes),
          (bytes) => formatWithCommas(bytes)
        )}
      </>
    );
  }

  renderComputeUsage(selection: usage.Usage) {
    const rows = [
      ...selection.fixedComputeUsec.map((row) => executionUsageRow("Fixed compute", row, true)),
      ...selection.flexibleComputeUsec.map((row) => executionUsageRow("Flexible compute", row, true)),
    ];
    // Don't show the section to orgs that don't use remote execution.
    if (!rows.length) return null;
    const totalUsec = rows.reduce((sum, row) => sum + row.value, 0);
    return (
      <>
        <div className="usage-resource-name usage-resource-name-with-help">
          <span>Compute unit</span>
          <HelpTooltip>
            A compute unit is 1 CPU, 2.5GiB of memory or 25GiB of disk, whichever an action needs the most of. Fixed
            compute charges the units an action reserves, for Firecracker actions and actions that set
            EstimatedComputeUnits. Flexible compute charges the units the scheduler estimated for the action. Units are
            multiplied by execution time and shown in minutes.
          </HelpTooltip>
        </div>
        <div className="usage-value">{formatMinutes(totalUsec)}</div>
        {renderBreakdownTable(
          breakdownTableRows(rows, COMPUTE_USAGE_LEVELS),
          COMPUTE_USAGE_LEVELS.length + 1,
          formatMinutes
        )}
      </>
    );
  }

  private usageExportUrl(): string {
    const { start, end } = usageDateRange(this.props.search);
    return rpcService.getAuthenticatedUrl("/usage/download", { start, end });
  }

  render() {
    // Wait for the bill so the top panel does not switch after it renders.
    if (capabilities.config.usageBillEnabled && this.state.bill === undefined) return null;

    const orgName = this.props.user?.selectedGroup.name;
    // Selected period may not be found because of a pending or failed RPC.
    const selection = this.state.response?.usage;
    const range = usageDateRange(this.props.search);
    const detailed = shouldShowDetailedView(range.start);
    const periodHeader = (
      <div className="usage-period-header">
        <div>
          {orgName && <div className="org-name">{orgName}</div>}
          <div className="selected-period-label">BuildBuddy usage (UTC)</div>
        </div>
        <div className="usage-period-controls">
          {capabilities.config.usageExportEnabled && (
            <OutlinedLinkButton
              href={this.usageExportUrl()}
              target="_blank"
              title="Download daily usage for this period as CSV">
              <Download className="icon" />
              <span>Download</span>
            </OutlinedLinkButton>
          )}
        </div>
      </div>
    );
    // The bill covers the current month only, and replaces the usage summary for it.
    const month = currentMonth();
    if (this.state.bill && range.start === month.start && range.end === month.end) {
      return (
        <>
          <UsageBillCard bill={this.state.bill} periodHeader={periodHeader} />
          {this.renderCharts(detailed)}
        </>
      );
    }
    return (
      <>
        <div className="card usage-card">
          <div className="content">
            {periodHeader}
            {this.state.loading && <div className="loading" />}
            {!this.state.loading && !selection && <span>Failed to load usage data.</span>}
            {!this.state.loading && selection && (
              <div className="usage-period-table">
                <div className="usage-resource-name">Invocations</div>
                <div className="usage-value">{formatWithCommas(selection.invocations)}</div>
                <div className="usage-resource-name">Action cache hits</div>
                <div className="usage-value">{formatWithCommas(selection.actionCacheHits)}</div>
                <div className="usage-resource-name">Cached build minutes</div>
                <div className="usage-value">{formatMinutes(Number(selection.totalCachedActionExecUsec))}</div>
                <div className="usage-resource-name">Content addressable storage cache hits</div>
                <div className="usage-value">{formatWithCommas(selection.casCacheHits)}</div>
                <div className="usage-resource-name">Total bytes downloaded from cache</div>
                <div className="usage-value" title={formatWithCommas(selection.totalDownloadSizeBytes)}>
                  {formatBytes(selection.totalDownloadSizeBytes, selection.totalDownloadSizeBytes)}
                </div>
                {detailed && (
                  <>
                    <div className="usage-resource-subcategory">External downloads</div>
                    <div
                      className="usage-value-subcategory"
                      title={formatWithCommas(selection.totalExternalDownloadSizeBytes)}>
                      {formatBytes(selection.totalExternalDownloadSizeBytes, selection.totalDownloadSizeBytes)}
                    </div>
                    <div className="usage-resource-subcategory">Internal downloads</div>
                    <div
                      className="usage-value-subcategory"
                      title={formatWithCommas(selection.totalInternalDownloadSizeBytes)}>
                      {formatBytes(selection.totalInternalDownloadSizeBytes, selection.totalDownloadSizeBytes)}
                    </div>
                    {Boolean(selection.totalWorkflowDownloadSizeBytes) && (
                      <>
                        <div className="usage-resource-subcategory">Workflow downloads</div>
                        <div
                          className="usage-value-subcategory"
                          title={formatWithCommas(selection.totalWorkflowDownloadSizeBytes)}>
                          {formatBytes(selection.totalWorkflowDownloadSizeBytes, selection.totalDownloadSizeBytes)}
                        </div>
                      </>
                    )}
                  </>
                )}
                <div className="usage-resource-name">Total bytes uploaded to cache</div>
                <div className="usage-value" title={formatWithCommas(selection.totalUploadSizeBytes)}>
                  {formatBytes(selection.totalUploadSizeBytes, selection.totalUploadSizeBytes)}
                </div>
                {detailed && (
                  <>
                    <div className="usage-resource-subcategory">External uploads</div>
                    <div
                      className="usage-value-subcategory"
                      title={formatWithCommas(selection.totalExternalUploadSizeBytes)}>
                      {formatBytes(selection.totalExternalUploadSizeBytes, selection.totalUploadSizeBytes)}
                    </div>
                    <div className="usage-resource-subcategory">Internal uploads</div>
                    <div
                      className="usage-value-subcategory"
                      title={formatWithCommas(selection.totalInternalUploadSizeBytes)}>
                      {formatBytes(selection.totalInternalUploadSizeBytes, selection.totalUploadSizeBytes)}
                    </div>
                    {Boolean(selection.totalWorkflowUploadSizeBytes) && (
                      <>
                        <div className="usage-resource-subcategory">Workflow uploads</div>
                        <div
                          className="usage-value-subcategory"
                          title={formatWithCommas(selection.totalWorkflowUploadSizeBytes)}>
                          {formatBytes(selection.totalWorkflowUploadSizeBytes, selection.totalUploadSizeBytes)}
                        </div>
                      </>
                    )}
                  </>
                )}
                {this.renderSnapshotUsage(selection)}
                <div className="usage-resource-name">Linux remote execution</div>
                <div className="usage-value">{formatMinutes(Number(selection.linuxExecutionDurationUsec))}</div>
                {detailed && (
                  <>
                    {Boolean(selection.cloudRbeLinuxExecutionDurationUsec) && (
                      <>
                        <div className="usage-resource-subcategory">Cloud RBE</div>
                        <div className="usage-value-subcategory">
                          {formatMinutes(Number(selection.cloudRbeLinuxExecutionDurationUsec))}
                        </div>
                      </>
                    )}
                    {Boolean(selection.cloudWorkflowLinuxExecutionDurationUsec) && (
                      <>
                        <div className="usage-resource-subcategory">Workflows</div>
                        <div className="usage-value-subcategory">
                          {formatMinutes(Number(selection.cloudWorkflowLinuxExecutionDurationUsec))}
                        </div>
                      </>
                    )}
                  </>
                )}
                <div className="usage-resource-name">Linux cpu</div>
                <div className="usage-value">{formatMinutes(+selection.cloudCpuNanos / 1000)}</div>
                {detailed && (
                  <>
                    {Boolean(selection.cloudRbeCpuNanos) && (
                      <>
                        <div className="usage-resource-subcategory">Cloud RBE</div>
                        <div className="usage-value-subcategory">
                          {formatMinutes(+selection.cloudRbeCpuNanos / 1000)}
                        </div>
                      </>
                    )}
                    {Boolean(selection.cloudWorkflowCpuNanos) && (
                      <>
                        <div className="usage-resource-subcategory">Workflows</div>
                        <div className="usage-value-subcategory">
                          {formatMinutes(+selection.cloudWorkflowCpuNanos / 1000)}
                        </div>
                      </>
                    )}
                  </>
                )}
                {this.renderComputeUsage(selection)}
                {Boolean(selection.totalCustomerProxyDownloadSizeBytes) && (
                  <>
                    <div className="usage-resource-name">Total bytes downloaded from self-hosted cache proxy</div>
                    <div
                      className="usage-value"
                      title={formatWithCommas(selection.totalCustomerProxyDownloadSizeBytes)}>
                      {formatBytes(
                        selection.totalCustomerProxyDownloadSizeBytes,
                        selection.totalCustomerProxyDownloadSizeBytes
                      )}
                    </div>
                  </>
                )}
                {Boolean(selection.totalCustomerProxyUploadSizeBytes) && (
                  <>
                    <div className="usage-resource-name">Total bytes uploaded only to self-hosted cache proxy</div>
                    <div className="usage-value" title={formatWithCommas(selection.totalCustomerProxyUploadSizeBytes)}>
                      {formatBytes(
                        selection.totalCustomerProxyUploadSizeBytes,
                        selection.totalCustomerProxyUploadSizeBytes
                      )}
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
        {this.renderCharts(detailed)}
      </>
    );
  }
}

/** The current UTC month's first and last days. */
function currentMonth(): { start: string; end: string } {
  const now = moment.utc();
  return { start: now.format("YYYY-MM-01"), end: now.endOf("month").format("YYYY-MM-DD") };
}

/**
 * The inclusive "YYYY-MM-DD" date range selected in the URL, defaulting to the
 * current month. Also returns the URL params with that default filled in, for
 * the date range picker.
 */
function usageDateRange(search: URLSearchParams): { search: URLSearchParams; start: string; end: string } {
  if (![START_DATE_PARAM_NAME, END_DATE_PARAM_NAME, LAST_N_DAYS_PARAM_NAME].some((name) => search.get(name))) {
    const month = currentMonth();
    search = new URLSearchParams(search);
    search.set(START_DATE_PARAM_NAME, month.start);
    search.set(END_DATE_PARAM_NAME, month.end);
  }
  const { startDate, endDate } = getDateRangeForPicker(search);
  return {
    search,
    start: moment(startDate).format("YYYY-MM-DD"),
    end: moment(endDate ?? new Date()).format("YYYY-MM-DD"),
  };
}

function formatBytes(bytes: Long | number, totalBytes: Long | number) {
  totalBytes = +totalBytes;
  if (totalBytes < 1e3) {
    return bytes + "B";
  }
  if (totalBytes < 1e6) {
    if (+bytes / 1e3 < 0.001) {
      return "< .001KB";
    }
    return (+bytes / 1e3).toFixed(3) + "KB";
  }
  if (totalBytes < 1e9) {
    if (+bytes / 1e6 < 0.001) {
      return "< .001MB";
    }
    return (+bytes / 1e6).toFixed(3) + "MB";
  }
  if (totalBytes < 1e12) {
    if (+bytes / 1e9 < 0.001) {
      return "< .001GB";
    }
    return (+bytes / 1e9).toFixed(3) + "GB";
  }
  if (totalBytes < 1e15) {
    if (+bytes / 1e12 < 0.001) {
      return "< .001TB";
    }
    return (+bytes / 1e12).toFixed(3) + "TB";
  }
  if (+bytes / 1e15 < 0.001) {
    return "< .001PB";
  }
  return (+bytes / 1e15).toFixed(3) + "PB";
}

function formatMinutes(usec: number, category?: string): string {
  return `${formatWithCommas(Math.round(usec / 60e6))}${category ? " " + category : ""} minutes`;
}

const HOSTING_ORDER = ["Cloud", "Self-hosted"];
const POOL_ORDER = ["RBE", "Workflows"];

/**
 * A usage count resolved into the dimensions shown in a breakdown table: the
 * value of each grouping level, from outermost to innermost, and optionally
 * the leaf columns shown below the last group.
 */
interface BreakdownUsageRow {
  groups: string[];
  leaf?: string[];
  value: number;
}

/**
 * Resolves a server row into the dimensions shown in a breakdown table:
 * hosting, then the given type name, then pool, and if requested the
 * isolation type, OS and arch as leaf columns.
 */
function executionUsageRow(typeName: string, row: usage.ExecutionUsage, withLeafColumns: boolean): BreakdownUsageRow {
  return {
    groups: [row.selfHosted ? "Self-hosted" : "Cloud", typeName, row.workflow ? "Workflows" : "RBE"],
    leaf: withLeafColumns ? [row.isolationType, row.os, row.arch] : undefined,
    value: Number(row.count),
  };
}

/** Grouping levels of the compute usage table, from outermost to innermost,
 * with the display order of each level's values. Leaf rows below the last
 * level show the isolation type, OS and arch. */
const COMPUTE_USAGE_LEVELS = [HOSTING_ORDER, ["Fixed compute", "Flexible compute"], POOL_ORDER];

/** Grouping levels of the snapshot usage table, from outermost to innermost. */
const SNAPSHOT_USAGE_LEVELS = [HOSTING_ORDER, ["Remote snapshots", "Local snapshots"], POOL_ORDER];

/** A row of a rendered breakdown table: either a group heading carrying the
 * sum of everything nested under it, or a leaf row. */
interface BreakdownTableRow {
  // 1-based nesting depth.
  level: number;
  // Cells shown before the value: the group name, or the leaf columns.
  cells: string[];
  value: number;
}

/**
 * Flattens usage rows into table rows, grouped by each of the levels in turn.
 * Group rows carry the sum of the rows nested under them. Rows whose value is
 * zero are omitted.
 */
function breakdownTableRows(rows: BreakdownUsageRow[], levels: string[][], level = 0): BreakdownTableRow[] {
  if (level === levels.length) {
    return breakdownLeafRows(rows, level + 1);
  }
  const out: BreakdownTableRow[] = [];
  for (const name of levels[level]) {
    const group = rows.filter((row) => row.groups[level] === name);
    const value = group.reduce((sum, row) => sum + row.value, 0);
    if (value === 0) continue;
    out.push({ level: level + 1, cells: [name], value }, ...breakdownTableRows(group, levels, level + 1));
  }
  return out;
}

function breakdownLeafRows(rows: BreakdownUsageRow[], level: number): BreakdownTableRow[] {
  // The server returns one row per combination of leaf dimensions, already
  // sorted by them.
  const out: BreakdownTableRow[] = [];
  for (const { leaf, value } of rows) {
    if (leaf && value !== 0) {
      out.push({ level, cells: leaf, value });
    }
  }
  return out;
}

function renderBreakdownTable(
  rows: BreakdownTableRow[],
  // The level of leaf rows, which are styled differently from group rows.
  leafLevel: number,
  formatValue: (value: number) => string,
  // If set, formats the tooltip shown when hovering over a value.
  formatTitle?: (value: number) => string
) {
  if (!rows.length) return null;
  // Group rows have a single cell, which spans all of the leaf columns.
  const columns = Math.max(...rows.map((row) => row.cells.length));
  return (
    <table className="usage-breakdown-table">
      <tbody>
        {rows.map((row, i) => (
          <tr
            key={i}
            className={`usage-breakdown-level-${row.level} ${row.level === leafLevel ? "usage-breakdown-leaf" : ""}`}>
            {row.cells.map((cell, j) => (
              <td
                key={j}
                colSpan={row.cells.length === 1 ? columns : 1}
                style={j === 0 ? { paddingLeft: 24 + 16 * (row.level - 1) } : undefined}>
                {cell}
              </td>
            ))}
            <td className="usage-breakdown-value" title={formatTitle?.(row.value)}>
              {formatValue(row.value)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
