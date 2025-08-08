import moment from "moment";
import React from "react";
import { User } from "../../../app/auth/auth_service";
import Select, { Option } from "../../../app/components/select/select";
import errorService from "../../../app/errors/error_service";
import { bytes, count, formatWithCommas } from "../../../app/format/format";
import router, { TrendsChartId } from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";
import TrendsChartComponent, { ChartColor } from "../trends/trends_chart";

export interface UsageProps {
  user?: User;
}

interface State {
  response?: usage.GetUsageResponse;
  selectedPeriod: string;
  loading?: boolean;
}

// This is the first month with usage numbers broken down by internal/external,
// workflows, etc.  Prior months will still show the "old" charts.
const FIRST_DETAILED_MONTH = "2025-08";

function shouldShowDetailedView(periodStart: string): boolean {
  return new Date(periodStart) >= new Date(FIRST_DETAILED_MONTH);
}

export default class UsageComponent extends React.Component<UsageProps, State> {
  // TODO: remove getDefaultTimePeriodString() after the server
  // is updated to unconditionally send the current period
  state: State = { selectedPeriod: getDefaultTimePeriodString() };
  pendingRequest?: CancelablePromise<any>;

  componentDidMount() {
    document.title = "Usage | BuildBuddy";
    this.fetchUsageForPeriod(this.state.selectedPeriod);
  }

  private onChangePeriod(e: React.ChangeEvent<HTMLSelectElement>) {
    const period = e.target.value;
    this.setState({
      selectedPeriod: period,
    });
    this.fetchUsageForPeriod(period);
  }

  private fetchUsageForPeriod(period: string) {
    this.pendingRequest?.cancel();
    this.setState({ loading: true });

    rpcService.service
      .getUsage(new usage.GetUsageRequest({ usagePeriod: period }))
      .then((response) => {
        console.log(response);
        if (!response.usage) {
          throw new Error("Server did not return usage data.");
        }
        this.setState({ response, selectedPeriod: period });
      })
      .catch((e) => errorService.handleError(e))
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
    const daysInMonth = moment(this.state.selectedPeriod, "YYYY-MM").daysInMonth();
    const dates: number[] = [];
    for (let i = 1; i <= daysInMonth; i++) {
      dates.push(
        moment(this.state.selectedPeriod + i.toString().padStart(2, "0"), "YYYY-MM-DD")
          .startOf("day")
          .unix()
      );
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
                    name: "internal",
                    extractValue: (ts) => +(this.getUsage(ts).totalInternalDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " intern downloaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "dl",
                    color: ChartColor.GREY,
                  },
                  {
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).totalWorkflowDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " wf downloaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "dl",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    name: "external",
                    extractValue: (ts) => +(this.getUsage(ts).totalExternalDownloadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " extern downloaded",
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
                    name: "internal",
                    extractValue: (ts) => +(this.getUsage(ts).totalInternalUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " intern uploaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "ul",
                    color: ChartColor.GREY,
                  },
                  {
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).totalWorkflowUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " wf uploaded",
                    onClick: this.onBarClicked.bind(this, "cas"),
                    stackId: "ul",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
                    name: "external",
                    extractValue: (ts) => +(this.getUsage(ts).totalExternalUploadSizeBytes ?? 0),
                    formatHoverValue: (value) => bytes(value || 0) + " extern uploaded",
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
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).cloudWorkflowLinuxExecutionDurationUsec ?? 0),
                    formatHoverValue: (value) => formatMinutes(value || 0, "workflow"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "bt",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
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
                    name: "workflows",
                    extractValue: (ts) => +(this.getUsage(ts).cloudWorkflowCpuNanos ?? 0) / 1000,
                    formatHoverValue: (value) => formatMinutes(value || 0, "workflow"),
                    onClick: this.onBarClicked.bind(this, "build_time"),
                    stackId: "cpu",
                    color: ChartColor.BASICALLY_BLACK,
                  },
                  {
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
                    name: "linux remote execution cpu time",
                    extractValue: (ts) => +(this.getUsage(ts).cloudRbeCpuNanos ?? 0 / 1000),
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

  render() {
    const orgName = this.props.user?.selectedGroup.name;
    // Selected period may not be found because of a pending or failed RPC.
    const selection = this.state.response?.usage;
    const detailed = shouldShowDetailedView(this.state.selectedPeriod);

    return (
      <div className="usage-page">
        <div className="container usage-page-container">
          <div className="usage-header">
            <div className="usage-title">Usage</div>
          </div>
          {this.state.response && (
            <>
              <div className="card usage-card">
                <div className="content">
                  <div className="usage-period-header">
                    <div>
                      {orgName && <div className="org-name">{orgName}</div>}
                      <div className="selected-period-label">
                        BuildBuddy usage for <span className="usage-period">{this.state.selectedPeriod} (UTC)</span>
                      </div>
                    </div>
                    <Select title="Usage period" onChange={this.onChangePeriod.bind(this)}>
                      {this.state.response.availableUsagePeriods.map((period, i) => (
                        <Option key={period} value={period}>
                          {period}
                          {i === 0 ? " (Current period)" : ""}
                        </Option>
                      ))}
                    </Select>
                  </div>
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
                          {selection.totalWorkflowDownloadSizeBytes && (
                            <>
                              <div className="usage-resource-subcategory">Workflow downloads</div>
                              <div
                                className="usage-value-subcategory"
                                title={formatWithCommas(selection.totalWorkflowDownloadSizeBytes)}>
                                {formatBytes(
                                  selection.totalWorkflowDownloadSizeBytes,
                                  selection.totalDownloadSizeBytes
                                )}
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
                          {selection.totalWorkflowUploadSizeBytes && (
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
                      <div className="usage-resource-name">Linux remote execution duration</div>
                      <div className="usage-value">{formatMinutes(Number(selection.linuxExecutionDurationUsec))}</div>
                      {detailed && (
                        <>
                          {selection.cloudRbeLinuxExecutionDurationUsec && (
                            <>
                              <div className="usage-resource-subcategory">Cloud RBE</div>
                              <div className="usage-value-subcategory">
                                {formatMinutes(Number(selection.cloudRbeLinuxExecutionDurationUsec))}
                              </div>
                            </>
                          )}
                          {selection.cloudWorkflowLinuxExecutionDurationUsec && (
                            <>
                              <div className="usage-resource-subcategory">Workflows</div>
                              <div className="usage-value-subcategory">
                                {formatMinutes(Number(selection.cloudWorkflowLinuxExecutionDurationUsec))}
                              </div>
                            </>
                          )}
                        </>
                      )}
                      <div className="usage-resource-name">Linux cpu duration</div>
                      <div className="usage-value">{formatMinutes(+selection.cloudCpuNanos / 1000)}</div>
                      {detailed && (
                        <>
                          {selection.cloudRbeCpuNanos && (
                            <>
                              <div className="usage-resource-subcategory">Cloud RBE</div>
                              <div className="usage-value-subcategory">
                                {formatMinutes(+selection.cloudRbeCpuNanos / 1000)}
                              </div>
                            </>
                          )}
                          {selection.cloudWorkflowCpuNanos && (
                            <>
                              <div className="usage-resource-subcategory">Workflows</div>
                              <div className="usage-value-subcategory">
                                {formatMinutes(+selection.cloudWorkflowCpuNanos / 1000)}
                              </div>
                            </>
                          )}
                        </>
                      )}
                    </div>
                  )}
                </div>
              </div>
              {this.renderCharts(detailed)}
            </>
          )}
        </div>
      </div>
    );
  }
}

function getDefaultTimePeriodString(): string {
  return moment.utc().format("YYYY-MM");
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
  return `${formatWithCommas(usec / 60e6, { minimumFractionDigits: 3 })}${category ? " " + category : ""} minutes`;
}
