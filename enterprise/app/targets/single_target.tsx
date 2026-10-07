import {
  Activity,
  BarChart2,
  Clock,
  Cpu,
  Download,
  Hash,
  Layers,
  MemoryStick,
  Monitor,
  PieChart,
  Terminal,
  Upload,
} from "lucide-react";
import React from "react";
import { User } from "../../../app/auth/user";
import Breadcrumbs from "../../../app/components/breadcrumbs/breadcrumbs";
import DonutChart, { makeColorPicker, NamedValue } from "../../../app/components/chart/donut_chart";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Link from "../../../app/components/link/link";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import * as format from "../../../app/format/format";
import router, { Path } from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { getChartColor } from "../../../app/util/color";
import * as proto from "../../../app/util/proto";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import FilterComponent from "../filter/filter";
import { getProtoFilterParams } from "../filter/filter_util";
import { computeTimeKeys, getQuantile } from "../trends/common";
import { SeriesType } from "../trends/trends_chart";
import SingleActionComponent from "./single_action";
import TargetChartComponent, { TimelineDataSeries } from "./target_chart";

/**
 * Sentinel value used by the filter dropdowns to indicate that no filtering
 * should be applied for that dimension.
 */
const ALL_VALUES = "all";

const OS_ARCH_SEPARATOR = "&&";

function formatMnemonic(mnemonic: string): string {
  return mnemonic || "Unknown mnemonic";
}

function formatPlatform(os: string, arch: string): string {
  return [os, arch].filter(Boolean).join("/") || "Unknown platform";
}

/**
 * Scrolls `el` and every scrollable ancestor of it back to the top.  The
 * enterprise layout scrolls the main content area rather than the window, so
 * scrolling the window alone isn't enough.
 */
function scrollToTop(el: HTMLElement | null) {
  for (let node: HTMLElement | null = el; node; node = node.parentElement) {
    if (node.scrollTop > 0) {
      node.scrollTop = 0;
    }
  }
  window.scrollTo(0, 0);
}

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  timeline?: execution_stats.GetExecutionTimelineResponse;
  filterText: string;
  mnemonic?: string;
  os?: string;
  arch?: string;
}

export default class SingleTargetComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    filterText: "",
  };

  private rootRef = React.createRef<HTMLDivElement>();
  private currentRequestKey?: string;
  private pendingTimelineRequest?: CancelablePromise;

  private getPageTitle() {
    return this.props.search.get("target") ?? "Target Data";
  }

  private updateDocumentTitle() {
    document.title = `${this.getPageTitle()} | BuildBuddy`;
  }

  componentDidMount(): void {
    this.updateDocumentTitle();
    this.fetchExecutionTimeline();
  }

  componentDidUpdate(prevProps: Props): void {
    this.updateDocumentTitle();
    if (this.props.search === prevProps.search) {
      return;
    }
    this.fetchExecutionTimeline();
    // Moving between the target overview and a single action is a page change,
    // so start the reader at the top of the new page.
    if (
      this.props.search.get("output_path") !== prevProps.search.get("output_path") ||
      this.props.search.get("target") !== prevProps.search.get("target")
    ) {
      scrollToTop(this.rootRef.current);
    }
  }

  componentWillUnmount(): void {
    this.pendingTimelineRequest?.cancel();
  }

  /**
   * Returns the URL params that affect the timeline request.  The output path
   * only selects which of the returned timelines to display, so moving between
   * the target overview and a single action doesn't need a new request.
   */
  private getRequestKey(): string {
    const params = new URLSearchParams(this.props.search);
    params.delete("output_path");
    params.sort();
    return params.toString();
  }

  private fetchExecutionTimeline(): void {
    const requestKey = this.getRequestKey();
    // Avoid re-fetching when nothing relevant to the request has changed.
    if (requestKey === this.currentRequestKey) {
      return;
    }
    this.currentRequestKey = requestKey;

    this.pendingTimelineRequest?.cancel();

    const target = this.getTarget();
    if (!target) {
      this.setState({ loading: false, timeline: undefined });
      return;
    }

    const filterParams = getProtoFilterParams(this.props.search);
    let query = new execution_stats.ExecutionQuery({
      invocationHost: filterParams.host,
      invocationUser: filterParams.user,
      repoUrl: filterParams.repo,
      branchName: filterParams.branch,
      commitSha: filterParams.commit,
      command: filterParams.command,
      pattern: filterParams.pattern,
      tags: filterParams.tags,
      role: filterParams.role || [],
      updatedAfter: filterParams.updatedAfter,
      updatedBefore: filterParams.updatedBefore,
      invocationStatus: filterParams.status || [],
      filter: [],
      dimensionFilter: filterParams.dimensionFilters,
      genericFilters: filterParams.genericFilters,
    });

    const request = new execution_stats.GetExecutionTimelineRequest({
      target,
      query,
    });

    this.setState({ loading: true, timeline: undefined });
    this.pendingTimelineRequest = rpcService.service
      .getExecutionTimeline(request)
      .then((response) => {
        if (requestKey !== this.currentRequestKey) {
          return;
        }
        // Keep the mnemonic and platform selections across refreshes, unless
        // they no longer match anything in the new response.
        const mnemonics = new Set(response.timelines.map((tl) => tl.mnemonic));
        const platforms = new Set(response.timelines.map((tl) => tl.os + OS_ARCH_SEPARATOR + tl.arch));
        const keepMnemonic = this.state.mnemonic !== undefined && mnemonics.has(this.state.mnemonic);
        const keepPlatform =
          this.state.os !== undefined && platforms.has(this.state.os + OS_ARCH_SEPARATOR + this.state.arch);
        this.setState({
          timeline: response,
          mnemonic: keepMnemonic ? this.state.mnemonic : undefined,
          os: keepPlatform ? this.state.os : undefined,
          arch: keepPlatform ? this.state.arch : undefined,
        });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        if (requestKey === this.currentRequestKey) {
          this.setState({ loading: false });
        }
      });
  }

  private getSingleActionTimeline(): execution_stats.ExecutionTimeline | undefined {
    const outputPath = this.getOutputPath();
    if (outputPath == null) {
      return undefined;
    }
    return this.state.timeline?.timelines.find((tl) => tl.outputPath === outputPath);
  }

  /**
   * Returns the timelines that match every active (non-"all") filter selection.
   * An unset selection means "all"; an empty string is a real value that only
   * matches timelines with that field empty.
   */
  private getFilteredTimelines(rsp: execution_stats.GetExecutionTimelineResponse): execution_stats.ExecutionTimeline[] {
    return rsp.timelines.filter((tl) => {
      if (this.state.mnemonic !== undefined && tl.mnemonic !== this.state.mnemonic) {
        return false;
      } else if (this.state.os !== undefined && tl.os !== this.state.os) {
        return false;
      } else if (this.state.arch !== undefined && tl.arch !== this.state.arch) {
        return false;
      }
      return tl.outputPath.indexOf(this.state.filterText) >= 0;
    });
  }

  private renderTimelineCard(
    interval: stats.StatsInterval,
    domain: [Date, Date],
    timelines: execution_stats.ExecutionTimeline[],
    colorPicker: (tl: execution_stats.ExecutionTimeline) => string
  ): React.ReactNode {
    const durationSeries: TimelineDataSeries[] = [];
    const cpuSeries: TimelineDataSeries[] = [];
    const memorySeries: TimelineDataSeries[] = [];
    let i = 0;

    let { timeKeys, ticks } = computeTimeKeys(interval, domain);
    timeKeys = timeKeys.map((v) => v * 1000);
    ticks = ticks.map((v) => v * 1000);

    for (const timeline of timelines) {
      const durationByStartTime = new Map<number, number>();
      const memoryByStartTime = new Map<number, number>();
      const cpuByStartTime = new Map<number, number>();
      let endOfPath = timeline.outputPath;
      const lastSlash = timeline.outputPath.lastIndexOf("/");
      if (lastSlash >= 0) {
        endOfPath = "..." + endOfPath.slice(lastSlash);
      }
      const endOfPathElement = () => <span>{endOfPath}</span>;
      for (const e of timeline.aggregatedStats) {
        durationByStartTime.set(+(e.bucketStartTimeUsec ?? 0), getQuantile(e.summary?.durationUsec, 50));
        memoryByStartTime.set(+(e.bucketStartTimeUsec ?? 0), getQuantile(e.summary?.peakMemory, 50));
        cpuByStartTime.set(+(e.bucketStartTimeUsec ?? 0), getQuantile(e.summary?.cpuNanos, 50));
      }
      const color = colorPicker(timeline);
      durationSeries.push({
        series: {
          name: "duration" + i,
          type: SeriesType.LINE,
          extractValue: (startTimeUsec) => {
            const d = durationByStartTime.get(startTimeUsec);
            return d || null;
          },
          formatHoverValue: (durationUsec) => (
            <>
              {endOfPathElement()} <span>{format.durationUsec(durationUsec ?? 0)}</span>
            </>
          ),
          color: color,
        },
        timeline: timeline,
      });
      cpuSeries.push({
        series: {
          name: "cpu" + i,
          type: SeriesType.LINE,
          extractValue: (startTimeUsec) => {
            const d = cpuByStartTime.get(startTimeUsec);
            return d || null;
          },
          formatHoverValue: (value) => (
            <>
              {endOfPathElement()}
              <div>{format.durationUsec((value ?? 0) / 1e3)}</div>
            </>
          ),
          color: color,
        },
        timeline: timeline,
      });
      memorySeries.push({
        series: {
          name: "memory" + i,
          type: SeriesType.LINE,
          extractValue: (startTimeUsec) => {
            const d = memoryByStartTime.get(startTimeUsec);
            return d || null;
          },
          formatHoverValue: (value) => (
            <>
              {endOfPathElement()}
              <div>{format.bytes(value || 0)}</div>
            </>
          ),
          color: color,
        },
        timeline: timeline,
      });

      i++;
    }

    return (
      <div className="card">
        <div className="content">
          <div className="title single-target-card-title">
            <BarChart2 /> Action history
          </div>
          {this.renderTimelineFilters()}
          <div className="details">
            <TargetChartComponent
              title="Execution duration"
              target={this.getTarget()}
              timeKeys={timeKeys}
              ticks={ticks}
              series={durationSeries}
              formatValues={format.durationUsec}
              getQuantiles={(tl) => tl.durationUsec}
              getTotal={(tl) => +tl.durationUsecTotal}
              colorPicker={colorPicker}
            />
            <TargetChartComponent
              title="CPU usage"
              target={this.getTarget()}
              timeKeys={timeKeys}
              ticks={ticks}
              series={cpuSeries}
              formatValues={(v) => format.durationUsec(v / 1e3)}
              getQuantiles={(tl) => tl.cpuNanos}
              getTotal={(tl) => +tl.cpuNanosTotal}
              colorPicker={colorPicker}
            />
            <TargetChartComponent
              title="Peak memory usage"
              target={this.getTarget()}
              timeKeys={timeKeys}
              ticks={ticks}
              series={memorySeries}
              formatValues={(value) => format.bytes(value)}
              getQuantiles={(tl) => tl.peakMemory}
              colorPicker={colorPicker}
            />
          </div>
        </div>
      </div>
    );
  }

  getTarget() {
    return this.props.search.get("target") ?? "";
  }

  getOutputPath() {
    return this.props.search.get("output_path");
  }

  /** Summarizes every remote action of the target that matched the filters. */
  private renderTargetDetails(): React.ReactNode {
    const timelines = this.state.timeline?.timelines ?? [];
    if (timelines.length === 0) {
      return null;
    }
    const mnemonics = Array.from(new Set(timelines.map((tl) => formatMnemonic(tl.mnemonic)))).sort();
    const platforms = Array.from(new Set(timelines.map((tl) => formatPlatform(tl.os, tl.arch)))).sort();
    const total = (get: (summary: execution_stats.ExecutionTimelineSummary) => number) =>
      timelines.reduce((sum, tl) => sum + (tl.summary ? get(tl.summary) : 0), 0);
    const totalExecutions = total((s) => +s.totalExecutions);
    return (
      <>
        <div className="detail" title="Distinct remote actions run for this target">
          <Activity />
          {format.formatWithCommas(timelines.length)} {timelines.length === 1 ? "remote action" : "remote actions"}
        </div>
        <div className="detail" title="Total execution">
          <Hash />
          {totalExecutions} execution{totalExecutions === 1 ? "" : "s"}
        </div>
        <div className="detail" title={mnemonics.join(", ")}>
          <Terminal />
          {mnemonics.length === 1 ? mnemonics[0] : `${mnemonics.length} mnemonics`}
        </div>
        <div className="detail" title={platforms.join(", ")}>
          <Monitor />
          {platforms.length === 1 ? platforms[0] : `${platforms.length} platforms`}
        </div>
        <div className="detail" title="Total wall time spent running remote actions">
          <Clock />
          {format.durationUsec(total((s) => +s.durationUsecTotal))} wall time
        </div>
        <div className="detail" title="Total CPU time used by remote actions">
          <Cpu />
          {format.durationMillis(total((s) => +s.cpuNanosTotal) / 1e6)} CPU time
        </div>
        <div className="detail" title="Total inputs downloaded by remote actions">
          <Download />
          {format.bytes(total((s) => +s.downloadedBytesTotal))} downloaded
        </div>
        <div className="detail" title="Total outputs uploaded by remote actions">
          <Upload />
          {format.bytes(total((s) => +s.uploadedBytesTotal))} uploaded
        </div>
      </>
    );
  }

  /** Summarizes the single action being viewed. */
  private renderActionDetails(): React.ReactNode {
    const timeline = this.getSingleActionTimeline();
    if (!timeline) {
      return null;
    }
    const summary = timeline.summary;
    const sampleCount = timeline.executionSamples.length;
    return (
      <>
        <div className="detail" title="Action mnemonic">
          <Terminal />
          {formatMnemonic(timeline.mnemonic)}
        </div>
        <div className="detail" title="Execution platform">
          <Monitor />
          {formatPlatform(timeline.os, timeline.arch)}
        </div>
        {+timeline.shard > 0 && (
          <div className="detail" title="Test shard">
            <Layers />
            Shard {String(timeline.shard)}
          </div>
        )}
        {summary && (
          <>
            <div className="detail" title="Total executions">
              <Hash />
              {summary.totalExecutions} execution{+summary.totalExecutions === 1 ? "" : "s"}
            </div>
            <div className="detail" title="Median wall time">
              <Clock />
              {format.durationUsec(getQuantile(summary.durationUsec, 50))} median wall time
            </div>
            <div className="detail" title="Median CPU time">
              <Cpu />
              {format.durationMillis(getQuantile(summary.cpuNanos, 50) / 1e6)} median CPU time
            </div>
            <div className="detail" title="Median peak memory">
              <MemoryStick />
              {format.bytes(getQuantile(summary.peakMemory, 50))} median peak memory
            </div>
            <div className="detail" title="Total inputs downloaded">
              <Download />
              {format.bytes(+summary.downloadedBytesTotal)} downloaded
            </div>
            <div className="detail" title="Total outputs uploaded">
              <Upload />
              {format.bytes(+summary.uploadedBytesTotal)} uploaded
            </div>
          </>
        )}
      </>
    );
  }

  renderHeader() {
    const target = this.getTarget();
    const outputPath = this.getOutputPath();

    const title = outputPath ? outputPath : (target ?? "");

    return (
      <div className="shelf">
        <div className="container">
          <div className="top-bar">
            <Breadcrumbs>
              {this.props.user && this.props.user?.selectedGroupName() && (
                <Link className="clickable" href={Path.home}>
                  {this.props.user?.selectedGroupName()}
                </Link>
              )}
              <Link className="clickable" href={Path.targetsPath}>
                Targets
              </Link>
              <Link className="clickable" href={router.getSingleTargetPath(target ?? "")}>
                {target}
              </Link>
              {Boolean(outputPath) && (
                <Link className="clickable" href={router.getSingleTargetPath(target ?? "", outputPath ?? "")}>
                  {outputPath}
                </Link>
              )}
            </Breadcrumbs>
            <FilterComponent search={this.props.search} />
          </div>
          <div className="titles">
            <div className="title" title={title}>
              {title}
            </div>
          </div>
          <div className="details">{outputPath ? this.renderActionDetails() : this.renderTargetDetails()}</div>
        </div>
      </div>
    );
  }

  onFilterTextChange(event: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ filterText: event.target.value ?? "" });
  }

  onMnemonicChange(event: React.ChangeEvent<HTMLSelectElement>) {
    if (event.target.value == ALL_VALUES) {
      this.setState({ mnemonic: undefined });
    } else {
      this.setState({ mnemonic: event.target.value });
    }
  }

  onPlatformChange(event: React.ChangeEvent<HTMLSelectElement>) {
    if (event.target.value == ALL_VALUES) {
      this.setState({ os: undefined, arch: undefined });
    } else {
      const [os, arch] = event.target.value.split(OS_ARCH_SEPARATOR);
      this.setState({ os, arch });
    }
  }

  renderTimelineFilters(): React.ReactNode {
    const mnemonicSet = new Set<string>();
    const osArchSet = new Set<string>();

    this.state.timeline?.timelines.forEach((tl) => {
      osArchSet.add(tl.os + OS_ARCH_SEPARATOR + tl.arch);
      mnemonicSet.add(tl.mnemonic);
    });

    const mnemonics = [...mnemonicSet].sort();
    const platforms = [...osArchSet].sort();
    mnemonics.unshift(ALL_VALUES);
    platforms.unshift(ALL_VALUES);

    return (
      <>
        <div className="controls row">
          <label>Mnemonic</label>
          <Select
            debug-id="filter-mnemonic"
            value={this.state.mnemonic ?? ALL_VALUES}
            onChange={this.onMnemonicChange.bind(this)}>
            {mnemonics.map((m) => (
              <Option key={m} value={m}>
                {m === ALL_VALUES ? "All" : formatMnemonic(m)}
              </Option>
            ))}
          </Select>
          <div className="separator" />
          <label>OS/Arch</label>
          <Select
            debug-id="filter-platform"
            value={
              this.state.os != undefined || this.state.arch != undefined
                ? (this.state.os ?? "") + OS_ARCH_SEPARATOR + (this.state.arch ?? "")
                : ALL_VALUES
            }
            onChange={this.onPlatformChange.bind(this)}>
            {platforms.map((p) => {
              const [os, arch] = p.split(OS_ARCH_SEPARATOR);
              return (
                <Option key={p} value={p}>
                  {p === ALL_VALUES ? "All" : formatPlatform(os, arch)}
                </Option>
              );
            })}
          </Select>
        </div>
        <div className="controls row">
          <FilterInput value={this.state.filterText} onChange={this.onFilterTextChange.bind(this)} />
        </div>
      </>
    );
  }

  renderSingleTarget(domain: [Date, Date]): React.ReactNode {
    const cpuData = new Map<string, number>();
    const durData = new Map<string, number>();
    const dlData = new Map<string, number>();
    const ulData = new Map<string, number>();
    const tlColorMap = new Map<execution_stats.ExecutionTimeline, string>();

    this.state.timeline?.timelines.forEach((tl, i) => {
      tlColorMap.set(tl, getChartColor(i));

      const k = tl.mnemonic ?? "";

      cpuData.set(k, (cpuData.get(k) ?? 0) + +(tl.summary?.cpuNanosTotal ?? 0));
      durData.set(k, (durData.get(k) ?? 0) + +(tl.summary?.durationUsecTotal ?? 0));
      dlData.set(k, (dlData.get(k) ?? 0) + +(tl.summary?.downloadedBytesTotal ?? 0));
      ulData.set(k, (ulData.get(k) ?? 0) + +(tl.summary?.uploadedBytesTotal ?? 0));
    });

    const cpuValues: NamedValue[] = [];
    const durValues: NamedValue[] = [];
    const dlValues: NamedValue[] = [];
    const ulValues: NamedValue[] = [];

    cpuData?.forEach((v, k) => cpuValues.push({ name: k, value: v })) ?? [];
    durData?.forEach((v, k) => durValues.push({ name: k, value: v })) ?? [];
    dlData?.forEach((v, k) => dlValues.push({ name: k, value: v })) ?? [];
    ulData?.forEach((v, k) => ulValues.push({ name: k, value: v })) ?? [];

    const mnemonicColorPicker = makeColorPicker([
      ...[...cpuData.entries()].sort((a, b) => b[1] - a[1]).map((v) => v[0]),
      ...durData.keys(),
      ...dlData.keys(),
      ...ulData.keys(),
    ]);

    return (
      <div className="container">
        {this.state.timeline && (
          <>
            <div className="card">
              <div className="content">
                <div className="title single-target-card-title">
                  <PieChart />
                  Stats overview
                </div>
                <div className="details">
                  <div className="target-donuts">
                    <div className="target-donut">
                      <DonutChart
                        title="CPU usage"
                        subtitle="Total CPU utilization (core-nanos) on RBE"
                        data={cpuValues}
                        colorPicker={mnemonicColorPicker}
                        valueFormatter={(v) => format.compactDurationMillis(v / 1e6)}
                      />
                    </div>
                    <div className="target-donut">
                      <DonutChart
                        title="Wall time"
                        subtitle="Elapsed time spent physically running RBE actions"
                        data={durValues}
                        colorPicker={mnemonicColorPicker}
                        valueFormatter={(v) => format.compactDurationMillis(v / 1e3)}
                      />
                    </div>
                    <div className="target-donut">
                      <DonutChart
                        title="Cache download"
                        subtitle="Inputs downloaded to run remote actions"
                        data={dlValues}
                        colorPicker={mnemonicColorPicker}
                        valueFormatter={(v) => format.bytes(v)}
                      />
                    </div>
                    <div className="target-donut">
                      <DonutChart
                        title="Cache upload"
                        subtitle="Outputs uploaded by remote actions"
                        data={ulValues}
                        colorPicker={mnemonicColorPicker}
                        valueFormatter={(v) => format.bytes(v)}
                      />
                    </div>
                  </div>
                </div>
              </div>
            </div>
            {this.renderTimelineCard(
              this.state.timeline.interval!,
              domain,
              this.getFilteredTimelines(this.state.timeline),
              (tl) => tlColorMap.get(tl) ?? getChartColor(0)
            )}
          </>
        )}
      </div>
    );
  }

  private renderEmptyState(message: string): React.ReactNode {
    return (
      <div className="container">
        <div className="targets-empty">
          <div className="empty-message">{message}</div>
        </div>
      </div>
    );
  }

  render(): React.ReactNode {
    const p = getProtoFilterParams(this.props.search);
    const domain: [Date, Date] = [
      proto.timestampToDate(p.updatedAfter!),
      p.updatedBefore ? proto.timestampToDate(p.updatedBefore) : new Date(),
    ];

    let pageContent: React.ReactNode;
    const outputPath = this.getOutputPath();
    if (this.state.loading) {
      pageContent = (
        <div className="container">
          <div className="loading-section">
            <Spinner />
          </div>
        </div>
      );
    } else if (!this.state.timeline) {
      pageContent = this.renderEmptyState("No remote execution data is available for this target.");
    } else if (outputPath != null) {
      // Single action page.
      const singleAction = this.getSingleActionTimeline();
      pageContent = singleAction ? (
        <SingleActionComponent
          domain={domain}
          interval={this.state.timeline.interval!}
          outputPath={outputPath}
          target={this.getTarget()}
          timeline={singleAction}
        />
      ) : (
        this.renderEmptyState("No remote executions of this action matched the current filters.")
      );
    } else if (this.state.timeline.timelines.length === 0) {
      pageContent = this.renderEmptyState("No remote executions of this target matched the current filters.");
    } else {
      pageContent = this.renderSingleTarget(domain);
    }

    return (
      <div className="single-target" ref={this.rootRef}>
        {this.renderHeader()}
        {pageContent}
      </div>
    );
  }
}
