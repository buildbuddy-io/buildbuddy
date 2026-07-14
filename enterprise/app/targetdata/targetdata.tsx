import moment from "moment";
import React from "react";
import { User } from "../../../app/auth/user";
import { OutlinedButton } from "../../../app/components/button/button";
import Select, { Option } from "../../../app/components/select/select";
import errorService from "../../../app/errors/error_service";
import * as format from "../../../app/format/format";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { computeDiffs } from "../../../app/util/diff";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { getProtoFilterParams } from "../filter/filter_util";
import TrendsChartComponent, { ChartColor, ChartDataSeries } from "../trends/trends_chart";

const MICROSECONDS_PER_SECOND = 1e6;

// Sentinel value used by the filter dropdowns to indicate that no filtering
// should be applied for that dimension.
const ALL_VALUES = "all";

// The dimensions of an `ExecutionTimeline` that the user can filter on. Each
// dimension gets its own dropdown, populated from the values present in the
// current response.
const FILTER_DIMENSIONS: Array<{
  key: string;
  label: string;
  getValue: (timeline: execution_stats.ExecutionTimeline) => string;
  // Optional display formatting for an option's value in the dropdown.
  formatValue?: (value: string) => string;
}> = [
  { key: "mnemonic", label: "Mnemonic", getValue: (t) => t.mnemonic },
  { key: "os", label: "OS", getValue: (t) => t.os },
  { key: "arch", label: "Arch", getValue: (t) => t.arch },
  {
    key: "shard",
    label: "Shard",
    getValue: (t) => String(t.shard),
    formatValue: (value) => (value === "0" ? "--" : value),
  },
];

const COLORS = [
  ChartColor.BLUE,
  ChartColor.RED,
  ChartColor.GREEN,
  ChartColor.ORANGE,
  ChartColor.GREY,
  ChartColor.BASICALLY_BLACK,
];

/**
 * Formats an output path for display.
 *
 * By default (no `comparePath`), the full path is returned unchanged. When a
 * `comparePath` is supplied, the path is abbreviated relative to it by diffing
 * the two: parts unique to `outputPath` are kept, shared directory segments
 * collapse to "...", and the file name is always shown in full. For example,
 * abbreviating "output/path/b.o" against "output/path/a.o" yields ".../b.o".
 */
function formatOutputPath(outputPath: string, comparePath?: string): JSX.Element[] {
  if (!comparePath) {
    return [<>{outputPath}</>];
  }

  const shortPath = comparePath.length < 60;

  const diffs = computeDiffs(comparePath, outputPath);

  // The file name lives in the final "equal" span, which we always render in
  // full; every earlier "equal" span collapses to "...".
  let lastEqualIndex = -1;
  for (let i = 0; i < diffs.length; i++) {
    if (diffs[i].type === 0) {
      lastEqualIndex = i;
    }
  }
  // Nothing is shared between the paths, so there's nothing to abbreviate.
  if (lastEqualIndex === -1) {
    return [<>outputPath</>];
  }

  const result: JSX.Element[] = [<>&lrm;</>];
  let lastAddedDiff = "";
  for (let i = 0; i < diffs.length; i++) {
    const diff = diffs[i];
    if (diff.type === 1) {
      // "Added": unique to this path, so keep it.
      lastAddedDiff = diff.text;
      result.push(<strong>{diff.text}</strong>);
    } else if (diff.type === -1) {
      // "Removed": present only in the comparison path, so drop it.
      continue;
    } else if (i === diffs.length - 1) {
      // Final "equal": elide up to the last "/", then keep the full file name
      // (including the leading "/" so ".../b.o" reads naturally).
      const slash = diff.text.lastIndexOf("/");
      if (shortPath || slash === -1) {
        result.push(<>{diff.text}</>);
      } else {
        if (lastAddedDiff != "...") {
          result.push(<>...</>);
        }
        result.push(<>{diff.text.slice(slash)}</>);
      }
    } else {
      // Earlier "equal": collapse to "...".
      if (shortPath) {
        result.push(<>{diff.text}</>);
      } else if (lastAddedDiff != "...") {
        lastAddedDiff = "...";
        result.push(<>...</>);
      }
    }
  }
  return result;
}

interface Props {
  user: User;
  search: URLSearchParams;
}

interface State {
  loading: boolean;
  timeline?: execution_stats.GetExecutionTimelineResponse;
  // Selected value for each filter dimension, keyed by dimension key. A missing
  // entry or `ALL_VALUES` means the dimension is not being filtered.
  filters: Record<string, string>;
  // The table column currently used for sorting, and whether it's ascending.
  sortColumn: string;
  sortAscending: boolean;
  // Once the user clicks "Show all", we show every matching action for the rest
  // of their time in this view, even as the filter set changes.
  showAllRows: boolean;
}

// The number of actions shown in the table before the user clicks "Show all".
const INITIAL_ROW_LIMIT = 5;

// Identifies the output path column for sorting; the filter dimensions use
// their own keys and the p50 columns use the keys below.
const OUTPUT_PATH_COLUMN = "output_path";
const DURATION_P50_COLUMN = "duration_p50";
const CPU_P50_COLUMN = "cpu_p50";
const MEMORY_P50_COLUMN = "memory_p50";

export default class TargetDataComponent extends React.Component<Props, State> {
  state: State = {
    loading: false,
    filters: {},
    sortColumn: OUTPUT_PATH_COLUMN,
    sortAscending: true,
    showAllRows: false,
  };

  private currentTarget?: string;
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

  componentDidUpdate(): void {
    this.updateDocumentTitle();
    this.fetchExecutionTimeline();
  }

  componentWillUnmount(): void {
    this.pendingTimelineRequest?.cancel();
  }

  private fetchExecutionTimeline(): void {
    const target = this.props.search.get("target") ?? "";
    // Avoid re-fetching when the target hasn't changed across updates.
    if (target === this.currentTarget) {
      return;
    }
    this.currentTarget = target;

    this.pendingTimelineRequest?.cancel();

    if (!target) {
      this.setState({ loading: false, timeline: undefined, filters: {} });
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

    this.setState({ loading: true, timeline: undefined, filters: {} });
    this.pendingTimelineRequest = rpcService.service
      .getExecutionTimeline(request)
      .then((response) => {
        console.log(response);
        this.setState({ timeline: response });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private handleFilterChange(key: string, value: string): void {
    const filters = { ...this.state.filters, [key]: value };
    // Shard numbers are only meaningful within a single mnemonic, so changing
    // the mnemonic resets the shard selection back to "All".
    if (key === "mnemonic") {
      filters["shard"] = ALL_VALUES;
    }
    // Changing one selection can constrain the others (e.g. picking a mnemonic
    // with no shards). Drop any remaining selections that are no longer
    // available given the new constraints so we never show a value that filters
    // everything out.
    const rsp = this.state.timeline;
    if (rsp) {
      for (const dimension of FILTER_DIMENSIONS) {
        const selected = filters[dimension.key];
        if (
          selected &&
          selected !== ALL_VALUES &&
          !this.getAvailableValues(rsp, dimension, filters).includes(selected)
        ) {
          filters[dimension.key] = ALL_VALUES;
        }
      }
    }
    this.setState({ filters });
  }

  private resetFilters(): void {
    this.setState({ filters: {} });
  }

  // Sets every dropdown to the values of the clicked action, narrowing the
  // charts and table down to (approximately) that single action.
  private handleRowClick(timeline: execution_stats.ExecutionTimeline): void {
    const filters: Record<string, string> = {};
    for (const dimension of FILTER_DIMENSIONS) {
      filters[dimension.key] = dimension.getValue(timeline);
    }
    this.setState({ filters });
  }

  private hasActiveFilters(): boolean {
    return FILTER_DIMENSIONS.some((dimension) => {
      const selected = this.state.filters[dimension.key];
      return selected && selected !== ALL_VALUES;
    });
  }

  // Returns the distinct values available for `dimension`, constrained to the
  // timelines that match every OTHER active filter selection. This narrows each
  // dropdown's options based on what the user has already picked elsewhere.
  private getAvailableValues(
    rsp: execution_stats.GetExecutionTimelineResponse,
    dimension: (typeof FILTER_DIMENSIONS)[number],
    filters: Record<string, string>
  ): string[] {
    const matching = rsp.timelines.filter((timeline) =>
      FILTER_DIMENSIONS.every((other) => {
        if (other.key === dimension.key) return true;
        const selected = filters[other.key];
        return !selected || selected === ALL_VALUES || other.getValue(timeline) === selected;
      })
    );
    return Array.from(new Set(matching.map((t) => dimension.getValue(t)))).sort();
  }

  // Returns the timelines that match every active (non-"all") filter selection.
  private getFilteredTimelines(rsp: execution_stats.GetExecutionTimelineResponse): execution_stats.ExecutionTimeline[] {
    return rsp.timelines.filter((timeline) =>
      FILTER_DIMENSIONS.every((dimension) => {
        const selected = this.state.filters[dimension.key];
        return !selected || selected === ALL_VALUES || dimension.getValue(timeline) === selected;
      })
    );
  }

  // Handles a click on a sortable column header. Clicking a new column sorts it
  // descending; clicking the active column toggles between descending and
  // ascending.
  private handleSort(column: string): void {
    if (this.state.sortColumn === column) {
      this.setState({ sortAscending: !this.state.sortAscending });
    } else {
      this.setState({ sortColumn: column, sortAscending: false });
    }
  }

  // Returns the value used to sort a timeline by the given column. Numeric
  // columns return numbers so they sort numerically; the rest return strings.
  private getSortValue(timeline: execution_stats.ExecutionTimeline, column: string): number | string {
    switch (column) {
      case OUTPUT_PATH_COLUMN:
        return timeline.outputPath;
      case DURATION_P50_COLUMN:
        return +(timeline.summary?.durationUsecP50 ?? 0);
      case CPU_P50_COLUMN:
        return +(timeline.summary?.cpuNanosP50 ?? 0);
      case MEMORY_P50_COLUMN:
        return +(timeline.summary?.peakMemoryP50 ?? 0);
      case "shard":
        return +(timeline.shard ?? 0);
      default: {
        const dimension = FILTER_DIMENSIONS.find((d) => d.key === column);
        return dimension ? dimension.getValue(timeline) : "";
      }
    }
  }

  // Returns a copy of `timelines` sorted by the active sort column and
  // direction.
  private getSortedTimelines(timelines: execution_stats.ExecutionTimeline[]): execution_stats.ExecutionTimeline[] {
    const { sortColumn, sortAscending } = this.state;
    return [...timelines].sort((a, b) => {
      const av = this.getSortValue(a, sortColumn);
      const bv = this.getSortValue(b, sortColumn);
      let cmp: number;
      if (typeof av === "number" && typeof bv === "number") {
        cmp = av - bv;
      } else {
        cmp = String(av).localeCompare(String(bv));
      }
      return sortAscending ? cmp : -cmp;
    });
  }

  private renderSortHeader(column: string, label: string, className?: string): JSX.Element {
    const active = this.state.sortColumn === column;
    const arrow = active ? (this.state.sortAscending ? " ▲" : " ▼") : "";
    return (
      <th key={column} className={className} onClick={() => this.handleSort(column)}>
        {label}
        {arrow}
      </th>
    );
  }

  private renderFilters(rsp: execution_stats.GetExecutionTimelineResponse): React.ReactNode {
    return (
      <div className="target-data-filters">
        {FILTER_DIMENSIONS.map((dimension) => {
          // Only show values that are still reachable given the other
          // selections, sorted for stable, readable ordering.
          const values = this.getAvailableValues(rsp, dimension, this.state.filters);
          const selected = this.state.filters[dimension.key] ?? ALL_VALUES;
          return (
            <div className="target-data-filter" key={dimension.key}>
              <span className="target-data-filter-label">{dimension.label}</span>
              <Select value={selected} onChange={(e) => this.handleFilterChange(dimension.key, e.target.value)}>
                <Option value={ALL_VALUES}>All</Option>
                {values.map((value) => (
                  <Option key={value} value={value}>
                    {(dimension.formatValue ? dimension.formatValue(value) : value) || "(empty)"}
                  </Option>
                ))}
              </Select>
            </div>
          );
        })}
        <OutlinedButton onClick={() => this.resetFilters()} disabled={!this.hasActiveFilters()}>
          Reset
        </OutlinedButton>
      </div>
    );
  }

  private renderMatchingActionsCard(rsp: execution_stats.GetExecutionTimelineResponse): React.ReactNode {
    const allTimelines = this.getSortedTimelines(this.getFilteredTimelines(rsp));
    // Cap the table to the first few rows until the user opts into the full
    // list. The charts are unaffected and always plot every matching action.
    const timelines = this.state.showAllRows ? allTimelines : allTimelines.slice(0, INITIAL_ROW_LIMIT);
    // Once a specific mnemonic is selected, abbreviate output paths so the
    // distinguishing parts stand out. Each row is diffed against another matching
    // action: rows compare against the first row, and the first row compares
    // against the second so it too gets abbreviated.
    const selectedMnemonic = this.state.filters["mnemonic"];
    const abbreviate = Boolean(selectedMnemonic) && selectedMnemonic !== ALL_VALUES;
    const comparePathFor = (i: number): string | undefined => {
      if (!abbreviate) return undefined;
      return (i === 0 ? timelines[1] : timelines[0])?.outputPath;
    };
    return (
      <div className="card target-data-actions-card">
        <div className="target-data-card-title">Matching remote actions</div>
        {this.renderFilters(rsp)}
        <div className="target-data-table-wrapper">
          <table className="target-data-table">
            <thead>
              <tr>
                {this.renderSortHeader(OUTPUT_PATH_COLUMN, "Output path", "output-path-column")}
                {FILTER_DIMENSIONS.map((dimension) => this.renderSortHeader(dimension.key, dimension.label))}
                {this.renderSortHeader(DURATION_P50_COLUMN, "Duration (p50)")}
                {this.renderSortHeader(CPU_P50_COLUMN, "CPU (p50)")}
                {this.renderSortHeader(MEMORY_P50_COLUMN, "Memory (p50)")}
              </tr>
            </thead>
            <tbody>
              {timelines.map((timeline, i) => (
                <tr key={i} className="target-data-table-row" onClick={() => this.handleRowClick(timeline)}>
                  <td className="output-path-column" title={timeline.outputPath}>
                    {formatOutputPath(timeline.outputPath, comparePathFor(i)) || "(empty)"}
                  </td>
                  {FILTER_DIMENSIONS.map((dimension) => {
                    const value = dimension.getValue(timeline);
                    return (
                      <td key={dimension.key}>
                        {(dimension.formatValue ? dimension.formatValue(value) : value) || "(empty)"}
                      </td>
                    );
                  })}
                  <td>{format.durationUsec(timeline.summary?.durationUsecP50 ?? 0)}</td>
                  <td>{format.durationMillis(+(timeline.summary?.cpuNanosP50 ?? 0) / 1e6)}</td>
                  <td>{format.bytes(timeline.summary?.peakMemoryP50 ?? 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {allTimelines.length > INITIAL_ROW_LIMIT && (
          <OutlinedButton
            className="target-data-show-all"
            onClick={() => this.setState({ showAllRows: !this.state.showAllRows })}>
            {this.state.showAllRows ? "Hide" : `Show all (${allTimelines.length})`}
          </OutlinedButton>
        )}
      </div>
    );
  }

  private renderTimelineCharts(timelines: execution_stats.ExecutionTimeline[]): React.ReactNode {
    const startTimes: number[] = [];
    const durationSeries: ChartDataSeries[] = [];
    const cpuSeries: ChartDataSeries[] = [];
    const memorySeries: ChartDataSeries[] = [];
    let i = 0;
    for (const timeline of timelines) {
      // X-axis values are the execution start times (in microseconds), and each
      // maps to its duration so the line series can extract it.
      startTimes.push(...timeline.execution.map((e) => +(e.startTimeUsec ?? 0)));
      const durationByStartTime = new Map<number, number>();
      const memoryByStartTime = new Map<number, number>();
      const cpuByStartTime = new Map<number, number>();
      let endOfPath = timeline.outputPath;
      const lastSlash = timeline.outputPath.lastIndexOf("/");
      if (lastSlash >= 0) {
        endOfPath = "..." + endOfPath.slice(lastSlash);
      }
      const endOfPathElement = () => <div>{endOfPath}</div>;
      for (const e of timeline.execution) {
        durationByStartTime.set(+(e.startTimeUsec ?? 0), +(e.durationUsec ?? 0));
        memoryByStartTime.set(+(e.startTimeUsec ?? 0), +(e.peakMemoryBytes ?? 0));
        cpuByStartTime.set(+(e.startTimeUsec ?? 0), +(e.cpuNanos ?? 0));
      }
      durationSeries.push({
        name: "duration" + i,
        isLine: true,
        extractValue: (startTimeUsec) => {
          const d = durationByStartTime.get(startTimeUsec);
          return d ? d / MICROSECONDS_PER_SECOND : null;
        },
        formatHoverValue: (value) => (
          <>
            {endOfPathElement()}
            <div>{format.durationSec(value || 0)}</div>
          </>
        ),
        color: COLORS[i % COLORS.length],
      });
      cpuSeries.push({
        name: "cpu" + i,
        isLine: true,
        extractValue: (startTimeUsec) => cpuByStartTime.get(startTimeUsec) ?? null,
        formatHoverValue: (value) => (
          <>
            {endOfPathElement()}
            <div>{format.durationMillis((value ?? 0) / 1e6)}</div>
          </>
        ),
        color: COLORS[i % COLORS.length],
      });
      memorySeries.push({
        name: "memory" + i,
        isLine: true,
        extractValue: (startTimeUsec) => memoryByStartTime.get(startTimeUsec) ?? null,
        formatHoverValue: (value) => (
          <>
            {endOfPathElement()}
            <div>{format.bytes(value || 0)}</div>
          </>
        ),
        color: COLORS[i % COLORS.length],
      });

      i++;
    }

    startTimes.sort();

    return (
      <>
        <TrendsChartComponent
          title="Execution duration"
          data={startTimes}
          ticks={[]}
          dataSeries={durationSeries}
          primaryYAxis={{
            formatTickValue: format.durationSec,
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
        <TrendsChartComponent
          title="CPU usage"
          data={startTimes}
          ticks={[]}
          dataSeries={cpuSeries}
          primaryYAxis={{
            formatTickValue: (value) => format.durationMillis(value / 1e6),
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
        <TrendsChartComponent
          title="Peak memory usage"
          data={startTimes}
          ticks={[]}
          dataSeries={memorySeries}
          primaryYAxis={{
            formatTickValue: (value) => format.bytes(value),
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
        />
      </>
    );
  }

  render(): React.ReactNode {
    return (
      <div className="target-data">
        <div className="container">
          <div className="target-data-header">
            <div className="target-data-title">{this.getPageTitle()}</div>
          </div>
          {this.state.timeline && this.renderMatchingActionsCard(this.state.timeline)}
          {this.state.timeline && this.renderTimelineCharts(this.getFilteredTimelines(this.state.timeline))}
        </div>
      </div>
    );
  }
}
