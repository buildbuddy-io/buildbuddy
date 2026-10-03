import { BarChart2, List, SortAsc, SortDesc } from "lucide-react";
import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Link from "../../../app/components/link/link";
import Select, { Option } from "../../../app/components/select/select";
import { Tooltip, pinBottomLeftOffsetFromMouse } from "../../../app/components/tooltip/tooltip";
import format from "../../../app/format/format";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import ExecutionMenuButtonComponent, { getExecutionPath } from "./execution_menu_button";
import SingleActionChartComponent from "./single_action_chart";
import moment from "moment";

interface Props {
  target: string;
  outputPath: string;
  timeline: execution_stats.ExecutionTimeline;
  interval: stats.StatsInterval;
  domain: [Date, Date];
}

interface ExecStat {
  name: string;
  extractor: (e: execution_stats.ExecutionTimelineEntry) => number;
  formatter: (v: number) => string;
}

interface TimingPhase {
  stat: ExecStat;
  className: string;
}

interface DerivedSummaryStats {
  longestExecutionUsec: number;
}

// A column of the sampled executions table.
interface Column {
  name: string;
  className: string;
  render: (e: execution_stats.ExecutionTimelineEntry, s: DerivedSummaryStats) => React.ReactNode;
}

interface StatSet {
  name: string;
  // The stats that the table can be sorted by.
  stats: ExecStat[];
  // The columns shown in the table.
  columns: Column[];
}

const START_TIME: ExecStat = {
  name: "Start time",
  extractor: (e) => +e.startTimeUsec,
  formatter: (v) => moment(+v / 1000).format("YYYY-MM-DD [at] hh:mm:ss a"),
};

const WALL_TIME: ExecStat = { name: "Wall time", extractor: (e) => +e.durationUsec, formatter: format.durationUsec };

const TIMING_PHASES: TimingPhase[] = [
  {
    stat: { name: "Queue", extractor: (e) => +e.workerQueueUsec, formatter: format.durationUsec },
    className: "timing-phase-queue",
  },
  {
    stat: { name: "Runner setup", extractor: (e) => +e.runnerSetupUsec, formatter: format.durationUsec },
    className: "timing-phase-runner-setup",
  },
  {
    stat: { name: "Input download", extractor: (e) => +e.inputDownloadUsec, formatter: format.durationUsec },
    className: "timing-phase-input-download",
  },
  {
    stat: { name: "Execution", extractor: (e) => +e.executionUsec, formatter: format.durationUsec },
    className: "timing-phase-execution",
  },
  {
    stat: { name: "Output upload", extractor: (e) => +e.outputUploadUsec, formatter: format.durationUsec },
    className: "timing-phase-output-upload",
  },
];

const RESOURCE_STATS: ExecStat[] = [
  { name: "CPU time", extractor: (e) => +e.cpuNanos, formatter: (v) => format.durationMillis(v / 1e6) },
  { name: "Peak memory", extractor: (e) => +e.peakMemoryBytes, formatter: format.bytes },
  { name: "Downloaded", extractor: (e) => +e.downloadedBytes, formatter: format.bytes },
  { name: "Uploaded", extractor: (e) => +e.uploadedBytes, formatter: format.bytes },
];

// Groups of columns that can be shown in the sampled executions table.
const STAT_SETS: StatSet[] = [
  {
    name: "Timing",
    stats: [WALL_TIME, ...TIMING_PHASES.map((p) => p.stat)],
    columns: [statColumn(WALL_TIME), { name: "Timing", className: "timing-column", render: renderTimingBar }],
  },
  {
    name: "Resources",
    stats: RESOURCE_STATS,
    columns: RESOURCE_STATS.map(statColumn),
  },
];

// The number of sampled executions shown in the table before "Show more".
const MORE_RESULTS_LIMIT = 20;

// The number of leading characters of an action digest shown in the table.
const DIGEST_PREFIX_LENGTH = 8;

function statColumn(stat: ExecStat): Column {
  return { name: stat.name, className: "stat-column", render: (e) => stat.formatter(stat.extractor(e)) };
}

function renderTimingTooltip(e: execution_stats.ExecutionTimelineEntry): React.ReactNode {
  return (
    <div className="trend-chart-hover timing-tooltip">
      {TIMING_PHASES.map((p) => (
        <div key={p.stat.name} className="timing-tooltip-row">
          <span className={`timing-swatch ${p.className}`} />
          <span className="timing-tooltip-label">{p.stat.name}</span>
          <span className="timing-tooltip-value">{p.stat.formatter(p.stat.extractor(e))}</span>
        </div>
      ))}
    </div>
  );
}

// Renders a segmented bar where each segment represents an execution phase.
// The total width of the bar is set based on the longest execution, so that
// an execution that is about half as long takes up about half as much
// horizontal space.
function renderTimingBar(e: execution_stats.ExecutionTimelineEntry, d: DerivedSummaryStats): React.ReactNode {
  let totalUsec = 0;
  const segments = TIMING_PHASES.map((p) => ({ phase: p, usec: p.stat.extractor(e) })).filter((s) => {
    if (s.usec > 0) {
      totalUsec += s.usec;
    }
    return s.usec > 0;
  });
  return (
    <Tooltip className="timing-bar" pin={pinBottomLeftOffsetFromMouse} renderContent={() => renderTimingTooltip(e)}>
      <div className={`timing-track ${segments.length ? "" : "empty"}`}>
        {segments.map((s) => (
          <div key={s.phase.stat.name} className={`timing-segment ${s.phase.className}`} style={{ flexGrow: s.usec }} />
        ))}
        <div className="timing-segment" style={{ flexGrow: d.longestExecutionUsec - totalUsec }}></div>
      </div>
    </Tooltip>
  );
}

interface State {
  resultLimit: number;
  orderBy: ExecStat;
  ascending: boolean;
  tableStats: StatSet;
}

export default class SingleActionComponent extends React.Component<Props, State> {
  state: State = {
    orderBy: START_TIME,
    resultLimit: MORE_RESULTS_LIMIT,
    ascending: false,
    tableStats: STAT_SETS[0],
  };

  renderSingleActionChart(
    title: string,
    fmt: (v: number) => string,
    quantiles: (as: execution_stats.ExecutionTimelineSummary) => execution_stats.Quantile[],
    scat: (as: execution_stats.ExecutionTimelineEntry) => number
  ): React.ReactNode {
    return (
      <SingleActionChartComponent
        domain={this.props.domain}
        title={title}
        formatValue={fmt}
        getQuantiles={quantiles}
        getScatterValue={scat}
        interval={this.props.interval}
        timeline={this.props.timeline}
      />
    );
  }

  private findOrderBy(name: string) {
    return this.state.tableStats.stats.find((s) => s.name === name) ?? START_TIME;
  }

  private findStatSet(name: string) {
    return STAT_SETS.find((s) => s.name === name) ?? STAT_SETS[0];
  }

  private onOrderByChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ orderBy: this.findOrderBy(event.target.value) });
  }

  private onDescendingChange() {
    this.setState({ ascending: !this.state.ascending });
  }

  private onStatSetChange(event: React.ChangeEvent<HTMLSelectElement>) {
    const tableStats = this.findStatSet(event.target.value);
    // The sort column may have belonged to the previous set of columns.
    const orderBy = tableStats.stats.includes(this.state.orderBy) ? this.state.orderBy : START_TIME;
    this.setState({ tableStats, orderBy });
  }

  private isSampled(): boolean {
    return +(this.props.timeline.summary?.totalExecutions ?? 0) > this.props.timeline.executionSamples.length;
  }

  private tableRowSummary(shown: number, total: number): string {
    const noun = total === 1 ? "execution" : "executions";
    if (this.isSampled()) {
      return `Showing ${shown} of ${total} sampled ${noun} (${+(this.props.timeline.summary?.totalExecutions ?? 0) - total} more not in sample)`;
    } else {
      return `Showing ${shown} of ${total} ${noun}`;
    }
  }

  private renderExecutionTable() {
    const samples = this.props.timeline.executionSamples;
    const rows = [...samples]
      .sort(
        (a, b) => (this.state.ascending ? 1 : -1) * (this.state.orderBy.extractor(a) - this.state.orderBy.extractor(b))
      )
      .slice(0, this.state.resultLimit);

    const derivedStats: DerivedSummaryStats = {
      longestExecutionUsec: 0,
    };

    this.props.timeline.executionSamples.forEach((e) => {
      derivedStats.longestExecutionUsec = Math.max(+e.durationUsec, derivedStats.longestExecutionUsec);
    });

    const sortOptions = [START_TIME, ...this.state.tableStats.stats];

    return (
      <>
        <div className="controls row">
          <label>Sort by</label>
          <Select value={this.state.orderBy.name} onChange={this.onOrderByChange.bind(this)}>
            {sortOptions.map((o) => (
              <Option key={o.name} value={o.name}>
                {o.name}
              </Option>
            ))}
          </Select>
          <OutlinedButton
            className="icon-button"
            title={this.state.ascending ? "Sorted ascending" : "Sorted descending"}
            onClick={this.onDescendingChange.bind(this)}>
            {this.state.ascending ? <SortAsc /> : <SortDesc />}
          </OutlinedButton>
          <div className="separator" />
          <label>Show stats</label>
          <Select value={this.state.tableStats.name} onChange={this.onStatSetChange.bind(this)}>
            {STAT_SETS.map((s) => (
              <Option key={s.name} value={s.name}>
                {s.name}
              </Option>
            ))}
          </Select>
        </div>
        <div className="chart-table-container">
          <div className="results-table executions-table">
            <div className="row column-headers">
              <div className="digest-column">Digest</div>
              <div className="date-column">{START_TIME.name}</div>
              {this.state.tableStats.columns.map((c) => (
                <div key={c.name} className={c.className}>
                  {c.name}
                </div>
              ))}
              <div className="compare-column"></div>
            </div>
            <div className="results-list column">
              {rows.map((e) => (
                <div key={`${e.invocationId}/${e.actionDigestHash}/${e.startTimeUsec}`} className="row result-row">
                  <div className="digest-column">
                    <Link
                      className="digest-bubble"
                      href={getExecutionPath(e.invocationId, e.actionDigestHash)}
                      title={e.actionDigestHash}
                      style={{ "--digest-hue": format.colorHashHue(e.actionDigestHash) } as React.CSSProperties}>
                      {e.actionDigestHash.slice(0, DIGEST_PREFIX_LENGTH)}
                    </Link>
                  </div>
                  <div className="date-column">{START_TIME.formatter(+e.startTimeUsec)}</div>
                  {this.state.tableStats.columns.map((c) => (
                    <div key={c.name} className={c.className}>
                      {c.render(e, derivedStats)}
                    </div>
                  ))}
                  <div className="compare-column">
                    <ExecutionMenuButtonComponent actionDigest={e.actionDigestHash} invocationId={e.invocationId} />
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="table-summary">{this.tableRowSummary(rows.length, samples.length)}</div>
          {this.state.resultLimit < samples.length && (
            <div className="table-footer-controls">
              <FilledButton
                className="load-more-button"
                onClick={() => {
                  this.setState({ resultLimit: this.state.resultLimit + MORE_RESULTS_LIMIT });
                }}>
                <span>Show more</span>
              </FilledButton>
            </div>
          )}
        </div>
      </>
    );
  }

  render() {
    // TODO(jdhollen): Add some more data here--exit code stats, CI stats,
    // phase durations, etc.
    return (
      <div className="container">
        <div className="card">
          <div className="content">
            <div className="title target-data-card-title">
              <BarChart2 /> Execution history
            </div>
            <div className="details">
              <div className="single-action-charts">
                {this.renderSingleActionChart(
                  "CPU usage",
                  (v) => format.durationMillis(v / 1e6),
                  (es) => es.cpuNanos,
                  (e) => +e.cpuNanos
                )}
                {this.renderSingleActionChart(
                  "Wall time",
                  (v) => format.durationMillis(v / 1e3),
                  (es) => es.durationUsec,
                  (e) => +e.durationUsec
                )}
                {this.renderSingleActionChart(
                  "Input download",
                  (v) => format.bytes(v),
                  (es) => es.downloadedBytes,
                  (e) => +e.downloadedBytes
                )}
                {this.renderSingleActionChart(
                  "Output upload",
                  (v) => format.bytes(v),
                  (es) => es.uploadedBytes,
                  (e) => +e.uploadedBytes
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="card">
          <div className="content">
            <div className="title target-data-card-title">
              <List /> {this.isSampled() ? "Sampled executions" : "Executions"}
            </div>
            <div className="details">{this.renderExecutionTable()}</div>
          </div>
        </div>
      </div>
    );
  }
}
