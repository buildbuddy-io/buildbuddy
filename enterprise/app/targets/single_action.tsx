import { BarChart2, List, SortAsc, SortDesc } from "lucide-react";
import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Link from "../../../app/components/link/link";
import Select, { Option } from "../../../app/components/select/select";
import format from "../../../app/format/format";
import ActionCompareButtonComponent from "../../../app/invocation/action_compare_button";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import SingleActionChartComponent from "./single_action_chart";

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

interface StatSet {
  name: string;
  stats: ExecStat[];
}

const START_TIME: ExecStat = {
  name: "Start time",
  extractor: (e) => +e.startTimeUsec,
  formatter: (v) => format.formatTimestampUsec(v),
};

// Groups of columns that can be shown in the sampled executions table.
const STAT_SETS: StatSet[] = [
  {
    name: "Timing",
    stats: [
      { name: "Wall time", extractor: (e) => +e.durationUsec, formatter: format.durationUsec },
      { name: "Queue", extractor: (e) => +e.workerQueueUsec, formatter: format.durationUsec },
      { name: "Input download", extractor: (e) => +e.inputDownloadUsec, formatter: format.durationUsec },
      { name: "Execution", extractor: (e) => +e.executionUsec, formatter: format.durationUsec },
      { name: "Output upload", extractor: (e) => +e.outputUploadUsec, formatter: format.durationUsec },
    ],
  },
  {
    name: "Resources",
    stats: [
      { name: "CPU time", extractor: (e) => +e.cpuNanos, formatter: (v) => format.durationMillis(v / 1e6) },
      { name: "Peak memory", extractor: (e) => +e.peakMemoryBytes, formatter: format.bytes },
      { name: "Downloaded", extractor: (e) => +e.downloadedBytes, formatter: format.bytes },
      { name: "Uploaded", extractor: (e) => +e.uploadedBytes, formatter: format.bytes },
    ],
  },
];

interface State {
  resultLimit: number;
  orderBy: ExecStat;
  ascending: boolean;
  tableStats: StatSet;
}

// The number of sampled executions shown in the table before "Show more".
const MORE_RESULTS_LIMIT = 20;

// The number of leading characters of an action digest shown in the table.
const DIGEST_PREFIX_LENGTH = 8;

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

  private renderExecutionTable() {
    const samples = this.props.timeline.executionSamples;
    const rows = [...samples]
      .sort(
        (a, b) => (this.state.ascending ? 1 : -1) * (this.state.orderBy.extractor(a) - this.state.orderBy.extractor(b))
      )
      .slice(0, this.state.resultLimit);

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
          <div className="results-table">
            <div className="row column-headers">
              <div className="digest-column">Digest</div>
              <div className="date-column">{START_TIME.name}</div>
              {this.state.tableStats.stats.map((s) => (
                <div key={s.name} className="stat-column">
                  {s.name}
                </div>
              ))}
              <div className="compare-column"></div>
            </div>
            <div className="results-list column">
              {rows.map((e) => (
                <div key={`${e.invocationId}/${e.actionDigestHash}/${e.startTimeUsec}`} className="row result-row">
                  <div className="digest-column" title={e.actionDigestHash}>
                    <Link href={`/invocation/${e.invocationId}?actionDigest=${e.actionDigestHash}#action`}>
                      {e.actionDigestHash.slice(0, DIGEST_PREFIX_LENGTH)}
                    </Link>
                  </div>
                  <div className="date-column">{START_TIME.formatter(+e.startTimeUsec)}</div>
                  {this.state.tableStats.stats.map((s) => (
                    <div key={s.name} className="stat-column">
                      {s.formatter(s.extractor(e))}
                    </div>
                  ))}
                  <div className="compare-column">
                    <ActionCompareButtonComponent
                      actionDigest={e.actionDigestHash}
                      invocationId={e.invocationId}
                      mini={true}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="table-summary">
            Showing {format.formatWithCommas(rows.length)} of {format.formatWithCommas(samples.length)} sampled{" "}
            {samples.length === 1 ? "execution" : "executions"}
          </div>
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
    // TODO: Exit code chart

    // TODO: Phase duration charts.

    return (
      <div className="container">
        <div className="card">
          <div className="content">
            <div className="title">
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
            <div className="title">
              <List /> Sampled executions
            </div>
            <div className="details">{this.renderExecutionTable()}</div>
          </div>
        </div>
      </div>
    );
  }
}
