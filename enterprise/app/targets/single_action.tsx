import React from "react";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import format from "../../../app/format/format";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Select, { Option } from "../../../app/components/select/select";
import { SortAsc, SortDesc } from "lucide-react";
import ActionCompareButtonComponent from "../../../app/invocation/action_compare_button";
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

const STAT_SETS: StatSet[] = [
  {
    name: "Duration",
    stats: [
      { name: "Duration", extractor: (e) => +e.durationUsec, formatter: format.durationUsec },
      { name: "Duration * 2", extractor: (e) => +e.durationUsec * 2, formatter: format.durationUsec },
    ],
  },
];

interface State {
  resultLimit: number;
  orderBy: ExecStat;
  ascending: boolean;
  tableStats: StatSet;
}

// The number of actions shown in the table on a single page.
const MORE_RESULTS_LIMIT = 20;

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
    p10: (as: execution_stats.ExecutionTimelineSummary) => number,
    p50: (as: execution_stats.ExecutionTimelineSummary) => number,
    p90: (as: execution_stats.ExecutionTimelineSummary) => number,
    scat: (as: execution_stats.ExecutionTimelineEntry) => number
  ): React.ReactNode {
    return (
      <SingleActionChartComponent
        domain={this.props.domain}
        title={title}
        formatValue={fmt}
        getP10={p10}
        getP50={p50}
        getP90={p90}
        getScatterValue={scat}
        interval={this.props.interval}
        timeline={this.props.timeline}></SingleActionChartComponent>
    );
  }

  private findOrderBy(name: string) {
    return this.state.tableStats.stats.find((s) => s.name === name) ?? START_TIME;
  }

  private findStatSet(name: string) {
    return STAT_SETS.find((s) => s.name === name) ?? STAT_SETS[0];
  }

  private onOrderByChange(event: React.ChangeEvent<HTMLSelectElement>) {
    console.log(this.findOrderBy(event.target.value));
    this.setState({ orderBy: this.findOrderBy(event.target.value) });
  }

  private onDescendingChange() {
    this.setState({ ascending: !this.state.ascending });
  }

  private onStatSetChange(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ tableStats: this.findStatSet(event.target.value) });
  }

  private renderExecutionTable() {
    const rows = [...this.props.timeline.execution]
      .sort(
        (a, b) => (this.state.ascending ? 1 : -1) * (this.state.orderBy.extractor(a) - this.state.orderBy.extractor(b))
      )
      .slice(0, this.state.resultLimit);

    console.log("new rows");
    console.log(this.state.orderBy);
    console.log(rows);

    const sortOptions = [START_TIME, ...this.state.tableStats.stats];

    return (
      <>
        <div className="controls row">
          <label>Sort by</label>
          <Select value={this.state.orderBy.name} onChange={this.onOrderByChange.bind(this)}>
            {sortOptions.map((o) => {
              return <Option value={o.name}>{o.name}</Option>;
            })}
          </Select>{" "}
          <OutlinedButton className="icon-button" onClick={this.onDescendingChange.bind(this)}>
            {this.state.ascending ? <SortAsc /> : <SortDesc />}
          </OutlinedButton>
          <div className="separator" />
          <label>Show stats</label>
          <Select value={this.state.tableStats.name} onChange={this.onStatSetChange.bind(this)}>
            {STAT_SETS.map((s) => (
              <Option value={s.name}>{s.name}</Option>
            ))}
          </Select>
        </div>
        <div className="chart-table-container">
          <div className="results-table">
            <div className="row column-headers">
              <div className="digest-column">Digest</div>
              <div className="date-column">{START_TIME.name}</div>
              {this.state.tableStats.stats.map((s) => (
                <div className="stat-column">{s.name}</div>
              ))}
              <div className="compare-column"></div>
            </div>
            <div className="results-list column">
              {rows.map((e) => {
                return (
                  <div className="row result-row clickable">
                    <div className="digest-column">TODO</div>
                    <div className="date-column">{START_TIME.formatter(+e.startTimeUsec)}</div>
                    {this.state.tableStats.stats.map((s) => (
                      <div className="stat-column">{s.formatter(s.extractor(e))}</div>
                    ))}
                    <div className="compare-column">
                      <ActionCompareButtonComponent
                        actionDigest={e.actionDigestHash}
                        invocationId={e.invocationId}
                        mini={true}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
          {this.state.resultLimit < this.props.timeline.execution.length && (
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
    // OK, here we go!

    // TODO: Exit code chart

    // TODO: Phase duration charts.

    // TODO: Stat charts.

    // TODO: Table of sampled / all runs with compare buttons.

    return (
      <>
        {this.renderSingleActionChart(
          "CPU usage",
          (v) => format.durationMillis(v / 1e6),
          (es) => +es.cpuNanosP10,
          (es) => +es.cpuNanosP50,
          (es) => +es.cpuNanosP90,
          (e) => +e.cpuNanos
        )}
        {this.renderSingleActionChart(
          "Wall time",
          (v) => format.durationMillis(v / 1e3),
          (es) => +es.durationUsecP10,
          (es) => +es.durationUsecP50,
          (es) => +es.durationUsecP90,
          (e) => +e.durationUsec
        )}
        {this.renderSingleActionChart(
          "Input download (bytes)",
          (v) => format.bytes(v),
          (es) => +es.downloadedBytesP10,
          (es) => +es.downloadedBytesP50,
          (es) => +es.downloadedBytesP90,
          (e) => +e.downloadedBytes
        )}
        {this.renderSingleActionChart(
          "Output upload (bytes)",
          (v) => format.bytes(v),
          (es) => +es.uploadedBytesP10,
          (es) => +es.uploadedBytesP50,
          (es) => +es.uploadedBytesP90,
          (e) => +e.uploadedBytes
        )}
        {this.renderExecutionTable()}
      </>
    );
  }
}
