import moment from "moment";
import React from "react";
import * as format from "../../../app/format/format";
import TrendsChartComponent, { ChartDataSeries, ChartYAxis } from "../trends/trends_chart";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import FilledButton from "../../../app/components/button/button";
import router from "../../../app/router/router";

export interface TimelineDataSeries {
  series: ChartDataSeries;
  timeline: execution_stats.ExecutionTimeline;
}

interface Props {
  title: string;
  target: string;
  timeKeys: number[];
  ticks: number[];
  formatValues: (datum: number) => string;
  series: TimelineDataSeries[];
  getP10: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getP50: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getP90: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getTotal?: (tl: execution_stats.ExecutionTimelineSummary) => number;
  colorPicker: (tl: execution_stats.ExecutionTimeline) => string;
}

interface State {
  legendPage: number;
  hoveredTimeline?: string;
}

// The number of actions shown in the table on a single page.
const LEGEND_PAGE_SIZE = 5;

export default class TargetChartComponent extends React.Component<Props, State> {
  state: State = {
    legendPage: 0,
    hoveredTimeline: undefined,
  };

  private renderResults() {
    let sortedTimelines = [...this.props.series].sort(
      (a, b) => this.props.getP50(b.timeline.summary!) - this.props.getP50(a.timeline.summary!)
    );
    const pageStart = this.state.legendPage * LEGEND_PAGE_SIZE;
    sortedTimelines = sortedTimelines.slice(pageStart, pageStart + LEGEND_PAGE_SIZE);

    const fv = this.props.formatValues;
    const rows = sortedTimelines.map((s) =>
      s.timeline.summary ? (
        <div
          className="row result-row clickable"
          onMouseEnter={() => this.setState({ hoveredTimeline: s.series.name })}
          onMouseLeave={() => this.setState({ hoveredTimeline: undefined })}
          onClick={() => router.navigateToSingleTarget(this.props.target, s.timeline.outputPath)}>
          <div className="legend-column" title={s.timeline.outputPath}>
            <div className="legend-square" style={{ backgroundColor: this.props.colorPicker(s.timeline) }} />
          </div>
          <div className="action-column" title={s.timeline.outputPath}>
            {s.timeline.outputPath}
          </div>
          <div className="stat-column">{fv(this.props.getP10(s.timeline.summary))}</div>
          <div className="stat-column">{fv(this.props.getP50(s.timeline.summary))}</div>
          <div className="stat-column">{fv(this.props.getP90(s.timeline.summary))}</div>
          {Boolean(this.props.getTotal) && (
            <div className="stat-column">{fv(this.props.getTotal!(s.timeline.summary))}</div>
          )}
        </div>
      ) : (
        <></>
      )
    );
    if (this.state.legendPage > 0) {
      for (let i = rows.length; i < LEGEND_PAGE_SIZE; i++) {
        rows.push(<div className="row result-row empty-row"></div>);
      }
    }
    return rows;
  }

  private hasPreviousPage() {
    return this.state.legendPage > 0;
  }

  private hasNextPage() {
    return (this.state.legendPage + 1) * LEGEND_PAGE_SIZE < this.props.series.length;
  }

  private renderTimelineLegendTable() {
    return (
      <div className="chart-table-container">
        <div className="results-table">
          <div className="row column-headers">
            <div className="legend-column"></div>
            <div className="action-column">Action</div>
            <div className="stat-column">p10</div>
            <div className="stat-column">p50</div>
            <div className="stat-column">p90</div>
            {Boolean(this.props.getTotal) && <div className="stat-column">Total</div>}
          </div>
          <div className="results-list column">{this.renderResults()}</div>
        </div>
        <div className="table-footer-controls">
          <div>
            Page {this.state.legendPage + 1} of {Math.ceil(this.props.series.length / LEGEND_PAGE_SIZE)}
          </div>
          <FilledButton
            className="load-more-button"
            disabled={!this.hasPreviousPage()}
            onClick={() => {
              this.setState({ legendPage: Math.max(0, this.state.legendPage - 1) });
            }}>
            <span>Previous page</span>
          </FilledButton>
          <FilledButton
            className="load-more-button"
            disabled={!this.hasNextPage()}
            onClick={() => {
              this.setState({
                legendPage: Math.min(
                  Math.ceil(this.props.series.length / LEGEND_PAGE_SIZE) - 1,
                  this.state.legendPage + 1
                ),
              });
            }}>
            <span>Next page</span>
          </FilledButton>
        </div>
      </div>
    );
  }

  render(): React.ReactNode {
    return (
      <>
        <TrendsChartComponent
          title={this.props.title}
          data={this.props.timeKeys}
          ticks={this.props.ticks}
          dataSeries={this.props.series.map((s) => s.series)}
          highlightSeries={this.state.hoveredTimeline}
          primaryYAxis={{
            formatTickValue: this.props.formatValues,
            allowDecimals: false,
          }}
          formatXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("MMM D, h:mm a")}
          formatHoverXAxisLabel={(startTimeUsec) =>
            moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")
          }
          hideLegend={true}
          standaloneChart={true}
        />
        {this.renderTimelineLegendTable()}
      </>
    );
  }
}
