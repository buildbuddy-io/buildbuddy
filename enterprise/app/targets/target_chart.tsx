import { ChevronLeft, ChevronRight } from "lucide-react";
import moment from "moment";
import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import router from "../../../app/router/router";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import TrendsChartComponent, { ChartDataSeries } from "../trends/trends_chart";

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
  getQuantiles: (tl: execution_stats.ExecutionTimelineSummary) => execution_stats.Quantile[];
  getTotal?: (tl: execution_stats.ExecutionTimelineSummary) => number;
  colorPicker: (tl: execution_stats.ExecutionTimeline) => string;
}

interface State {
  legendPage: number;
  hoveredTimeline?: string;
}

// The number of actions shown in the legend table on a single page.  The legend
// is paginated so that the user can keep the chart on the screen while going
// through the legend.
const LEGEND_PAGE_SIZE = 5;

export default class TargetChartComponent extends React.Component<Props, State> {
  state: State = {
    legendPage: 0,
    hoveredTimeline: undefined,
  };

  private getPageCount(): number {
    return Math.max(1, Math.ceil(this.props.series.length / LEGEND_PAGE_SIZE));
  }

  private getPage(): number {
    return Math.max(0, Math.min(this.state.legendPage, this.getPageCount() - 1));
  }

  private renderResults() {
    const p10 = (s: execution_stats.ExecutionTimelineSummary) =>
      +(this.props.getQuantiles(s).find((q) => q.quantile === 10)?.value ?? 0);
    const p50 = (s: execution_stats.ExecutionTimelineSummary) =>
      +(this.props.getQuantiles(s).find((q) => q.quantile === 50)?.value ?? 0);
    const p90 = (s: execution_stats.ExecutionTimelineSummary) =>
      +(this.props.getQuantiles(s).find((q) => q.quantile === 90)?.value ?? 0);
    let sortedTimelines = [...this.props.series].sort((a, b) => p50(b.timeline.summary!) - p50(a.timeline.summary!));
    const page = this.getPage();
    const pageStart = page * LEGEND_PAGE_SIZE;
    sortedTimelines = sortedTimelines.slice(pageStart, pageStart + LEGEND_PAGE_SIZE);

    const fv = this.props.formatValues;
    const rows = sortedTimelines.map((s) =>
      s.timeline.summary ? (
        <div
          key={s.series.name}
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
          <div className="stat-column">{fv(p10(s.timeline.summary))}</div>
          <div className="stat-column">{fv(p50(s.timeline.summary))}</div>
          <div className="stat-column">{fv(p90(s.timeline.summary))}</div>
          {Boolean(this.props.getTotal) && (
            <div className="stat-column">{fv(this.props.getTotal!(s.timeline.summary))}</div>
          )}
        </div>
      ) : (
        <React.Fragment key={s.series.name}></React.Fragment>
      )
    );
    // Pad short trailing pages so the table (and the chart above it) doesn't
    // jump around while paging.
    if (page > 0) {
      for (let i = rows.length; i < LEGEND_PAGE_SIZE; i++) {
        rows.push(<div key={`empty-${i}`} className="row result-row empty-row"></div>);
      }
    }
    return rows;
  }

  private renderPager() {
    const total = this.props.series.length;
    if (total <= LEGEND_PAGE_SIZE) {
      return null;
    }
    const page = this.getPage();
    const pageStart = page * LEGEND_PAGE_SIZE;
    const pageEnd = Math.min(pageStart + LEGEND_PAGE_SIZE, total);
    return (
      <div className="table-pager">
        <span className="table-pager-summary">
          {pageStart + 1}&ndash;{pageEnd} of {total}
        </span>
        <div className="table-pager-buttons">
          <OutlinedButton
            className="icon-button"
            title="Previous page"
            aria-label="Previous page"
            disabled={page === 0}
            onClick={() => this.setState({ legendPage: page - 1 })}>
            <ChevronLeft />
          </OutlinedButton>
          <OutlinedButton
            className="icon-button"
            title="Next page"
            aria-label="Next page"
            disabled={page >= this.getPageCount() - 1}
            onClick={() => this.setState({ legendPage: page + 1 })}>
            <ChevronRight />
          </OutlinedButton>
        </div>
      </div>
    );
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
        {this.renderPager()}
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
          tooltipEntryLimit={3}
        />
        {this.renderTimelineLegendTable()}
      </>
    );
  }
}
