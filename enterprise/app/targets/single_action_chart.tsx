import moment from "moment";
import React from "react";
import format from "../../../app/format/format";
import ActionCompareButtonComponent from "../../../app/invocation/action_compare_button";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import { stats } from "../../../proto/stats_ts_proto";
import {
  computeTimeKeys,
  getQuantile,
  intervalEndUsec,
  longFormatTsForInterval,
  shortFormatTsForAxis,
} from "../trends/common";
import TrendsChartComponent, {
  ChartColor,
  ChartDataSeries,
  NearestScatterPoint,
  SeriesType,
} from "../trends/trends_chart";

interface Props {
  title: string;
  formatValue: (v: number) => string;
  getQuantiles: (tl: execution_stats.ExecutionTimelineSummary) => execution_stats.Quantile[];
  getScatterValue: (as: execution_stats.ExecutionTimelineEntry) => number;
  timeline: execution_stats.ExecutionTimeline;
  interval: stats.StatsInterval;
  domain: [Date, Date];
}

/**
 * The user's mouse pointer must be at least this close to a scatter point
 * before we'll consider showing it as a tooltip.
 */
const SCATTER_TOOLTIP_RADIUS_PX = 15;

const LOWER_QUANTILE = 10;
const MIDDLE_QUANTILE = 50;
const UPPER_QUANTILE = 90;

/**
 * Plots a single metric of one action over time, with the median as a solid line,
 * the p10-p90 range as a shaded band, and a collection of representative executions
 * (sampled or otherwise) shown in a scatter plot.
 */
export default class SingleActionChartComponent extends React.Component<Props> {
  /**
   * Finds the time bucket that contains a given timestamp.  When we are showing
   * a tooltip for a single scatter plot point, we use this to find and show the
   * corresponding aggregate statistics of the bucket that the point belongs to.
   */
  private findBucket(timeUsec: number | undefined): execution_stats.AggregatedExecutionTimelineEntry | undefined {
    if (timeUsec === undefined) {
      return undefined;
    }
    for (const entry of this.props.timeline.aggregatedStats) {
      const start = +entry.bucketStartTimeUsec;
      const end = intervalEndUsec(this.props.interval, start);
      if (start <= timeUsec && end > timeUsec) {
        return entry;
      }
    }
    return undefined;
  }

  private formatBucketRange(bucket: execution_stats.AggregatedExecutionTimelineEntry): string {
    const start = moment(+bucket.bucketStartTimeUsec / 1000);
    if (this.props.interval.type === stats.IntervalType.INTERVAL_TYPE_DAY) {
      return start.format("dddd, MMMM Do");
    }
    const end = moment(intervalEndUsec(this.props.interval, +bucket.bucketStartTimeUsec) / 1000);
    const endFormat = start.isSame(end, "day") ? "h:mm a" : "MMM D, h:mm a";
    return `${start.format("MMM D, h:mm a")} – ${end.format(endFormat)}`;
  }

  private renderTooltip = (
    datum: number | undefined,
    point: NearestScatterPoint | undefined,
    pinned: boolean
  ): JSX.Element | null => {
    const exec = point ? this.props.timeline.executionSamples.find((e) => +e.startTimeUsec === point.datum) : undefined;
    // Show the bucket that the hovered execution falls in, or failing that,
    // the bucket under the mouse.
    const bucket = this.findBucket(exec ? +exec.startTimeUsec : datum);
    if (!exec && !bucket) {
      return null;
    }
    const quantiles = bucket?.summary ? this.props.getQuantiles(bucket.summary) : [];
    return (
      <div className="trend-chart-hover single-action-chart-tooltip">
        {exec && (
          <div className="tooltip-section">
            <div className="trend-chart-hover-label">{format.formatTimestampUsec(exec.startTimeUsec)}</div>
            <div className="tooltip-row">
              <span>{this.props.title}</span>
              <span className="tooltip-value">{this.props.formatValue(this.props.getScatterValue(exec))}</span>
            </div>
          </div>
        )}
        {bucket && (
          <div className="tooltip-section">
            <div className="trend-chart-hover-label">{this.formatBucketRange(bucket)}</div>
            <div className="tooltip-row">
              <span>p{LOWER_QUANTILE}</span>
              <span className="tooltip-value">{this.props.formatValue(getQuantile(quantiles, LOWER_QUANTILE))}</span>
            </div>
            <div className="tooltip-row">
              <span>Median</span>
              <span className="tooltip-value">{this.props.formatValue(getQuantile(quantiles, MIDDLE_QUANTILE))}</span>
            </div>
            <div className="tooltip-row">
              <span>p{UPPER_QUANTILE}</span>
              <span className="tooltip-value">{this.props.formatValue(getQuantile(quantiles, UPPER_QUANTILE))}</span>
            </div>
          </div>
        )}
        {exec && (
          <div className="tooltip-section">
            {pinned ? (
              <ActionCompareButtonComponent actionDigest={exec.actionDigestHash} invocationId={exec.invocationId} />
            ) : (
              <div className="tooltip-hint">Click to pin</div>
            )}
          </div>
        )}
      </div>
    );
  };

  render(): React.ReactNode {
    const { timeKeys: bucketKeys, ticks: tickKeys } = computeTimeKeys(this.props.interval, this.props.domain);
    const ticks = tickKeys.map((v) => v * 1000);

    const lineData = new Map<number, number>();
    const areaData = new Map<number, [number, number]>();
    for (const entry of this.props.timeline.aggregatedStats) {
      if (!entry.summary) {
        continue;
      }
      const quantiles = this.props.getQuantiles(entry.summary);
      lineData.set(+entry.bucketStartTimeUsec, getQuantile(quantiles, MIDDLE_QUANTILE));
      areaData.set(+entry.bucketStartTimeUsec, [
        getQuantile(quantiles, LOWER_QUANTILE),
        getQuantile(quantiles, UPPER_QUANTILE),
      ]);
    }

    const scatterData = new Map<number, number>();
    // TODO(jdhollen): this obliterates points with matching start times, and
    // relies on iteration order to make sure that we pick the same point for
    // the scatter and the tooltip. Recharts would accept multiple data points,
    // so maybe we can do something better here some day.
    for (const e of this.props.timeline.executionSamples) {
      if (!scatterData.has(+e.startTimeUsec)) {
        scatterData.set(+e.startTimeUsec, this.props.getScatterValue(e));
      }
    }

    // Every series reads its values from the chart's data entries, so there
    // needs to be an entry for every bucket start as well as for every sampled
    // execution.  Empty buckets get an entry too, so that hovering over one
    // doesn't snap to a neighboring bucket.
    const timeKeys = Array.from(new Set([...bucketKeys.map((v) => v * 1000), ...scatterData.keys()])).sort(
      (a, b) => a - b
    );

    // Space the x axis by time rather than by entry, so that buckets without
    // executions still take up room and sampled executions land at their
    // actual start times.  The axis runs from the start of the first bucket
    // to the end of the queried range.
    const xAxisDomain: [number, number] = [
      (bucketKeys[0] ?? this.props.domain[0].getTime()) * 1000,
      this.props.domain[1].getTime() * 1000,
    ];

    const series: ChartDataSeries[] = [];

    series.push({
      name: this.props.title + "line",
      type: SeriesType.LINE,
      extractValue: (startTimeUsec: number) => lineData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      color: ChartColor.BLUE,
      connectNulls: true,
      hideActiveDot: true,
    });

    if (areaData.size > 0) {
      series.push({
        name: this.props.title + "area",
        type: SeriesType.AREA,
        extractValue: (startTimeUsec: number) => areaData.get(startTimeUsec) ?? null,
        color: ChartColor.BLUE,
        connectNulls: true,
        hideActiveDot: true,
      });
    }

    series.push({
      name: this.props.title + "scatter",
      type: SeriesType.SCATTER,
      extractValue: (startTimeUsec: number) => scatterData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      color: ChartColor.BLUE,
    });

    return (
      <TrendsChartComponent
        title={this.props.title}
        data={timeKeys}
        ticks={ticks}
        xAxisDomain={xAxisDomain}
        dataSeries={series}
        primaryYAxis={{
          formatTickValue: this.props.formatValue,
          allowDecimals: false,
        }}
        formatXAxisLabel={(startTimeUsec) => shortFormatTsForAxis(startTimeUsec / 1000)}
        formatHoverXAxisLabel={(startTimeUsec) =>
          longFormatTsForInterval(this.props.interval.type, startTimeUsec / 1000)
        }
        hideLegend={true}
        standaloneChart={true}
        pointTooltip={{
          maxDistancePx: SCATTER_TOOLTIP_RADIUS_PX,
          render: this.renderTooltip,
          pinnable: true,
        }}
      />
    );
  }
}
