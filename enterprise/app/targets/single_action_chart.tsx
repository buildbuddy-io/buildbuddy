import React from "react";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import TrendsChartComponent, { ChartColor, SeriesType } from "../trends/trends_chart";
import moment from "moment";
import { stats } from "../../../proto/stats_ts_proto";
import { computeTimeKeys } from "../trends/common";

interface Props {
  title: string;
  formatValue: (v: number) => string;
  getP10: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getP50: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getP90: (tl: execution_stats.ExecutionTimelineSummary) => number;
  getScatterValue: (as: execution_stats.ExecutionTimelineEntry) => number;
  timeline: execution_stats.ExecutionTimeline;
  interval: stats.StatsInterval;
  domain: [Date, Date];
}

interface State {
  selectedDataPoint?: number;
}

export default class SingleActionChartComponent extends React.Component<Props, State> {
  state: State = {
    selectedDataPoint: undefined,
  };

  render(): React.ReactNode {
    let { timeKeys, ticks } = computeTimeKeys(this.props.interval, this.props.domain);
    timeKeys = timeKeys.map((v) => v * 1000);
    ticks = ticks.map((v) => v * 1000);

    const lineData = new Map<number, number>();
    const areaData = new Map<number, [number, number]>();
    const scatterData = new Map<number, number>();

    for (const e of this.props.timeline.aggregatedStats) {
      if (e.summary) {
        lineData.set(+e.bucketStartTimeUsec, this.props.getP50(e.summary));
        areaData.set(+e.bucketStartTimeUsec, [this.props.getP10(e.summary), this.props.getP90(e.summary)]);
      }
    }

    for (const e of this.props.timeline.execution) {
      scatterData.set(+e.startTimeUsec, this.props.getScatterValue(e));
      timeKeys.push(+e.startTimeUsec);
    }
    timeKeys = timeKeys.sort();

    // TODO: This isn't a journal article and we value aesthetics a bit.  Extend
    // the line and area components out to the edge of whatever the minimum
    // observed scatter points are so that the chart looks nice.
    const lineSeries = {
      name: this.props.title + "line",
      type: SeriesType.LINE,
      extractValue: (startTimeUsec: number) => lineData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      color: ChartColor.BLUE,
    };

    const areaSeries = {
      name: this.props.title + "area",
      type: SeriesType.AREA,
      extractValue: (startTimeUsec: number) => areaData.get(startTimeUsec) ?? null,
      // TODO: Make this non-ugly (custom tooltip)
      formatHoverValue: (_: number) => "",
      color: ChartColor.BLUE,
    };

    const scatterSeries = {
      name: this.props.title + "scatter",
      type: SeriesType.SCATTER,
      extractValue: (startTimeUsec: number) => scatterData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      onClick: (startTimeUsec: number) => {
        console.log(scatterData.get(startTimeUsec) ?? null);
        this.setState({ selectedDataPoint: startTimeUsec });
      },
      color: ChartColor.BLUE,
    };

    return (
      <TrendsChartComponent
        title={this.props.title}
        data={timeKeys}
        ticks={ticks}
        dataSeries={[scatterSeries, lineSeries, areaSeries]}
        primaryYAxis={{
          formatTickValue: this.props.formatValue,
          allowDecimals: false,
        }}
        formatXAxisLabel={(startTimeUsec) =>
          moment(startTimeUsec / 1000).format(
            this.props.interval.type === stats.IntervalType.INTERVAL_TYPE_DAY ? "MMM D" : "MMM D, h:mm a"
          )
        }
        formatHoverXAxisLabel={(startTimeUsec) => moment(startTimeUsec / 1000).format("dddd, MMMM Do YYYY, h:mm:ss a")}
        hideLegend={true}
      />
    );
  }
}
