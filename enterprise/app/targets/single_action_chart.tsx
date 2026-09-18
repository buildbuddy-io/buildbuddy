import React, { useEffect } from "react";
import { execution_stats } from "../../../proto/execution_stats_ts_proto";
import TrendsChartComponent, {
  ChartColor,
  ChartDataSeries,
  ClickCoordinateInfo,
  SeriesType,
} from "../trends/trends_chart";
import moment from "moment";
import { stats } from "../../../proto/stats_ts_proto";
import { computeTimeKeys } from "../trends/common";
import ActionCompareButtonComponent from "../../../app/invocation/action_compare_button";
import { Tooltip, TooltipProps } from "recharts";
import format from "../../../app/format/format";

interface Props {
  title: string;
  formatValue: (v: number) => string;
  getQuantiles: (tl: execution_stats.ExecutionTimelineSummary) => execution_stats.Quantile[];
  getScatterValue: (as: execution_stats.ExecutionTimelineEntry) => number;
  timeline: execution_stats.ExecutionTimeline;
  interval: stats.StatsInterval;
  domain: [Date, Date];
}

interface State {
  selectedDataPoint?: execution_stats.ExecutionTimelineEntry;
  selectedCoord: ClickCoordinateInfo;
}

interface TooltipContentProps {
  exec: execution_stats.ExecutionTimelineEntry;
  formatValue: (v: number) => string;
  getScatterValue: (as: execution_stats.ExecutionTimelineEntry) => number;
}

function TooltipContent({ exec, formatValue, getScatterValue }: TooltipContentProps) {
  return (
    <div className="trend-chart-hover">
      <div>{format.formatTimestampUsec(exec.startTimeUsec)}</div>
      <div>{formatValue(getScatterValue(exec))}</div>
      <ActionCompareButtonComponent actionDigest={exec.actionDigestHash} invocationId={exec.invocationId} />
    </div>
  );
}

function getQuantile(quantiles: execution_stats.Quantile[], target: number): number {
  return +(quantiles.find((v) => v.quantile === target)?.value ?? 0);
}

export default class SingleActionChartComponent extends React.Component<Props, State> {
  state: State = {
    selectedDataPoint: undefined,
    selectedCoord: { x: 0, y: 0, chartWidth: 0, chartHeight: 0 },
  };

  pickPosition() {
    let x = Math.min(this.state.selectedCoord.x, this.state.selectedCoord.chartWidth - 205);
    let y = this.state.selectedCoord.y;
    if (y + 100 > this.state.selectedCoord.chartHeight) {
      y = y - 105;
    }
    return { x, y };
  }

  private pickQuantiles() {
    return {
      lower: 10,
      middle: 50,
      upper: 90,
    };
  }

  render(): React.ReactNode {
    let { timeKeys, ticks } = computeTimeKeys(this.props.interval, this.props.domain);
    timeKeys = timeKeys.map((v) => v * 1000);
    ticks = ticks.map((v) => v * 1000);

    const lineData = new Map<number, number>();
    const areaData = new Map<number, [number, number]>();
    const scatterData = new Map<number, number>();

    const q = this.pickQuantiles();

    for (const e of this.props.timeline.aggregatedStats) {
      if (e.summary) {
        const sq = this.props.getQuantiles(e.summary);
        lineData.set(+e.bucketStartTimeUsec, getQuantile(sq, q.middle));
        areaData.set(+e.bucketStartTimeUsec, [getQuantile(sq, q.lower), getQuantile(sq, q.upper)]);
      }
    }

    for (const e of this.props.timeline.execution) {
      scatterData.set(+e.startTimeUsec, this.props.getScatterValue(e));
      timeKeys.push(+e.startTimeUsec);
    }
    timeKeys = timeKeys.sort();

    const series: ChartDataSeries[] = [];

    // TODO: This isn't a journal article and we value aesthetics a bit.  Extend
    // the line and area components out to the edge of whatever the minimum
    // observed scatter points are so that the chart looks nice.
    series.push({
      name: this.props.title + "line",
      type: SeriesType.LINE,
      extractValue: (startTimeUsec: number) => lineData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      color: ChartColor.BLUE,
    });

    if (areaData.size > 0) {
      series.push({
        name: this.props.title + "area",
        type: SeriesType.AREA,
        extractValue: (startTimeUsec: number) => areaData.get(startTimeUsec) ?? null,
        // TODO: Make this non-ugly (custom tooltip)
        formatHoverValue: (_: number) => "",
        color: ChartColor.BLUE,
      });
    }

    series.push({
      name: this.props.title + "scatter",
      type: SeriesType.SCATTER,
      extractValue: (startTimeUsec: number) => scatterData.get(startTimeUsec) ?? null,
      formatHoverValue: this.props.formatValue,
      onClick: (startTimeUsec, e, c) => {
        if (startTimeUsec === +(this.state.selectedDataPoint?.startTimeUsec ?? 0)) {
          this.setState({ selectedDataPoint: undefined });
          return;
        }
        console.log("Stoppin!");
        e.stopPropagation();
        console.log(scatterData.get(startTimeUsec) ?? null);
        const exec = this.props.timeline.execution.find((e) => +e.startTimeUsec === startTimeUsec);
        if (exec) {
          this.setState({ selectedDataPoint: exec, selectedCoord: c });
        }
      },
      color: ChartColor.BLUE,
    });

    const tooltipProps: TooltipProps | undefined = this.state.selectedDataPoint ? {} : undefined;

    console.log("rendering....");
    return (
      <TrendsChartComponent
        title={this.props.title}
        data={timeKeys}
        ticks={ticks}
        dataSeries={series}
        onClick={(_) => this.setState({ selectedDataPoint: undefined })}
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
        customTooltip={
          this.state.selectedDataPoint ? (
            <Tooltip
              active={Boolean(this.state.selectedDataPoint)}
              wrapperStyle={{ pointerEvents: "auto" }}
              content={
                <TooltipContent
                  exec={this.state.selectedDataPoint}
                  formatValue={this.props.formatValue}
                  getScatterValue={this.props.getScatterValue}
                />
              }
              cursor={false}
              position={this.pickPosition()}
            />
          ) : undefined
        }
      />
    );
  }
}
