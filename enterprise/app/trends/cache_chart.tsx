import React from "react";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  LegendPayload,
  Line,
  MouseHandlerDataParam,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  TooltipContentProps,
  XAxis,
  YAxis,
} from "recharts";
import * as format from "../../../app/format/format";
import { getHiddenSeriesAfterLegendClick } from "./chart_series";

export interface CacheChartProps {
  title: string;
  id?: string;
  data: number[];
  ticks: number[];
  secondaryBarName: string;
  extractLabel: (datum: number) => string;
  formatHoverLabel: (datum: number) => string;
  extractHits: (datum: number) => number;
  totalHits: number;
  extractSecondary: (datum: number) => number;
  totalSecondary: number;
  totalHitPercentage: number;
  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface CacheChartDataSeries {
  name: string;
  type: "bar" | "line";
  yAxisId: "hits" | "percent";
  dataKey: (datum: number) => number;
  color: string;
  formatHoverValue: (value: number) => string;
}

interface State {
  refAreaLeft?: string | number;
  refAreaRight?: string | number;
  hiddenSeries: ReadonlySet<number>;
}

// Recharts injects `active` and `payload` into the custom tooltip element; the
// rest are our own props. The prop is named `formatLabel` rather than
// `labelFormatter` so it can't collide with the Tooltip prop of that name.
interface CacheChartTooltipProps extends Partial<Pick<TooltipContentProps<any, any>, "active" | "payload">> {
  formatLabel: (datum: number) => string;
  shouldRender: () => boolean;
  dataSeries: CacheChartDataSeries[];
  hiddenSeries: ReadonlySet<number>;
}

const CacheChartTooltip = ({
  active,
  payload,
  formatLabel,
  shouldRender,
  dataSeries,
  hiddenSeries,
}: CacheChartTooltipProps) => {
  if (!active || !payload || payload.length < 1 || !shouldRender()) {
    return null;
  }
  let data = payload[0].payload;
  return (
    <div className="trend-chart-hover">
      <div className="trend-chart-hover-label">{formatLabel(data)}</div>
      <div className="trend-chart-hover-value">
        {dataSeries.map(
          (series, index) =>
            !hiddenSeries.has(index) && <div key={series.name}>{series.formatHoverValue(series.dataKey(data))}</div>
        )}
      </div>
    </div>
  );
};

export default class CacheChartComponent extends React.Component<CacheChartProps, State> {
  state: State = { hiddenSeries: new Set() };

  getDataSeries(): CacheChartDataSeries[] {
    return [
      {
        name: `hits (${format.count(this.props.totalHits)})`,
        type: "bar",
        yAxisId: "hits",
        dataKey: (datum) => this.props.extractHits(datum),
        color: "#8BC34A",
        formatHoverValue: (value) => `${value || 0} hits`,
      },
      {
        name: `${this.props.secondaryBarName} (${format.count(this.props.totalSecondary)})`,
        type: "bar",
        yAxisId: "hits",
        dataKey: (datum) => this.props.extractSecondary(datum),
        color: "#f44336",
        formatHoverValue: (value) => `${value || 0} ${this.props.secondaryBarName}`,
      },
      {
        name: `hit percentage (${format.percent(this.props.totalHitPercentage)}%)`,
        type: "line",
        yAxisId: "percent",
        dataKey: (datum) =>
          (100 * this.props.extractHits(datum)) / (this.props.extractHits(datum) + this.props.extractSecondary(datum)),
        color: "#03A9F4",
        formatHoverValue: (value) => `${(value || 0).toFixed(2)}% hit percentage`,
      },
    ];
  }

  onLegendClick(payload: LegendPayload, seriesIndex: number, event: React.MouseEvent) {
    event.stopPropagation();
    const name = ((payload.payload ?? null) as CacheChartDataSeries | null)?.name;
    const legendIndex = this.getDataSeries().findIndex((s) => s.name === name);
    if (legendIndex >= 0) {
      this.setState((state) => ({
        hiddenSeries: getHiddenSeriesAfterLegendClick(
          state.hiddenSeries,
          legendIndex,
          this.getDataSeries().length,
          event.ctrlKey || event.metaKey || event.shiftKey
        ),
      }));
    }
  }

  onMouseDown(e: MouseHandlerDataParam) {
    if (!this.props.onZoomSelection || !e) {
      this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
      return;
    }
    this.setState({ refAreaLeft: e.activeLabel, refAreaRight: e.activeLabel });
  }

  onMouseMove(e: MouseHandlerDataParam) {
    if (!this.props.onZoomSelection || !e) {
      this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
      return;
    }
    if (!this.state.refAreaLeft) {
      return;
    }
    this.setState({ refAreaRight: e.activeLabel });
  }

  onMouseUp(e: MouseHandlerDataParam) {
    if (!this.props.onZoomSelection || !e) {
      this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
      return;
    }
    const finalRightValue = e.activeLabel;
    if (this.state.refAreaLeft && finalRightValue) {
      let v1 = Number(this.state.refAreaLeft);
      let v2 = Number(finalRightValue);
      if (v1 > v2) {
        // Aaahh!!! Real Javascript
        [v1, v2] = [v2, v1];
      }
      this.props.onZoomSelection(v1, v2);
    }
    this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
  }

  shouldRenderTooltip(): boolean {
    return !Boolean(this.state.refAreaLeft);
  }

  render() {
    const dataSeries = this.getDataSeries();

    return (
      <div id={this.props.id} className={`trend-chart ${this.props.onZoomSelection ? "zoomable" : ""}`}>
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart
            accessibilityLayer={false}
            data={this.props.data}
            onMouseDown={this.props.onZoomSelection && this.onMouseDown.bind(this)}
            onMouseMove={this.props.onZoomSelection && this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" yAxisId="hits" />
            <Legend onClick={this.onLegendClick.bind(this)} />
            <XAxis dataKey={(v) => v} tickFormatter={this.props.extractLabel} ticks={this.props.ticks} />
            <YAxis yAxisId="hits" tickFormatter={format.count} allowDecimals={false} width={84} />
            <YAxis
              domain={[0, 100]}
              yAxisId="percent"
              orientation="right"
              width={84}
              tickFormatter={(value: number) => `${value}%`}
            />
            <Tooltip
              content={
                <CacheChartTooltip
                  formatLabel={this.props.formatHoverLabel}
                  shouldRender={() => this.shouldRenderTooltip()}
                  dataSeries={dataSeries}
                  hiddenSeries={this.state.hiddenSeries}
                />
              }
            />
            {dataSeries.map((series, index) =>
              series.type === "bar" ? (
                <Bar
                  key={series.name}
                  yAxisId={series.yAxisId}
                  name={series.name}
                  dataKey={series.dataKey}
                  fill={series.color}
                  hide={this.state.hiddenSeries.has(index)}
                  isAnimationActive={false}
                />
              ) : (
                <Line
                  key={series.name}
                  yAxisId={series.yAxisId}
                  name={series.name}
                  dataKey={series.dataKey}
                  stroke={series.color}
                  dot={false}
                  hide={this.state.hiddenSeries.has(index)}
                  isAnimationActive={false}
                />
              )
            )}
            {this.state.refAreaLeft && this.state.refAreaRight ? (
              <ReferenceArea
                yAxisId="percent"
                ifOverflow="visible"
                x1={Math.min(+this.state.refAreaLeft, +this.state.refAreaRight)}
                x2={Math.max(+this.state.refAreaLeft, +this.state.refAreaRight)}
                strokeOpacity={0.3}
              />
            ) : null}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
