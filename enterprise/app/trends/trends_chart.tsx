import React from "react";

import {
  Area,
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Dot,
  Legend,
  LegendPayload,
  Line,
  MouseHandlerDataParam,
  ReferenceArea,
  ResponsiveContainer,
  Scatter,
  ScatterPointItem,
  Tooltip,
  TooltipContentProps,
  useYAxisScale,
  XAxis,
  YAxis,
} from "recharts";
import { TrendsChartId } from "../../../app/router/router";
import { getHiddenSeriesAfterLegendClick } from "./chart_series";

export enum SeriesType {
  BAR,
  LINE,
  SCATTER,
  AREA,
}

export interface ChartDataSeries {
  name: string;
  formatHoverValue?: (datum: number) => string | JSX.Element;
  extractValue: (datum: number) => any | null;
  onClick?: (datum: number) => void;
  type: SeriesType;
  usesSecondaryAxis?: boolean;
  stackId?: string;
  color?: ChartColor | string;
  dot?: boolean;
}

export interface ChartYAxis {
  allowDecimals?: boolean;
  formatTickValue?: (datum: number, index: number) => string;
}

interface Props {
  title: string;
  data: number[];
  ticks: number[];
  id?: TrendsChartId;
  standaloneChart?: boolean;

  formatXAxisLabel: (datum: number) => string;
  formatHoverXAxisLabel: (datum: number) => string;
  dataSeries: ChartDataSeries[];
  highlightSeries?: string;
  primaryYAxis: ChartYAxis;
  secondaryYAxis?: ChartYAxis;
  hideLegend?: boolean;

  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface State {
  refAreaLeft?: string | number;
  refAreaRight?: string | number;
  hiddenSeries: ReadonlySet<number>;
}

interface TrendsChartTooltipProps
  extends Partial<Pick<TooltipContentProps<any, any>, "active" | "payload" | "coordinate">> {
  formatLabel: (datum: any) => string;
  shouldRender: () => boolean;
  dataSeries: ChartDataSeries[];
}

export enum ChartColor {
  GREEN = "#8BC34A",
  RED = "#F44336",
  ORANGE = "#FF6F00",
  BLUE = "#03A9F4",
  GREY = "#AAAAAA",
  BASICALLY_BLACK = "#212121",
}

function getResolvedColor(color: ChartColor | string): string {
  if (color === ChartColor.BASICALLY_BLACK) {
    return getComputedStyle(document.documentElement).getPropertyValue("--color-chart-black").trim() || color;
  }
  return color;
}

function chartColorToCssClass(c: ChartColor | string): string {
  switch (c) {
    case ChartColor.BLUE:
      return "blue";
    case ChartColor.GREY:
      return "grey";
    case ChartColor.RED:
      return "red";
    case ChartColor.ORANGE:
      return "orange";
    case ChartColor.GREEN:
      return "green";
    case ChartColor.BASICALLY_BLACK:
      return "black";
  }
  return c;
}

function TrendsChartTooltip({
  active,
  payload,
  formatLabel,
  shouldRender,
  dataSeries,
  coordinate,
}: TrendsChartTooltipProps) {
  if (!active || !payload || payload.length < 1 || !coordinate || !shouldRender()) {
    return null;
  }

  const seriesByName = new Map(dataSeries.map((ds) => [ds.name, ds]));
  const primaryScale = useYAxisScale("primary");
  const secondaryScale = useYAxisScale("secondary");

  // Show the 3 closest lines.
  const payloadsToShow = payload
    .map((e) => {
      const s = seriesByName.get(e.name as string);
      if (!s) {
        return undefined;
      }
      const axis = s.usesSecondaryAxis ? secondaryScale! : primaryScale!;
      if (!axis) {
        return undefined;
      }
      return {
        yCoord: axis(e.value as number)!,
        dataSeries: s,
        payloadEntry: e,
      };
    })
    .filter((v) => v !== undefined)
    .sort((a, b) => Math.abs(a.yCoord - coordinate.y) - Math.abs(b.yCoord - coordinate.y))
    .slice(0, 3)
    .sort((a, b) => a.yCoord - b.yCoord);

  return (
    <div className="trend-chart-hover">
      <div className="trend-chart-hover-label">{formatLabel(payload[0].payload)}</div>
      <div className="trend-chart-hover-value">
        {payloadsToShow.map((data, index) => {
          const value = data.payloadEntry.value as number;
          return (
            <div key={data.payloadEntry.name}>
              <div className="color-swatch" style={{ backgroundColor: data.dataSeries.color }} />
              {data.dataSeries.formatHoverValue ? data.dataSeries.formatHoverValue(value) : value}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default class TrendsChartComponent extends React.Component<Props, State> {
  state: State = { hiddenSeries: new Set() };

  onLegendClick(payload: LegendPayload, seriesIndex: number, event: React.MouseEvent) {
    event.stopPropagation();
    const name = ((payload.payload ?? null) as ChartDataSeries | null)?.name;
    const legendIndex = this.props.dataSeries.findIndex((s) => s.name === name);
    if (legendIndex >= 0) {
      this.setState((state) => ({
        hiddenSeries: getHiddenSeriesAfterLegendClick(
          state.hiddenSeries,
          legendIndex,
          this.props.dataSeries.length,
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
    if (!this.props.onZoomSelection) {
      return;
    }

    if (!e) {
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

  renderDataSeries(ds: ChartDataSeries, seriesIndex: number): JSX.Element {
    const axis = ds.usesSecondaryAxis ? "secondary" : "primary";
    switch (ds.type) {
      case SeriesType.BAR:
        const color = ds.color ?? ChartColor.GREEN;
        <Bar
          className={ds.onClick ? "trends-clickable-bar " + chartColorToCssClass(color) : ""}
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={this.state.hiddenSeries.has(seriesIndex)}
          stackId={ds.stackId}
          fill={getResolvedColor(color)}>
          {this.props.data.map((date, datumIndex) => (
            <Cell
              cursor={ds.onClick ? "pointer" : "default"}
              key={`cell-${datumIndex}`}
              onClick={!this.props.onZoomSelection && ds.onClick ? ds.onClick.bind(this, date) : undefined}
            />
          ))}
        </Bar>;
      case SeriesType.LINE:
        return (
          <Line
            activeDot={{ pointerEvents: "none" }}
            yAxisId={axis}
            name={ds.name}
            dot={false}
            dataKey={ds.extractValue}
            isAnimationActive={false}
            hide={this.state.hiddenSeries.has(seriesIndex)}
            connectNulls={true}
            stroke={getResolvedColor(ds.color ?? ChartColor.BLUE)}
            {...(this.props.highlightSeries === ds.name && { strokeWidth: 3 })}
          />
        );
      case SeriesType.SCATTER:
        const scatterColor = getResolvedColor(ds.color ?? ChartColor.BLUE);
        return (
          <Scatter
            yAxisId={axis}
            name={ds.name}
            dataKey={ds.extractValue}
            isAnimationActive={false}
            hide={this.state.hiddenSeries.has(seriesIndex)}
            stroke={scatterColor}
            fill={"#fff"}
            fillOpacity={1}
            onClick={(d: ScatterPointItem) => {
              if (ds.onClick) {
                ds.onClick(d.payload);
              }
            }}
            shape={<Dot r={3} />}
            activeShape={<Dot r={3} fill={scatterColor} fillOpacity={0.8} />}
          />
        );
      case SeriesType.AREA:
        return (
          <Area
            yAxisId={axis}
            name={ds.name}
            dataKey={ds.extractValue}
            isAnimationActive={false}
            hide={this.state.hiddenSeries.has(seriesIndex)}
            stroke={"rgba(0,0,0,0)"}
            opacity={0.2}
            connectNulls={true}
          />
        );
    }
    return <></>;
  }

  render() {
    const hasSecondaryAxis = this.props.secondaryYAxis !== undefined;

    return (
      <div
        id={this.props.id}
        className={`trend-chart ${this.props.onZoomSelection ? "zoomable" : ""} ${
          this.props.standaloneChart ? "standalone" : ""
        }`}>
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart
            accessibilityLayer={false}
            data={this.props.data}
            onMouseDown={this.props.onZoomSelection && this.onMouseDown.bind(this)}
            onMouseMove={this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" yAxisId="primary" />
            {!this.props.hideLegend && <Legend onClick={this.onLegendClick.bind(this)} />}
            <XAxis
              type="number"
              domain={["dataMin", "dataMax"]}
              dataKey={(v) => v}
              tickFormatter={this.props.formatXAxisLabel}
              ticks={this.props.ticks}
            />
            <YAxis
              yAxisId="primary"
              tickFormatter={this.props.primaryYAxis.formatTickValue}
              allowDecimals={this.props.primaryYAxis.allowDecimals}
              width={84}
            />
            {/* If no secondary axis should be shown, still render one (with no
                ticks, tick lines, or axis line) so that it reserves its width
                and right-padding is consistent across all charts. */}
            <YAxis
              yAxisId="secondary"
              orientation="right"
              tick={hasSecondaryAxis}
              tickLine={hasSecondaryAxis}
              axisLine={hasSecondaryAxis}
              tickFormatter={this.props.secondaryYAxis?.formatTickValue}
              allowDecimals={this.props.secondaryYAxis?.allowDecimals}
              width={84}
            />
            <Tooltip
              content={
                <TrendsChartTooltip
                  formatLabel={this.props.formatHoverXAxisLabel}
                  shouldRender={() => this.shouldRenderTooltip()}
                  dataSeries={this.props.dataSeries.filter((_, index) => !this.state.hiddenSeries.has(index))}
                />
              }
            />
            {this.props.dataSeries.map(this.renderDataSeries.bind(this))}
            {this.state.refAreaLeft && this.state.refAreaRight ? (
              <ReferenceArea
                yAxisId="primary"
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
