import React, { MouseEvent, MouseEventHandler } from "react";

import {
  Area,
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Coordinate,
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
  TooltipProps,
  useChartHeight,
  useChartWidth,
  useXAxisInverseScale,
  useXAxisScale,
  useYAxisScale,
  XAxis,
  YAxis,
} from "recharts";
import { TrendsChartId } from "../../../app/router/router";
import { getHiddenSeriesAfterLegendClick } from "./chart_series";
import { ScatterCustomizedShape } from "recharts/types/cartesian/Scatter";

export enum SeriesType {
  BAR,
  LINE,
  SCATTER,
  AREA,
}

export interface ClickCoordinateInfo {
  x: number;
  y: number;
  chartWidth: number;
  chartHeight: number;
}

export interface ChartDataSeries {
  name: string;
  formatHoverValue?: (datum: number) => string | JSX.Element;
  extractValue: (datum: number) => any | null;
  onClick?: (datum: number, e: MouseEvent<SVGElement>, s: ClickCoordinateInfo) => void;
  type: SeriesType;
  color: ChartColor | string;
  usesSecondaryAxis?: boolean;
  stackId?: string;
  dot?: boolean;
  connectNulls?: boolean;
}

interface ChartYAxis {
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
  tooltipEntryLimit?: number;
  customTooltip?: JSX.Element;
  onClick?: MouseEventHandler<SVGGraphicsElement>;

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
  limit: number;
}

interface RenderedDataSeriesProps {
  ds: ChartDataSeries;
  hidden: boolean;
  highlight: boolean;
  data: number[];
  zoomFn?: Object;
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

/**
 * Creates a scatter dot with a bigger clickable area.  This overrides recharts'
 * (infuriating) default behavior, which only looks at the X axis when deciding
 * hover in composed charts.
 */
function customScatterDot(color: string, clickable: boolean): ScatterCustomizedShape {
  if (clickable) {
    return <circle r={3} fill={color} stroke="transparent" strokeWidth={15} cursor="pointer" />;
  }
  return <Dot r={3} />;
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
  limit,
}: TrendsChartTooltipProps) {
  if (!active || !payload || payload.length < 1 || !coordinate || !shouldRender()) {
    return null;
  }

  // If there are more than `limit` series, show the ones that are closest to
  // the mouse.  Otherwise, show them in a consistent order.
  let renderedPayloads: JSX.Element[] = [];
  if (limit > 0) {
    const seriesByName = new Map(dataSeries.map((ds) => [ds.name, ds]));
    const primaryScale = useYAxisScale("primary");
    const secondaryScale = useYAxisScale("secondary");
    renderedPayloads = payload
      .map((e) => {
        const s = seriesByName.get(e.name as string);
        if (!s) {
          return undefined;
        }
        const axis = s.usesSecondaryAxis ? secondaryScale : primaryScale;
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
      .sort((a, b) => a.yCoord - b.yCoord)
      .map((data) => {
        const value = data.payloadEntry.value as number;
        return (
          <div key={data.payloadEntry.name}>
            <div className="color-swatch" style={{ backgroundColor: getResolvedColor(data.dataSeries.color) }} />
            {data.dataSeries.formatHoverValue ? data.dataSeries.formatHoverValue(value) : value}
          </div>
        );
      });
  } else {
    renderedPayloads = dataSeries.map((ds, index) => {
      if (index >= payload.length) {
        return <></>;
      }
      const data = payload[index];
      if (data === undefined) {
        return <></>;
      }
      const value = data.value as number;
      return (
        <div key={index}>
          <div className="color-swatch" style={{ backgroundColor: getResolvedColor(ds.color) }} />
          {ds.formatHoverValue ? ds.formatHoverValue(value) : value}
        </div>
      );
    });
  }

  return (
    <div className="trend-chart-hover">
      <div className="trend-chart-hover-label">{formatLabel(payload[0].payload)}</div>
      <div className="trend-chart-hover-value">{renderedPayloads}</div>
    </div>
  );
}

function RenderedDataSeries({ ds, hidden, highlight, data, zoomFn }: RenderedDataSeriesProps) {
  const axis = ds.usesSecondaryAxis ? "secondary" : "primary";
  const clickHandler = ds.onClick;
  const xAxis = useXAxisScale(axis);
  const chartWidth = useChartWidth() ?? 0;
  const chartHeight = useChartHeight() ?? 0;
  switch (ds.type) {
    case SeriesType.BAR:
      return (
        <Bar
          className={ds.onClick ? "trends-clickable-bar " + chartColorToCssClass(ds.color) : ""}
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          stackId={ds.stackId}
          fill={getResolvedColor(ds.color)}>
          {data.map((date, datumIndex) => {
            return (
              <Cell
                cursor={clickHandler ? "pointer" : "default"}
                key={`cell-${datumIndex}`}
                onClick={
                  !zoomFn && clickHandler
                    ? (e) =>
                        clickHandler(date, e, {
                          x: xAxis ? (xAxis(date) ?? 0) : 0,
                          y: chartHeight / 2, // who cares
                          chartWidth,
                          chartHeight,
                        })
                    : undefined
                }
              />
            );
          })}
        </Bar>
      );
    case SeriesType.LINE:
      return (
        <Line
          activeDot={false}
          yAxisId={axis}
          name={ds.name}
          dot={false}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          connectNulls={ds.connectNulls}
          focusable={false}
          stroke={getResolvedColor(ds.color)}
          {...(highlight && { strokeWidth: 3 })}
        />
      );
    case SeriesType.SCATTER:
      const scatterColor = getResolvedColor(ds.color);
      return (
        <Scatter
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          stroke={scatterColor}
          fill={"#fff"}
          fillOpacity={1}
          onClick={
            clickHandler
              ? (d: ScatterPointItem, _, e) => {
                  clickHandler(d.payload, e, { x: d.cx ?? 0, y: d.cy ?? 0, chartWidth, chartHeight });
                }
              : undefined
          }
          shape={customScatterDot(scatterColor, Boolean(clickHandler))}
        />
      );
    case SeriesType.AREA:
      return (
        <Area
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          stroke={"rgba(0,0,0,0)"}
          opacity={0.2}
          connectNulls={ds.connectNulls}
          activeDot={false}
          focusable={false}
        />
      );
  }
  return <></>;
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
            onClick={(_, e) => this.props.onClick && this.props.onClick(e)}
            accessibilityLayer={false}
            data={this.props.data}
            onMouseDown={this.props.onZoomSelection && this.onMouseDown.bind(this)}
            onMouseMove={this.props.onZoomSelection && this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" yAxisId="primary" />
            {!this.props.hideLegend && <Legend onClick={this.onLegendClick.bind(this)} />}
            <XAxis dataKey={(v) => v} tickFormatter={this.props.formatXAxisLabel} ticks={this.props.ticks} />
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
            {this.props.customTooltip ?? (
              <Tooltip
                content={
                  <TrendsChartTooltip
                    limit={this.props.tooltipEntryLimit ?? 0}
                    formatLabel={this.props.formatHoverXAxisLabel}
                    shouldRender={() => this.shouldRenderTooltip()}
                    dataSeries={this.props.dataSeries.filter((_, index) => !this.state.hiddenSeries.has(index))}
                  />
                }
              />
            )}

            {this.props.dataSeries.map((ds, index) => {
              const hidden = this.state.hiddenSeries.has(index);
              const highlight = this.props.highlightSeries === ds.name;
              return (
                <RenderedDataSeries
                  ds={ds}
                  hidden={hidden}
                  highlight={highlight}
                  zoomFn={this.props.onZoomSelection}
                  data={this.props.data}
                />
              );
            })}
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
