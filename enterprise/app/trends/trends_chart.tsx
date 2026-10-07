import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

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
  useChartHeight,
  useChartWidth,
  usePlotArea,
  useXAxisScale,
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

export interface ClickCoordinateInfo {
  x: number;
  y: number;
  chartWidth: number;
  chartHeight: number;
}

export interface ChartDataSeries {
  name: string;
  formatHoverValue?: (datum: number) => string | JSX.Element;
  extractValue: (datum: number) => any;
  onClick?: (datum: number, e: React.MouseEvent<SVGElement>, s: ClickCoordinateInfo) => void;
  type: SeriesType;
  color: ChartColor | string;
  usesSecondaryAxis?: boolean;
  stackId?: string;
  hideActiveDot?: boolean;
  connectNulls?: boolean;
}

interface ChartYAxis {
  allowDecimals?: boolean;
  formatTickValue?: (datum: number, index: number) => string;
}

/** A scatter point identified by the series it belongs to and its x-axis datum. */
export interface NearestScatterPoint {
  series: ChartDataSeries;
  // The x-axis datum (one of the chart's `data` entries) and the series' value
  // at that datum.
  datum: number;
  value: number;
}

/**
 * Configures a tooltip that shows the scatter point nearest to the mouse
 * (within a threshold of `maxDistancePx`). When provided, this replaces
 * a normal tooltip.
 */
export interface PointTooltipConfig {
  // Scatter points farther than this many pixels from the mouse are ignored.
  maxDistancePx: number;
  // Renders the tooltip contents.  `datum` is the x-axis datum nearest to the
  // mouse, `point` is the closest scatter point within `maxDistancePx` (if
  // there is one), and `pinned` is true when the tooltip has been pinned to
  // `point` by clicking on it, in which case the tooltip is interactive.
  // Return null to hide the tooltip.
  render: (datum: number | undefined, point: NearestScatterPoint | undefined, pinned: boolean) => JSX.Element | null;
  // If true, clicking on a scatter point pins the tooltip to it until the chart
  // is clicked again.
  pinnable?: boolean;
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
  // When set, replaces the default tooltip with one driven by the scatter
  // point nearest to the mouse.
  pointTooltip?: PointTooltipConfig;
  // When set, the x axis is a continuous numeric axis spanning this range, so
  // that `data` entries are positioned by their value rather than by their
  // index.  Otherwise the x axis is categorical, with one equally sized slot
  // per `data` entry.
  // TODO(jdhollen): make all charts set this value and simplify.
  xAxisDomain?: [number, number];

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

function TrendsChartTooltip({
  active,
  payload,
  formatLabel,
  shouldRender,
  dataSeries,
  coordinate,
  limit,
}: TrendsChartTooltipProps) {
  const primaryScale = useYAxisScale("primary");
  const secondaryScale = useYAxisScale("secondary");

  if (!active || !payload || payload.length < 1 || !coordinate || !shouldRender()) {
    return null;
  }

  // If there are more than `limit` series, show the ones that are closest to
  // the mouse.  Otherwise, show them in a consistent order.
  let renderedPayloads: JSX.Element[] = [];
  if (limit > 0) {
    const seriesByName = new Map(dataSeries.map((ds) => [ds.name, ds]));
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
      .slice(0, limit)
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
  const xAxis = useXAxisScale();
  const chartWidth = useChartWidth() ?? 0;
  const chartHeight = useChartHeight() ?? 0;
  switch (ds.type) {
    case SeriesType.BAR:
      return (
        <Bar
          key={ds.name}
          className={clickHandler ? "trends-clickable-bar" : ""}
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
          key={ds.name}
          activeDot={ds.hideActiveDot ? false : { pointerEvents: "none" }}
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
          key={ds.name}
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          onClick={
            clickHandler
              ? (d: ScatterPointItem, _, e) => {
                  clickHandler(d.payload, e, { x: d.cx ?? 0, y: d.cy ?? 0, chartWidth, chartHeight });
                }
              : undefined
          }
          shape={(p, _) => <Dot cx={p.cx} cy={p.cy} r={3} fill={scatterColor} stroke={scatterColor} />}
        />
      );
    case SeriesType.AREA:
      return (
        <Area
          key={ds.name}
          yAxisId={axis}
          name={ds.name}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          hide={hidden}
          stroke={"rgba(0,0,0,0)"}
          fill={getResolvedColor(ds.color)}
          opacity={0.2}
          connectNulls={ds.connectNulls}
          activeDot={false}
          focusable={false}
        />
      );
  }
  return <></>;
}

interface PixelPosition {
  x: number;
  y: number;
}

interface LocatedScatterPoint extends NearestScatterPoint {
  // Pixel position relative to the top-left corner of the chart's SVG.
  x: number;
  y: number;
}

interface PointTooltipLayerProps {
  config: PointTooltipConfig;
  data: number[];
  dataSeries: ChartDataSeries[];
}

// Gap between the tooltip and the mouse (or the pinned point).
const POINT_TOOLTIP_OFFSET_PX = 12;

/**
 * Converts a mouse event into a position relative to the SVG's top-left corner
 * (the coordinate system used by the axis scales), accounting for any CSS
 * scaling applied to the chart.
 */
function svgRelativePosition(svg: SVGSVGElement, e: MouseEvent): PixelPosition {
  const rect = svg.getBoundingClientRect();
  const width = svg.width.baseVal.value;
  const height = svg.height.baseVal.value;
  const scaleX = width > 0 && rect.width > 0 ? rect.width / width : 1;
  const scaleY = height > 0 && rect.height > 0 ? rect.height / height : 1;
  return { x: (e.clientX - rect.left) / scaleX, y: (e.clientY - rect.top) / scaleY };
}

/**
 * Places the tooltip below and to the right of `anchor`, flipping it above
 * and/or to the left when it would otherwise overflow the chart.
 */
function pointTooltipPosition(
  anchor: PixelPosition,
  size: { width: number; height: number },
  chartWidth: number,
  chartHeight: number
): PixelPosition {
  let x = anchor.x + POINT_TOOLTIP_OFFSET_PX;
  if (x + size.width > chartWidth) {
    x = Math.max(0, anchor.x - POINT_TOOLTIP_OFFSET_PX - size.width);
  }
  let y = anchor.y + POINT_TOOLTIP_OFFSET_PX;
  if (y + size.height > chartHeight) {
    y = Math.max(0, anchor.y - POINT_TOOLTIP_OFFSET_PX - size.height);
  }
  return { x, y };
}

/**
 * Renders a tooltip that shows the scatter point nearest to the mouse.
 *
 * Recharts' own tooltip only snaps to the x-axis.  With scatter plots,
 * this means it can highlight a point waaaay at the bottom of the chart
 * instead of one 2 pixels away from the mouse at the top of the chart.
 * This component instead listens to the chart's SVG directly and passes
 * the actual closest data point for rendering.
 *
 * The tooltip itself is rendered into the chart wrapper with a portal,
 * just like the normal recharts tooltip.  This is all implemented as a
 * function component, because recharts uses stateful functions to grant
 * access to positioning data.
 */
function PointTooltipLayer({ config, data, dataSeries }: PointTooltipLayerProps) {
  const anchorRef = useRef<SVGGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [portalTarget, setPortalTarget] = useState<HTMLElement | null>(null);
  const [mouse, setMouse] = useState<PixelPosition | undefined>(undefined);
  const [pinned, setPinned] = useState<{ seriesName: string; datum: number } | undefined>(undefined);
  const [tooltipSize, setTooltipSize] = useState({ width: 0, height: 0 });

  const xScale = useXAxisScale();
  const primaryScale = useYAxisScale("primary");
  const secondaryScale = useYAxisScale("secondary");
  const plotArea = usePlotArea();
  const chartWidth = useChartWidth() ?? 0;
  const chartHeight = useChartHeight() ?? 0;

  // Pixel positions of every visible scatter point.
  const points = useMemo<LocatedScatterPoint[]>(() => {
    const located: LocatedScatterPoint[] = [];
    if (!xScale) {
      return located;
    }
    for (const series of dataSeries) {
      if (series.type !== SeriesType.SCATTER) {
        continue;
      }
      const yScale = series.usesSecondaryAxis ? secondaryScale : primaryScale;
      if (!yScale) {
        continue;
      }
      for (const d of data) {
        const value = series.extractValue(d);
        if (value === null || value === undefined) {
          continue;
        }
        const x = xScale(d, { position: "middle" });
        const y = yScale(value);
        if (x === undefined || y === undefined) {
          continue;
        }
        located.push({ series, datum: d, value, x, y });
      }
    }
    return located;
  }, [data, dataSeries, xScale, primaryScale, secondaryScale]);

  const findNearestScatterPoint = (position: PixelPosition): LocatedScatterPoint | undefined => {
    let nearest: LocatedScatterPoint | undefined;
    let nearestDistance = config.maxDistancePx;
    for (const point of points) {
      const distance = Math.hypot(point.x - position.x, point.y - position.y);
      if (distance <= nearestDistance) {
        nearest = point;
        nearestDistance = distance;
      }
    }
    return nearest;
  };

  // This finds the closest data point in *any* series in the chart. In cases
  // where the user's mouse isn't close enough to a scatter point to show it
  // in the tooltip, the caller may still wish to show a tooltip with other
  // data from the chart.  This lets them do that.
  const findNearestXValueInAnySeries = (position: PixelPosition): number | undefined => {
    if (!xScale) {
      return undefined;
    }
    let nearest: number | undefined;
    let nearestDistance = Infinity;
    for (const datum of data) {
      const x = xScale(datum, { position: "middle" });
      if (x === undefined) {
        continue;
      }
      const distance = Math.abs(x - position.x);
      if (distance < nearestDistance) {
        nearest = datum;
        nearestDistance = distance;
      }
    }
    return nearest;
  };

  // The SVG listeners below are attached once, so route clicks through a ref
  // that always points at a handler with the current props and state.
  const onChartClickRef = useRef<(position: PixelPosition) => void>(() => {});
  onChartClickRef.current = (position) => {
    if (!config.pinnable) {
      return;
    }
    const point = findNearestScatterPoint(position);
    setPinned((current) => {
      // Clear the pinned point if the user clicks it again.
      if (!point || (current && current.seriesName === point.series.name && current.datum === point.datum)) {
        return undefined;
      }
      return { seriesName: point.series.name, datum: point.datum };
    });
  };

  useEffect(() => {
    const svg = anchorRef.current?.ownerSVGElement;
    if (!svg) {
      return;
    }
    // Recharts positions the wrapper relatively and sizes it to the SVG, so
    // absolute positions inside of it line up with SVG coordinates.
    setPortalTarget(svg.parentElement);
    const onMouseMove = (e: MouseEvent) => setMouse(svgRelativePosition(svg, e));
    const onMouseLeave = () => setMouse(undefined);
    const onClick = (e: MouseEvent) => onChartClickRef.current(svgRelativePosition(svg, e));
    svg.addEventListener("mousemove", onMouseMove);
    svg.addEventListener("mouseleave", onMouseLeave);
    svg.addEventListener("click", onClick);
    return () => {
      svg.removeEventListener("mousemove", onMouseMove);
      svg.removeEventListener("mouseleave", onMouseLeave);
      svg.removeEventListener("click", onClick);
    };
  }, []);

  // On every render, re-find the point so that we handle resizes.
  // This effect also ensures that we drop the pinned point if the data changes
  // out from under us.
  const pinnedPoint = pinned
    ? points.find((p) => p.series.name === pinned.seriesName && p.datum === pinned.datum)
    : undefined;
  useEffect(() => {
    if (pinned && !pinnedPoint) {
      setPinned(undefined);
    }
  }, [pinned, pinnedPoint]);

  let datum: number | undefined;
  let point: LocatedScatterPoint | undefined;
  let anchor: PixelPosition | undefined;
  if (pinnedPoint) {
    datum = pinnedPoint.datum;
    point = pinnedPoint;
    anchor = pinnedPoint;
  } else if (
    mouse &&
    plotArea &&
    mouse.x >= plotArea.x &&
    mouse.x <= plotArea.x + plotArea.width &&
    mouse.y >= plotArea.y &&
    mouse.y <= plotArea.y + plotArea.height
  ) {
    point = findNearestScatterPoint(mouse);
    datum = point ? point.datum : findNearestXValueInAnySeries(mouse);
    anchor = mouse;
  }
  const isPinned = Boolean(pinnedPoint);
  const content = anchor ? config.render(datum, point, isPinned) : null;

  // Show a pointer cursor when there's something to pin.
  useEffect(() => {
    const svg = anchorRef.current?.ownerSVGElement;
    if (svg) {
      svg.style.cursor = config.pinnable && point && !isPinned ? "pointer" : "";
    }
  }, [config.pinnable, point, isPinned]);

  // Measure the rendered tooltip so it can be kept inside the chart.  Layout
  // effects run before paint, so we draw in the corrected position.
  useLayoutEffect(() => {
    const el = tooltipRef.current;
    if (el && (el.offsetWidth !== tooltipSize.width || el.offsetHeight !== tooltipSize.height)) {
      setTooltipSize({ width: el.offsetWidth, height: el.offsetHeight });
    }
  });

  const position = anchor ? pointTooltipPosition(anchor, tooltipSize, chartWidth, chartHeight) : undefined;

  return (
    <g ref={anchorRef} className="trend-chart-point-layer">
      {point && (
        <circle
          className="trend-chart-point-highlight"
          cx={point.x}
          cy={point.y}
          r={6}
          fill="none"
          stroke={getResolvedColor(point.series.color)}
          strokeWidth={2}
          pointerEvents="none"
        />
      )}
      {portalTarget &&
        content &&
        position &&
        createPortal(
          <div
            ref={tooltipRef}
            className="trend-chart-point-tooltip"
            style={{
              position: "absolute",
              left: position.x,
              top: position.y,
              // Size to the content rather than to the space left of the
              // chart's edge, so the tooltip doesn't reflow as it moves.
              width: "max-content",
              pointerEvents: isPinned ? "auto" : "none",
              zIndex: 1,
            }}>
            {content}
          </div>,
          portalTarget
        )}
    </g>
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
    const visibleSeries = this.props.dataSeries.filter((_, index) => !this.state.hiddenSeries.has(index));

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
            onMouseMove={this.props.onZoomSelection && this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" yAxisId="primary" />
            {!this.props.hideLegend && <Legend onClick={this.onLegendClick.bind(this)} />}
            <XAxis
              dataKey={(v) => v}
              tickFormatter={this.props.formatXAxisLabel}
              ticks={this.props.ticks}
              {...(this.props.xAxisDomain ? { type: "number" as const, domain: this.props.xAxisDomain } : {})}
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
            {!this.props.pointTooltip && (
              <Tooltip
                content={
                  <TrendsChartTooltip
                    limit={this.props.tooltipEntryLimit ?? 0}
                    formatLabel={this.props.formatHoverXAxisLabel}
                    shouldRender={() => this.shouldRenderTooltip()}
                    dataSeries={visibleSeries}
                  />
                }
              />
            )}

            {this.props.dataSeries.map((ds, index) => {
              const hidden = this.state.hiddenSeries.has(index);
              const highlight = this.props.highlightSeries === ds.name;
              return (
                <RenderedDataSeries
                  key={index}
                  ds={ds}
                  hidden={hidden}
                  highlight={highlight}
                  zoomFn={this.props.onZoomSelection}
                  data={this.props.data}
                />
              );
            })}
            {/* Rendered after the series so the highlighted point draws on top. */}
            {this.props.pointTooltip && (
              <PointTooltipLayer config={this.props.pointTooltip} data={this.props.data} dataSeries={visibleSeries} />
            )}
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
