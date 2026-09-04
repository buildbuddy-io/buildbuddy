import React from "react";

import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  TooltipProps,
  XAxis,
  YAxis,
} from "recharts";
import { CategoricalChartState } from "recharts/types/chart/types";
import * as format from "../../../app/format/format";
import { getHiddenSeriesAfterLegendClick } from "./chart_series";

export interface PercentilesChartProps {
  title: string;
  id?: string;
  data: number[];
  ticks: number[];
  extractLabel: (datum: number) => string;
  formatHoverLabel: (datum: number) => string;
  extractP50: (datum: number) => number;
  extractP75: (datum: number) => number;
  extractP90: (datum: number) => number;
  extractP95: (datum: number) => number;
  extractP99: (datum: number) => number;
  onColumnClicked?: (datum: number) => void;
  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface PercentileDataSeries {
  name: string;
  dataKey: (datum: number) => number;
  stroke: string;
}

interface State {
  refAreaLeft?: string;
  refAreaRight?: string;
  hiddenSeries: ReadonlySet<number>;
}

export default class PercentilesChartComponent extends React.Component<PercentilesChartProps, State> {
  state: State = { hiddenSeries: new Set() };
  private lastDataFromHover?: number;

  getDataSeries(): PercentileDataSeries[] {
    return [
      { name: "P50", dataKey: (datum) => this.props.extractP50(datum), stroke: "#067BC2" },
      { name: "P75", dataKey: (datum) => this.props.extractP75(datum), stroke: "#84BCDA" },
      { name: "P90", dataKey: (datum) => this.props.extractP90(datum), stroke: "#ECC30B" },
      { name: "P95", dataKey: (datum) => this.props.extractP95(datum), stroke: "#F37748" },
      { name: "P99", dataKey: (datum) => this.props.extractP99(datum), stroke: "#D56062" },
    ];
  }

  onLegendClick(_data: unknown, seriesIndex: number, event: React.MouseEvent) {
    event.stopPropagation();
    this.setState((state) => ({
      hiddenSeries: getHiddenSeriesAfterLegendClick(
        state.hiddenSeries,
        seriesIndex,
        this.getDataSeries().length,
        event.ctrlKey || event.metaKey || event.shiftKey
      ),
    }));
  }

  handleRowClick() {
    if (!this.props.onColumnClicked || !this.lastDataFromHover) {
      return;
    }
    this.props.onColumnClicked(this.lastDataFromHover);
  }

  onMouseDown(e: CategoricalChartState) {
    if (!this.props.onZoomSelection || !e) {
      this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
      return;
    }
    this.setState({ refAreaLeft: e.activeLabel, refAreaRight: e.activeLabel });
  }

  onMouseMove(e: CategoricalChartState) {
    if (!this.props.onZoomSelection || !e) {
      this.setState({ refAreaLeft: undefined, refAreaRight: undefined });
      return;
    }
    if (!this.state.refAreaLeft) {
      return;
    }
    this.setState({ refAreaRight: e.activeLabel });
  }

  onMouseUp(e: CategoricalChartState) {
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
            data={this.props.data}
            style={this.props.onColumnClicked ? { cursor: "pointer" } : {}}
            onClick={this.props.onZoomSelection ? undefined : this.handleRowClick.bind(this)}
            onMouseDown={this.props.onZoomSelection && this.onMouseDown.bind(this)}
            onMouseMove={this.props.onZoomSelection && this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend onClick={this.onLegendClick.bind(this)} />
            <XAxis dataKey={(v) => v} tickFormatter={this.props.extractLabel} ticks={this.props.ticks} />
            <YAxis yAxisId="duration" tickFormatter={format.durationSec} allowDecimals={false} width={84} />
            <Tooltip
              content={
                <PercentilesChartTooltip
                  labelFormatter={this.props.formatHoverLabel}
                  shouldRender={() => this.shouldRenderTooltip()}
                  dataSeries={dataSeries}
                  hiddenSeries={this.state.hiddenSeries}
                  triggerCallback={(data) => (this.lastDataFromHover = data)}
                />
              }
            />
            {dataSeries.map((series, index) => (
              <Line
                key={series.name}
                yAxisId="duration"
                name={series.name}
                dataKey={series.dataKey}
                stroke={series.stroke}
                dot={false}
                hide={this.state.hiddenSeries.has(index)}
                isAnimationActive={false}
              />
            ))}
            {this.state.refAreaLeft && this.state.refAreaRight ? (
              <ReferenceArea
                yAxisId="duration"
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

interface PercentilesChartTooltipProps extends TooltipProps<any, any> {
  labelFormatter: (datum: number) => string;
  shouldRender: () => boolean;
  dataSeries: PercentileDataSeries[];
  hiddenSeries: ReadonlySet<number>;
  triggerCallback: (datum: number) => void;
}

class PercentilesChartTooltip extends React.Component<PercentilesChartTooltipProps> {
  componentDidUpdate(prevProps: PercentilesChartTooltipProps) {
    if (this.props.payload && this.props.payload.length > 0) {
      this.props.triggerCallback(this.props.payload[0].payload);
    }
  }

  render() {
    if (!this.props.active || !this.props.payload || this.props.payload.length < 1 || !this.props.shouldRender()) {
      return null;
    }

    const data = this.props.payload[0].payload;
    if (!data) {
      return null;
    }

    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{this.props.labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          {this.props.dataSeries
            .map((series, index) => ({ series, index }))
            .reverse()
            .map(
              ({ series, index }) =>
                !this.props.hiddenSeries.has(index) && (
                  <div key={series.name}>
                    {series.name.toLowerCase()}: {format.durationSec(series.dataKey(data))}
                  </div>
                )
            )}
        </div>
      </div>
    );
  }
}
