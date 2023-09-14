import React from "react";

import * as format from "../../../app/format/format";
import {
  ResponsiveContainer,
  ComposedChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Line,
  Legend,
  Tooltip,
  TooltipProps,
} from "recharts";

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
}

export default class PercentilesChartComponent extends React.Component<PercentilesChartProps> {
  private lastDataFromHover?: number;

  handleRowClick() {
    if (!this.props.onColumnClicked || !this.lastDataFromHover) {
      return;
    }
    this.props.onColumnClicked(this.lastDataFromHover);
  }

  render() {
    return (
      <div id={this.props.id} className="trend-chart">
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart
            data={this.props.data}
            style={this.props.onColumnClicked ? { cursor: "pointer" } : {}}
            onClick={this.handleRowClick.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend />
            <XAxis dataKey={(v) => v} tickFormatter={this.props.extractLabel} ticks={this.props.ticks} />
            <YAxis yAxisId="duration" tickFormatter={format.durationSec} allowDecimals={false} width={84} />
            <Tooltip
              content={
                <PercentilesChartTooltip
                  labelFormatter={this.props.formatHoverLabel}
                  extractP50={this.props.extractP50}
                  extractP75={this.props.extractP75}
                  extractP90={this.props.extractP90}
                  extractP95={this.props.extractP95}
                  extractP99={this.props.extractP99}
                  triggerCallback={(data) => (this.lastDataFromHover = data)}
                />
              }
            />
            <Line
              yAxisId="duration"
              name="P50"
              dataKey={(datum) => this.props.extractP50(datum)}
              stroke="#067BC2"
              dot={false}
            />
            <Line
              yAxisId="duration"
              name="P75"
              dataKey={(datum) => this.props.extractP75(datum)}
              stroke="#84BCDA"
              dot={false}
            />
            <Line
              yAxisId="duration"
              name="P90"
              dataKey={(datum) => this.props.extractP90(datum)}
              stroke="#ECC30B"
              dot={false}
            />
            <Line
              yAxisId="duration"
              name="P95"
              dataKey={(datum) => this.props.extractP95(datum)}
              stroke="#F37748"
              dot={false}
            />
            <Line
              yAxisId="duration"
              name="P99"
              dataKey={(datum) => this.props.extractP99(datum)}
              stroke="#D56062"
              dot={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}

interface PercentilesChartTooltipProps extends TooltipProps<any, any> {
  labelFormatter: (datum: number) => string;
  extractP50: (datum: number) => number;
  extractP75: (datum: number) => number;
  extractP90: (datum: number) => number;
  extractP95: (datum: number) => number;
  extractP99: (datum: number) => number;
  triggerCallback: (datum: number) => void;
}

class PercentilesChartTooltip extends React.Component<PercentilesChartTooltipProps> {
  componentDidUpdate(prevProps: PercentilesChartTooltipProps) {
    if (this.props.payload && this.props.payload.length > 0) {
      this.props.triggerCallback(this.props.payload[0].payload);
    }
  }

  render() {
    if (!this.props.active || !this.props.payload || this.props.payload.length < 1) return null;

    const data = this.props.payload[0].payload;
    if (!data) {
      return null;
    }

    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{this.props.labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          <div>p99: {format.durationSec(this.props.extractP99(data))}</div>
          <div>p95: {format.durationSec(this.props.extractP95(data))}</div>
          <div>p90: {format.durationSec(this.props.extractP90(data))}</div>
          <div>p75: {format.durationSec(this.props.extractP75(data))}</div>
          <div>p50: {format.durationSec(this.props.extractP50(data))}</div>
        </div>
      </div>
    );
  }
}
