import React from "react";

import * as format from "../../../app/format/format";
import { ResponsiveContainer, ComposedChart, CartesianGrid, XAxis, YAxis, Line, Legend, Tooltip } from "recharts";

export interface PercentilesChartProps {
  title: string;
  data: any[];
  extractLabel: (datum: string) => string;
  formatHoverLabel: (datum: string) => string;
  extractP50: (datum: string) => number;
  extractP75: (datum: string) => number;
  extractP90: (datum: string) => number;
  extractP95: (datum: string) => number;
  extractP99: (datum: string) => number;
}

export default class PercentilesChartComponent extends React.Component<PercentilesChartProps> {
  render() {
    return (
      <div className="trend-chart">
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={this.props.data}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend />
            <XAxis dataKey={this.props.extractLabel} />
            <YAxis yAxisId="duration" tickFormatter={format.durationSec} allowDecimals={false} />
            <Tooltip
              content={
                <PercentilesChartTooltip
                  labelFormatter={this.props.formatHoverLabel}
                  extractP50={this.props.extractP50}
                  extractP75={this.props.extractP75}
                  extractP90={this.props.extractP90}
                  extractP95={this.props.extractP95}
                  extractP99={this.props.extractP99}
                />
              }
            />
            <Line yAxisId="duration" name="P50" dataKey={(datum) => this.props.extractP50(datum)} stroke="#067BC2" />
            <Line yAxisId="duration" name="P75" dataKey={(datum) => this.props.extractP75(datum)} stroke="#84BCDA" />
            <Line yAxisId="duration" name="P90" dataKey={(datum) => this.props.extractP90(datum)} stroke="#ECC30B" />
            <Line yAxisId="duration" name="P95" dataKey={(datum) => this.props.extractP95(datum)} stroke="#F37748" />
            <Line yAxisId="duration" name="P99" dataKey={(datum) => this.props.extractP99(datum)} stroke="#D56062" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}

function PercentilesChartTooltip ({
  active,
  payload,
  labelFormatter,
  extractP50,
  extractP75,
  extractP90,
  extractP95,
  extractP99,
}: any) {
  if (active) {
    let data = payload[0].payload;
    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          <div>p50: {format.compactDurationSec(extractP50(data))}</div>
          <div>p75: {format.compactDurationSec(extractP75(data))}</div>
          <div>p90: {format.compactDurationSec(extractP90(data))}</div>
          <div>p95: {format.compactDurationSec(extractP95(data))}</div>
          <div>p99: {format.compactDurationSec(extractP99(data))}</div>
        </div>
      </div>
    );
  }

  return null;
};

