import React from "react";

import * as format from "../../../app/format/format";
import { ResponsiveContainer, ComposedChart, CartesianGrid, XAxis, YAxis, Line, Legend, Tooltip } from "recharts";

interface Props {
  title: string;
  data: any[];
  extractLabel: (datum: any) => string;
  formatHoverLabel: (datum: any) => string;
  extractP50: (datum: any) => number;
  extractP75: (datum: any) => number;
  extractP90: (datum: any) => number;
  extractP95: (datum: any) => number;
  extractP99: (datum: any) => number;
}

const PercentilesChartTooltip = ({
  active,
  payload,
  labelFormatter,
  extractP50,
  extractP75,
  extractP90,
  extractP95,
  extractP99,
}: any) => {
  if (active) {
    let data = payload[0].payload;
    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          <div>p50: {format.durationSec(extractP50(data) || 0)}</div>
          <div>p75: {format.durationSec(extractP75(data) || 0)}</div>
          <div>p90: {format.durationSec(extractP90(data) || 0)}</div>
          <div>p95: {format.durationSec(extractP95(data) || 0)}</div>
          <div>p99: {format.durationSec(extractP99(data) || 0)}</div>
        </div>
      </div>
    );
  }

  return null;
};

export default class PercentilesChartComponent extends React.Component<Props> {
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
