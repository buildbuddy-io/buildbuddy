import React from "react";
import { ResponsiveContainer, ComposedChart, CartesianGrid, XAxis, YAxis, Bar, Line, Legend, Tooltip } from "recharts";
import * as format from "../../../app/format/format";
import { invocation } from "../../../proto/invocation_ts_proto";

interface Props {
  title: string;
  data: any[];
  extractLabel: (datum: any) => string;
  formatHoverLabel: (datum: any) => string;
  extractHits: (datum: any) => number;
  extractMisses?: (datum: any) => number;
  extractWrites?: (datum: any) => number;
}

const CacheChartTooltip = ({ active, payload, labelFormatter, extractHits, extractMisses, extractWrites }: any) => {
  if (active) {
    let data = payload[0].payload;
    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          <div>{extractHits(data) || 0} hits</div>
          {extractMisses && <div>{extractMisses(data) || 0} misses</div>}
          {extractWrites && <div>{extractWrites(data) || 0} writes</div>}
          <div>
            {(
              (100 * extractHits(data)) /
                (extractHits(data) + (extractMisses ? extractMisses(data) : extractWrites(data))) || 0
            ).toFixed(2)}
            % hit percentage
          </div>
        </div>
      </div>
    );
  }

  return null;
};

export default class CacheChartComponent extends React.Component<Props> {
  render() {
    return (
      <div className="trend-chart">
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={this.props.data}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend />
            <XAxis dataKey={this.props.extractLabel} />
            <YAxis yAxisId="hits" tickFormatter={format.count} allowDecimals={false} />
            <YAxis
              domain={[0, 100]}
              yAxisId="percent"
              orientation="right"
              tickFormatter={(value: number) => `${value}%`}
            />
            <Tooltip
              content={
                <CacheChartTooltip
                  labelFormatter={this.props.formatHoverLabel}
                  extractHits={this.props.extractHits}
                  extractMisses={this.props.extractMisses}
                  extractWrites={this.props.extractWrites}
                />
              }
            />
            <Bar yAxisId="hits" name="hits" dataKey={(datum) => this.props.extractHits(datum)} fill="#8BC34A" />
            {this.props.extractMisses && (
              <Bar yAxisId="hits" name="misses" dataKey={(datum) => this.props.extractMisses(datum)} fill="#f44336" />
            )}
            {this.props.extractWrites && (
              <Bar yAxisId="hits" name="writes" dataKey={(datum) => this.props.extractWrites(datum)} fill="#f44336" />
            )}
            <Line
              yAxisId="percent"
              name="hit percentage"
              dot={false}
              dataKey={(datum) =>
                (100 * this.props.extractHits(datum)) /
                (this.props.extractHits(datum) +
                  (this.props.extractMisses ? this.props.extractMisses(datum) : this.props.extractWrites(datum)))
              }
              stroke="#03A9F4"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
