import React from "react";
import { ResponsiveContainer, ComposedChart, CartesianGrid, XAxis, YAxis, Bar, Line, Legend, Tooltip } from "recharts";
import * as format from "../../../app/format/format";
import { invocation } from "../../../proto/invocation_ts_proto";

interface Props {
  title: string;
  data: any[];
  secondaryBarName: string;
  extractLabel: (datum: any) => string;
  formatHoverLabel: (datum: any) => string;
  extractHits: (datum: any) => number;
  extractSecondary: (datum: any) => number;
}

const CacheChartTooltip = ({
  active,
  payload,
  labelFormatter,
  extractHits,
  secondaryBarName,
  extractSecondary,
}: any) => {
  if (active) {
    let data = payload[0].payload;
    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{labelFormatter(data)}</div>
        <div className="trend-chart-hover-value">
          <div>{extractHits(data) || 0} hits</div>
          <div>
            {extractSecondary(data) || 0} {secondaryBarName}
          </div>
          <div>
            {((100 * extractHits(data)) / (extractHits(data) + extractSecondary(data)) || 0).toFixed(2)}% hit percentage
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
                  secondaryBarName={this.props.secondaryBarName}
                  extractSecondary={this.props.extractSecondary}
                />
              }
            />
            <Bar yAxisId="hits" name="hits" dataKey={(datum) => this.props.extractHits(datum)} fill="#8BC34A" />
            <Bar
              yAxisId="hits"
              name={this.props.secondaryBarName}
              dataKey={(datum) => this.props.extractSecondary(datum)}
              fill="#f44336"
            />
            <Line
              yAxisId="percent"
              name="hit percentage"
              dot={false}
              dataKey={(datum) =>
                (100 * this.props.extractHits(datum)) /
                (this.props.extractHits(datum) + this.props.extractSecondary(datum))
              }
              stroke="#03A9F4"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
