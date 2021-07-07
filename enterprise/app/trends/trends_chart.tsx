import React from "react";
import { ResponsiveContainer, ComposedChart, CartesianGrid, XAxis, YAxis, Bar, Line, Legend, Tooltip } from "recharts";

interface Props {
  title: string;
  data: any[];

  extractLabel: (datum: any) => string;
  formatHoverLabel: (datum: any) => string;
  formatHoverValue?: (datum: number) => string;
  extractValue: (datum: any) => number;
  name: string;

  extractSecondaryValue?: (datum: any) => number;
  formatSecondaryHoverValue?: (datum: number) => string;
  secondaryName?: string;
  secondaryLine?: boolean;
  separateAxis?: boolean;
}

const TrendsChartTooltip = ({
  active,
  payload,
  label,
  labelFormatter,
  valueFormatter,
  secondaryValueFormatter,
  extractSecondaryValue,
}: any) => {
  if (active) {
    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">{labelFormatter(payload[0].payload)}</div>
        <div className="trend-chart-hover-value">
          <div>{valueFormatter ? valueFormatter(payload[0].value) : payload[0].value}</div>
          {extractSecondaryValue && (
            <div>
              {secondaryValueFormatter
                ? secondaryValueFormatter(extractSecondaryValue(payload[0].payload))
                : extractSecondaryValue(payload[0].payload)}
            </div>
          )}
        </div>
      </div>
    );
  }

  return null;
};

export default class TrendsChartComponent extends React.Component {
  props: Props;

  render() {
    const hasSecondaryAxis = this.props.extractSecondaryValue && this.props.separateAxis;

    return (
      <div className="trend-chart">
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={this.props.data}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend />
            <XAxis dataKey={this.props.extractLabel} />
            <YAxis yAxisId="primary" />
            {/* If no secondary axis should be shown, render an invisible one
                by setting height="0" so that right-padding is consistent across
                all charts. */}
            <YAxis yAxisId="secondary" orientation="right" height={hasSecondaryAxis ? undefined : 0} />
            <Tooltip
              content={
                <TrendsChartTooltip
                  labelFormatter={this.props.formatHoverLabel}
                  valueFormatter={this.props.formatHoverValue}
                  extractSecondaryValue={this.props.extractSecondaryValue}
                  secondaryValueFormatter={this.props.formatSecondaryHoverValue}
                />
              }
            />
            <Bar yAxisId="primary" name={this.props.name} dataKey={this.props.extractValue} fill="#607D8B" />
            {this.props.extractSecondaryValue && this.props.secondaryLine && (
              <Line
                yAxisId={this.props.separateAxis ? "secondary" : "primary"}
                name={this.props.secondaryName}
                dot={false}
                dataKey={this.props.extractSecondaryValue}
                stroke="#03A9F4"
              />
            )}
            {this.props.extractSecondaryValue && !this.props.secondaryLine && (
              <Bar
                yAxisId={this.props.separateAxis ? "secondary" : "primary"}
                name={this.props.secondaryName}
                dataKey={this.props.extractSecondaryValue}
                fill="#03A9F4"
              />
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
