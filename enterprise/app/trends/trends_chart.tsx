import React from "react";

import {
  ResponsiveContainer,
  ComposedChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Bar,
  Line,
  Legend,
  Tooltip,
  Cell,
  YAxisProps,
} from "recharts";

interface Props {
  title: string;
  data: any[];

  extractLabel: (datum: any) => string;
  formatHoverLabel: (datum: any) => string;
  formatHoverValue?: (datum: number) => string;
  formatTickValue?: (datum: number, index: number) => string;
  extractValue: (datum: any) => number;
  allowDecimals?: boolean;
  name: string;

  extractSecondaryValue?: (datum: any) => number;
  formatSecondaryHoverValue?: (datum: number) => string;
  formatSecondaryTickValue?: (datum: number, index: number) => string;
  secondaryAllowDecimals?: boolean;
  secondaryName?: string;
  secondaryLine?: boolean;
  separateAxis?: boolean;

  onBarClicked?: (date: string) => void;
  onSecondaryBarClicked?: (date: string) => void;
}

const TrendsChartTooltip = ({
  active,
  payload,
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

export default class TrendsChartComponent extends React.Component<Props> {
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
            <YAxis
              yAxisId="primary"
              tickFormatter={this.props.formatTickValue}
              allowDecimals={this.props.allowDecimals}
              width={84}
            />
            {/* If no secondary axis should be shown, render an invisible one
                by setting height="0" so that right-padding is consistent across
                all charts. */}
            <YAxis
              yAxisId="secondary"
              orientation="right"
              height={hasSecondaryAxis ? undefined : 0}
              tickFormatter={this.props.formatSecondaryTickValue}
              allowDecimals={this.props.secondaryAllowDecimals}
              width={84}
            />
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
            <Bar
              className={this.props.onBarClicked ? "trends-clickable-bar-primary" : ""}
              yAxisId="primary"
              name={this.props.name}
              dataKey={this.props.extractValue}
              fill="#607D8B">
              {this.props.data.map((date, index) => (
                <Cell
                  cursor={this.props.onBarClicked ? "pointer" : "default"}
                  key={`cell-${index}`}
                  onClick={this.props.onBarClicked ? this.props.onBarClicked.bind(this, date) : null}
                />
              ))}
            </Bar>
            {this.props.extractSecondaryValue && this.props.secondaryLine && (
              <Line
                activeDot={{ pointerEvents: "none" }}
                yAxisId={this.props.separateAxis ? "secondary" : "primary"}
                name={this.props.secondaryName}
                dot={false}
                dataKey={this.props.extractSecondaryValue}
                stroke="#03A9F4"
              />
            )}
            {this.props.extractSecondaryValue && !this.props.secondaryLine && (
              <Bar
                className={
                  this.props.onBarClicked || this.props.onSecondaryBarClicked ? "trends-clickable-bar-secondary" : ""
                }
                yAxisId={this.props.separateAxis ? "secondary" : "primary"}
                name={this.props.secondaryName}
                dataKey={this.props.extractSecondaryValue}
                fill="#03A9F4">
                {this.props.data.map((date, index) => (
                  <Cell
                    cursor={this.props.onBarClicked || this.props.onSecondaryBarClicked ? "pointer" : "default"}
                    key={`cell-${index}`}
                    onClick={
                      this.props.onSecondaryBarClicked
                        ? this.props.onSecondaryBarClicked.bind(this, date)
                        : this.props.onBarClicked
                        ? this.props.onBarClicked.bind(this, date)
                        : null
                    }
                  />
                ))}
              </Bar>
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    );
  }
}
