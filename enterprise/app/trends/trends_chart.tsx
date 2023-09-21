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
  TooltipProps,
  Cell,
  ReferenceArea,
} from "recharts";
import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";

interface Props {
  title: string;
  data: number[];
  ticks: number[];
  id?: string;

  extractLabel: (datum: number) => string;
  formatHoverLabel: (datum: number) => string;
  formatHoverValue?: (datum: number) => string;
  formatTickValue?: (datum: number, index: number) => string;
  extractValue: (datum: number) => number;
  allowDecimals?: boolean;
  name: string;

  extractSecondaryValue?: (datum: number) => number;
  formatSecondaryHoverValue?: (datum: number) => string;
  formatSecondaryTickValue?: (datum: number, index: number) => string;
  secondaryAllowDecimals?: boolean;
  secondaryName?: string;
  secondaryLine?: boolean;
  separateAxis?: boolean;

  onBarClicked?: (date: number) => void;
  onSecondaryBarClicked?: (date: number) => void;
  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface State {
  refAreaLeft?: string;
  refAreaRight?: string;
}

interface TrendsChartTooltipProps extends TooltipProps<any, any> {
  labelFormatter: (datum: any) => string;
  shouldRender: () => boolean;
  valueFormatter?: (datum: number) => string;
  secondaryValueFormatter?: (datum: number) => string;
  extractSecondaryValue?: (datum: any) => number;
}

function TrendsChartTooltip({
  active,
  payload,
  labelFormatter,
  shouldRender,
  valueFormatter,
  secondaryValueFormatter,
  extractSecondaryValue,
}: TrendsChartTooltipProps) {
  if (!active || !payload || payload.length < 1 || !shouldRender()) {
    return null;
  }
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

export default class TrendsChartComponent extends React.Component<Props, State> {
  state: State = {};

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
    const hasSecondaryAxis = this.props.extractSecondaryValue && this.props.separateAxis;
    return (
      <div id={this.props.id} className={`trend-chart ${this.props.onZoomSelection ? "zoomable" : ""}`}>
        <div className="trend-chart-title">{this.props.title}</div>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart
            data={this.props.data}
            onMouseDown={this.props.onZoomSelection && this.onMouseDown.bind(this)}
            onMouseMove={this.props.onZoomSelection && this.onMouseMove.bind(this)}
            onMouseUp={this.props.onZoomSelection && this.onMouseUp.bind(this)}>
            <CartesianGrid strokeDasharray="3 3" />
            <Legend />
            <XAxis dataKey={(v) => v} tickFormatter={this.props.extractLabel} ticks={this.props.ticks} />
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
                  shouldRender={() => this.shouldRenderTooltip()}
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
              isAnimationActive={false}
              fill="#607D8B">
              {this.props.data.map((date, index) => (
                <Cell
                  cursor={this.props.onBarClicked ? "pointer" : "default"}
                  key={`cell-${index}`}
                  onClick={
                    !this.props.onZoomSelection && this.props.onBarClicked
                      ? this.props.onBarClicked.bind(this, date)
                      : undefined
                  }
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
                isAnimationActive={false}
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
                isAnimationActive={false}
                fill="#03A9F4">
                {this.props.data.map((date, index) => (
                  <Cell
                    cursor={this.props.onBarClicked || this.props.onSecondaryBarClicked ? "pointer" : "default"}
                    key={`cell-${index}`}
                    onClick={
                      this.props.onZoomSelection !== undefined
                        ? undefined
                        : this.props.onSecondaryBarClicked
                        ? this.props.onSecondaryBarClicked.bind(this, date)
                        : this.props.onBarClicked
                        ? this.props.onBarClicked.bind(this, date)
                        : undefined
                    }
                  />
                ))}
              </Bar>
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
