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

interface ChartDataSeries {
  name: string;
  formatHoverValue?: (datum: number) => string;
  extractValue: (datum: number) => number;
  onClick?: (datum: number) => void;
  isLine?: boolean;
  usesSecondaryAxis?: boolean;
  stackId?: string;
}

interface ChartYAxis {
  allowDecimals?: boolean;
  formatTickValue?: (datum: number, index: number) => string;
}

interface Props {
  title: string;
  data: number[];
  ticks: number[];
  id?: string;

  formatXAxisLabel: (datum: number) => string;
  formatHoverXAxisLabel: (datum: number) => string;
  dataSeries: ChartDataSeries[];
  primaryYAxis: ChartYAxis;
  secondaryYAxis?: ChartYAxis;

  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface State {
  refAreaLeft?: string;
  refAreaRight?: string;
}

interface TrendsChartTooltipProps extends TooltipProps<any, any> {
  labelFormatter: (datum: any) => string;
  shouldRender: () => boolean;
  dataSeries: ChartDataSeries[];
}

function TrendsChartTooltip({ active, payload, labelFormatter, shouldRender, dataSeries }: TrendsChartTooltipProps) {
  if (!active || !payload || payload.length < 1 || !shouldRender()) {
    return null;
  }
  return (
    <div className="trend-chart-hover">
      <div className="trend-chart-hover-label">{labelFormatter(payload[0].payload)}</div>
      <div className="trend-chart-hover-value">
        {dataSeries.map((ds, index) => {
          if (index >= payload.length) {
            return <></>;
          }
          const data = payload[index];
          if (data === undefined) {
            return <></>;
          }
          return <div>{ds.formatHoverValue ? ds.formatHoverValue(data.value) : data.value}</div>;
        })}
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

  renderDataSeries(ds: ChartDataSeries, index: number): JSX.Element {
    const axis = ds.usesSecondaryAxis ? "secondary" : "primary";
    if (ds.isLine) {
      return (
        <Line
          activeDot={{ pointerEvents: "none" }}
          yAxisId={axis}
          name={ds.name}
          dot={false}
          dataKey={ds.extractValue}
          isAnimationActive={false}
          stroke="#03A9F4"
        />
      );
    }

    return (
      <Bar
        className={
          ds.onClick ? (ds.usesSecondaryAxis ? "trends-clickable-bar-secondary" : "trends-clickable-bar-primary") : ""
        }
        yAxisId={axis}
        name={ds.name}
        dataKey={ds.extractValue}
        isAnimationActive={false}
        fill="#8BC34A">
        {this.props.data.map((date, index) => (
          <Cell
            cursor={ds.onClick ? "pointer" : "default"}
            key={`cell-${index}`}
            onClick={!this.props.onZoomSelection && ds.onClick ? ds.onClick.bind(this, date) : undefined}
          />
        ))}
      </Bar>
    );
  }

  render() {
    const hasSecondaryAxis = this.props.secondaryYAxis !== undefined;

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
            <XAxis dataKey={(v) => v} tickFormatter={this.props.formatXAxisLabel} ticks={this.props.ticks} />
            <YAxis
              yAxisId="primary"
              tickFormatter={this.props.primaryYAxis.formatTickValue}
              allowDecimals={this.props.primaryYAxis.allowDecimals}
              width={84}
            />
            {/* If no secondary axis should be shown, render an invisible one
                by setting height="0" so that right-padding is consistent across
                all charts. */}
            <YAxis
              yAxisId="secondary"
              orientation="right"
              height={hasSecondaryAxis ? undefined : 0}
              tickFormatter={this.props.secondaryYAxis?.formatTickValue}
              allowDecimals={this.props.secondaryYAxis?.allowDecimals}
              width={84}
            />
            <Tooltip
              content={
                <TrendsChartTooltip
                  labelFormatter={this.props.formatHoverXAxisLabel}
                  shouldRender={() => this.shouldRenderTooltip()}
                  dataSeries={this.props.dataSeries}
                />
              }
            />
            {this.props.dataSeries.map(this.renderDataSeries.bind(this))}
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
