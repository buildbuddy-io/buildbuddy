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
  ReferenceArea,
} from "recharts";
import * as format from "../../../app/format/format";
import { CategoricalChartState } from "recharts/types/chart/generateCategoricalChart";

interface Props {
  title: string;
  id?: string;
  data: number[];
  ticks: number[];
  secondaryBarName: string;
  extractLabel: (datum: number) => string;
  formatHoverLabel: (datum: number) => string;
  extractHits: (datum: number) => number;
  extractSecondary: (datum: number) => number;
  onZoomSelection?: (startDate: number, endDate: number) => void;
}

interface State {
  refAreaLeft?: string;
  refAreaRight?: string;
}

interface CacheChartTooltipProps extends TooltipProps<any, any> {
  labelFormatter: (datum: number) => string;
  shouldRender: () => boolean;
  extractHits: (datum: number) => number;
  secondaryBarName: string;
  extractSecondary: (datum: number) => number;
}

const CacheChartTooltip = ({
  active,
  payload,
  labelFormatter,
  shouldRender,
  extractHits,
  secondaryBarName,
  extractSecondary,
}: CacheChartTooltipProps) => {
  if (!active || !payload || payload.length < 1 || !shouldRender()) {
    return null;
  }
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
};

export default class CacheChartComponent extends React.Component<Props, State> {
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
                  shouldRender={() => this.shouldRenderTooltip()}
                  extractHits={this.props.extractHits}
                  secondaryBarName={this.props.secondaryBarName}
                  extractSecondary={this.props.extractSecondary}
                />
              }
            />
            <Bar
              yAxisId="hits"
              name="hits"
              dataKey={(datum) => this.props.extractHits(datum)}
              fill="#8BC34A"
              isAnimationActive={false}
            />
            <Bar
              yAxisId="hits"
              name={this.props.secondaryBarName}
              dataKey={(datum) => this.props.extractSecondary(datum)}
              fill="#f44336"
              isAnimationActive={false}
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
              isAnimationActive={false}
            />
            {this.state.refAreaLeft && this.state.refAreaRight ? (
              <ReferenceArea
                yAxisId="percent"
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
