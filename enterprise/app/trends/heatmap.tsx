import React from "react";
import moment from "moment";

import { stats } from "../../../proto/stats_ts_proto";
import { ScaleBand, scaleBand } from "d3-scale";
import { withResizeDetector } from "react-resize-detector";
import { pinBottomLeftOffsetFromMouse, MouseCoords, Tooltip } from "../../../app/components/tooltip/tooltip";

interface HeatmapProps {
  heatmapData: stats.GetStatHeatmapResponse;
  width: number;
  height: number;
  valueFormatter: (value: number) => string;
  metricBucketName: string;
  metricBucketFormatter: (value: number) => string;
  selectionCallback?: (s?: HeatmapSelection) => void;
  zoomCallback?: (s?: HeatmapSelection) => void;
}

interface State {
  // This pair indicates the cells in the heatmap that the user selected.  The
  // first entry is the cell that the user clicked on first when making the
  // selection.
  selectionToRender?: [SelectedCellData, SelectedCellData];
}

type SelectedCellData = {
  timestamp: number;
  timestampBucketIndex: number;
  metric: number;
  metricBucketIndex: number;
  value: number;
};

export type HeatmapSelection = {
  dateRangeMicros: { startInclusive: number; endExclusive: number };
  bucketRange: { startInclusive: number; endExclusive: number };
  invocationsSelected: number;
};

const CHART_MARGINS = {
  top: 1,
  right: 1,
  bottom: 39,
  left: 89,
};

// This is a magic number that states there will only be one axis label for
// every N pixels of rendered axis length (currently 100).
const TICK_LABEL_SPACING_MAGIC_NUMBER = 100;
// This is a discretized dump of the 'Purples' scale from d3-scale-chromatic,
// starting arbitrarily from .3 because it looked nice.  The scale is great but
// the package itself is heavy.  The d3-scale-chromatic scale is interpolated
// from https://colorbrewer2.org/#type=sequential&scheme=Purples&n=3
const heatmapColorScale = [
  "#d9d8ea", // .30
  "#cecee5", // .35
  "#c2c2df", // ...
  "#b6b5d8",
  "#aaa8d0",
  "#9e9bc9",
  "#928ec3",
  "#8782bc",
  "#7c73b5",
  "#7363ac",
  "#6a51a3",
  "#61409b",
  "#583093",
  "#501f8c",
  "#471084", // 1
];

type SelectionData = {
  x: number;
  y: number;
  width: number;
  height: number;
  selectionXStart: number;
  selectionYStart: number;
  selectionXEnd: number;
  selectionYEnd: number;
};

class HeatmapComponentInternal extends React.Component<HeatmapProps, State> {
  state: State = {};
  svgRef: React.RefObject<SVGSVGElement> = React.createRef();
  xScaleBand: ScaleBand<number> = scaleBand();
  yScaleBand: ScaleBand<number> = scaleBand();

  // These ordered pairs indicate which cell was first clicked on by the user.
  // By keeping them in order, we ensure that as the user moves their mouse, the
  // selection will "pivot" around that cell.
  pendingClick?: [SelectedCellData, SelectedCellData];
  selectedData?: [SelectedCellData, SelectedCellData];

  renderYBucketValue(v: number) {
    return this.props.metricBucketFormatter(v);
  }

  private numHeatmapRows(): number {
    return Math.max(this.props.heatmapData.bucketBracket.length - 1, 1);
  }

  private computeBucket(x: number, y: number): SelectedCellData | undefined {
    if (!this.svgRef.current) {
      return undefined;
    }

    const { top, left } = this.svgRef.current.getBoundingClientRect();
    const mouseX = x - left - CHART_MARGINS.left - this.xScaleBand.step() / 2;
    const mouseY = y - top - CHART_MARGINS.top - this.yScaleBand.step() / 2;
    const stepX = Math.round(mouseX / this.xScaleBand.step());
    const stepY = this.yScaleBand.domain().length - Math.round(mouseY / this.yScaleBand.step()) - 1;

    const timestamp = this.props.heatmapData.timestampBracket[stepX];
    const metric = this.props.heatmapData.bucketBracket[stepY];
    if (timestamp === undefined || metric === undefined) {
      return undefined;
    }
    const column = this.props.heatmapData.column[stepX];
    if (!column || column.value.length <= stepY) {
      return undefined;
    }

    return {
      timestamp: +timestamp,
      timestampBucketIndex: stepX,
      metric: +metric,
      metricBucketIndex: stepY,
      value: +column.value[stepY],
    };
  }

  renderTooltip(c: MouseCoords) {
    if (this.pendingClick) {
      return null;
    }
    const data = this.computeBucket(c.clientX, c.clientY);
    if (!data) {
      return null;
    }
    const metricBucket =
      this.renderYBucketValue(+data.metric) +
      " - " +
      this.renderYBucketValue(+this.props.heatmapData.bucketBracket[data.metricBucketIndex + 1]);

    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">Date: {moment(+data.timestamp / 1000).format("YYYY-MM-DD")}</div>
        <div className="trend-chart-hover-label">
          {this.props.metricBucketName}: {metricBucket}
        </div>
        <div className="trend-chart-hover-value">{this.props.valueFormatter(data.value)}</div>
      </div>
    );
  }

  onMouseDown(e: React.MouseEvent<SVGSVGElement, MouseEvent>) {
    const data = this.computeBucket(e.clientX, e.clientY);
    if (!data) {
      return;
    }
    document.addEventListener("mouseup", (e) => this.onMouseUp(e), { once: true });
    this.pendingClick = [data, data];
    this.setState({ selectionToRender: this.pendingClick });
  }

  onMouseMove(e: React.MouseEvent<SVGSVGElement, MouseEvent>) {
    if (!this.pendingClick) {
      return;
    }
    const data = this.computeBucket(e.clientX, e.clientY);
    if (!data) {
      return;
    }
    this.pendingClick = [this.pendingClick[0], data];
    this.setState({ selectionToRender: this.pendingClick });
  }

  computeHeatmapSelection(): HeatmapSelection | null {
    if (!this.selectedData || !this.props.selectionCallback) {
      return null;
    }

    const xDomain = this.xScaleBand.domain();

    const t1Index = this.selectedData[0].timestampBucketIndex;
    const t2Index = this.selectedData[1].timestampBucketIndex;

    const dateRangeMicros = {
      startInclusive: Math.min(
        +this.props.heatmapData.timestampBracket[t1Index],
        +this.props.heatmapData.timestampBracket[t2Index]
      ),
      endExclusive: Math.max(
        +this.props.heatmapData.timestampBracket[t1Index + 1],
        +this.props.heatmapData.timestampBracket[t2Index + 1]
      ),
    };
    const m1Index = this.selectedData[0].metricBucketIndex;
    const m2Index = this.selectedData[1].metricBucketIndex;
    const bucketRange = {
      startInclusive: Math.min(
        +this.props.heatmapData.bucketBracket[m1Index],
        +this.props.heatmapData.bucketBracket[m2Index]
      ),
      endExclusive: Math.max(
        +this.props.heatmapData.bucketBracket[m1Index + 1],
        +this.props.heatmapData.bucketBracket[m2Index + 1]
      ),
    };

    let invocationsSelected = 0;
    for (let i = Math.min(t1Index, t2Index); i <= Math.max(t1Index, t2Index); i++) {
      for (let j = Math.min(m1Index, m2Index); j <= Math.max(m1Index, m2Index); j++) {
        invocationsSelected += +this.props.heatmapData.column[i].value[j];
      }
    }

    return { dateRangeMicros, bucketRange, invocationsSelected };
  }

  maybeFireSelectionCallback() {
    const selection = this.computeHeatmapSelection();
    if (selection && this.props.selectionCallback) {
      this.props.selectionCallback(selection);
    }
  }

  maybeFireZoomCallback() {
    const selection = this.computeHeatmapSelection();
    if (selection && this.props.zoomCallback) {
      this.props.zoomCallback(selection);
    }
  }

  onMouseUp(e: MouseEvent) {
    if (!this.pendingClick) {
      this.selectedData = undefined;
      return;
    }
    const data = this.computeBucket(e.clientX, e.clientY);
    if (!data) {
      this.pendingClick = undefined;
      this.selectedData = undefined;
      this.setState({ selectionToRender: undefined });
      this.maybeFireSelectionCallback();
      return;
    }

    if (this.pendingClick[0].metric == data.metric && this.pendingClick[0].timestamp == data.timestamp) {
      this.selectedData = this.pendingClick;
      this.pendingClick = undefined;
      this.setState({ selectionToRender: this.selectedData });
      this.maybeFireSelectionCallback();
    } else {
      this.selectedData = [this.pendingClick[0], data];
      this.pendingClick = undefined;
      this.setState({ selectionToRender: this.selectedData });
      this.maybeFireSelectionCallback();
    }
    return;
  }

  computeSelectionData(): SelectionData | undefined {
    const selectionToDraw = this.pendingClick || this.selectedData;
    if (!selectionToDraw) {
      return;
    }

    const aScreenX = this.xScaleBand(+selectionToDraw[0].timestamp);
    const aScreenY = this.yScaleBand(+selectionToDraw[0].metric);
    const bScreenX = this.xScaleBand(+selectionToDraw[1].timestamp);
    const bScreenY = this.yScaleBand(+selectionToDraw[1].metric);

    if (aScreenX === undefined || aScreenY === undefined || bScreenX === undefined || bScreenY === undefined) {
      return;
    }

    const top = Math.min(aScreenY, bScreenY) + CHART_MARGINS.top;
    const height = Math.max(aScreenY, bScreenY) + CHART_MARGINS.top + this.yScaleBand.step() - top;
    const left = Math.min(aScreenX, bScreenX) + CHART_MARGINS.left;
    const width = Math.max(aScreenX, bScreenX) + CHART_MARGINS.left + this.xScaleBand.step() - left;
    const aTimestampIndex = selectionToDraw[0].timestampBucketIndex;
    const aMetricIndex = selectionToDraw[0].metricBucketIndex;
    const bTimestampIndex = selectionToDraw[1].timestampBucketIndex;
    const bMetricIndex = selectionToDraw[1].metricBucketIndex;

    return {
      x: left,
      y: top,
      width,
      height,
      selectionXStart: Math.min(aTimestampIndex, bTimestampIndex),
      selectionXEnd: Math.max(aTimestampIndex, bTimestampIndex),
      selectionYStart: Math.min(aMetricIndex, bMetricIndex),
      selectionYEnd: Math.max(aMetricIndex, bMetricIndex),
    };
  }

  getCellColor(x: number, y: number, v: number, interpolator: (v: number) => string, s?: SelectionData) {
    if (s && x >= s.selectionXStart && x <= s.selectionXEnd && y >= s.selectionYStart && y <= s.selectionYEnd) {
      return "#82ca9d";
    }
    return interpolator(v);
  }

  renderXAxis(width: number): JSX.Element | null {
    if (!width) {
      return null;
    }
    const numColumns = this.props.heatmapData.column.length || 1;
    const xTickMod = Math.ceil(numColumns / Math.min(numColumns, width / TICK_LABEL_SPACING_MAGIC_NUMBER));

    return (
      <g color="#666" transform={`translate(${CHART_MARGINS.left}, ${this.props.height - CHART_MARGINS.bottom})`}>
        <line stroke="#666" x1="0" y1="0" x2={width} y2="0"></line>
        {this.xScaleBand.domain().map((v, i) => {
          const tickX = this.xScaleBand.bandwidth() * i;

          return (
            <g transform={`translate(${this.xScaleBand.bandwidth() * i}, 0)`}>
              {i % xTickMod == 0 && (
                <text fill="#666" x={this.xScaleBand.bandwidth() / 2} y="18" fontSize="12" textAnchor="middle">
                  {moment(+v / 1000).format("MMM D")}
                </text>
              )}
              <line stroke="#666" x1="0" y1="0" x2="0" y2="4"></line>;
            </g>
          );
        })}
      </g>
    );
  }

  renderYAxis(height: number): JSX.Element | null {
    if (!height) {
      return null;
    }
    const numRows = this.numHeatmapRows();
    const yTickMod = Math.ceil(numRows / Math.min(numRows, height / TICK_LABEL_SPACING_MAGIC_NUMBER));

    return (
      <g
        color="#666"
        transform={`translate(${CHART_MARGINS.left}, ${this.props.height - CHART_MARGINS.bottom - height})`}>
        <line stroke="#666" x1="0" y1="0" x2="0" y2={height}></line>
        {this.yScaleBand.domain().map((v, i) => {
          return (
            <g transform={`translate(0, ${height - this.yScaleBand.bandwidth() * (numRows - 1 - i)})`}>
              {(numRows - 1 - i) % yTickMod == 0 && (
                <text fill="#666" x="-8" y="3" fontSize="12" textAnchor="end">
                  {this.renderYBucketValue(v)}
                </text>
              )}
              <line stroke="#666" x1="0" y1="0" x2="-4" y2="0"></line>
            </g>
          );
        })}
      </g>
    );
  }

  render() {
    const width = this.props.width - CHART_MARGINS.left - CHART_MARGINS.right;
    const height = this.props.height - CHART_MARGINS.top - CHART_MARGINS.bottom;

    const xDomain = this.props.heatmapData.timestampBracket.slice(0, -1).map((v) => +v);
    const yDomain = this.props.heatmapData.bucketBracket
      .slice(0, -1)
      .map((v) => +v)
      .reverse();

    this.xScaleBand = scaleBand<number>().range([0, width]).domain(xDomain);
    this.yScaleBand = scaleBand<number>().range([0, height]).domain(yDomain);

    let min = 0;
    let max = 0;
    this.props.heatmapData.column.forEach((column) => {
      column.value.forEach((value) => {
        min = Math.min(+value, min);
        max = Math.max(+value, max);
      });
    });

    const selection = this.computeSelectionData();
    const interpolator = (v: number) =>
      heatmapColorScale[Math.floor(((heatmapColorScale.length - 1) * (v - min)) / (max - min))];

    return (
      <div>
        <div style={{ position: "relative" }}>
          <Tooltip pin={pinBottomLeftOffsetFromMouse} renderContent={(c) => this.renderTooltip(c)}>
            <svg
              className="heatmap-svg"
              onMouseDown={(e) => this.onMouseDown(e)}
              onMouseMove={(e) => this.onMouseMove(e)}
              width={this.props.width}
              height={275}
              ref={this.svgRef}>
              <g transform={`translate(${CHART_MARGINS.left}, ${CHART_MARGINS.top})`}>
                {this.props.heatmapData.column.map((column, xIndex) => (
                  <>
                    {column.value.map((value, yIndex) =>
                      +value <= 0 ? null : (
                        <rect
                          x={this.xScaleBand(+column.timestampUsec) || 0}
                          y={this.yScaleBand(+this.props.heatmapData.bucketBracket[yIndex]) || 0}
                          width={this.xScaleBand.bandwidth() || 0}
                          height={this.yScaleBand.bandwidth() || 0}
                          fill={this.getCellColor(xIndex, yIndex, +value, interpolator, selection)}></rect>
                      )
                    )}
                  </>
                ))}
              </g>
              {this.renderXAxis(width)}
              {this.renderYAxis(height)}
              {selection && (
                <rect
                  x={selection.x}
                  y={selection.y}
                  width={selection.width}
                  height={selection.height}
                  fillOpacity="0"
                  stroke="#f00"></rect>
              )}
            </svg>
          </Tooltip>
        </div>
      </div>
    );
  }
}

export const HeatmapComponent = withResizeDetector<HeatmapProps, HTMLElement>(HeatmapComponentInternal, {
  handleHeight: false,
  refreshMode: "throttle",
  refreshRate: 500,
});

export default HeatmapComponent;
