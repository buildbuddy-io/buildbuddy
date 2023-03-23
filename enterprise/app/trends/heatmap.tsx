import React from "react";
import moment from "moment";
import Long from "long";

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
  selectedData?: HeatmapSelection;
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
  eventsSelected: number;
};

const CHART_MARGINS = {
  top: 1,
  right: 1,
  bottom: 39,
  left: 89,
};

const ZOOM_BUTTON_ATTRIBUTES = {
  width: 26,
  height: 26,
  sideMargin: 12,
};

// This is a magic number that states there will only be one axis label for
// every N pixels of rendered axis length (currently 100).
const TICK_LABEL_SPACING_MAGIC_NUMBER = 100;
// This is a discretized dump of the 'Purples' and 'Greens' scales
// from d3-scale-chromatic, starting arbitrarily from .25 because it looked
// nice.  The scale is great but the package itself is heavy.  The
// d3-scale-chromatic scale is interpolated from
// https://colorbrewer2.org/#type=sequential&scheme=Purples&n=3
const heatmapPurples = [
  "#d9d8ea", // .25
  "#cecee5", // .30
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
  "#471084", // .95
];

const heatmapGreens = [
  "#86cc85", // .45
  "#7bc77d", // .48
  "#6fc176", // ...
  "#63bc6f",
  "#58b668",
  "#4daf62",
  "#43a95c",
  "#3aa256",
  "#329a51",
  "#2a934b",
  "#228b45",
  "#1a843f",
  "#137c39",
  "#0c7533",
  "#076d2e", // .87
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

  overlapsWithZoomButton(c: MouseCoords): boolean {
    if (!this.svgRef.current) {
      return false;
    }
    const el = this.svgRef.current.querySelector(".heatmap-zoom");
    if (!el) {
      return false;
    }
    const r = el.getBoundingClientRect();
    return c.clientX >= r.x && c.clientX <= r.x + r.width && c.clientY >= r.y && c.clientY <= r.y + r.height;
  }

  renderTooltip(c: MouseCoords) {
    if (this.pendingClick) {
      return null;
    }
    const data = this.computeBucket(c.clientX, c.clientY);
    if (!data) {
      return null;
    }
    if (this.overlapsWithZoomButton(c)) {
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
    if (e.target instanceof SVGElement && e.target.closest(".heatmap-zoom")) {
      this.maybeFireZoomCallback();
      return;
    }
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

  convertSelectionToCells(selection: HeatmapSelection): [SelectedCellData, SelectedCellData] | null {
    if (!this.props.heatmapData.timestampBracket || !this.props.heatmapData.bucketBracket) {
      return null;
    }
    const longNumberCompare = (n: number) => (l: Long) => +l === n;
    const lowDate = selection.dateRangeMicros.startInclusive;
    const lowDateIndex = this.props.heatmapData.timestampBracket.findIndex(longNumberCompare(lowDate));
    const highDateIndex =
      this.props.heatmapData.timestampBracket.findIndex(longNumberCompare(selection.dateRangeMicros.endExclusive)) - 1;

    const lowMetric = selection.bucketRange.startInclusive;
    const lowMetricIndex = this.props.heatmapData.bucketBracket.findIndex(longNumberCompare(lowMetric));
    const highMetricIndex =
      this.props.heatmapData.bucketBracket.findIndex(longNumberCompare(selection.bucketRange.endExclusive)) - 1;

    if (lowDateIndex < 0 || highDateIndex < 0 || lowMetricIndex < 0 || highMetricIndex < 0) {
      return null;
    }

    const highDate = +this.props.heatmapData.timestampBracket[highDateIndex];
    const highMetric = +this.props.heatmapData.bucketBracket[highMetricIndex];

    return [
      {
        timestamp: lowDate,
        timestampBucketIndex: lowDateIndex,
        metric: lowMetric,
        metricBucketIndex: lowMetricIndex,
        value: +this.props.heatmapData.column[lowDateIndex].value[lowMetricIndex],
      },
      {
        timestamp: highDate,
        timestampBucketIndex: highDateIndex,
        metric: highMetric,
        metricBucketIndex: highMetricIndex,
        value: +this.props.heatmapData.column[highDateIndex].value[highMetricIndex],
      },
    ];
  }

  convertCellsToSelection(selectedCells: [SelectedCellData, SelectedCellData]): HeatmapSelection {
    const xDomain = this.xScaleBand.domain();

    const t1Index = selectedCells[0].timestampBucketIndex;
    const t2Index = selectedCells[1].timestampBucketIndex;

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
    const m1Index = selectedCells[0].metricBucketIndex;
    const m2Index = selectedCells[1].metricBucketIndex;
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

    let eventsSelected = 0;
    for (let i = Math.min(t1Index, t2Index); i <= Math.max(t1Index, t2Index); i++) {
      for (let j = Math.min(m1Index, m2Index); j <= Math.max(m1Index, m2Index); j++) {
        eventsSelected += +this.props.heatmapData.column[i].value[j];
      }
    }

    return { dateRangeMicros, bucketRange, eventsSelected };
  }

  maybeFireSelectionCallback(selectedCells: [SelectedCellData, SelectedCellData]) {
    const selection = this.convertCellsToSelection(selectedCells);
    if (selection && this.props.selectionCallback) {
      this.props.selectionCallback(selection);
    }
  }

  maybeFireZoomCallback() {
    if (this.props.selectedData && this.props.zoomCallback) {
      this.props.zoomCallback(this.props.selectedData);
    }
  }

  onMouseUp(e: MouseEvent) {
    if (!this.pendingClick) {
      return;
    }

    const data = this.computeBucket(e.clientX, e.clientY);
    // If the user has dragged the mouse of the heatmap or only clicked one
    // cell, then we just use the already-pending click.
    if (!data || (this.pendingClick[0].metric == data.metric && this.pendingClick[0].timestamp == data.timestamp)) {
      const selectedData = this.pendingClick;
      this.pendingClick = undefined;
      this.maybeFireSelectionCallback(selectedData);
    } else {
      const selectedData: [SelectedCellData, SelectedCellData] = [this.pendingClick[0], data];
      this.pendingClick = undefined;
      this.maybeFireSelectionCallback(selectedData);
    }
    return;
  }

  computeSelectionData(): SelectionData | undefined {
    const selectionToDraw =
      this.pendingClick || (this.props.selectedData && this.convertSelectionToCells(this.props.selectedData));
    if (!selectionToDraw) {
      return undefined;
    }

    const aScreenX = this.xScaleBand(+selectionToDraw[0].timestamp);
    const aScreenY = this.yScaleBand(+selectionToDraw[0].metric);
    const bScreenX = this.xScaleBand(+selectionToDraw[1].timestamp);
    const bScreenY = this.yScaleBand(+selectionToDraw[1].metric);

    if (aScreenX === undefined || aScreenY === undefined || bScreenX === undefined || bScreenY === undefined) {
      return undefined;
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

  getCellColor(
    x: number,
    y: number,
    value: number,
    interpolator: (v: number, selected: boolean) => string,
    s?: SelectionData
  ) {
    const selected =
      !!s && x >= s.selectionXStart && x <= s.selectionXEnd && y >= s.selectionYStart && y <= s.selectionYEnd;
    return interpolator(value, selected);
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

  maybeRenderZoomButton(positioningData: SelectionData): JSX.Element | null {
    if (!this.props.zoomCallback || this.pendingClick) {
      return null;
    }
    if (!this.props.selectedData || this.props.selectedData.eventsSelected < 2) {
      return null;
    }

    const selectionRightEdge = positioningData.x + positioningData.width;
    let zoomLeftEdge = selectionRightEdge + ZOOM_BUTTON_ATTRIBUTES.sideMargin;
    let zoomTopEdge = positioningData.y;

    if (selectionRightEdge + ZOOM_BUTTON_ATTRIBUTES.width + 2 * ZOOM_BUTTON_ATTRIBUTES.sideMargin > this.props.width) {
      zoomLeftEdge = positioningData.x - ZOOM_BUTTON_ATTRIBUTES.width - ZOOM_BUTTON_ATTRIBUTES.sideMargin;
    }

    return (
      <g className="heatmap-zoom" transform={`translate(${zoomLeftEdge},${zoomTopEdge})`}>
        <title>Zoom in on this selection</title>
        <rect x="0" y="0" width="26" height="26" rx="4"></rect>
        <g transform="translate(1.5,1)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8" fillOpacity="0"></circle>
          <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
          <line x1="11" y1="8" x2="11" y2="14"></line>
          <line x1="8" y1="11" x2="14" y2="11"></line>
        </g>
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
    const interpolator = (v: number, selected: boolean) =>
      selected
        ? heatmapGreens[Math.floor(((heatmapGreens.length - 1) * (v - min)) / (max - min))]
        : heatmapPurples[Math.floor(((heatmapPurples.length - 1) * (v - min)) / (max - min))];

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
                <rect fill="#f3f3f3" x="0" y="0" width={width} height={height}></rect>
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
                <>
                  <rect
                    x={selection.x}
                    y={selection.y}
                    width={selection.width}
                    height={selection.height}
                    fillOpacity="0"
                    stroke="#f00"></rect>
                  {this.maybeRenderZoomButton(selection)}
                </>
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
