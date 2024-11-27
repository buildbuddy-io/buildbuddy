import React from "react";
import moment from "moment";
import Long from "long";
import { clamp } from "../../../app/util/math";
import { stats } from "../../../proto/stats_ts_proto";
import { ScaleBand, scaleBand } from "d3-scale";
import { useResizeDetector } from "react-resize-detector";
import { pinBottomLeftOffsetFromMouse, MouseCoords, Tooltip } from "../../../app/components/tooltip/tooltip";

interface HeatmapProps {
  heatmapData: stats.GetStatHeatmapResponse;
  valueFormatter: (value: number) => string;
  metricBucketName: string;
  metricBucketFormatter: (value: number) => string;
  selectionCallback?: (s?: HeatmapSelection) => void;
  zoomCallback?: (s?: HeatmapSelection) => void;
  selectedData?: HeatmapSelection;
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

// If the heatmap we're showing contains more than 49 hours (two daysish) of
// data, then we'll only show tick marks for days.
const MINIMUM_DURATION_FOR_DAY_LABELS_MICROS = 1e6 * 60 * 60 * 49;

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

function HeatmapComponent(props: HeatmapProps) {
  // This pair indicates the cells in the heatmap that the user selected.  The
  // first entry is the cell that the user clicked on first when making the
  // selection.
  const [_, setSelectionToRender] = React.useState<[SelectedCellData, SelectedCellData] | undefined>();
  let svgRef = React.createRef<SVGSVGElement>();
  let chartGroupRef = React.createRef<SVGGElement>();
  let xScaleBand: ScaleBand<number> = scaleBand();
  let yScaleBand: ScaleBand<number> = scaleBand();

  React.useEffect(() => {
    document.addEventListener("mousemove", onMouseMove);

    return () => {
      document.removeEventListener("mousemove", onMouseMove);
    };
  });

  // These ordered pairs indicate which cell was first clicked on by the user.
  // By keeping them in order, we ensure that as the user moves their mouse, the
  // selection will "pivot" around that cell.
  let pendingClick: [SelectedCellData, SelectedCellData] | undefined;

  const renderYBucketValue: (value: number) => string = (v: number) => {
    return props.metricBucketFormatter(v);
  };

  const getHeight: () => number = () => {
    return 275;
  };

  const numHeatmapRows: () => number = () => {
    return Math.max(props.heatmapData.bucketBracket.length - 1, 1);
  };

  const computeBucket: (x: number, y: number) => SelectedCellData | undefined = (x: number, y: number) => {
    if (!svgRef.current || !chartGroupRef.current) {
      return undefined;
    }

    // Clamp client x/y to chart area bounding client rect. This allows dragging
    // the mouse outside the chart bounds while making a selection.
    const chartRect = chartGroupRef.current.getBoundingClientRect();
    x = clamp(x, chartRect.left + 1, chartRect.left + chartRect.width - 1);
    y = clamp(y, chartRect.top + 1, chartRect.top + chartRect.height - 1);

    const { top, left } = svgRef.current.getBoundingClientRect();
    const mouseX = x - left - CHART_MARGINS.left - xScaleBand.step() / 2;
    const mouseY = y - top - CHART_MARGINS.top - yScaleBand.step() / 2;
    const stepX = Math.round(mouseX / xScaleBand.step());
    const stepY = yScaleBand.domain().length - Math.round(mouseY / yScaleBand.step()) - 1;

    const timestamp = props.heatmapData.timestampBracket[stepX];
    const metric = props.heatmapData.bucketBracket[stepY];
    if (timestamp === undefined || metric === undefined) {
      return undefined;
    }
    const column = props.heatmapData.column[stepX];
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
  };

  const overlapsWithZoomButton: (c: MouseCoords) => boolean = (c: MouseCoords) => {
    if (!svgRef.current) {
      return false;
    }
    const el = svgRef.current.querySelector(".heatmap-zoom");
    if (!el) {
      return false;
    }
    const r = el.getBoundingClientRect();
    return c.clientX >= r.x && c.clientX <= r.x + r.width && c.clientY >= r.y && c.clientY <= r.y + r.height;
  };

  const renderTooltip: (c: MouseCoords) => JSX.Element | null = (c: MouseCoords) => {
    if (pendingClick) {
      return null;
    }
    const data = computeBucket(c.clientX, c.clientY);
    if (!data) {
      return null;
    }
    if (overlapsWithZoomButton(c)) {
      return null;
    }
    const metricBucket =
      renderYBucketValue(+data.metric) +
      " - " +
      renderYBucketValue(+props.heatmapData.bucketBracket[data.metricBucketIndex + 1]);

    return (
      <div className="trend-chart-hover">
        <div className="trend-chart-hover-label">Start: {moment(+data.timestamp / 1000).format("lll")}</div>
        <div className="trend-chart-hover-label">
          End: {moment(+props.heatmapData.timestampBracket[data.timestampBucketIndex + 1] / 1000).format("lll")}
        </div>
        <div className="trend-chart-hover-label">
          {props.metricBucketName}: {metricBucket}
        </div>
        <div className="trend-chart-hover-value">{props.valueFormatter(data.value)}</div>
      </div>
    );
  };

  const onMouseDown: (e: React.MouseEvent<SVGSVGElement, MouseEvent>) => void = (
    e: React.MouseEvent<SVGSVGElement, MouseEvent>
  ) => {
    if (e.target instanceof SVGElement && e.target.closest(".heatmap-zoom")) {
      maybeFireZoomCallback();
      return;
    }
    const data = computeBucket(e.clientX, e.clientY);
    if (!data) {
      return;
    }
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp, { once: true });
    pendingClick = [data, data];
    setSelectionToRender(pendingClick);
  };

  const onMouseMove: (e: MouseEvent) => void = (e: MouseEvent) => {
    if (!pendingClick) {
      return;
    }
    const data = computeBucket(e.clientX, e.clientY);
    if (!data) {
      return;
    }
    pendingClick = [pendingClick[0], data];
    setSelectionToRender(pendingClick);
  };

  const convertSelectionToCells: (selection: HeatmapSelection) => [SelectedCellData, SelectedCellData] | null = (
    selection: HeatmapSelection
  ) => {
    if (!props.heatmapData.timestampBracket || !props.heatmapData.bucketBracket) {
      return null;
    }
    const longNumberCompare = (n: number) => (l: Long) => +l === n;
    const lowDate = selection.dateRangeMicros.startInclusive;
    const lowDateIndex = props.heatmapData.timestampBracket.findIndex(longNumberCompare(lowDate));
    const highDateIndex =
      props.heatmapData.timestampBracket.findIndex(longNumberCompare(selection.dateRangeMicros.endExclusive)) - 1;

    const lowMetric = selection.bucketRange.startInclusive;
    const lowMetricIndex = props.heatmapData.bucketBracket.findIndex(longNumberCompare(lowMetric));
    const highMetricIndex =
      props.heatmapData.bucketBracket.findIndex(longNumberCompare(selection.bucketRange.endExclusive)) - 1;

    if (lowDateIndex < 0 || highDateIndex < 0 || lowMetricIndex < 0 || highMetricIndex < 0) {
      return null;
    }

    const highDate = +props.heatmapData.timestampBracket[highDateIndex];
    const highMetric = +props.heatmapData.bucketBracket[highMetricIndex];

    return [
      {
        timestamp: lowDate,
        timestampBucketIndex: lowDateIndex,
        metric: lowMetric,
        metricBucketIndex: lowMetricIndex,
        value: +props.heatmapData.column[lowDateIndex].value[lowMetricIndex],
      },
      {
        timestamp: highDate,
        timestampBucketIndex: highDateIndex,
        metric: highMetric,
        metricBucketIndex: highMetricIndex,
        value: +props.heatmapData.column[highDateIndex].value[highMetricIndex],
      },
    ];
  };

  const convertCellsToSelection: (selectedCells: [SelectedCellData, SelectedCellData]) => HeatmapSelection = (
    selectedCells: [SelectedCellData, SelectedCellData]
  ) => {
    const t1Index = selectedCells[0].timestampBucketIndex;
    const t2Index = selectedCells[1].timestampBucketIndex;

    const dateRangeMicros = {
      startInclusive: Math.min(
        +props.heatmapData.timestampBracket[t1Index],
        +props.heatmapData.timestampBracket[t2Index]
      ),
      endExclusive: Math.max(
        +props.heatmapData.timestampBracket[t1Index + 1],
        +props.heatmapData.timestampBracket[t2Index + 1]
      ),
    };
    const m1Index = selectedCells[0].metricBucketIndex;
    const m2Index = selectedCells[1].metricBucketIndex;
    const bucketRange = {
      startInclusive: Math.min(+props.heatmapData.bucketBracket[m1Index], +props.heatmapData.bucketBracket[m2Index]),
      endExclusive: Math.max(
        +props.heatmapData.bucketBracket[m1Index + 1],
        +props.heatmapData.bucketBracket[m2Index + 1]
      ),
    };

    let eventsSelected = 0;
    for (let i = Math.min(t1Index, t2Index); i <= Math.max(t1Index, t2Index); i++) {
      for (let j = Math.min(m1Index, m2Index); j <= Math.max(m1Index, m2Index); j++) {
        eventsSelected += +props.heatmapData.column[i].value[j];
      }
    }

    return { dateRangeMicros, bucketRange, eventsSelected };
  };

  const maybeFireSelectionCallback: (selectedCells: [SelectedCellData, SelectedCellData]) => void = (
    selectedCells: [SelectedCellData, SelectedCellData]
  ) => {
    const selection = convertCellsToSelection(selectedCells);
    if (selection && props.selectionCallback) {
      props.selectionCallback(selection);
    }
  };

  const maybeFireZoomCallback: () => void = () => {
    if (props.selectedData && props.zoomCallback) {
      props.zoomCallback(props.selectedData);
    }
  };

  const onMouseUp: (e: MouseEvent) => void = (e: MouseEvent) => {
    if (!pendingClick) {
      return;
    }

    document.removeEventListener("mousemove", onMouseMove);

    const data = computeBucket(e.clientX, e.clientY);
    // If the user has dragged the mouse of the heatmap or only clicked one
    // cell, then we just use the already-pending click.
    if (!data || (pendingClick[0].metric == data.metric && pendingClick[0].timestamp == data.timestamp)) {
      const selectedData = pendingClick;
      pendingClick = undefined;
      maybeFireSelectionCallback(selectedData);
    } else {
      const selectedData: [SelectedCellData, SelectedCellData] = [pendingClick[0], data];
      pendingClick = undefined;
      maybeFireSelectionCallback(selectedData);
    }
    return;
  };

  const computeSelectionData: () => SelectionData | undefined = () => {
    const selectionToDraw = pendingClick || (props.selectedData && convertSelectionToCells(props.selectedData));
    if (!selectionToDraw) {
      return undefined;
    }

    const aScreenX = xScaleBand(+selectionToDraw[0].timestamp);
    const aScreenY = yScaleBand(+selectionToDraw[0].metric);
    const bScreenX = xScaleBand(+selectionToDraw[1].timestamp);
    const bScreenY = yScaleBand(+selectionToDraw[1].metric);

    if (aScreenX === undefined || aScreenY === undefined || bScreenX === undefined || bScreenY === undefined) {
      return undefined;
    }

    const top = Math.min(aScreenY, bScreenY) + CHART_MARGINS.top;
    const height = Math.max(aScreenY, bScreenY) + CHART_MARGINS.top + yScaleBand.step() - top;
    const left = Math.min(aScreenX, bScreenX) + CHART_MARGINS.left;
    const width = Math.max(aScreenX, bScreenX) + CHART_MARGINS.left + xScaleBand.step() - left;
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
  };

  const getCellColor: (
    x: number,
    y: number,
    value: number,
    interpolator: (v: number, selected: boolean) => string,
    s?: SelectionData
  ) => string = (
    x: number,
    y: number,
    value: number,
    interpolator: (v: number, selected: boolean) => string,
    s?: SelectionData
  ) => {
    const selected =
      !!s && x >= s.selectionXStart && x <= s.selectionXEnd && y >= s.selectionYStart && y <= s.selectionYEnd;
    return interpolator(value, selected);
  };

  const computeXAxisLabel: (timestampMicros: number, rounding: moment.unitOfTime.StartOf) => string | undefined = (
    timestampMicros: number,
    rounding: moment.unitOfTime.StartOf
  ) => {
    const timestampMillis = timestampMicros / 1e3;
    const time = moment(timestampMillis);
    const start = moment(timestampMillis).startOf(rounding);
    if (!time.isSame(start)) {
      return undefined;
    }
    if (rounding === "day") {
      return time.format("MMM D");
    }
    const startOfDay = moment(timestampMillis).startOf("day");
    if (time.isSame(startOfDay)) {
      return time.format("MMM D");
    }
    return time.format("HH:mm");
  };

  const renderXAxis: (width: number) => JSX.Element | null = (width: number) => {
    if (!width) {
      return null;
    }

    const timeBrackets = props.heatmapData.timestampBracket;
    const heatmapTimespan = +timeBrackets[timeBrackets.length - 1] - +timeBrackets[0];
    const labelType = heatmapTimespan > MINIMUM_DURATION_FOR_DAY_LABELS_MICROS ? "day" : "hour";

    const numColumns = props.heatmapData.column.length || 1;
    const labelSpacing = Math.ceil(numColumns / Math.min(numColumns, width / TICK_LABEL_SPACING_MAGIC_NUMBER));
    let lastLabelDistance = labelSpacing;

    return (
      <g color="#666" transform={`translate(${CHART_MARGINS.left}, ${getHeight() - CHART_MARGINS.bottom})`}>
        <line stroke="#666" x1="0" y1="0" x2={width} y2="0"></line>
        {xScaleBand.domain().map((v, i) => {
          lastLabelDistance++;
          if (lastLabelDistance < labelSpacing) {
            return null;
          }
          const label = computeXAxisLabel(v /* micros */, labelType);
          if (!label) {
            return null;
          }
          lastLabelDistance = 0;
          return (
            <g transform={`translate(${xScaleBand.bandwidth() * i}, 0)`}>
              <text fill="#666" x={xScaleBand.bandwidth() / 2} y="18" fontSize="12" textAnchor="middle">
                {label}
              </text>
              <line stroke="#666" x1="0" y1="0" x2="0" y2="4"></line>
            </g>
          );
        })}
      </g>
    );
  };

  const renderYAxis: (height: number) => JSX.Element | null = (height: number) => {
    if (!height) {
      return null;
    }
    const numRows = numHeatmapRows();
    const yTickMod = Math.ceil(numRows / Math.min(numRows, height / TICK_LABEL_SPACING_MAGIC_NUMBER));

    return (
      <g color="#666" transform={`translate(${CHART_MARGINS.left}, ${getHeight() - CHART_MARGINS.bottom - height})`}>
        <line stroke="#666" x1="0" y1="0" x2="0" y2={height}></line>
        {yScaleBand.domain().map((v, i) => {
          return (
            <g transform={`translate(0, ${height - yScaleBand.bandwidth() * (numRows - 1 - i)})`}>
              {(numRows - 1 - i) % yTickMod == 0 && (
                <text fill="#666" x="-8" y="3" fontSize="12" textAnchor="end">
                  {renderYBucketValue(v)}
                </text>
              )}
              <line stroke="#666" x1="0" y1="0" x2="-4" y2="0"></line>
            </g>
          );
        })}
      </g>
    );
  };

  const maybeRenderZoomButton: (positioningData: SelectionData, width: number) => JSX.Element | null = (
    positioningData: SelectionData,
    width: number
  ) => {
    if (!props.zoomCallback || pendingClick) {
      return null;
    }
    if (!props.selectedData || props.selectedData.eventsSelected < 2) {
      return null;
    }

    const selectionRightEdge = positioningData.x + positioningData.width;
    let zoomLeftEdge = selectionRightEdge + ZOOM_BUTTON_ATTRIBUTES.sideMargin;
    let zoomTopEdge = positioningData.y;

    if (selectionRightEdge + ZOOM_BUTTON_ATTRIBUTES.width + 2 * ZOOM_BUTTON_ATTRIBUTES.sideMargin > width) {
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
  };

  const { width, ref } = useResizeDetector({
    handleHeight: false,
    refreshMode: "throttle",
    refreshRate: 500,
  });

  const normalizedWidth = Math.max(width || 0, 400);
  const chartWidth = normalizedWidth - CHART_MARGINS.left - CHART_MARGINS.right;
  const chartHeight = getHeight() - CHART_MARGINS.top - CHART_MARGINS.bottom;

  const xDomain = props.heatmapData.timestampBracket.slice(0, -1).map((v) => +v);
  const yDomain = props.heatmapData.bucketBracket
    .slice(0, -1)
    .map((v) => +v)
    .reverse();

  xScaleBand = scaleBand<number>().range([0, chartWidth]).domain(xDomain);
  yScaleBand = scaleBand<number>().range([0, chartHeight]).domain(yDomain);

  let min = 0;
  let max = 0;
  props.heatmapData.column.forEach((column) => {
    column.value.forEach((value) => {
      min = Math.min(+value, min);
      max = Math.max(+value, max);
    });
  });

  const selection = computeSelectionData();
  const interpolator = (v: number, selected: boolean) =>
    selected
      ? heatmapGreens[Math.floor(((heatmapGreens.length - 1) * (v - min)) / (max - min))]
      : heatmapPurples[Math.floor(((heatmapPurples.length - 1) * (v - min)) / (max - min))];

  return (
    <div ref={ref}>
      <div style={{ position: "relative" }}>
        <Tooltip pin={pinBottomLeftOffsetFromMouse} renderContent={(c) => renderTooltip(c)}>
          <svg
            className="heatmap-svg"
            onMouseDown={(e) => onMouseDown(e)}
            width={normalizedWidth}
            height={getHeight()}
            ref={svgRef}>
            <g transform={`translate(${CHART_MARGINS.left}, ${CHART_MARGINS.top})`} ref={chartGroupRef}>
              <rect fill="#f3f3f3" x="0" y="0" width={chartWidth} height={chartHeight}></rect>
              {props.heatmapData.column.map((column, xIndex) => (
                <>
                  {column.value.map((value, yIndex) =>
                    +value <= 0 ? null : (
                      <rect
                        x={xScaleBand(+column.timestampUsec) || 0}
                        y={yScaleBand(+props.heatmapData.bucketBracket[yIndex]) || 0}
                        width={xScaleBand.bandwidth() || 0}
                        height={yScaleBand.bandwidth() || 0}
                        fill={getCellColor(xIndex, yIndex, +value, interpolator, selection)}></rect>
                    )
                  )}
                </>
              ))}
            </g>
            {renderXAxis(chartWidth)}
            {renderYAxis(chartHeight)}
            {selection && (
              <>
                <rect
                  x={selection.x}
                  y={selection.y}
                  width={selection.width}
                  height={selection.height}
                  fillOpacity="0"
                  stroke="#f00"></rect>
                {maybeRenderZoomButton(selection, normalizedWidth)}
              </>
            )}
          </svg>
        </Tooltip>
      </div>
    </div>
  );
}

export default HeatmapComponent;
