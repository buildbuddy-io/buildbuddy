import { getUniformBrightnessColor, getMaterialChartColor, getLightMaterialChartColor } from "../util/color";
import { buildThreadTimelines, buildTimeSeries, ThreadEvent, TraceEvent, TimeSeriesEvent } from "./profile_model";
import capabilities from "../capabilities/capabilities";
import {
  BLOCK_HEIGHT,
  BLOCK_VERTICAL_GAP,
  SECTION_LABEL_HEIGHT,
  SECTION_PADDING_BOTTOM,
  SECTION_PADDING_TOP,
  TIME_SERIES_HEIGHT,
  POINT_RADIUS,
} from "./style_constants";

const MICROSECONDS_PER_SECOND = 1000 * 1000;

export type FlameChartModel = {
  sections: SectionDecorationModel[];
  blocks: BlockModel[];
  lines: LineModel[];
  lineSections: SectionDecorationModel[];
};

export type SectionDecorationModel = {
  name: string;
  y: number;
  height: number;
};

export type BlockModel = {
  rectProps: {
    x: number;
    y: number;
    width: number;
    fill: string;
  };
  event: ThreadEvent;
};

export type PointModel = {
  lineProps: {
    x1: number;
    y1: number;
    x2: number;
    y2: number;
    stroke: string;
  };
  event: TimeSeriesEvent;
};

export type LineModel = {
  pathProps: {
    d: string;
    stroke: string;
    fill: string;
  };
  pointsByXCoord: Map<number, PointModel>;
  upperBoundY: number;
  lowerBoundY: number;
  upperBoundX: number;
  // The interval between x coords.
  dt: number;
};

export function buildFlameChartModel(events: TraceEvent[], { visibilityThreshold = 0 } = {}): FlameChartModel {
  let currentThreadY = 0;

  const timelines = buildThreadTimelines(events, { visibilityThreshold });
  const timeSeries = buildTimeSeries(events);
  const sections: SectionDecorationModel[] = [];
  const lineSections: SectionDecorationModel[] = [];
  const blocks: BlockModel[] = [];
  const lines: LineModel[] = [];

  for (const { threadName, events, maxDepth } of timelines) {
    if (!events.length) continue;

    blocks.push(
      ...events.map((event) => {
        const { ts, dur, cat, name, depth } = event;

        return {
          rectProps: {
            x: ts / MICROSECONDS_PER_SECOND,
            width: dur / MICROSECONDS_PER_SECOND,
            y:
              currentThreadY + depth * (BLOCK_HEIGHT + BLOCK_VERTICAL_GAP) + SECTION_LABEL_HEIGHT + SECTION_PADDING_TOP,
            fill: getUniformBrightnessColor(`${cat}#${name}`),
          },
          event,
        };
      })
    );

    const sectionHeight =
      SECTION_LABEL_HEIGHT +
      SECTION_PADDING_TOP +
      SECTION_PADDING_BOTTOM +
      (maxDepth + 1) * (BLOCK_HEIGHT + BLOCK_VERTICAL_GAP) -
      BLOCK_VERTICAL_GAP;

    sections.push({
      name: threadName,
      y: currentThreadY,
      height: sectionHeight,
    });
    currentThreadY += sectionHeight;
  }

  if (capabilities.config.timeseriesChartsInTimingProfileEnabled) {
    currentThreadY = 0;
    let index = 0;
    const sectionHeight = SECTION_LABEL_HEIGHT + SECTION_PADDING_TOP + SECTION_PADDING_BOTTOM + TIME_SERIES_HEIGHT;
    for (const { name, events } of timeSeries) {
      const points = new Map<number, PointModel>();
      let lowerBoundY = currentThreadY + sectionHeight;
      let upperBoundY = lowerBoundY - TIME_SERIES_HEIGHT;

      const darkColor = getMaterialChartColor(index);
      const lightColor = getLightMaterialChartColor(index);
      const yMax = Math.max(...events.map((event) => event.value));
      const xMax = Math.max(...events.map((event) => event.ts / MICROSECONDS_PER_SECOND));
      const xMin = Math.min(...events.map((event) => event.ts / MICROSECONDS_PER_SECOND));
      let d = `M ${xMin} ${lowerBoundY}`;
      const dt = events.length >= 2 ? (events[1].ts - events[0].ts) / MICROSECONDS_PER_SECOND : 1;
      for (const event of events) {
        const { name, ts, value } = event;
        const x = ts / MICROSECONDS_PER_SECOND;
        const y = lowerBoundY - (value / yMax) * TIME_SERIES_HEIGHT;
        d += `L ${x} ${y} `;
        // We use the rounded x as lookup key (instead of the exact value of x)
        // when we render a reference vertical line and point on the path; so that
        // a vertical line and point that is closest to the mouse's x coordinate
        // will always show when the mouse hover on the graph. Otherwise, the
        // condition to show the reference line is too strict.
        points.set(Math.round(x / dt) * dt, {
          lineProps: {
            x1: x,
            y1: y,
            x2: x,
            y2: y,
            stroke: darkColor,
          },
          event: event,
        });
      }
      d += `V ${lowerBoundY} `;
      lines.push({
        pathProps: {
          d: d,
          fill: lightColor,
          stroke: darkColor,
        },
        upperBoundY: upperBoundY,
        lowerBoundY: lowerBoundY,
        upperBoundX: xMax,
        pointsByXCoord: points,
        dt: dt,
      });
      lineSections.push({
        name: name,
        y: currentThreadY,
        height: sectionHeight,
      });
      currentThreadY += sectionHeight;
      index++;
    }
  }

  return {
    sections,
    blocks,
    lines,
    lineSections,
  };
}
