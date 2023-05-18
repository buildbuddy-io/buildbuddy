import { getUniformBrightnessColor, getMaterialChartColorPairs } from "../util/color";
import { buildThreadTimelines, buildTimeSeries, ThreadEvent, TraceEvent, TimeSeriesEvent } from "./profile_model";
import {
  BLOCK_HEIGHT,
  BLOCK_VERTICAL_GAP,
  TIME_SERIES_VERTICAL_GAP,
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
};

export function buildFlameChartModel(events: TraceEvent[], { visibilityThreshold = 0 } = {}): FlameChartModel {
  let currentThreadY = 0;

  const timelines = buildThreadTimelines(events, { visibilityThreshold });
  const timeSeries = buildTimeSeries(events);
  const sections: SectionDecorationModel[] = [];
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

  let index = 0;
  for (const { name, events } of timeSeries) {
    const points = new Map<number, PointModel>();
    const sectionHeight =
      SECTION_LABEL_HEIGHT +
      SECTION_PADDING_TOP +
      SECTION_PADDING_BOTTOM +
      TIME_SERIES_VERTICAL_GAP +
      TIME_SERIES_HEIGHT;
    let lowerBoundY = currentThreadY + sectionHeight;
    let upperBoundY = currentThreadY + SECTION_LABEL_HEIGHT;
    let d = `M 0 ${lowerBoundY} `;

    const [color, lightColor] = getMaterialChartColorPairs(index);
    const yMax = Math.max(...events.map((event) => event.value));
    console.log(`yMax ${yMax}`);
    console.log(`lowerBoundY ${lowerBoundY}`);
    for (const event of events) {
      const { name, ts, value } = event;
      const x = ts / MICROSECONDS_PER_SECOND;
      const y = lowerBoundY - (value / yMax) * TIME_SERIES_HEIGHT;
      d += `L ${x} ${y} `;
      points.set(Math.round(x), {
        lineProps: {
          x1: x,
          y1: y,
          x2: x,
          y2: y,
          stroke: color,
        },
        event: event,
      });
    }
    d += `V ${lowerBoundY} `;
    lines.push({
      pathProps: {
        d: d,
        fill: lightColor,
        stroke: color,
      },
      upperBoundY: upperBoundY,
      lowerBoundY: lowerBoundY,
      pointsByXCoord: points,
    });
    sections.push({
      name: name,
      y: currentThreadY,
      height: sectionHeight,
    });
    currentThreadY += sectionHeight;
    index++;
  }

  return {
    sections,
    blocks,
    lines,
  };
}
