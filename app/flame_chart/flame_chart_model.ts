import colors from "./colors";
import { buildThreadTimelines, ThreadEvent, TraceEvent } from "./profile_model";
import {
  BLOCK_HEIGHT,
  BLOCK_VERTICAL_GAP,
  SECTION_LABEL_HEIGHT,
  SECTION_PADDING_BOTTOM,
  SECTION_PADDING_TOP,
} from "./style_constants";

const MICROSECONDS_PER_SECOND = 1000 * 1000;

export type FlameChartModel = {
  sections: SectionDecorationModel[];
  blocks: BlockModel[];
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

/**
 * Returns a random color for the given ID.
 *
 * Calls for the same ID will return the same color.
 *
 * All colors returned have the same approximate perceived brightness
 * to avoid issues with color contrast.
 */
const getColor = (id: string) => colors[Math.abs(hash(id) % colors.length)];

function hash(value: string) {
  let hash = 0;
  for (let i = 0; i < value.length; i++) {
    hash = ((hash << 5) - hash + value.charCodeAt(i)) | 0;
  }
  return hash;
}

export function buildFlameChartModel(events: TraceEvent[], { visibilityThreshold = 0 } = {}): FlameChartModel {
  let currentThreadY = 0;

  const timelines = buildThreadTimelines(events, { visibilityThreshold });
  const sections: SectionDecorationModel[] = [];
  const blocks: BlockModel[] = [];

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
            fill: getColor(`${cat}#${name}`),
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

  return {
    sections,
    blocks,
  };
}
