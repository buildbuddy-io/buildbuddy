import colors from "./colors";
import { buildThreadTimelines, ThreadEvent, TraceEvent } from "./profile_model";
import {
  BLOCK_HEIGHT,
  BLOCK_VERTICAL_GAP,
  SECTION_LABEL_HEIGHT,
  SECTION_PADDING_BOTTOM,
  SECTION_PADDING_TOP,
} from "./style_constants";

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

export function buildFlameChartModel(events: TraceEvent[]): FlameChartModel {
  let currentThreadY = 0;

  const timelines = buildThreadTimelines(events);
  const sections: SectionDecorationModel[] = [];
  const blocks: BlockModel[] = [];

  for (const { threadName, events, maxDepth } of timelines) {
    blocks.push(
      ...events
        .map((event) => {
          const { ts, dur, cat, tid, name, depth } = event;
          if (ts === undefined || dur === undefined || cat === undefined || tid === undefined) {
            console.debug("Skipping trace event", { ts, dur, cat, tid });
            return null;
          }
          return {
            rectProps: {
              x: ts,
              width: dur,
              y:
                currentThreadY +
                depth * (BLOCK_HEIGHT + BLOCK_VERTICAL_GAP) +
                SECTION_LABEL_HEIGHT +
                SECTION_PADDING_TOP,
              fill: getColor(`${cat}#${name}`),
            },
            event,
          };
        })
        .filter(Boolean)
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
