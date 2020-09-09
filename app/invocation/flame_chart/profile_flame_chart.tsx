import colors from "buildbuddy/app/components/flame_chart/colors";
import Timeline, {
  BLOCK_HEIGHT,
  TimelineBlock,
  TimelineBlocks,
} from "buildbuddy/app/components/flame_chart/timeline";
import { memoizeSingleArgumentFunction } from "buildbuddy/app/util/memo";
import React from "react";
import { buildThreadTimelines, ThreadEvent, TimelineEvent } from "./profile_model";

// Tweakable values
const SECTION_OPACITY = 0.1;
const SECTION_LABEL_HEIGHT = 20;
const SECTION_PADDING_TOP = 8;
const SECTION_PADDING_BOTTOM = 8;
const BLOCK_VERTICAL_GAP = 1;

// Constant values
const MICROSECONDS_TO_SECONDS = 0.000001;
const SECTION_OPACITY_HEX = floatToHexCouple(SECTION_OPACITY);

export type ProfileFlameChartProps = {
  profile: {
    traceEvents: TimelineEvent[];
  };
};

export type SectionDecoration = {
  name: string;
  fill: string;
  y: number;
  height: number;
};

export type FlameChartModel = {
  sections: SectionDecoration[];
  blocks: TimelineBlock[];
  error?: string;
};

export type ProfileTimelineBlock = TimelineBlock & {
  event: ThreadEvent;
};

export type BlockInfoProps = { block: ProfileTimelineBlock };

// TODO: empty state for chart

type ProfileFlameChartState = {
  hoveredBlock: ProfileTimelineBlock | null;
};

export default class ProfileFlameChart extends React.Component<
  ProfileFlameChartProps,
  ProfileFlameChartState
> {
  state: ProfileFlameChartState = { hoveredBlock: null };

  private chartModel: FlameChartModel;

  constructor(props: ProfileFlameChartProps) {
    super(props);

    this.chartModel = buildFlameChartModel(props.profile.traceEvents);
  }

  private onHoverBlock(hoveredBlock: ProfileTimelineBlock) {
    this.setState({ hoveredBlock });
  }

  render() {
    const sectionDecorations = (
      <div>
        {this.chartModel.sections.map(({ name, height, fill }) => (
          <div
            style={{
              height,
              // background: fill,
              boxShadow: "0 1px -3px rgba(0, 0, 0, 0.12) inset",
              borderBottom: "1px solid #aaa",
              boxSizing: "border-box",
            }}
          >
            <div
              style={{
                paddingLeft: 8,
                fontSize: 13,
                fontWeight: "bold",
                height: SECTION_LABEL_HEIGHT,
                background: "rgba(0, 0, 0, 0.05)",
                boxShadow: "0 1px 3px rgba(0, 0, 0, 0.27)",
              }}
            >
              {name}
            </div>
          </div>
        ))}
      </div>
    );

    return (
      <>
        <div className="profile-flame-chart">
          <Timeline secondsPerX={MICROSECONDS_TO_SECONDS} sectionDecorations={sectionDecorations}>
            <TimelineBlocks
              blocks={this.chartModel.blocks}
              onHover={this.onHoverBlock.bind(this)}
            />
          </Timeline>
        </div>
        <BlockInfo block={this.state.hoveredBlock} />
      </>
    );
  }
}

export function BlockInfo({ block }: BlockInfoProps) {
  const height = 64;
  if (!block) {
    return <div style={{ height }}>Hover a block in the flame chart to see more info.</div>;
  }
  const {
    event: { name, cat },
  } = block;
  return (
    <div style={{ height }}>
      <div style={{ fontSize: 16, fontWeight: "bold" }}>{name}</div>
      <div style={{ color: "#888" }}>{cat}</div>
    </div>
  );
}

export function buildFlameChartModel(events: TimelineEvent[]): FlameChartModel {
  let currentThreadY = 0;

  const timelines = buildThreadTimelines(events);
  const sections: SectionDecoration[] = [];
  const blocks: TimelineBlock[] = [];

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
              fill: getColor(`${cat}#${name}`),
              y:
                currentThreadY +
                depth * (BLOCK_HEIGHT + BLOCK_VERTICAL_GAP) +
                SECTION_LABEL_HEIGHT +
                SECTION_PADDING_TOP,
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
      fill: `${getColor(threadName)}${SECTION_OPACITY_HEX}`,
    });
    currentThreadY += sectionHeight;
  }

  return {
    sections,
    blocks,
  };
}

function floatToHexCouple(value: number) {
  return Math.floor(value * 256)
    .toString(16)
    .padStart(2, "0");
}

/**
 * Returns the next color from the palette for the given string ID.
 *
 * Subsequent calls for the same ID will return the same color.
 */
const getColor: (id: string) => string = (() => {
  let paletteIndex = 0;
  return memoizeSingleArgumentFunction((_: string) => {
    paletteIndex = (paletteIndex + 1) % colors.length;
    return colors[paletteIndex];
  });
})();
