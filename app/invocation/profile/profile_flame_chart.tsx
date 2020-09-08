import { memoize1 } from "buildbuddy/app/lib/memo";
import React from "react";
import palette from "./palette";
import { TimelineEvent, buildThreadTimelines, ThreadEvent } from "./profile_model";
import Timeline, { useTimeline } from "./timeline";

type ProfileFlameChartProps = {
  profile: {
    traceEvents: TimelineEvent[];
  };
};

const MICROSECONDS_TO_SECONDS = 0.000001;

const BLOCK_HEIGHT = 16;

// TODO: empty state

type ChartModel = {
  blocks: TimelineBlock[];
  error?: string;
};

export default function ProfileFlameChart({ profile }: ProfileFlameChartProps) {
  const chartModel: ChartModel = React.useMemo(() => {
    try {
      return buildChartModel(profile.traceEvents);
    } catch (e) {
      console.error(e);
      return { blocks: [], error: String(e) };
    }
  }, [profile]);

  const [hoveredBlock, setHoveredBlock] = React.useState(null);

  return (
    <>
      <div className="profile-flame-chart">
        <SvgTimeline>
          <g transform={`scale(${MICROSECONDS_TO_SECONDS} 1)`}>
            <TimelineBlocks blocks={chartModel.blocks} onHover={setHoveredBlock} />
          </g>
        </SvgTimeline>
      </div>
      <BlockInfo block={hoveredBlock} />
      {/* <pre>{JSON.stringify(profile.traceEvents, null, 2)}</pre> */}
    </>
  );
}

export function BlockInfo({ block }: { block: TimelineBlock }) {
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

export function TimelineBlocks({
  timeline,
  blocks,
  onHover,
}: {
  timeline: Timeline;
  blocks: TimelineBlock[];
  onHover: (block: TimelineBlock) => void;
}) {
  React.useEffect(() => {
    if (!timeline) return;
    const rightBoundary = Math.max(...blocks.map(({ rectProps: { x, width } }) => x + width));
    timeline.setGridSize(rightBoundary * MICROSECONDS_TO_SECONDS);
    console.debug("timeline: set grid size to ", rightBoundary);
  }, [timeline, blocks]);

  const state = React.useMemo<{
    hoveredBlockIndex?: number;
    hoveredBlock?: SVGRectElement;
  }>(() => ({}), []);

  const onMouseMove = React.useCallback(
    (e: React.MouseEvent<SVGGElement, MouseEvent>) => {
      if ((e.target as Element).tagName !== "rect") return;
      const rect = e.target as SVGRectElement;
      const index = Number(rect.dataset["index"]);
      if (state.hoveredBlock) {
        state.hoveredBlock.classList.remove("hover");
      }
      state.hoveredBlock = rect;
      rect.classList.add("hover");
      onHover(blocks[index]);
    },
    [onHover]
  );

  return (
    <g onMouseMove={onMouseMove}>
      {blocks.map((block: any, i: number) => (
        <TimelineBlock key={i} index={i} block={block} />
      ))}
    </g>
  );
}

function TimelineBlock({ index, block: { rectProps } }: { index: number; block: TimelineBlock }) {
  return (
    <rect
      {...rectProps}
      height={BLOCK_HEIGHT}
      shapeRendering="crispEdges"
      vectorEffect="non-scaling-stroke"
      data-index={index}
    />
  );
}

export type TimelineBlock = {
  rectProps: {
    x: number;
    y: number;
    width: number;
    fill: string;
  };
  event: ThreadEvent;
};

export function buildChartModel(events: TimelineEvent[]): ChartModel {
  const blockVerticalGap = 1;
  const threadVerticalGap = 32;
  const threadYPositions = [];
  let currentThreadY = threadVerticalGap;

  return {
    blocks: buildThreadTimelines(events).flatMap(({ events, maxDepth }) => {
      const blocks = events
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
              fill: getColor(`${cat}`),
              y: currentThreadY + depth * (BLOCK_HEIGHT + blockVerticalGap),
            },
            event,
          };
        })
        .filter((e) => e);

      currentThreadY += threadVerticalGap + maxDepth * (BLOCK_HEIGHT + blockVerticalGap);

      return blocks;
    }),
  };
}

/**
 * Returns the next color from the palette for the given string ID.
 *
 * Subsequent calls for the same ID will return the same color.
 */
const getColor: (id: string) => string = (() => {
  let paletteIndex = 0;
  return memoize1((_: string) => {
    paletteIndex = (paletteIndex + 1) % palette.length;
    return palette[paletteIndex];
  });
})();
