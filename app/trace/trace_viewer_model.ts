import { getLightMaterialChartColor, getMaterialChartColor } from "../util/color";
import { TypedArrayBuilder } from "../util/typed_arrays";
import { Profile, Thread } from "./compact_trace";
import * as constants from "./constants";

/**
 * A trace event profile structured for easier rendering in the trace viewer.
 */
export type TraceViewerModel = {
  panels: PanelModel[];
  xMax: number;
};

export type PanelModel = {
  height: number;
  sections: SectionModel[];
};

export type SectionModel = {
  name: string;
  y: number;
  height: number;

  // A section will contain either a list of tracks or a line plot.

  tracks?: TrackModel[];
  linePlot?: LinePlotModel;
};

export type TrackModel = {
  thread: Thread;
  eventIndices: Uint32Array;
};

export type LinePlotModel = {
  xs: ArrayLike<number>;
  ys: ArrayLike<number>;
  yMax: number;
  darkColor: string;
  lightColor: string;
  unit?: string;
};

export function buildTraceViewerModel(trace: Profile, fitToContent?: boolean): TraceViewerModel {
  let panels = [buildEventsPanel(trace, fitToContent), buildLinePlotsPanel(trace, fitToContent)];
  // If there is no data available (e.g. the executor doesn't have timeseries
  // recording enabled yet) then the panel will be empty - just remove the panel
  // in this case since we don't handle this empty state in a good way yet.
  panels = panels.filter((panel) => panel.sections.length);

  return {
    panels,
    xMax: trace.xMax || computeXMax(panels),
  };
}

function buildEventsPanel(trace: Profile, fitToContent?: boolean): PanelModel {
  const sections: SectionModel[] = [];
  let sectionY = 0;
  for (const thread of trace.threads) {
    if (!thread.length) continue;

    const trackBuilders: TypedArrayBuilder<Uint32Array>[] = [];
    for (let eventIndex = 0; eventIndex < thread.length; eventIndex++) {
      const depth = thread.depth[eventIndex];
      const trackBuilder = (trackBuilders[depth] ??= TypedArrayBuilder.of(Uint32Array));
      trackBuilder.append(eventIndex);
    }
    const tracks = trackBuilders.map((builder) => ({
      thread,
      eventIndices: builder.toArray(),
    }));

    const sectionHeight =
      constants.SECTION_LABEL_HEIGHT +
      constants.SECTION_LABEL_PADDING_BOTTOM +
      constants.SECTION_PADDING_BOTTOM +
      tracks.length * constants.TRACK_HEIGHT +
      (tracks.length - 1) * constants.TRACK_VERTICAL_GAP;

    sections.push({
      name: thread.name,
      y: sectionY,
      height: sectionHeight,
      tracks,
    });
    sectionY += sectionHeight;
  }

  return {
    height: fitToContent
      ? constants.TIMESTAMP_HEADER_SIZE + sectionY + constants.BOTTOM_CONTROLS_HEIGHT + constants.SCROLLBAR_SIZE
      : constants.EVENTS_PANEL_HEIGHT,
    sections,
  };
}

function buildLinePlotsPanel(trace: Profile, fitToContent?: boolean): PanelModel {
  let sectionY = 0;
  let index = 0;
  const sectionHeight =
    constants.SECTION_LABEL_HEIGHT +
    constants.SECTION_LABEL_PADDING_BOTTOM +
    constants.TIME_SERIES_HEIGHT +
    constants.SECTION_PADDING_BOTTOM;
  const sections: SectionModel[] = [];
  for (const series of trace.timeseries) {
    let yMax = 0;
    for (let i = 0; i < series.val.length; i++) {
      const y = series.val[i];
      if (y > yMax) yMax = y;
    }
    sections.push({
      name: series.name,
      y: sectionY,
      height: sectionHeight,
      linePlot: {
        xs: series.ts,
        ys: series.val,
        yMax,
        darkColor: getMaterialChartColor(index),
        lightColor: getLightMaterialChartColor(index),
        unit: series.unit,
      },
    });
    sectionY += sectionHeight;
    index++;
  }

  return {
    height: fitToContent
      ? constants.TIMESTAMP_HEADER_SIZE + sectionY + constants.BOTTOM_CONTROLS_HEIGHT + constants.SCROLLBAR_SIZE
      : constants.LINE_PLOTS_PANEL_HEIGHT,
    sections,
  };
}

/**
 * Returns the max x-coordinate needed to render all of the data in the model.
 */
function computeXMax(panels: PanelModel[]): number {
  let width = 0;
  for (const panel of panels) {
    for (const section of panel.sections) {
      for (const track of section.tracks ?? []) {
        const eventCount = track.eventIndices.length;
        if (!eventCount) continue;
        const lastEventIndex = track.eventIndices[eventCount - 1];
        const trackWidth = track.thread.ts[lastEventIndex] + track.thread.dur[lastEventIndex];
        if (trackWidth > width) width = trackWidth;
      }
      if (section.linePlot) {
        if (!section.linePlot.xs.length) continue;
        const lastX = section.linePlot.xs[section.linePlot.xs.length - 1];
        if (lastX > width) width = lastX;
      }
    }
  }
  return width;
}

/**
 * Returns the total scrollable height of all panel contents, including the
 * header and footer.
 */
export function panelScrollHeight(panel: PanelModel): number {
  let height = constants.TIMESTAMP_HEADER_SIZE + constants.BOTTOM_CONTROLS_HEIGHT;
  if (!panel.sections.length) return height;

  const lastSection = panel.sections[panel.sections.length - 1];
  height += lastSection.y + lastSection.height;

  return height;
}
