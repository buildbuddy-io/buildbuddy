import { ProfileBuilder } from "./compact_trace";
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel } from "./trace_viewer_model";
import { collectFocusedTracePathEventIndices } from "./trace_viewer_search";

function makeTraceEvent(overrides: Partial<TraceEvent> & Pick<TraceEvent, "name">): TraceEvent {
  return {
    pid: 1,
    tid: 1,
    ts: 0,
    ph: "X",
    cat: "category",
    dur: 10,
    tdur: 0,
    tts: 0,
    out: "",
    args: {},
    ...overrides,
  };
}

describe("collectFocusedTracePathEventIndices", () => {
  it("includes ancestors and descendants of the focused event", () => {
    const root = makeTraceEvent({ name: "root", ts: 0, dur: 100 });
    const parent = makeTraceEvent({ name: "parent", ts: 10, dur: 70 });
    const focused = makeTraceEvent({ name: "focused", ts: 20, dur: 40 });
    const child = makeTraceEvent({ name: "child", ts: 30, dur: 5 });
    const sibling = makeTraceEvent({ name: "sibling", ts: 80, dur: 10 });
    const partialOverlap = makeTraceEvent({ name: "partialOverlap", ts: 55, dur: 20 });
    const events = [root, parent, focused, partialOverlap, sibling, child];

    const builder = new ProfileBuilder();
    for (const event of events) {
      builder.addEvent(event);
    }
    const section = buildTraceViewerModel(builder.build()).panels[0].sections[0];
    const tracks = section.tracks ?? [];
    const thread = tracks[0].thread;
    const eventIndexByName = (name: string) => {
      for (let i = 0; i < thread.length; i++) {
        if (thread.getName(i) === name) return i;
      }
      throw new Error(`event not found: ${name}`);
    };
    const focusedEventIndex = eventIndexByName("focused");
    const focusedTrackIndex = tracks.findIndex((track) => Array.from(track.eventIndices).includes(focusedEventIndex));

    const focusedPathEventIndices = collectFocusedTracePathEventIndices(section, focusedEventIndex, focusedTrackIndex);
    const focusedPathEventNames = Array.from(focusedPathEventIndices)
      .map((eventIndex) => thread.getName(eventIndex))
      .sort();

    expect(focusedPathEventNames).toEqual(["child", "focused", "parent", "root"]);
  });
});
