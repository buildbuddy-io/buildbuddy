import { ProfileBuilder } from "./compact_trace";
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel } from "./trace_viewer_model";
import { computeFocusedStack, isFocusedStackEvent } from "./trace_viewer_search";

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

describe("focused stack", () => {
  function checkStack(events: TraceEvent[], expectedNames: string[]) {
    const builder = new ProfileBuilder();
    for (const event of events) builder.addEvent(event);
    const section = buildTraceViewerModel(builder.build()).panels[0].sections[0];
    const tracks = section.tracks ?? [];
    const thread = tracks[0].thread;
    const focusedEventIndex = Array.from({ length: thread.length }, (_, i) => i).find(
      (i) => thread.getName(i) === "focused"
    )!;
    const focusedTrackIndex = tracks.findIndex((track) => Array.from(track.eventIndices).includes(focusedEventIndex));
    const stack = computeFocusedStack(section, focusedEventIndex, focusedTrackIndex)!;
    const names = Array.from({ length: thread.length }, (_, i) => i)
      .filter((i) => isFocusedStackEvent(i, stack))
      .map((i) => thread.getName(i))
      .sort();
    expect(names).toEqual(expectedNames);
  }

  it("includes ancestors and descendants of the focused event", () => {
    const root = makeTraceEvent({ name: "root", ts: 0, dur: 100 });
    const parent = makeTraceEvent({ name: "parent", ts: 10, dur: 70 });
    const focused = makeTraceEvent({ name: "focused", ts: 20, dur: 40 });
    const child = makeTraceEvent({ name: "child", ts: 30, dur: 5 });
    const sibling = makeTraceEvent({ name: "sibling", ts: 80, dur: 10 });
    const partialOverlap = makeTraceEvent({ name: "partialOverlap", ts: 55, dur: 20 });
    const events = [root, parent, focused, partialOverlap, sibling, child];

    checkStack(events, ["child", "focused", "parent", "root"]);
  });

  it("uses the active rendered ancestors when spans cross", () => {
    checkStack(
      [
        makeTraceEvent({ name: "rootA", ts: 0, dur: 100 }),
        makeTraceEvent({ name: "A", ts: 1, dur: 90 }),
        makeTraceEvent({ name: "rootB", ts: 10, dur: 110 }),
        makeTraceEvent({ name: "B", ts: 11, dur: 50 }),
        makeTraceEvent({ name: "C", ts: 20, dur: 60 }),
        makeTraceEvent({ name: "focused", ts: 21, dur: 49 }),
      ],
      ["C", "focused", "rootB"]
    );
  });

  it("keeps equal-timestamp events in profile stack order", () => {
    checkStack(
      [
        makeTraceEvent({ name: "root", ts: 0, dur: 100 }),
        makeTraceEvent({ name: "parent", ts: 0, dur: 100 }),
        makeTraceEvent({ name: "focused", ts: 0, dur: 50 }),
        makeTraceEvent({ name: "child", ts: 0, dur: 50 }),
        makeTraceEvent({ name: "sibling", ts: 60, dur: 40 }),
      ],
      ["child", "focused", "parent", "root"]
    );
  });
});
