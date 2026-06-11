import { StringInterner } from "../util/intern";
import { Thread } from "./compact_trace";
import { TraceEvent } from "./trace_events";
import { SectionModel } from "./trace_viewer_model";
import { collectFocusedTracePathEventIndices, traceEventMatchesFilter } from "./trace_viewer_search";

function makeTraceEvent(overrides: Partial<TraceEvent> = {}): TraceEvent {
  return {
    pid: 1,
    tid: 1,
    ts: 0,
    ph: "X",
    cat: "category",
    name: "event",
    dur: 10,
    tdur: 0,
    tts: 0,
    out: "",
    args: {},
    ...overrides,
  };
}

describe("traceEventMatchesFilter", () => {
  it("matches searchable trace event fields case-insensitively", () => {
    expect(traceEventMatchesFilter(makeTraceEvent({ name: "Compile Target" }), "compile")).toBeTrue();
    expect(traceEventMatchesFilter(makeTraceEvent({ cat: "Remote Execution" }), "remote")).toBeTrue();
    expect(traceEventMatchesFilter(makeTraceEvent({ args: { target: "//app:bundle" } }), "bundle")).toBeTrue();
    expect(traceEventMatchesFilter(makeTraceEvent({ args: { mnemonic: "GoCompilePkg" } }), "gocompilepkg")).toBeTrue();
    expect(traceEventMatchesFilter(makeTraceEvent({ out: "cache hit" }), "cache")).toBeTrue();
  });

  it("matches all events for an empty filter", () => {
    expect(traceEventMatchesFilter(makeTraceEvent(), "")).toBeTrue();
  });

  it("does not match unrelated fields", () => {
    expect(traceEventMatchesFilter(makeTraceEvent({ name: "Compile Target" }), "download")).toBeFalse();
  });
});

describe("collectFocusedTracePathEventIndices", () => {
  it("includes ancestors and descendants of the focused event", () => {
    const root = makeTraceEvent({ name: "root", ts: 0, dur: 100 });
    const parent = makeTraceEvent({ name: "parent", ts: 10, dur: 70 });
    const focused = makeTraceEvent({ name: "focused", ts: 20, dur: 40 });
    const child = makeTraceEvent({ name: "child", ts: 30, dur: 5 });
    const sibling = makeTraceEvent({ name: "sibling", ts: 80, dur: 10 });
    const partialOverlap = makeTraceEvent({ name: "partialOverlap", ts: 55, dur: 20 });
    const events = [root, parent, focused, partialOverlap, sibling, child];
    const strings = new StringInterner();
    const catIDs = new Uint32Array(events.length);
    const nameIDs = new Uint32Array(events.length);
    const targetIDs = new Uint32Array(events.length);
    const mnemonicIDs = new Uint32Array(events.length);
    const outIDs = new Uint32Array(events.length);
    const ts = new Float64Array(events.length);
    const dur = new Float64Array(events.length);
    const depth = new Uint16Array([0, 1, 2, 2, 1, 3]);
    for (let i = 0; i < events.length; i++) {
      catIDs[i] = strings.intern(events[i].cat);
      nameIDs[i] = strings.intern(events[i].name);
      if (events[i].args?.target) targetIDs[i] = strings.intern(events[i].args.target);
      if (events[i].args?.mnemonic) mnemonicIDs[i] = strings.intern(events[i].args.mnemonic);
      if (events[i].out) outIDs[i] = strings.intern(events[i].out);
      ts[i] = events[i].ts;
      dur[i] = events[i].dur;
    }
    strings.freeze();
    const thread = new Thread(
      1,
      1,
      "thread",
      strings,
      catIDs,
      nameIDs,
      ts,
      dur,
      depth,
      targetIDs,
      mnemonicIDs,
      outIDs,
      3
    );
    const section: SectionModel = {
      name: "thread",
      y: 0,
      height: 0,
      tracks: [
        { thread, eventIndices: new Uint32Array([0]) },
        { thread, eventIndices: new Uint32Array([1, 4]) },
        { thread, eventIndices: new Uint32Array([2, 3]) },
        { thread, eventIndices: new Uint32Array([5]) },
      ],
    };

    const focusedPathEventIndices = collectFocusedTracePathEventIndices(section, 2, 2);

    expect(focusedPathEventIndices.has(0)).toBeTrue();
    expect(focusedPathEventIndices.has(1)).toBeTrue();
    expect(focusedPathEventIndices.has(2)).toBeTrue();
    expect(focusedPathEventIndices.has(5)).toBeTrue();
    expect(focusedPathEventIndices.has(4)).toBeFalse();
    expect(focusedPathEventIndices.has(3)).toBeFalse();
  });
});
