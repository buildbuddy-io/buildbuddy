import { ProfileBuilder, readProfile } from "./compact_trace";
import type { TraceEvent } from "./trace_events";

function traceEvent(overrides: Partial<TraceEvent> & Pick<TraceEvent, "name">): TraceEvent {
  return {
    pid: 1,
    tid: 1,
    ts: 0,
    ph: "X",
    cat: "",
    dur: 1,
    tdur: 0,
    tts: 0,
    out: "",
    args: {},
    ...overrides,
  };
}

// NOTE: in the following profile data, whitespace is significant (unlike regular JSON):
// - The `"traceEvents":[` list opening has to end with a newline
// - Each entry in the traceEvents list has to end with ",\n", except for the last one
// - The traceEvents list is closed with the exact sequence "\n  ]\n}"
const INCOMPLETE_PROFILE = `
{"otherData":{"build_id":"799cc58b-8695-4e70-a772-7a15472aa55b","output_base":"/output-base","date":"Wed Oct 26 20:24:01 UTC 2022"},"traceEvents":[
  {"name":"thread_name","ph":"M","pid":1,"tid":0,"args":{"name":"Critical Path"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":0,"args":{"sort_index":0}},
  {"name":"thread_name","ph":"M","pid":1,"tid":29,"args":{"name":"Main Thread"}},
  {"name":"thread_sort_index","ph":"M","pid":1,"tid":29,"args":{"sort_index":"1"}},
  {"cat":"build phase marker","name":"Launch Blaze","ph":"X","ts":-899000,"dur":899000,"pid":1,"tid":29},
  {"cat":"build phase marker","name":"Initialize command","ph":"i","ts":0,"pid":1,"tid":29},
  {"cat":"general information","name":"BazelStartupOptionsModule.beforeCommand","ph":"X","ts":200868,"dur":206,"pid":1,"tid":29}`.trim();

const COMPLETE_PROFILE = INCOMPLETE_PROFILE + "\n  ]\n}";

function readableStreamFromString(value: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const reader: ReadableStreamDefaultReader<Uint8Array> = {
    read() {
      if (!value) {
        return Promise.resolve({ done: true });
      }
      // Read a small-ish, random length (between 1 and 10 bytes)
      const length = Math.min(value.length, 1 + Math.floor(Math.random() * 10));
      const before = value.substring(0, length);
      const after = value.substring(length);
      value = after;
      return Promise.resolve({ value: encoder.encode(before), done: false });
    },
    releaseLock() {
      throw new Error("releaseLock(): not implemented");
    },
    cancel() {
      throw new Error("cancel(): not implemented");
    },
    get closed() {
      return Promise.reject("closed: not implemented");
    },
  };
  const stream = {
    getReader(): ReadableStreamDefaultReader<Uint8Array> {
      return reader;
    },
  } as ReadableStream<Uint8Array>;

  return stream;
}

describe("readProfile", () => {
  it("parses a complete profile", async () => {
    const stream = readableStreamFromString(COMPLETE_PROFILE);
    let numBytesRead = 0;
    const profile = await readProfile(stream, (n) => {
      numBytesRead = n;
    });

    expect(profile.eventCount).toBe(7);
    expect(profile.threads[0].name).toBe("Main Thread");
    expect(numBytesRead).toBe(COMPLETE_PROFILE.length);
  });

  it("parses a complete profile blob", async () => {
    const profile = await readProfile(new Blob([COMPLETE_PROFILE]));

    expect(profile.eventCount).toBe(7);
    expect(profile.threads[0].name).toBe("Main Thread");
  });

  it("parses an incomplete profile", async () => {
    const stream = readableStreamFromString(INCOMPLETE_PROFILE);
    let numBytesRead = 0;
    const profile = await readProfile(stream, (n) => {
      numBytesRead = n;
    });

    expect(profile.eventCount).toBe(7);
    expect(profile.threads[0].name).toBe("Main Thread");
    expect(numBytesRead).toBe(INCOMPLETE_PROFILE.length);
  });

  it("reports final progress as done", async () => {
    const stream = readableStreamFromString(COMPLETE_PROFILE);
    const progress: { numBytesRead: number; done?: boolean }[] = [];
    const profile = await readProfile(stream, (numBytesRead, done) => {
      progress.push({ numBytesRead, done });
    });

    expect(profile.eventCount).toBe(7);
    expect(progress.length).toBeGreaterThan(0);
    expect(progress.slice(0, progress.length - 1).some(({ done }) => done)).toBe(false);
    expect(progress[progress.length - 1]).toEqual({ numBytesRead: COMPLETE_PROFILE.length, done: true });
  });
});

describe("ProfileBuilder", () => {
  it("builds duration spans into compact threads", () => {
    const builder = new ProfileBuilder();

    builder.addEvent(traceEvent({ name: "thread_name", ph: "M", tid: 7, args: { name: "skyframe-evaluator-1" } }));
    // Add the child before the parent to verify thread events are sorted before depths are assigned.
    builder.addEvent(
      traceEvent({
        name: "child span",
        cat: "child",
        tid: 7,
        ts: 20,
        dur: 10,
        args: { target: "//pkg:child", mnemonic: "GoCompile" },
        out: "bazel-out/child",
      })
    );
    builder.addEvent(traceEvent({ name: "parent span", cat: "parent", tid: 7, ts: 10, dur: 30 }));

    const profile = builder.build();

    expect(profile.eventCount).toBe(3);
    expect(profile.xMax).toBe(40);
    expect(profile.threads.length).toBe(1);
    expect(profile.threads[0].name).toBe("skyframe evaluator 1");
    expect(Array.from(profile.threads[0].ts)).toEqual([10, 20]);
    expect(Array.from(profile.threads[0].dur)).toEqual([30, 10]);
    expect(Array.from(profile.threads[0].depth)).toEqual([0, 1]);
    expect(profile.threads[0].maxDepth).toBe(1);
    expect(profile.threads[0].getColorKey(1)).toBe("child#child span");
    expect(profile.threads[0].matchesFilter(1, "gocompile")).toBeTrue();
    expect(profile.threads[0].getEvent(1)).toEqual(
      jasmine.objectContaining({
        name: "child span",
        cat: "child",
        ts: 20,
        dur: 10,
        out: "bazel-out/child",
        args: { target: "//pkg:child", mnemonic: "GoCompile" },
      })
    );
  });

  it("builds known and inferred time series", () => {
    const builder = new ProfileBuilder();

    // Add counter samples out of timestamp order to verify finalized time series values are sorted.
    builder.addEvent(traceEvent({ name: "Future metric", ph: "C", ts: 20, args: { beta: 3, label: "ignored" } }));
    builder.addEvent(traceEvent({ name: "CPU usage (Bazel)", ph: "C", ts: 5, args: { cpu: 0.5 } }));
    builder.addEvent(traceEvent({ name: "Future metric", ph: "C", ts: 10, args: { alpha: 1, beta: 2 } }));

    const profile = builder.build();

    expect(profile.xMax).toBe(20);
    expect(profile.timeseries.map((series) => series.name)).toEqual([
      "CPU usage (Bazel)",
      "Future metric: alpha",
      "Future metric: beta",
    ]);
    expect(profile.timeseries[0].unit).toBe("cores");
    expect(Array.from(profile.timeseries[0].ts)).toEqual([5]);
    expect(Array.from(profile.timeseries[0].val)).toEqual([0.5]);
    expect(Array.from(profile.timeseries[1].ts)).toEqual([10]);
    expect(Array.from(profile.timeseries[1].val)).toEqual([1]);
    expect(Array.from(profile.timeseries[2].ts)).toEqual([10, 20]);
    expect(Array.from(profile.timeseries[2].val)).toEqual([2, 3]);
  });
});
