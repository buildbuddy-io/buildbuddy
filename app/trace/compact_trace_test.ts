import { ProfileBuilder } from "./compact_trace";
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
