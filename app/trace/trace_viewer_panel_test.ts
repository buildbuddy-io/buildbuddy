import Panel from "./trace_viewer_panel";
import { ProfileBuilder } from "./compact_trace";
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel, TrackModel } from "./trace_viewer_model";
import { computeTraceEventColor } from "../util/color";

class FakeCanvasContext {
  fillStyles: string[] = [];
  fillRects: Array<{ x: number; y: number; width: number; height: number }> = [];
  strokeRects: Array<{ x: number; y: number; width: number; height: number }> = [];
  private currentFillStyle = "";

  get fillStyle() {
    return this.currentFillStyle;
  }

  set fillStyle(value: string) {
    this.currentFillStyle = value;
    this.fillStyles.push(value);
  }

  lineWidth = 1;
  strokeStyle = "";

  fillRect(x: number, y: number, width: number, height: number) {
    this.fillRects.push({ x, y, width, height });
  }

  strokeRect(x: number, y: number, width: number, height: number) {
    this.strokeRects.push({ x, y, width, height });
  }
}

let restoreDOM = () => {};

function installFakeDOM() {
  const originalDocument = Object.getOwnPropertyDescriptor(globalThis, "document");
  const originalGetComputedStyle = Object.getOwnPropertyDescriptor(globalThis, "getComputedStyle");
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const style = {
    getPropertyValue(name: string) {
      return (
        {
          "--color-trace-event-filtered": "filtered",
          "--trace-event-faded-chroma": "24",
          "--trace-event-lightness": "65%",
        }[name] ?? ""
      );
    },
  };
  const browser = globalThis as unknown as {
    document: { documentElement: { classList: { contains(name: string): boolean } } };
    getComputedStyle(element: unknown): typeof style;
    window: { devicePixelRatio: number };
  };
  browser.document = { documentElement: { classList: { contains: () => false } } };
  browser.getComputedStyle = () => style;
  browser.window = { devicePixelRatio: 1 };
  return () => {
    for (const [name, original] of [
      ["document", originalDocument],
      ["getComputedStyle", originalGetComputedStyle],
      ["window", originalWindow],
    ] as const) {
      if (original) {
        Object.defineProperty(globalThis, name, original);
      } else {
        delete (globalThis as Record<string, unknown>)[name];
      }
    }
  };
}

function makeEvent(overrides: Partial<TraceEvent> & Pick<TraceEvent, "name">): TraceEvent {
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

function makePanel(events: TraceEvent[]) {
  restoreDOM = installFakeDOM();
  const builder = new ProfileBuilder();
  for (const event of events) builder.addEvent(event);
  const model = buildTraceViewerModel(builder.build()).panels[0];
  const ctx = new FakeCanvasContext();
  const canvas = {
    parentElement: { clientWidth: 100, clientHeight: 100 },
    getContext: () => ctx,
  } as unknown as HTMLCanvasElement;
  return {
    panel: new Panel(model, canvas, "sans-serif"),
    ctx,
    tracks: model.sections.map((section) => section.tracks![0]),
  };
}

function drawTrack(panel: Panel, track: TrackModel, filter: string) {
  (
    panel as unknown as { drawTrack(track: TrackModel, y: number, xMin: number, xMax: number, filter: string): void }
  ).drawTrack(track, 0, 0, Infinity, filter);
}

describe("Panel.drawTrack", () => {
  afterEach(() => restoreDOM());

  it("keeps focused stack colors within the selected thread", () => {
    const { panel, ctx, tracks } = makePanel([
      makeEvent({ tid: 1, name: "selected-thread", dur: 10 }),
      makeEvent({ tid: 2, name: "other-thread", dur: 10 }),
    ]);
    const [selectedTrack, otherTrack] = tracks;
    panel.highlightEvent = { track: selectedTrack, index: 0 };

    drawTrack(panel, selectedTrack, "no-match");
    drawTrack(panel, otherTrack, "no-match");

    expect(ctx.fillStyles).toEqual([computeTraceEventColor("category#selected-thread", "24"), "filtered"]);
  });

  it("skips pixel-collapsed spans before filter and color work but renders a selected span", () => {
    const { panel, ctx, tracks } = makePanel([
      makeEvent({ name: "first", ts: 0, dur: 1 }),
      makeEvent({ name: "skipped", ts: 1, dur: 1 }),
      makeEvent({ name: "selected", ts: 2, dur: 1 }),
    ]);
    const track = tracks[0];
    panel.canvasXPerModelX = 0.01;
    panel.scrollX = 0.5;
    panel.highlightEvent = { track, index: 2 };
    const matchesFilter = spyOn(track.thread, "matchesFilter").and.callThrough();
    const getColorKey = spyOn(track.thread, "getColorKey").and.callThrough();

    drawTrack(panel, track, "selected");

    expect(matchesFilter.calls.count()).toBe(2);
    expect(getColorKey.calls.count()).toBe(2);
    expect(ctx.fillStyles).toEqual(["filtered", computeTraceEventColor("category#selected")]);
    expect(ctx.fillRects.length).toBe(2);
    expect(ctx.strokeRects.length).toBe(1);
  });
});
