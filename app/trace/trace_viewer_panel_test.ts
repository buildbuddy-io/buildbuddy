import Panel from "./trace_viewer_panel";
import * as constants from "./constants";
import { ProfileBuilder } from "./compact_trace";
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel } from "./trace_viewer_model";
import { computeTraceEventColor } from "../util/color";

class FakeCanvasContext {
  fillRects: Array<{ x: number; y: number; width: number; height: number; color: string; alpha: number }> = [];
  strokeRects: Array<{ x: number; y: number; width: number; height: number }> = [];
  labels: Array<{ text: string; alpha: number }> = [];
  fillStyle = "";
  globalAlpha = 1;
  lineWidth = 1;
  strokeStyle = "";
  private savedStates: Array<{ fillStyle: string; globalAlpha: number }> = [];

  get eventFills() {
    return this.fillRects.filter(
      (rect) =>
        rect.height === constants.TRACK_HEIGHT &&
        rect.y >=
          constants.TIMESTAMP_HEADER_SIZE + constants.SECTION_LABEL_HEIGHT + constants.SECTION_LABEL_PADDING_BOTTOM
    );
  }

  clearRect() {
    this.fillRects = [];
    this.strokeRects = [];
    this.labels = [];
  }
  beginPath() {}
  moveTo() {}
  lineTo() {}
  stroke() {}
  rect() {}
  clip() {}
  measureText() {
    return { width: 10, actualBoundingBoxAscent: 10 };
  }
  save() {
    this.savedStates.push({ fillStyle: this.fillStyle, globalAlpha: this.globalAlpha });
  }
  restore() {
    Object.assign(this, this.savedStates.pop());
  }
  fillText(text: string) {
    this.labels.push({ text, alpha: this.globalAlpha });
  }
  fillRect(x: number, y: number, width: number, height: number) {
    this.fillRects.push({ x, y, width, height, color: this.fillStyle, alpha: this.globalAlpha });
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
    parentElement: { clientWidth: 1000, clientHeight: 540 },
    getContext: () => ctx,
  } as unknown as HTMLCanvasElement;
  const panel = new Panel(model, canvas, "sans-serif");
  panel.canvasWidth = 1000;
  panel.canvasHeight = 540;
  return {
    panel,
    ctx,
    tracks: model.sections.map((section) => section.tracks![0]),
  };
}

function selectEvent(panel: Panel, name: string) {
  for (const section of panel.model.sections) {
    for (const track of section.tracks ?? []) {
      const index = track.eventIndices.findIndex((i) => track.thread.getName(i) === name);
      if (index < 0) continue;
      panel.highlightEvent = { track, index };
      return;
    }
  }
  throw new Error(`Missing event: ${name}`);
}

describe("Panel.draw", () => {
  afterEach(() => restoreDOM());

  it("keeps focused stack colors within the selected thread", () => {
    const { panel, ctx, tracks } = makePanel([
      makeEvent({ tid: 1, name: "selected-thread", dur: 10 }),
      makeEvent({ tid: 2, name: "other-thread", dur: 10 }),
    ]);
    const [selectedTrack] = tracks;
    panel.highlightEvent = { track: selectedTrack, index: 0 };

    panel.filter = "no-match";
    panel.draw();

    expect(ctx.eventFills.map((rect) => rect.color)).toEqual([
      computeTraceEventColor("category#selected-thread", "24"),
      "filtered",
    ]);
  });

  it("skips pixel-collapsed spans before filter and color work but renders a selected span", () => {
    const { panel, ctx, tracks } = makePanel([
      makeEvent({ name: "first", ts: 1, dur: 1 }),
      makeEvent({ name: "skipped", ts: 2, dur: 1 }),
      makeEvent({ name: "selected", ts: 3, dur: 1 }),
    ]);
    const track = tracks[0];
    panel.canvasXPerModelX = 0.01;
    panel.highlightEvent = { track, index: 2 };
    const matchesFilter = spyOn(track.thread, "matchesFilter").and.callThrough();
    const getColorKey = spyOn(track.thread, "getColorKey").and.callThrough();

    panel.filter = "selected";
    panel.draw();

    expect(matchesFilter.calls.count()).toBe(2);
    expect(getColorKey.calls.count()).toBe(1);
    expect(ctx.eventFills.map((rect) => rect.color)).toEqual(["filtered", computeTraceEventColor("category#selected")]);
    expect(ctx.eventFills.length).toBe(2);
    expect(ctx.strokeRects.length).toBe(1);
  });

  it("updates stack colors and label opacity when selecting and clearing matches", () => {
    const { panel, ctx } = makePanel([
      makeEvent({ name: "root", dur: 1000 }),
      makeEvent({ name: "parent", ts: 100, dur: 800 }),
      makeEvent({ name: "selected-first", ts: 200, dur: 300 }),
      makeEvent({ name: "child", ts: 210, dur: 60 }),
      makeEvent({ name: "selected-second", ts: 600, dur: 100 }),
      makeEvent({ tid: 2, name: "unrelated", dur: 1000 }),
    ]);
    const color = (name: string, faded = false) => computeTraceEventColor(`category#${name}`, faded ? "24" : undefined);
    panel.filter = "SELECTED";
    selectEvent(panel, "selected-first");
    panel.draw();
    expect(ctx.eventFills.map((rect) => rect.color)).toEqual([
      color("root", true),
      color("parent", true),
      color("selected-first"),
      color("selected-second"),
      color("child", true),
      "filtered",
    ]);
    for (const name of ["root", "parent", "child", "unrelated"]) {
      expect(ctx.labels.find((label) => label.text === name)?.alpha).toBe(0.45);
    }
    for (const name of ["selected-first", "selected-second"]) {
      expect(ctx.labels.find((label) => label.text === name)?.alpha).toBe(1);
    }
    expect(ctx.eventFills.every((rect) => rect.alpha === 1)).toBeTrue();

    selectEvent(panel, "selected-second");
    panel.draw();
    expect(ctx.eventFills.map((rect) => rect.color)).toEqual([
      color("root", true),
      color("parent", true),
      color("selected-first"),
      color("selected-second"),
      "filtered",
      "filtered",
    ]);

    panel.highlightEvent = undefined;
    panel.filter = "no-match";
    panel.draw();
    expect(ctx.eventFills.every((rect) => rect.color === "filtered" && rect.alpha === 1)).toBeTrue();
    expect(ctx.strokeRects.length).toBe(0);

    panel.filter = "";
    panel.draw();
    expect(ctx.eventFills.map((rect) => rect.color)).toEqual([
      color("root"),
      color("parent"),
      color("selected-first"),
      color("selected-second"),
      color("child"),
      color("unrelated"),
    ]);
    expect(ctx.labels.every((label) => label.alpha === 1)).toBeTrue();
  });

  it("colors only the active ancestors and descendants when spans cross", () => {
    const { panel, ctx } = makePanel([
      makeEvent({ name: "old-root", ts: 0, dur: 500 }),
      makeEvent({ name: "old-parent", ts: 10, dur: 450 }),
      makeEvent({ name: "root", ts: 20, dur: 700 }),
      makeEvent({ name: "parent", ts: 30, dur: 600 }),
      makeEvent({ name: "selected", ts: 40, dur: 400 }),
      makeEvent({ name: "child", ts: 50, dur: 100 }),
      makeEvent({ name: "crossing-peer", ts: 160, dur: 400 }),
      makeEvent({ name: "peer-child", ts: 170, dur: 100 }),
    ]);
    panel.filter = "selected";
    selectEvent(panel, "selected");
    panel.draw();
    expect(ctx.eventFills.map((rect) => rect.color)).toEqual([
      "filtered",
      computeTraceEventColor("category#root", "24"),
      "filtered",
      computeTraceEventColor("category#parent", "24"),
      computeTraceEventColor("category#selected"),
      "filtered",
      computeTraceEventColor("category#child", "24"),
      "filtered",
    ]);
  });
});
