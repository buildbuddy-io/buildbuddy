import React, { CSSProperties } from "react";
import { ZoomIn, ZoomOut } from "lucide-react";
import { AnimatedValue } from "../util/animated_value";
import { AnimationLoop } from "../util/animation_loop";
import { ClientXY } from "../util/dom";
import { clamp } from "../util/math";
import { modifierKey } from "../util/platform";
import * as constants from "./constants";
import EventHovercard from "./event_hovercard";
import Panel from "./trace_viewer_panel";
import { ThreadEvent, TraceEvent } from "./trace_events";
import { buildTraceViewerModel, panelScrollHeight } from "./trace_viewer_model";
import { Profile } from "./trace_events";
import router from "../router/router";
import { FilterInput } from "../components/filter_input/filter_input";

export interface TraceViewProps {
  profile: Profile;
  /** Fit each panel's height to exactly match its contents. */
  fitToContent?: boolean;
  /** Hide the filter bar. */
  filterHidden?: boolean;
}

// The browser starts struggling if we have a div much greater than this width
// in pixels. For now we rely on the browser for rendering the horizontal
// scrollbar, so we don't allow the horizontally scrollable width to exceed this
// value.
const SCROLL_WIDTH_LIMIT = 18_000_000;

const FILTER_URL_PARAM = "timingFilter";

/**
 * Renders an interactive trace profile viewer for an invocation.
 */
export default class TraceViewer extends React.Component<TraceViewProps, {}> {
  /*
   * NOTE: this component intentionally does not using React state.
   * Component updates are done manually by drawing to a Canvas.
   */

  private model = buildTraceViewerModel(this.props.profile, this.props.fitToContent);
  private rootRef = React.createRef<HTMLDivElement>();
  private canvasRefs: React.RefObject<HTMLCanvasElement>[] = this.model.panels.map((_) =>
    React.createRef<HTMLCanvasElement>()
  );
  private panels: Panel[] = [];

  private animation = new AnimationLoop((dt: number) => this.update(dt));

  /**
   * Current X axis scaling, which is smoothly animated as the user zooms in and
   * out. "canvasX" means canvas X coordinates, which are basically equivalent
   * to screen pixels. "modelX" means model X coordinates. These are in
   * microseconds.
   */
  private canvasXPerModelX = new AnimatedValue(0, { min: 0, max: 1 });
  private zoomOriginModelX = 0;
  private zoomOriginClientX = 0;
  private isUsingZoomButtons = false;

  private mouse: ClientXY = { clientX: 0, clientY: 0 };
  private mouseModelX = 0;
  private mouseScrollTop = 0;
  private panning?: Panel | null;

  private hovercardRef = React.createRef<EventHovercard>();

  private unobserveResize?: () => void;

  private focusedEventIndex: number = -1;
  private focusedEvent: TraceEvent | null = null;

  componentDidMount() {
    const fontFamily = window.getComputedStyle(document.body).fontFamily;
    this.panels = this.model.panels.map(
      (panelModel, i) => new Panel(panelModel, this.canvasRefs[i]!.current!, fontFamily, this.focusedEvent)
    );

    this.update();

    const resizeObserver = new ResizeObserver(() => this.update());
    resizeObserver.observe(this.rootRef.current!);
    this.unobserveResize = () => resizeObserver.disconnect();

    window.addEventListener("resize", this.onWindowResize);
    window.addEventListener("mousemove", this.onWindowMouseMove);
    window.addEventListener("mouseup", this.onWindowMouseUp);
    for (const panel of this.panels) {
      // Need to register a non-passive event listener because we may want to
      // prevent the default scrolling behavior on wheel (for scroll-to-zoom
      // functionality).
      panel.container.addEventListener("wheel", (e: WheelEvent) => this.onWheel(e), {
        passive: false,
      });
    }
  }

  componentWillUnmount(): void {
    window.removeEventListener("resize", this.onWindowResize);
    window.removeEventListener("mousemove", this.onWindowMouseMove);
    window.removeEventListener("mouseup", this.onWindowMouseUp);
    this.unobserveResize?.();
    document.body.style.cursor = "";
  }

  private getFilter() {
    return new URLSearchParams(window.location.search).get(FILTER_URL_PARAM) || "";
  }

  /**
   * Main update loop called in each animation frame by `this.animation`. This
   * steps the zoom animation if applicable and re-renders the canvas contents.
   * If the user is not interacting with the canvas or the zoom animation is at
   * its target value, this stops running.
   *
   * It can also be called with a time delta of zero just to do a one-off
   * re-render of the canvas (e.g. if the browser window is resized).
   *
   * @param dt the time elapsed since the previous animation frame.
   */
  private update(dt = 0) {
    this.canvasXPerModelX.min = this.panels[0].container.clientWidth / this.model.xMax;
    this.canvasXPerModelX.max = SCROLL_WIDTH_LIMIT / this.model.xMax;
    this.canvasXPerModelX.step(dt, { threshold: 1e-9 });

    for (const panel of this.panels) {
      panel.resize();
      panel.filter = this.getFilter();

      if (!this.canvasXPerModelX.isAtTarget || this.panning) {
        // If actively zooming or panning, set the panel's scrollX so that the
        // zoom origin stays fixed.
        const zoomOriginScrollX = this.zoomOriginModelX * this.canvasXPerModelX.value;
        const zoomOriginCanvasXDistanceFromPanelLeftEdge =
          this.zoomOriginClientX - panel.container.getBoundingClientRect().left;
        panel.scrollX = zoomOriginScrollX - zoomOriginCanvasXDistanceFromPanelLeftEdge;
      }
      // Set panel x scale
      panel.canvasXPerModelX = this.canvasXPerModelX.value;

      // Set sizer div width so that the horizontal scrollbar renders
      // appropriately.
      const sizer = panel.container.getElementsByClassName("sizer")[0] as HTMLDivElement;
      sizer.style.width = `${this.canvasXPerModelX.value * this.model.xMax}px`;

      panel.scrollX = clamp(
        panel.scrollX,
        0,
        this.canvasXPerModelX.value * this.model.xMax - panel.container.clientWidth
      );
      panel.container.scrollLeft = panel.scrollX;

      if (this.panning === panel) {
        const scrollTop = this.mouseScrollTop - this.mouse.clientY + panel.container.getBoundingClientRect().top;
        panel.scrollY = clamp(scrollTop, 0, panel.container.scrollHeight - panel.container.clientHeight);
        panel.container.scrollTop = panel.scrollY;
      }

      panel.draw();
    }

    if (this.canvasXPerModelX.isAtTarget) this.animation.stop();
  }

  private updateMouse(mouse: MouseEvent | React.MouseEvent) {
    this.mouse = { clientX: mouse.clientX, clientY: mouse.clientY };
    // Update the mouse's model X coordinate (i.e. hovered timestamp).
    // When panning, keep mouseModelX fixed.
    if (!this.panning) {
      const mouseCanvasX =
        this.panels[0].scrollX + (mouse.clientX - this.panels[0].container.getBoundingClientRect().left);
      this.mouseModelX = mouseCanvasX / this.panels[0].canvasXPerModelX;
    }
    this.zoomOriginClientX = mouse.clientX;
    this.zoomOriginModelX = this.mouseModelX;
    // When using zoom buttons, set the zoom origin to the center.
    if (this.isUsingZoomButtons) {
      const boundingRect = this.panels[0].container.getBoundingClientRect();
      this.zoomOriginClientX = boundingRect.left + boundingRect.width / 2;
      this.zoomOriginModelX = (this.panels[0].scrollX + boundingRect.width / 2) / this.canvasXPerModelX.value;
    }

    // Update hover state.
    let isHoveringAnyPanel = false;
    let hoveredEvent: TraceEvent | null = null;
    for (const panel of this.panels) {
      panel.mouse = mouse;
      const hovering = panel.containsClientXY(mouse);
      if (hovering) {
        hoveredEvent = panel.getHoveredEvent();
      }
      isHoveringAnyPanel = isHoveringAnyPanel || hovering;
    }
    for (const panel of this.panels) {
      panel.showMouseXGridline = isHoveringAnyPanel;
    }
    const hovercard = this.hovercardRef.current!;
    if (!hoveredEvent || this.panning) {
      hovercard.setState({ data: null });
    } else {
      hovercard.setState({
        data: { event: hoveredEvent, x: mouse.clientX, y: mouse.clientY },
      });
    }
    document.body.style.cursor = hoveredEvent?.args?.target ? "pointer" : "";
  }

  private onScroll(e: React.UIEvent<HTMLDivElement>, panelIndex: number) {
    if (this.panning) {
      // Scroll event was triggered by panning; do nothing.
      return;
    }
    this.panels[panelIndex].scrollY = (e.target as HTMLDivElement).scrollTop;
    // Apply horizontal scroll to all panels.
    for (let i = 0; i < this.panels.length; i++) {
      const panel = this.panels[i];
      panel.scrollX = (e.target as HTMLDivElement).scrollLeft;
      if (i !== panelIndex) {
        panel.container.scrollLeft = panel.scrollX;
      }
      panel.draw();
    }
  }

  private adjustZoom(amount: number) {
    // When zooming, the desired behavior is that each successive order of
    // magnitude difference in the duration of time that's currently displayed
    // should take the same amount of time to reach by scrolling. For example,
    // the scroll distance between 1/1e5 (pixels per microsecond) and 1/1e4
    // should be the same as the scroll distance between 1/1e4 and 1/1e3. The
    // power formula here achieves that.
    this.canvasXPerModelX.target *= Math.pow(0.92, -amount);
    this.animation.start();
  }

  private onWheel(e: WheelEvent) {
    this.isUsingZoomButtons = false;
    if (e.ctrlKey || e.shiftKey || e.altKey || e.metaKey) {
      e.preventDefault();
      e.stopPropagation();
      this.adjustZoom(-e.deltaY * 0.04);
    }
  }

  private onClickZoom(e: React.MouseEvent, direction: -1 | 1) {
    this.isUsingZoomButtons = true;
    this.updateMouse(e);
    this.adjustZoom(direction * 8);
  }

  private onWindowMouseMove = (e: MouseEvent) => {
    this.updateMouse(e);
    this.animation.start();
  };

  private onWindowResize = () => {
    this.update();
  };

  private onWindowMouseUp = () => {
    this.panning = null;
    document.body.style.cursor = "";
  };

  private updateFilter = (value: string) => {
    this.focusedEventIndex = -1;
    this.focusedEvent = null;
    router.setQueryParam(FILTER_URL_PARAM, value);
    this.update();
    this.scrollToFirstFilteredEvent();
  };

  private onCanvasMouseDown(e: React.MouseEvent, panelIndex: number) {
    this.panning = this.panels[panelIndex];
    this.isUsingZoomButtons = false;
    document.body.style.cursor = "grabbing";
    const container = this.panning.container;
    this.updateMouse(e);
    // Capture mouseScrollTop so we can keep it fixed while panning.
    this.mouseScrollTop = container.scrollTop + (this.mouse.clientY - container.getBoundingClientRect().top);
  }

  private onCanvasClick(e: React.MouseEvent, panelIndex: number) {
    const target = this.panels[panelIndex].getHoveredEvent()?.args?.target;
    if (target) {
      router.navigateTo(`?target=${target}#targets`);
    }
  }

  private scrollToFirstFilteredEvent = () => {
    if (!this.panels.length) return;
    const panel = this.panels[0]; // Assuming all panels have the same x-axis
    const filteredEvents = panel.getFilteredEvents();
    if (filteredEvents.length > 0) {
      this.focusedEventIndex = 0;
      this.focusedEvent = filteredEvents[0];
      this.scrollToEvent(this.focusedEvent);
    }
  };

  private scrollToEvent = (event: TraceEvent) => {
    if (!this.panels.length) return;
    this.focusedEvent = event;
    const panel = this.panels[0]; // Assuming all panels have the same x-axis
    const scrollX = event.ts * panel.canvasXPerModelX - panel.container.clientWidth / 2;
    panel.scrollX = Math.max(0, Math.min(scrollX, panel.canvasXPerModelX * this.model.xMax - panel.container.clientWidth));
    // Calculate vertical scroll position to center the event
    let eventY = 0;
    for (const section of panel.model.sections) {
      for (const track of section.tracks || []) {
        const eventIndex = track.events.indexOf(event);
        if (eventIndex !== -1) {
          const threadEvent = track.events[eventIndex] as ThreadEvent;
          const trackYInSection =
            constants.SECTION_LABEL_HEIGHT +
            constants.SECTION_LABEL_PADDING_BOTTOM +
            threadEvent.depth * (constants.TRACK_HEIGHT + constants.TRACK_VERTICAL_GAP);
          eventY =
            constants.TIMESTAMP_HEADER_SIZE +
            section.y +
            trackYInSection +
            constants.TRACK_HEIGHT / 2; // Center of the track
          break;
        }
      }
      if (eventY > 0) break; // Event found, exit section loop
    }

    if (eventY > 0) {
      const scrollY = eventY - panel.container.clientHeight / 2;
      panel.scrollY = Math.max(
        0,
        Math.min(scrollY, panel.container.scrollHeight - panel.container.clientHeight)
      );
    }

    this.update(); // Re-render to apply scroll and highlight (if needed later)
  };

  private scrollToNextFilteredEvent = () => {
    if (!this.panels.length) return;
    const panel = this.panels[0]; // Assuming all panels have the same x-axis
    const filteredEvents = panel.getFilteredEvents();
    if (filteredEvents.length === 0) {
      this.focusedEventIndex = -1;
      this.focusedEvent = null;
      return; // No filtered events
    }

    this.focusedEventIndex++;
    if (this.focusedEventIndex >= filteredEvents.length) {
      this.focusedEventIndex = 0; // Wrap around to the first event
    }
    this.focusedEvent = filteredEvents[this.focusedEventIndex];
    this.scrollToEvent(filteredEvents[this.focusedEventIndex]);
  }

  private onFilterKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      this.scrollToNextFilteredEvent();
    }
  }

  render() {
    return (
      <div
        ref={this.rootRef}
        className="trace-viewer"
        style={{
          ...({
            "--scrollbar-size": `${constants.SCROLLBAR_SIZE}px`,
          } as CSSProperties),
        }}>
        {!this.props.filterHidden && (
          <FilterInput
            className="filter"
            onChange={(e) => this.updateFilter(e.target.value)}
            onKeyDown={this.onFilterKeyDown}
            value={this.getFilter()}
            placeholder="Filter..."
          />
        )}
        <div className="trace-viewer-panels">
          {this.model.panels.map((panel, i) => (
            <div
              className="panel-container"
              style={{
                width: "100%",
                height: `${panel.height}px`,
                position: "relative",
              }}>
              <div key={i} className="panel" onScroll={(e) => this.onScroll(e, i)}>
                <canvas
                  ref={this.canvasRefs[i]}
                  onMouseDown={(e) => this.onCanvasMouseDown(e, i)}
                  onClick={(e) => this.onCanvasClick(e, i)}
                />
                {/*
                 * This sizer div is used to make the total scrollable area
                 * match the size of the panel contents. We can't use a very
                 * large canvas directly due to browser limitations.
                 */}
                <div
                  className="sizer"
                  style={{
                    height: `${panelScrollHeight(panel) - panel.height + constants.SCROLLBAR_SIZE}px`,
                  }}
                />
              </div>
              <div
                className="panel-controls"
                style={{
                  bottom: `${constants.SCROLLBAR_SIZE}px`,
                }}>
                <button
                  className="button icon-button"
                  onClick={(e) => this.onClickZoom(e, -1)}
                  title={`Zoom out (${modifierKey()}+scroll)`}>
                  <ZoomOut className="icon" />
                </button>
                <button
                  className="button icon-button"
                  onClick={(e) => this.onClickZoom(e, +1)}
                  title={`Zoom in (${modifierKey()}+scroll)`}>
                  <ZoomIn className="icon" />
                </button>
              </div>
            </div>
          ))}
        </div>
        <EventHovercard ref={this.hovercardRef} buildDuration={this.model.xMax} />
      </div>
    );
  }
}
