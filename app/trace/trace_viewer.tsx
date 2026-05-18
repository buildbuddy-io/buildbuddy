import { ZoomIn, ZoomOut } from "lucide-react";
import React, { CSSProperties } from "react";
import { FilterInput } from "../components/filter_input/filter_input";
import router from "../router/router";
import { AnimatedValue } from "../util/animated_value";
import { AnimationLoop } from "../util/animation_loop";
import { ClientXY } from "../util/dom";
import { clamp } from "../util/math";
import { modifierKey } from "../util/platform";
import { TypedArrayBuilder } from "../util/typed_arrays";
import { Profile } from "./compact_trace";
import * as constants from "./constants";
import EventHovercard from "./event_hovercard";
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel, panelScrollHeight } from "./trace_viewer_model";
import Panel from "./trace_viewer_panel";
import { collectFocusedTracePathEventIndices } from "./trace_viewer_search";

export interface TraceViewProps {
  profile: Profile;
  /** Fit each panel's height to exactly match its contents. */
  fitToContent?: boolean;
  /** Hide the filter bar. */
  filterHidden?: boolean;
  dark: boolean;
}

interface TraceViewerState {
  filter: string;
  totalMatches: number;
  currentMatch: number; // 1-based index of the currently highlighted match
}

// The browser starts struggling if we have a div much greater than this width
// in pixels. For now we rely on the browser for rendering the horizontal
// scrollbar, so we don't allow the horizontally scrollable width to exceed this
// value.
const SCROLL_WIDTH_LIMIT = 18_000_000;

const FILTER_URL_PARAM = "timingFilter";

const CRITICAL_PATH_ACTION_PREFIX = "action '";

/**
 * Renders an interactive trace profile viewer for an invocation.
 */
export default class TraceViewer extends React.Component<TraceViewProps, TraceViewerState> {
  /*
   * NOTE: Canvas redrawing and some UI updates are done manually by drawing
   * to a Canvas and managing element properties, but the search match counter
   * uses React state for efficient updates.
   */

  private model = buildTraceViewerModel(this.props.profile, this.props.fitToContent);
  private rootRef = React.createRef<HTMLDivElement>();
  private canvasRefs: React.RefObject<HTMLCanvasElement>[] = this.model.panels.map((_) =>
    React.createRef<HTMLCanvasElement>()
  );
  private zoomFactorRefs: React.RefObject<HTMLSpanElement>[] = this.model.panels.map((_) =>
    React.createRef<HTMLSpanElement>()
  );
  private zoomOutButtonRefs: React.RefObject<HTMLButtonElement>[] = this.model.panels.map((_) =>
    React.createRef<HTMLButtonElement>()
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

  private debounceTimer: number | undefined;

  // Index of the most recently highlighted search result in `this.matchSectionIndices`.
  // A value of -1 indicates that the next search should start from the
  // beginning.
  private searchIndex = -1;

  // Search matches are kept as parallel typed arrays to avoid allocating an
  // object per matching event on very large profiles.
  private matchSectionIndices = new Uint32Array();
  private matchTrackIndices = new Uint32Array();
  private matchEventIndices = new Uint32Array();

  constructor(props: TraceViewProps) {
    super(props);

    const initialFilter = this.getFilterFromUrl();
    this.state = {
      filter: initialFilter,
      totalMatches: 0, // Calculated after mounting
      currentMatch: 0, // Calculated after mounting / search
    };
  }

  componentDidMount() {
    const fontFamily = window.getComputedStyle(document.body).fontFamily;
    this.panels = this.model.panels.map(
      (panelModel, i) => new Panel(panelModel, this.canvasRefs[i]!.current!, fontFamily)
    );

    this.update(); // Initial render
    this.performSearch(); // Calculate initial matches and update UI

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

  componentDidUpdate(prevProps: TraceViewProps): void {
    if (prevProps.dark !== this.props.dark) {
      for (const panel of this.panels) panel.onThemeChange();
    }
  }

  componentWillUnmount(): void {
    window.removeEventListener("resize", this.onWindowResize);
    window.removeEventListener("mousemove", this.onWindowMouseMove);
    window.removeEventListener("mouseup", this.onWindowMouseUp);
    this.unobserveResize?.();
    document.body.style.cursor = "";
    window.clearTimeout(this.debounceTimer);
  }

  private getFilterFromUrl() {
    return new URLSearchParams(window.location.search).get(FILTER_URL_PARAM) || "";
  }

  // Primary source of truth for the filter is now state
  private getFilter() {
    return this.state.filter;
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
    // Don't decrease `max` if it has been increased past the default limit as a
    // result of user interaction (e.g. zooming in after a search match). This
    // ensures that once the user zooms in beyond the default scroll width
    // limit, they can keep zooming in instead of being clamped back down on
    // every animation frame.
    const defaultMax = SCROLL_WIDTH_LIMIT / this.model.xMax;
    this.canvasXPerModelX.max = Math.max(this.canvasXPerModelX.max, defaultMax);
    this.canvasXPerModelX.step(dt, { threshold: 1e-9 });

    for (const panel of this.panels) {
      panel.resize();
      panel.filter = this.getFilter(); // Panel filter depends on state

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

    const zoomMin = this.canvasXPerModelX.min;
    let zoomFactor = 1;
    if (zoomMin > 0 && isFinite(zoomMin)) {
      zoomFactor = this.canvasXPerModelX.value / zoomMin;
    }
    const roundedZoomFactor = Math.round(zoomFactor * 100) / 100;
    const zoomFactorString = `${roundedZoomFactor}x`;
    for (let i = 0; i < this.model.panels.length; i++) {
      const zoomFactorRef = this.zoomFactorRefs[i];
      if (zoomFactorRef.current) {
        zoomFactorRef.current.innerText = zoomFactorString;
        zoomFactorRef.current.style.display = roundedZoomFactor === 1 ? "none" : "inline-block";
      }
      const zoomOutButtonRef = this.zoomOutButtonRefs[i];
      if (zoomOutButtonRef.current) {
        zoomOutButtonRef.current.style.display = roundedZoomFactor === 1 ? "none" : "inline-block";
      }
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
    document.body.style.cursor =
      hoveredEvent?.args?.target ||
      (hoveredEvent?.tid == 0 && hoveredEvent?.name?.startsWith(CRITICAL_PATH_ACTION_PREFIX))
        ? "pointer"
        : "";
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

  private updateFilter = (value: string, callback?: () => void) => {
    router.setQueryParam(FILTER_URL_PARAM, value);
    this.setState({ filter: value });

    window.clearTimeout(this.debounceTimer);
    this.debounceTimer = window.setTimeout(() => {
      this.performSearch();
      callback?.();
    }, 300);
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
    let event = this.panels[panelIndex].getHoveredEvent();
    if (event?.args?.target) {
      router.navigateTo(`?target=${event.args.target}#targets`);
      return;
    }

    if (
      panelIndex === 0 &&
      event?.name &&
      event.name.startsWith(CRITICAL_PATH_ACTION_PREFIX) &&
      event.name.endsWith("'")
    ) {
      this.updateFilter(event.name.slice(CRITICAL_PATH_ACTION_PREFIX.length, -1), () => {
        this.scrollToNextMatch(1);
      });
    }
  }

  // Callback for keydown events in the search input.
  private onSearchKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      this.scrollToNextMatch(e.shiftKey ? -1 : 1);
    }
  };

  // Performs a search based on the current filter, updating the cached list
  // of matches. Then, scrolls to the first match if applicable.
  private performSearch() {
    const filter = this.getFilter();

    // Reset search state.
    this.searchIndex = -1;
    const matchSectionIndices = TypedArrayBuilder.of(Uint32Array);
    const matchTrackIndices = TypedArrayBuilder.of(Uint32Array);
    const matchEventIndices = TypedArrayBuilder.of(Uint32Array);
    if (this.panels.length) {
      this.panels[0].highlightEvent = undefined;
      this.panels[0].highlightPathEventIndices.clear();
    }

    // Find all matches for the new filter.
    if (filter && this.model.panels.length) {
      const lowerCaseFilter = filter.toLowerCase();
      const eventsPanel = this.model.panels[0];
      for (let sectionIndex = 0; sectionIndex < eventsPanel.sections.length; sectionIndex++) {
        const section = eventsPanel.sections[sectionIndex];
        if (!section.tracks?.length) continue;
        const thread = section.tracks[0].thread;
        // Iterate through thread events in timestamp order within each thread.
        // This needs a bit of extra bookkeeping for the current event index
        // within each track, since we don't store an explicit lookup table from
        // each event to its index within its depth track.
        const nextEventIndexByTrack = new Uint32Array(section.tracks.length);
        for (let threadEventIndex = 0; threadEventIndex < thread.length; threadEventIndex++) {
          const trackIndex = thread.depth[threadEventIndex];
          const eventIndex = nextEventIndexByTrack[trackIndex]++;
          if (thread.matchesFilter(threadEventIndex, lowerCaseFilter)) {
            matchSectionIndices.append(sectionIndex);
            matchTrackIndices.append(trackIndex);
            matchEventIndices.append(eventIndex);
          }
        }
      }
    }

    this.matchSectionIndices = matchSectionIndices.toArray();
    this.matchTrackIndices = matchTrackIndices.toArray();
    this.matchEventIndices = matchEventIndices.toArray();

    // Update the UI with the new match count.
    this.setState({ totalMatches: this.matchSectionIndices.length, currentMatch: 0 });
    this.update(); // Re-render panels with the new filter

    // Automatically jump to the first match.
    if (this.matchSectionIndices.length > 0) {
      // Needs a slight delay to allow panels to render before scrolling.
      requestAnimationFrame(() => this.scrollToNextMatch(1));
    }
  }

  // Scrolls to and highlights the next event that matches the current search filter.
  private scrollToNextMatch(direction: number) {
    if (!this.matchSectionIndices.length) {
      // If there are no matches, ensure any existing highlight is cleared.
      if (this.panels.length) {
        this.panels[0].highlightEvent = undefined;
        this.panels[0].highlightPathEventIndices.clear();
      }
      this.searchIndex = -1;
      this.setState({ currentMatch: 0 });
      this.update();
      return;
    }

    let nextMatchIndex;
    const currentMatchIndex = this.searchIndex;

    if (currentMatchIndex === -1) {
      // If no match is currently selected, start from the beginning or end.
      nextMatchIndex = direction > 0 ? 0 : this.matchSectionIndices.length - 1;
    } else {
      // Cycle through the matches.
      const total = this.matchSectionIndices.length;
      nextMatchIndex = (currentMatchIndex + direction + total) % total;
    }

    this.highlightAndScrollToEvent(nextMatchIndex);
    this.setState({ currentMatch: nextMatchIndex + 1 });
  }

  // Highlights the event with the given match index and scrolls it into view.
  private highlightAndScrollToEvent(index: number) {
    this.searchIndex = index;
    const sectionIndex = this.matchSectionIndices[index];
    const trackIndex = this.matchTrackIndices[index];
    const eventIndex = this.matchEventIndices[index];
    const track = this.model.panels[0].sections[sectionIndex].tracks![trackIndex];
    const threadEventIndex = track.eventIndices[eventIndex];
    const eventTs = track.thread.ts[threadEventIndex];
    const eventDur = track.thread.dur[threadEventIndex];

    // Highlight the matched event so it is visually selected no matter what.
    const eventsPanel = this.panels[0];
    eventsPanel.highlightEvent = { track, index: eventIndex };
    eventsPanel.highlightPathEventIndices = collectFocusedTracePathEventIndices(
      this.model.panels[0].sections[sectionIndex],
      threadEventIndex,
      trackIndex
    );

    // Determine whether the matched event is already fully visible in the
    // current viewport. If so, we simply update the highlight without
    // performing any scrolling or zooming so that the view remains stable.
    const panelContainer = eventsPanel.container;
    const scale = eventsPanel.canvasXPerModelX;

    // Horizontal visibility check.
    const eventStartX = eventTs * scale;
    const eventEndX = (eventTs + eventDur) * scale;
    const viewportStartX = eventsPanel.scrollX;
    const viewportEndX = viewportStartX + panelContainer.clientWidth;
    const isHorizontallyVisible = eventStartX >= viewportStartX && eventEndX <= viewportEndX;

    // Vertical visibility check. Compute the top of the track that the event
    // belongs to using the same math that is used when scrolling.
    const eventsPanelModel = this.model.panels[0];
    const trackTop =
      constants.TIMESTAMP_HEADER_SIZE +
      eventsPanelModel.sections[sectionIndex].y +
      constants.SECTION_LABEL_HEIGHT +
      constants.SECTION_LABEL_PADDING_BOTTOM +
      trackIndex * (constants.TRACK_HEIGHT + constants.TRACK_VERTICAL_GAP);
    const trackBottom = trackTop + constants.TRACK_HEIGHT;

    const viewportStartY = eventsPanel.scrollY;
    const viewportEndY = viewportStartY + panelContainer.clientHeight;
    const isVerticallyVisible = trackTop >= viewportStartY && trackBottom <= viewportEndY;

    const eventPixelWidth = eventDur * scale;
    const isTooSmall = eventDur && eventPixelWidth < constants.MIN_RENDER_PIXEL_WIDTH;

    if (isHorizontallyVisible && isVerticallyVisible && !isTooSmall) {
      // Already fully in view and large enough – only update the canvas so the highlight is
      // rendered.
      this.update();
      return;
    }

    // If the event is not fully visible, proceed with the existing behavior of
    // zooming (if needed) and scrolling to center the match.

    // Adjust zoom so that the span has a reasonable on-screen width.
    if (eventDur && eventDur > 0) {
      const currentScale = this.canvasXPerModelX.value;
      const currentPixelWidth = eventDur * currentScale;

      let desiredScale = currentScale;

      // Zoom in if span is too small.
      if (currentPixelWidth < constants.MIN_RENDER_PIXEL_WIDTH) {
        desiredScale = constants.MIN_RENDER_PIXEL_WIDTH / eventDur;
      }
      // Zoom out (all the way to min) if span is extremely large relative to
      // the viewport width.
      else if (currentPixelWidth > panelContainer.clientWidth) {
        desiredScale = this.canvasXPerModelX.min;
      }

      // Clamp within allowed range.
      desiredScale = clamp(desiredScale, this.canvasXPerModelX.min, this.canvasXPerModelX.max);

      // If the desired scale exceeds the current maximum, raise the maximum
      // so that the user can continue zooming in after the search jump.
      if (desiredScale > this.canvasXPerModelX.max) {
        this.canvasXPerModelX.max = desiredScale;
      }

      if (Math.abs(desiredScale - currentScale) > 1e-6) {
        // Instantly apply the desired scale (without animation) so scrolling
        // calculations below are based on the final zoom level. We update
        // both value and target to keep the AnimatedValue in sync.
        this.canvasXPerModelX.value = desiredScale;
        this.canvasXPerModelX.target = desiredScale;
      }
    }

    // Re-compute scale in case it changed above.
    const finalScale = this.canvasXPerModelX.value;

    // Vertical scrolling – center the track.
    const desiredScrollY = clamp(
      trackTop - panelContainer.clientHeight / 2,
      0,
      panelContainer.scrollHeight - panelContainer.clientHeight
    );

    this.panels[0].scrollY = desiredScrollY;
    panelContainer.scrollTop = desiredScrollY;

    // Horizontal scrolling – center the event start.
    const desiredScrollX = clamp(
      eventTs * finalScale - panelContainer.clientWidth / 2,
      0,
      finalScale * this.model.xMax - panelContainer.clientWidth
    );

    for (const panel of this.panels) {
      panel.scrollX = desiredScrollX;
      panel.container.scrollLeft = desiredScrollX;
    }

    // Redraw immediately.
    this.update();
  }

  render() {
    const { filter, totalMatches, currentMatch } = this.state;

    // Determine counter text (e.g., "3/10" or "0/0")
    const counterText = totalMatches > 0 ? `${currentMatch}/${totalMatches}` : "0/0";

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
            onKeyDown={this.onSearchKeyDown}
            value={filter} // Read from state
            placeholder="Search..."
            rightElement={counterText} // Read from state
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
                <span ref={this.zoomFactorRefs[i]} className="zoom-factor button" />
                <button
                  ref={this.zoomOutButtonRefs[i]}
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
