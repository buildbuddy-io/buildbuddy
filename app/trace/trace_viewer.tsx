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
import { TraceEvent } from "./trace_events";
import { buildTraceViewerModel, panelScrollHeight } from "./trace_viewer_model";
import { Profile } from "./trace_events";

export interface TraceViewProps {
  profile: Profile;
}

/**
 * Renders an interactive trace profile viewer for an invocation.
 */
export default class TraceViewer extends React.Component<TraceViewProps> {
  private model = buildTraceViewerModel(this.props.profile);
  private canvasRefs: React.RefObject<HTMLCanvasElement>[] = this.model.panels.map((_) =>
    React.createRef<HTMLCanvasElement>()
  );
  private panels: Panel[] = [];

  private animation = new AnimationLoop((dt: number) => this.update(dt));
  private zoom = new AnimatedValue(1, { min: 0, max: 1 });

  private mouse: ClientXY = { clientX: 0, clientY: 0 };
  private mouseModelX = 0;
  private mouseScrollTop = 0;
  private panning?: Panel | null;

  private hovercardRef = React.createRef<EventHovercard>();

  computeCanvasXPerModelX(): number {
    // When zoom is at 0 (min), the full canvas width should be equal to
    // the model width.
    // When zoom is at 1 (max), the full canvas width should be equal to
    // 1 second (this is arbitrary, but reasonable).

    // TODO: test that this works when modelMaxX < 1e6!
    const canvasWidth = this.panels[0].container().clientWidth;
    // Point-slope: y - y1 = m * ( x - x1 )
    // x is zoom level
    // y is canvasXPerModelX
    const x0 = 1;
    const y0 = canvasWidth / this.model.xMax;
    const x1 = 0;
    const y1 = canvasWidth / constants.MODEL_X_PER_SECOND;
    const m = (y1 - y0) / (x1 - x0);
    // Transform the zoom value to be slower on the initial part of the curve.
    // TODO: this formula works OK in practice but it's somewhat arbitrary.
    // Better approach: every <x> amount of mouse scrolling should zoom us in by
    // a fixed power of 10, such that the min zoom level lets us see the whole
    // graph and the max zoom level lets us see 1 usec per pixel or something.
    const x = 1 - Math.pow(1 - this.zoom.value, 4);
    return y1 + m * (x - x1);
  }

  componentDidMount() {
    const fontFamily = window.getComputedStyle(document.body).fontFamily;
    this.panels = this.model.panels.map(
      (panelModel, i) => new Panel(panelModel, this.canvasRefs[i]!.current!, fontFamily)
    );

    this.update();

    // TODO: use ResizeObserver on the container element to handle the sidebar
    // expanding/collapsing while on this page.
    window.addEventListener("resize", this.onWindowResize);
    window.addEventListener("mousemove", this.onWindowMouseMove);
    window.addEventListener("mouseup", this.onWindowMouseUp);
    for (const panel of this.panels) {
      // Need to register a non-passive event listener because we may want to
      // prevent the default scrolling behavior on wheel (for scroll-to-zoom
      // functionality).
      panel.container().addEventListener("wheel", (e: WheelEvent) => this.onWheel(e), {
        passive: false,
      });
    }
  }

  componentWillUnmount(): void {
    window.removeEventListener("resize", this.onWindowResize);
    window.addEventListener("mousemove", this.onWindowMouseMove);
    window.addEventListener("mouseup", this.onWindowMouseUp);
    document.body.style.cursor = "";
  }

  private update(dt = 0) {
    this.zoom.step(dt);

    const canvasXPerModelX = this.computeCanvasXPerModelX();
    for (const panel of this.panels) {
      panel.resize();

      // If actively zooming or panning, set scrollX so that mouseModelX stays fixed.
      if (!this.zoom.isAtTarget || this.panning) {
        panel.scrollX =
          this.mouseModelX * canvasXPerModelX - (this.mouse.clientX - panel.container().getBoundingClientRect().left);
      }
      // Set panel x scale
      panel.canvasXPerModelX = canvasXPerModelX;

      // Set sizer div width
      const sizer = panel.container().getElementsByClassName("sizer")[0] as HTMLDivElement;
      sizer.style.width = `${canvasXPerModelX * this.model.xMax}px`;

      // TODO: figure out a less error prone way to keep scrollX / scrollLeft in sync
      panel.scrollX = clamp(panel.scrollX, 0, canvasXPerModelX * this.model.xMax - panel.container().clientWidth);
      panel.container().scrollLeft = panel.scrollX;

      if (this.panning === panel) {
        const scrollTop = this.mouseScrollTop - this.mouse.clientY + panel.container().getBoundingClientRect().top;
        panel.scrollY = clamp(scrollTop, 0, panel.container().scrollHeight - panel.container().clientHeight);
        panel.container().scrollTop = panel.scrollY;
      }

      panel.draw();
    }

    if (this.zoom.isAtTarget) this.animation.stop();
  }

  private updateMouse(mouse: ClientXY) {
    this.mouse = mouse;
    // Update the mouse's model X coordinate (i.e. hovered timestamp).
    // When panning, keep mouseModelX fixed.
    if (!this.panning) {
      const mouseCanvasX =
        this.panels[0].scrollX + (mouse.clientX - this.panels[0].container().getBoundingClientRect().left);
      this.mouseModelX = mouseCanvasX / this.panels[0].canvasXPerModelX;
    }
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
        panel.container().scrollLeft = panel.scrollX;
      }
      panel.draw();
    }
  }

  private onWheel(e: WheelEvent) {
    if (e.ctrlKey || e.shiftKey || e.altKey || e.metaKey) {
      e.preventDefault();
      e.stopPropagation();
      this.updateMouse(e);
      this.zoom.target -= e.deltaY * 0.0003;
      // Start zoom animation.
      this.animation.start();
    }
  }

  private onClickZoom(direction: -1 | 1) {
    this.zoom.target -= direction * 0.1;
    this.animation.start();
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

  private onCanvasMouseDown(e: React.MouseEvent, panelIndex: number) {
    this.panning = this.panels[panelIndex];
    document.body.style.cursor = "grabbing";
    const container = this.panning.container();
    this.updateMouse(e);
    // Capture mouseScrollTop so we can keep it fixed while panning.
    this.mouseScrollTop = container.scrollTop + (this.mouse.clientY - container.getBoundingClientRect().top);
  }

  render() {
    return (
      <div
        className="trace-viewer"
        style={{
          ...({
            "--scrollbar-size": `${constants.SCROLLBAR_SIZE}px`,
          } as CSSProperties),
        }}>
        {this.model.panels.map((panel, i) => (
          <div
            className="panel-container"
            style={{
              width: "100%",
              height: `${panel.height}px`,
              position: "relative",
            }}>
            <div
              key={i}
              className="panel"
              style={{
                height: `${panel.height}px`,
              }}
              onScroll={(e) => this.onScroll(e, i)}>
              <canvas ref={this.canvasRefs[i]} onMouseDown={(e) => this.onCanvasMouseDown(e, i)} />
              {/* This next div is just to make the total content height
                match the size of the panel contents. We can't use very
                large heights on the canvas directly due to browser
                limitations. */}
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
                onClick={() => this.onClickZoom(-1)}
                title={`Zoom out (${modifierKey()}+scroll)`}>
                <ZoomOut className="icon" />
              </button>
              <button
                className="button icon-button"
                onClick={() => this.onClickZoom(+1)}
                title={`Zoom in (${modifierKey()}+scroll)`}>
                <ZoomIn className="icon" />
              </button>
            </div>
          </div>
        ))}
        <EventHovercard ref={this.hovercardRef} buildDuration={this.model.xMax} />
      </div>
    );
  }
}
