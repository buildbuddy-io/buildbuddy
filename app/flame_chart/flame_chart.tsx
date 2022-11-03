import { ZoomIn, ZoomOut } from "lucide-react";
import React from "react";
import { fromEvent, Subscription } from "rxjs";
import { HorizontalScrollbar } from "../components/scrollbar/scrollbar";
import { AnimatedValue } from "../util/animated_value";
import { AnimationLoop } from "../util/animation_loop";
import { createSvgElement } from "../util/dom";
import { truncateDecimals } from "../util/math";
import { BlockModel, buildFlameChartModel, FlameChartModel } from "./flame_chart_model";
import { Profile } from "./profile_model";
import {
  BLOCK_HEIGHT,
  INITIAL_END_TIME_SECONDS,
  SECTION_LABEL_HEIGHT,
  TIMESTAMP_FONT_SIZE,
  TIMESTAMP_HEADER_SIZE,
  VERTICAL_SCROLLBAR_WIDTH,
} from "./style_constants";

// NOTE: Do not add state to the flame chart since it is expensive to re-render.
type ProfileFlameChartState = {};

export type FlameChartProps = {
  profile: Profile;
};

const stopPropagation = (e: any) => e.stopPropagation();

const LEFT_MOUSE_BUTTON = 0;

export default class FlameChart extends React.Component<FlameChartProps, ProfileFlameChartState> {
  private animation = new AnimationLoop((dt: number) => this.draw(dt));

  private isDebugEnabled = window.localStorage.getItem("buildbuddy://debug/flame-chart") === "true";

  /* Viewport X offset in screen pixels. */
  private readonly scrollLeft = new AnimatedValue(0, { min: 0 });
  /** Zoom level. */
  private readonly screenPixelsPerSecond = new AnimatedValue(1, { min: 10 });
  /** The max timestamp that can be displayed, in seconds. */
  private endTimeSeconds = INITIAL_END_TIME_SECONDS;

  private subscription = new Subscription();

  private rulerRef = React.createRef<HTMLDivElement>();
  private viewportRef = React.createRef<HTMLDivElement>();
  private barsContainerRef = React.createRef<SVGSVGElement>();
  private headerRef = React.createRef<SVGGElement>();
  private contentContainerRef = React.createRef<SVGGElement>();
  private gridlinesRef = React.createRef<SVGGElement>();
  private debugRef = React.createRef<HTMLPreElement>();
  private horizontalScrollbarRef = React.createRef<HorizontalScrollbar>();
  private hoveredBlockInfoRef = React.createRef<HoveredBlockInfo>();

  private viewport: HTMLDivElement;
  private barsContainerSvg: SVGSVGElement;
  private header: SVGGElement;
  private contentContainer: SVGGElement;
  private gridlines: SVGGElement;

  private horizontalScrollbar: HorizontalScrollbar;

  private chartModel: FlameChartModel;
  private buildDuration: number;

  componentDidMount() {
    this.buildDuration = this.props.profile.traceEvents
      .map((event) => (event.ts || 0) + (event.dur || 0))
      .reduce((a, b) => Math.max(a, b));
    const rulerWidth = this.rulerRef.current.getBoundingClientRect().width;
    const singlePixelDuration = this.buildDuration / rulerWidth;

    this.chartModel = buildFlameChartModel(this.props.profile.traceEvents, {
      visibilityThreshold: singlePixelDuration,
    });

    // Re-render now that the chart model has been computed with the container
    // dimensions taken into account.
    this.forceUpdate();
  }

  componentDidUpdate() {
    if (!this.chartModel) return;

    this.viewport = this.viewportRef.current;
    this.barsContainerSvg = this.barsContainerRef.current;
    this.header = this.headerRef.current;
    this.contentContainer = this.contentContainerRef.current;
    this.gridlines = this.gridlinesRef.current;

    this.horizontalScrollbar = this.horizontalScrollbarRef.current;

    const xMax = this.chartModel.blocks
      .map((block) => block.rectProps.x + block.rectProps.width)
      .reduce((a, b) => Math.max(a, b));
    this.setMaxXCoordinate(xMax);
    // Zoom all the way out initially
    this.screenPixelsPerSecond.value = this.screenPixelsPerSecond.target = this.screenPixelsPerSecond.min;

    this.subscription
      .add(fromEvent(window, "mousemove").subscribe(this.onMouseMove.bind(this)))
      .add(fromEvent(window, "mouseup").subscribe(this.onMouseUp.bind(this)))
      .add(fromEvent(window, "resize").subscribe(this.onWindowResize.bind(this)))
      // NOTE: Can't do `<div onWheel={this.onWheel.bind(this)} >` since
      // the event target gets treated as "passive," which forbids us from calling
      // preventDefault() on Ctrl+Wheel
      .add(fromEvent(this.viewport, "wheel", { passive: false }).subscribe(this.onWheel.bind(this)));

    this.updateDOM();

    if (this.isDebugEnabled) {
      this.renderDebugInfo();
    }
  }

  private onHoverBlock(hoveredBlock: BlockModel) {
    this.hoveredBlockInfoRef.current?.setState({ block: hoveredBlock });
  }
  private onBlocksMouseMove(e: React.MouseEvent<SVGGElement, MouseEvent>) {
    const x = e.clientX;
    const y = e.clientY;
    this.hoveredBlockInfoRef.current?.setState({ x, y });
  }
  private onBlocksMouseLeave(e: React.MouseEvent<SVGGElement, MouseEvent>) {
    this.hoveredBlockInfoRef.current?.setState({ block: null });
  }

  private setMaxXCoordinate(value: number) {
    this.endTimeSeconds = value * this.secondsPerX;
    this.screenPixelsPerSecond.min = this.getMinPixelsPerSecond();
    this.screenPixelsPerSecond.max = this.getMaxPixelsPerSecond();

    this.screenPixelsPerSecond.target = Math.min(this.endTimeSeconds, this.viewportRightEdgeSeconds);
    this.scrollLeft.max = this.endTimeSeconds * this.screenPixelsPerSecond.target - this.barsContainerSvg.clientWidth;
    this.animation.start();
  }

  private getMinPixelsPerSecond() {
    return this.barsContainerSvg.clientWidth / this.endTimeSeconds;
  }
  private getMaxPixelsPerSecond() {
    // Don't allow zooming in more than 1ms per 100 pixels.
    return 100 / 0.001;
  }

  private draw(dt: number) {
    this.update(dt);

    if (this.scrollLeft.isAtTarget && this.screenPixelsPerSecond.isAtTarget) {
      this.animation.stop();
    }
  }

  private update(dt: number) {
    this.screenPixelsPerSecond.step(dt);
    this.scrollLeft.max = this.endTimeSeconds * this.screenPixelsPerSecond.value - this.barsContainerSvg.clientWidth;

    if (this.isPanning || !this.screenPixelsPerSecond.isAtTarget) {
      // ensure mouse.seconds does not move from the mouse x position
      const currentMouseSeconds = (this.scrollLeft.value + this.mouse.x) / this.screenPixelsPerSecond.value;
      const mouseTimeOffset = this.mouse.seconds - currentMouseSeconds;
      const xCorrection = mouseTimeOffset * this.screenPixelsPerSecond.value;

      this.scrollLeft.target += xCorrection;
      this.scrollLeft.value += xCorrection;
    } else {
      this.scrollLeft.step(dt);
    }
    this.screenPixelsPerSecond.min = this.getMinPixelsPerSecond();

    if (this.isPanning) {
      this.viewport.scrollTop = this.mouse.scrollTop - this.mouse.y;
    }

    this.updateDOM();
  }

  private get viewportRightEdgeSeconds() {
    return (this.barsContainerSvg.clientWidth + this.scrollLeft.value) / this.screenPixelsPerSecond.value;
  }

  private onWheel(e: WheelEvent) {
    if (e.ctrlKey || e.shiftKey || e.altKey || e.metaKey) {
      e.preventDefault();
      e.stopPropagation();
      this.updateMouse(e);
      this.adjustZoom(e.deltaY);
    }
  }
  private adjustZoom(delta: number) {
    const scrollSpeedMultiplier = 0.005;
    const y0 = Math.log(this.screenPixelsPerSecond.target);
    const y1 = y0 + delta * scrollSpeedMultiplier;
    this.screenPixelsPerSecond.target = Math.pow(Math.E, y1);
    this.animation.start();
  }
  private onMouseMove(e: MouseEvent) {
    this.updateMouse(e);
    if (this.isPanning) {
      this.animation.start();
    }
  }
  private onWindowResize() {
    this.animation.start();
  }

  private isPanning = false;
  private onMouseDown(e: MouseEvent) {
    // Non-left click cancels pan.
    if (e.button !== LEFT_MOUSE_BUTTON) {
      this.onMouseUp(e);
      return;
    }

    this.updateMouse(e);
    this.setCursorOverride("grabbing");
    this.isPanning = true;
  }
  private onMouseUp(e: MouseEvent) {
    this.setCursorOverride(null);

    if (e.button === LEFT_MOUSE_BUTTON) {
      this.isPanning = false;
    }
  }

  private setCursorOverride(cursor: "grabbing" | null) {
    document.body.style.cursor = cursor;
  }

  private mouse = { x: 0, seconds: 0, y: 0, scrollTop: 0 };
  private updateMouse(e: MouseEvent) {
    const x = e.clientX - this.barsContainerSvg.getBoundingClientRect().x;
    const y = e.clientY - this.viewport.getBoundingClientRect().y;
    this.mouse = {
      x,
      // If panning, do not allow changing the mouse grid
      seconds: this.isPanning ? this.mouse.seconds : (this.scrollLeft.value + x) / this.screenPixelsPerSecond.value,
      y,
      // If panning, keep scrollTop fixed so we can compute the delta
      scrollTop: this.isPanning ? this.mouse.scrollTop : this.viewport.scrollTop + y,
    };
  }
  private setZoomOriginToCenter() {
    const rect = this.barsContainerSvg.getBoundingClientRect();
    const x = rect.x + rect.width / 2;
    this.mouse.x = x;
    this.mouse.seconds = (this.scrollLeft.value + x) / this.screenPixelsPerSecond.value;
  }

  private onHorizontalScroll({ delta: deltaX, animate }: { delta: number; animate: boolean }) {
    this.scrollLeft.target = this.scrollLeft.target + deltaX;
    if (!animate) {
      this.scrollLeft.value = this.scrollLeft.target;
    }
    this.animation.start();
  }

  private get secondsPerX() {
    return 1;
  }

  private updateDOM() {
    this.contentContainer.setAttribute(
      "transform",
      `translate(${-this.scrollLeft} 0) scale(${this.secondsPerX * this.screenPixelsPerSecond.value} 1)`
    );
    this.gridlines.setAttribute("transform", `translate(${-this.scrollLeft} 0)`);
    this.header.setAttribute("transform", `translate(${-this.scrollLeft} 0)`);

    this.drawGridlines();

    this.horizontalScrollbar.update({
      scrollLeft: this.scrollLeft.value,
      scrollLeftMax: this.scrollLeft.max,
    });
  }

  private renderDebugInfo() {
    const el = this.debugRef.current;
    const debugDrawLoop = new AnimationLoop(() => {
      el.innerHTML = JSON.stringify(
        {
          panning: this.isPanning,
          pixelsPerSecond: this.screenPixelsPerSecond.toJson(),
          scrollLeft: this.scrollLeft.toJson(),
          mouse: this.mouse,
          timeline: {
            transform: this.contentContainer.getAttribute("transform"),
            endTimeSeconds: this.endTimeSeconds,
            rightBoundarySeconds: this.viewportRightEdgeSeconds,
          },
        },
        null,
        2
      );
    });
    debugDrawLoop.start();
    this.subscription.add(() => debugDrawLoop.stop());
  }

  private drawGridlines() {
    if (this.screenPixelsPerSecond.value === 0) {
      return;
    }
    const startSeconds = this.scrollLeft.value / this.screenPixelsPerSecond.value;
    const endSeconds = (this.scrollLeft.value + this.barsContainerSvg.clientWidth) / this.screenPixelsPerSecond.value;

    const { firstGridlineSeconds, intervalSeconds, count } = computeGridlines(
      startSeconds,
      endSeconds,
      this.barsContainerSvg.clientWidth
    );

    const gridlinesG = this.gridlines;
    const timestampG = this.header;
    gridlinesG.innerHTML = "";
    timestampG.innerHTML = "";

    if (window.innerWidth / count < 1) {
      console.error("Too many gridlines to draw. This probably indicates a bug. Stopping.", {
        firstGridlineSeconds,
        count,
        intervalSeconds,
      });
      return;
    }

    for (let i = 0; i < count; i++) {
      const seconds = firstGridlineSeconds + i * intervalSeconds;
      const x = seconds * this.screenPixelsPerSecond.value;

      const line = createSvgElement("line") as SVGLineElement;
      line.setAttribute("x1", String(x));
      line.setAttribute("x2", String(x));
      line.setAttribute("y1", "0");
      line.setAttribute("y2", String(this.barsContainerSvg.clientHeight));
      line.setAttribute("vector-effect", "non-scaling-stroke");
      // TODO: adjust gridline color based on order of magnitude?
      line.setAttribute("stroke", "#ccc");
      line.setAttribute("stroke-width", "1");
      line.setAttribute("shape-rendering", "crispEdges");
      gridlinesG.append(line);

      const label = createSvgElement("text") as SVGTextElement;
      label.innerHTML = `${truncateDecimals(seconds, 6)}s`;
      label.setAttribute("x", `${x + 2}`);
      label.setAttribute("y", `${TIMESTAMP_FONT_SIZE}`);
      label.setAttribute("font-size", `${TIMESTAMP_FONT_SIZE}px`);
      timestampG.append(label);
    }
  }

  private onZoomOutClick() {
    this.onZoomButtonClick(-1);
  }
  private onZoomInClick() {
    this.onZoomButtonClick(+1);
  }
  private onZoomButtonClick(direction: number) {
    this.setZoomOriginToCenter();
    this.adjustZoom(direction * 60);
  }

  render() {
    if (!this.chartModel) {
      // Return a div that is used to measure the area where the chart
      // will be rendered.
      return <div ref={this.rulerRef} />;
    }

    // TODO: empty state
    return (
      <div className="flame-chart-container">
        <div className="flame-chart">
          <div className="timeline" style={{ position: "relative" }}>
            <svg
              style={{
                pointerEvents: "none",
                position: "absolute",
                top: 0,
                left: 0,
                height: "100%",
                width: `calc(100% - ${VERTICAL_SCROLLBAR_WIDTH}px)`,
              }}>
              <g ref={this.gridlinesRef}></g>
            </svg>
            <div className="viewport" ref={this.viewportRef} onMouseDown={this.onMouseDown.bind(this)}>
              <div
                style={{
                  position: "absolute",
                  top: TIMESTAMP_HEADER_SIZE,
                  left: 0,
                  right: 0,
                  pointerEvents: "none",
                }}>
                <div className="flame-chart-sections">
                  {this.chartModel.sections.map(({ name, height }, i) => (
                    <div
                      key={i}
                      className="flame-chart-section"
                      style={{
                        height,
                      }}>
                      <div className="flame-chart-section-header" style={{ height: SECTION_LABEL_HEIGHT }}>
                        {name}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <svg style={{ position: "absolute" }} ref={this.barsContainerRef} className="tracks">
                <g transform={`translate(0 ${TIMESTAMP_HEADER_SIZE})`}>
                  <g ref={this.contentContainerRef} transform={`scale(${this.secondsPerX} 1)`}>
                    <FlameChartBlocks
                      blocks={this.chartModel.blocks}
                      onHover={this.onHoverBlock.bind(this)}
                      onMouseMove={this.onBlocksMouseMove.bind(this)}
                      onMouseLeave={this.onBlocksMouseLeave.bind(this)}
                    />
                  </g>
                </g>
              </svg>
              <pre
                ref={this.debugRef}
                hidden={!this.isDebugEnabled}
                style={{
                  background: "black",
                  position: "fixed",
                  color: "white",
                  bottom: 0,
                  left: 0,
                  opacity: 0.8,
                  zIndex: 100,
                  pointerEvents: "none",
                  fontSize: 10,
                  margin: 0,
                }}
              />
            </div>
            <svg
              style={{
                pointerEvents: "none",
                position: "absolute",
                top: 0,
                left: 0,
                height: "100%",
                width: `calc(100% - ${VERTICAL_SCROLLBAR_WIDTH}px)`,
              }}>
              <rect className="flame-chart-timestamp-header" x="0" width="100%" height={TIMESTAMP_HEADER_SIZE} />
              <g ref={this.headerRef}></g>
            </svg>
            <HorizontalScrollbar ref={this.horizontalScrollbarRef} onScroll={this.onHorizontalScroll.bind(this)} />
          </div>
          <div className="flame-chart-controls">
            <button
              aria-label="Zoom out"
              onClick={this.onZoomOutClick.bind(this)}
              onMouseMove={stopPropagation}
              title="Zoom out (Ctrl + scroll up)">
              <ZoomOut className="icon black" />
            </button>
            <button
              aria-label="Zoom in"
              onClick={this.onZoomInClick.bind(this)}
              onMouseMove={stopPropagation}
              title="Zoom in (Ctrl + scroll down)">
              <ZoomIn className="icon black" />
            </button>
          </div>
        </div>
        <HoveredBlockInfo ref={this.hoveredBlockInfoRef} buildDuration={this.buildDuration} />
      </div>
    );
  }

  componentWillUnmount() {
    this.animation.stop();
    this.subscription.unsubscribe();
  }
}

type HoveredBlockInfoState = { block?: BlockModel; x?: number; y?: number };

const MICROSECONDS_PER_SECOND = 1_000_000;

class HoveredBlockInfo extends React.Component<{ buildDuration: number }, HoveredBlockInfoState> {
  state: HoveredBlockInfoState = {};
  private blockRef = React.createRef<HTMLDivElement>();

  private getHorizontalOverflow() {
    if (!this.blockRef.current || !this.state.block) return 0;

    const width = this.blockRef.current.getBoundingClientRect().width;
    const rightEdgeX = this.state.x + width;
    return Math.max(0, rightEdgeX - window.innerWidth);
  }

  render() {
    const { block } = this.state;
    const { buildDuration } = this.props;

    if (!block) return <></>;

    const {
      event: { name, cat: category, ts, dur, out, args },
    } = block;

    const buildFraction = ((dur / buildDuration) * 100).toFixed(2);
    const percentage = buildFraction ? `${buildFraction} %` : "< 0.01 %";
    const duration = truncateDecimals(dur / MICROSECONDS_PER_SECOND, 3);
    const displayedDuration = duration === 0 ? "< 0.001" : `${duration}`;

    return (
      <div
        className="flame-chart-hovered-block-info"
        ref={this.blockRef}
        style={{
          position: "fixed",
          top: (this.state.y || 0) + 16,
          left: (this.state.x || 0) - 16 - this.getHorizontalOverflow(),
          pointerEvents: "none",
          // Make the block invisible on the first render while we compute the
          // horizontal overflow based on the actual rendered size.
          opacity: this.blockRef.current ? 1 : 0,
        }}>
        <div className="hovered-block-title">{name}</div>
        <div className="hovered-block-details">
          <div>
            {category}
            {(args?.target && <div>Target: {args?.target}</div>) || ""}
            {(out && <div>Out: {out}</div>) || ""}
          </div>

          <div className="duration">
            <span className="data">{displayedDuration}</span> seconds total (<span className="data">{percentage}</span>{" "}
            of total build duration)
          </div>
          <div>
            @ {truncateDecimals(ts / MICROSECONDS_PER_SECOND, 3)} s &ndash;{" "}
            {truncateDecimals((ts + dur) / MICROSECONDS_PER_SECOND, 3)} s
          </div>
        </div>
      </div>
    );
  }
}

const IDEAL_PIXELS_PER_GRIDLINE = 80;

function computeGridlines(startTimeSeconds: number, endTimeSeconds: number, widthPixels: number) {
  const displayedDuration = endTimeSeconds - startTimeSeconds;
  const targetCount = widthPixels / IDEAL_PIXELS_PER_GRIDLINE;
  const count = Math.ceil(targetCount);

  const targetInterval = displayedDuration / targetCount;
  const intervalSeconds = Math.pow(10, Math.ceil(Math.log10(targetInterval)));

  const firstGridlineSeconds = startTimeSeconds - (startTimeSeconds % intervalSeconds);

  return {
    firstGridlineSeconds,
    intervalSeconds,
    count,
  };
}

type FlameChartBlocksProps = {
  blocks: BlockModel[];
  onHover: (block: BlockModel) => void;
  onMouseMove: (e: React.MouseEvent<SVGGElement, MouseEvent>) => void;
  onMouseLeave: (e: React.MouseEvent<SVGGElement, MouseEvent>) => void;
};

/** The blocks rendered within the flame chart timeline. */
class FlameChartBlocks extends React.Component<FlameChartBlocksProps> {
  private hoveredBlock: {
    element: SVGRectElement;
    index: number;
  } | null = null;

  private onMouseMove(e: React.MouseEvent<SVGGElement, MouseEvent>) {
    if ((e.target as Element).tagName !== "rect") return;
    const rect = e.target as SVGRectElement;

    const index = Number(rect.dataset["index"]);
    if (this.hoveredBlock) {
      this.hoveredBlock.element.classList.remove("hover");
    }
    this.hoveredBlock = {
      element: rect,
      index,
    };
    rect.classList.add("hover");
    this.props.onHover(this.props.blocks[index]);
    this.props.onMouseMove(e);
  }

  private onMouseLeave(e: React.MouseEvent<SVGGElement, MouseEvent>) {
    if (this.hoveredBlock) {
      this.hoveredBlock.element.classList.remove("hover");
    }
    this.hoveredBlock = null;
    this.props.onMouseLeave(e);
  }

  render() {
    return (
      <g onMouseMove={this.onMouseMove.bind(this)} onMouseLeave={this.onMouseLeave.bind(this)}>
        {this.props.blocks.map((block: any, i: number) => (
          <rect
            key={i}
            data-index={i}
            {...block.rectProps}
            height={BLOCK_HEIGHT}
            shapeRendering="crispEdges"
            vectorEffect="non-scaling-stroke"
          />
        ))}
      </g>
    );
  }
}
