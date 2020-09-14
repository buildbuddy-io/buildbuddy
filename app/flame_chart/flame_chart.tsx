import React from "react";
import { fromEvent, Subscription } from "rxjs";
import { HorizontalScrollbar } from "../components/scrollbar/scrollbar";
import { AnimatedValue } from "../util/animated_value";
import { AnimationLoop } from "../util/animation_loop";
import { createSvgElement } from "../util/dom";
import { truncateDecimals } from "../util/math";
import { BlockModel, buildFlameChartModel, FlameChartModel } from "./flame_chart_model";
import { TraceEvent } from "./profile_model";
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
  profile: {
    traceEvents: TraceEvent[];
  };
};

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
    this.buildDuration = Math.max(
      ...this.props.profile.traceEvents.map((event) => (event.ts || 0) + (event.dur || 0))
    );
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

    this.setMaxXCoordinate(
      Math.max(...this.chartModel.blocks.map(({ rectProps: { x, width } }) => x + width))
    );
    // Zoom all the way out initially
    this.screenPixelsPerSecond.value = this.screenPixelsPerSecond.target = this.screenPixelsPerSecond.min;

    this.subscription
      .add(fromEvent(window, "mousemove").subscribe(this.onMouseMove.bind(this)))
      .add(fromEvent(window, "mouseup").subscribe(this.onMouseUp.bind(this)))
      .add(fromEvent(window, "resize").subscribe(this.onWindowResize.bind(this)))
      // NOTE: Can't do `<div onWheel={this.onWheel.bind(this)} >` since
      // the event target gets treated as "passive," which forbids us from calling
      // preventDefault() on Ctrl+Wheel
      .add(
        fromEvent(this.viewport, "wheel", { passive: false }).subscribe(this.onWheel.bind(this))
      );

    this.updateDOM();

    if (this.isDebugEnabled) {
      this.renderDebugInfo();
    }
  }

  private onHoverBlock(hoveredBlock: BlockModel) {
    this.hoveredBlockInfoRef.current?.setState({ block: hoveredBlock });
  }

  private setMaxXCoordinate(value: number) {
    this.endTimeSeconds = value * this.secondsPerX;
    this.screenPixelsPerSecond.min = this.getMinPixelsPerSecond();
    this.screenPixelsPerSecond.max = this.getMaxPixelsPerSecond();

    this.screenPixelsPerSecond.target = Math.min(
      this.endTimeSeconds,
      this.viewportRightEdgeSeconds
    );
    this.scrollLeft.max =
      this.endTimeSeconds * this.screenPixelsPerSecond.target - this.barsContainerSvg.clientWidth;
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
    this.scrollLeft.max =
      this.endTimeSeconds * this.screenPixelsPerSecond.value - this.barsContainerSvg.clientWidth;

    if (this.isPanning || !this.screenPixelsPerSecond.isAtTarget) {
      // ensure mouse.seconds does not move from the mouse x position
      const currentMouseSeconds =
        (this.scrollLeft.value + this.mouse.x) / this.screenPixelsPerSecond.value;
      const mouseTimeOffset = this.mouse.seconds - currentMouseSeconds;
      const xCorrection = mouseTimeOffset * this.screenPixelsPerSecond.value;

      this.scrollLeft.target += xCorrection;
      this.scrollLeft.value += xCorrection;
    } else {
      this.scrollLeft.step(dt);
    }
    this.screenPixelsPerSecond.min = this.getMinPixelsPerSecond();

    this.updateDOM();
  }

  private get viewportRightEdgeSeconds() {
    return (
      (this.barsContainerSvg.clientWidth + this.scrollLeft.value) / this.screenPixelsPerSecond.value
    );
  }

  private onWheel(e: WheelEvent) {
    if (e.ctrlKey) {
      e.preventDefault();
      e.stopPropagation();
      this.updateMouse(e);
      const scrollSpeedMultiplier = 0.005;
      const y0 = Math.log(this.screenPixelsPerSecond.target);
      const y1 = y0 + e.deltaY * scrollSpeedMultiplier;
      this.screenPixelsPerSecond.target = Math.pow(Math.E, y1);
      this.animation.start();
    }
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
    // Right mouse button is treated as mouse up.
    if (e.button === 2) {
      this.onMouseUp(e);
      return;
    }
    this.updateMouse(e);
    if (e.button === 1) {
      this.setCursorOverride("grabbing");
      this.isPanning = true;
    }
  }
  private onMouseUp(e: MouseEvent) {
    this.setCursorOverride(null);
    // middle click
    if (e.button === 1) {
      this.isPanning = false;
    }
  }

  private setCursorOverride(cursor: "grabbing" | null) {
    this.barsContainerSvg.style.cursor = cursor;
  }

  private mouse = { x: 0, seconds: 0 };
  private updateMouse(e: MouseEvent) {
    const x = e.clientX - this.barsContainerSvg.getBoundingClientRect().x;
    this.mouse = {
      x,
      // If panning, do not allow changing the mouse grid
      seconds: this.isPanning
        ? this.mouse.seconds
        : (this.scrollLeft.value + x) / this.screenPixelsPerSecond.value,
    };
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
      `translate(${-this.scrollLeft} 0) scale(${
        this.secondsPerX * this.screenPixelsPerSecond.value
      } 1)`
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
    const endSeconds =
      (this.scrollLeft.value + this.barsContainerSvg.clientWidth) /
      this.screenPixelsPerSecond.value;

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

  render() {
    if (!this.chartModel) {
      // Return a div that is used to measure the area where the chart
      // will be rendered.
      return <div ref={this.rulerRef} />;
    }

    // TODO: empty state
    return (
      <>
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
              }}
            >
              <g ref={this.gridlinesRef}></g>
            </svg>
            <div
              className="viewport"
              ref={this.viewportRef}
              onMouseDown={this.onMouseDown.bind(this)}
            >
              <div
                style={{
                  position: "absolute",
                  top: TIMESTAMP_HEADER_SIZE,
                  left: 0,
                  right: 0,
                  pointerEvents: "none",
                }}
              >
                <div>
                  {this.chartModel.sections.map(({ name, height }, i) => (
                    <div
                      key={i}
                      className="flame-chart-section"
                      style={{
                        height,
                      }}
                    >
                      <div
                        className="flame-chart-section-header"
                        style={{ height: SECTION_LABEL_HEIGHT }}
                      >
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
              }}
            >
              <rect
                className="flame-chart-timestamp-header"
                x="0"
                width="100%"
                height={TIMESTAMP_HEADER_SIZE}
              />
              <g ref={this.headerRef}></g>
            </svg>
            <HorizontalScrollbar
              ref={this.horizontalScrollbarRef}
              onScroll={this.onHorizontalScroll.bind(this)}
            />
          </div>
        </div>
        <HoveredBlockInfo ref={this.hoveredBlockInfoRef} buildDuration={this.buildDuration} />
      </>
    );
  }

  componentWillUnmount() {
    this.animation.stop();
    this.subscription.unsubscribe();
  }
}

type HoveredBlockInfoState = { block?: BlockModel };

const MICROSECONDS_PER_SECOND = 1_000_000;

class HoveredBlockInfo extends React.Component<{ buildDuration: number }, HoveredBlockInfoState> {
  state: HoveredBlockInfoState = {};

  render() {
    const { block } = this.state;
    const { buildDuration } = this.props;

    if (!block) {
      return (
        <div className="flame-chart-hovered-block-info no-block-hovered">
          Hover a block in the flame chart to see more info.
        </div>
      );
    }

    const {
      event: { name, cat: category, ts, dur },
    } = block;

    const buildFraction = ((dur / buildDuration) * 100).toFixed(2);
    const percentage = buildFraction ? `${buildFraction} %` : "< 0.01 %";

    return (
      <div className="flame-chart-hovered-block-info">
        <div className="hovered-block-title">{name}</div>
        <div className="hovered-block-details">
          <div>{category}</div>
          <div>
            <span className="data">{ts / MICROSECONDS_PER_SECOND}</span> seconds &ndash;{" "}
            <span className="data">{(ts + dur) / MICROSECONDS_PER_SECOND}</span> seconds
          </div>
          <div>
            <span className="data">{dur / MICROSECONDS_PER_SECOND}</span> seconds total (
            <span className="data">{percentage}</span> of total build duration)
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
};

/** The blocks rendered within the flame chart timeline. */
class FlameChartBlocks extends React.Component<FlameChartBlocksProps> {
  private hoveredBlock: { element: SVGRectElement; index: number } | null = null;

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
  }

  render() {
    return (
      <g onMouseMove={this.onMouseMove.bind(this)}>
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
