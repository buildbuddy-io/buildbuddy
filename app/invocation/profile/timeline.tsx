import { AnimatedValue, AnimationLoop } from "lib/animation";
import {
  HorizontalScrollbar,
  HorizontalScrollbarController,
} from "lib/components/scrollbar/Scrollbar";
import Disposer from "lib/dispose";
import { createSvgElement } from "lib/dom";
import { magnitude } from "lib/math";
import { setCursorOverride } from "lib/mouse";
import React from "react";

const INITIAL_GRID_SIZE = 20;

export class TimelineController {
  private svg: SVGElement;
  private grid: SVGGElement;
  private gridlines: SVGGElement;

  private disposer = new Disposer();
  private animation = new AnimationLoop((dt: number) => this.draw(dt));

  public scrollLeft = new AnimatedValue(0, { min: 0 });
  // Zoom in to one second by default.
  // TODO: scale to max event time
  public scale = new AnimatedValue(1, { min: 10 });

  constructor(private el: HTMLDivElement, private scrollController: HorizontalScrollbarController) {
    this.svg = el.querySelector(".tracks");
    this.grid = this.svg.querySelector("g.grid") as SVGGElement;
    this.gridlines = this.grid.querySelector("g.gridlines") as SVGGElement;

    this.scrollLeft.max = this.gridSize * this.scale.value - this.el.clientWidth;
    this.scale.max = 1000000;
    this.setGridSize(1);

    this.disposer
      .subscribe(this.el, "wheel", this.onWheel)
      .subscribe(window, "mousemove", this.onMouseMove)
      .subscribe(this.el, "mousedown", this.onMouseDown)
      .subscribe(window, "mouseup", this.onMouseUp)
      .subscribe(this.scrollController.events, "scroll", this.onHorizontalScroll);

    this.updateDOM();
  }

  public gridSize = INITIAL_GRID_SIZE;
  private displayedGridSize = this.gridSize;

  public setGridSize(value: number) {
    this.gridSize = value;
    this.scale.min = this.getMinScale();
    this.scale.target = Math.min(this.gridSize, this.displayedGridSize);
    this.scrollLeft.max = this.gridSize * this.scale.target - this.el.clientWidth;
    this.animation.start();
  }

  private getMinScale() {
    return this.el.clientWidth / this.gridSize;
  }

  private draw = (dt: number) => {
    this.update(dt);

    if (this.scrollLeft.isAtTarget && this.scale.isAtTarget) {
      this.animation.stop();
    }
  };

  private update = (dt: number) => {
    this.scale.step(dt);
    this.scrollLeft.max = this.gridSize * this.scale.value - this.el.clientWidth;

    if (this.isPanning || !this.scale.isAtTarget) {
      // ensure mouse.grid does not move from the mouse x position
      const currentMouseGrid = (this.scrollLeft.value + this.mouse.x) / this.scale.value;
      const gridCorrection = this.mouse.grid - currentMouseGrid;
      const xCorrection = gridCorrection * this.scale.value;
      this.scrollLeft.target += xCorrection;
      this.scrollLeft.value += xCorrection;
    } else {
      this.scrollLeft.step(dt);
    }

    this.displayedGridSize = (this.el.clientWidth + this.scrollLeft.value) / this.scale.value;
    this.scale.min = this.getMinScale();

    this.updateDOM();
  };

  private onWheel = (e: WheelEvent) => {
    if (e.ctrlKey) {
      e.preventDefault();
      this.updateMouse(e);
      const scrollSpeedMultiplier = 0.005;
      const y0 = Math.log(this.scale.target);
      const y1 = y0 + e.deltaY * scrollSpeedMultiplier;
      this.scale.target = Math.pow(Math.E, y1);
      this.animation.start();
    }
  };
  private onMouseMove = (e: MouseEvent) => {
    this.updateMouse(e);
    if (this.isPanning) {
      this.animation.start();
    }
  };

  private isPanning = false;
  private onMouseDown = (e: MouseEvent) => {
    // Right mouse button is treated as mouse up
    if (e.button === 2) {
      this.onMouseUp(e);
      return;
    }
    this.updateMouse(e);
    if (e.button === 1) {
      setCursorOverride("grabbing");
      this.isPanning = true;
    }
  };
  private onMouseUp = (e: MouseEvent) => {
    setCursorOverride(null);
    // middle click
    if (e.button === 1) {
      this.isPanning = false;
    }
  };

  private mouse = { x: 0, grid: 0 };
  private updateMouse = (e: MouseEvent) => {
    const x = e.clientX - this.svg.getBoundingClientRect().x;
    this.mouse = {
      x,
      // If panning, do not allow changing the mouse grid
      grid: this.isPanning ? this.mouse.grid : (this.scrollLeft.value + x) / this.scale.value,
    };
  };

  private onHorizontalScroll = ({
    delta: deltaX,
    animate,
  }: {
    delta: number;
    animate: boolean;
  }) => {
    this.scrollLeft.target = this.scrollLeft.target + deltaX;
    if (!animate) {
      this.scrollLeft.value = this.scrollLeft.target;
    }
    this.animation.start();
  };

  private updateDOM = () => {
    this.grid.setAttribute("transform", `translate(${-this.scrollLeft} 0) scale(${this.scale} 1)`);
    this.gridlines.setAttribute("transform", `scale(1 ${this.svg.clientHeight})`);
    drawGridlines(
      this.gridlines as SVGGElement,
      this.scrollLeft.value,
      this.scrollLeft.value + this.scale.value * this.el.clientWidth,
      this.el.clientWidth
    );

    this.scrollController.update({
      scrollLeft: this.scrollLeft.value,
      scrollLeftMax: this.scrollLeft.max,
    });
  };

  renderDebugInfo = (el: HTMLElement | null | undefined) => {
    if (el.getAttribute("hidden")) return;

    this.disposer.runAnimationLoop(() => {
      el.innerHTML = JSON.stringify(
        {
          panning: this.isPanning,
          scale: this.scale.toJson(),
          scrollLeft: this.scrollLeft.toJson(),
          mouse: this.mouse,
          grid: {
            transform: this.grid.getAttribute("transform"),
            size: this.gridSize,
            displayed: this.displayedGridSize,
          },
        },
        null,
        2
      );
    });
  };

  dispose() {
    this.disposer.dispose();
  }
}

export function getGridlineGap(startX: number, endX: number, viewportClientWidth: number) {
  const width = endX - startX;
  return magnitude(width) / 10;
}

export function drawGridlines(
  g: SVGGElement,
  startX: number,
  endX: number,
  viewportClientWidth: number
) {
  g.innerHTML = "";

  const gap = getGridlineGap(startX, endX, viewportClientWidth);

  for (let i = Math.floor(startX); i < endX + 1; i += gap) {
    const line = createSvgElement("line");
    line.setAttribute("x1", String(i));
    line.setAttribute("x2", String(i));
    line.setAttribute("y1", "0");
    line.setAttribute("y2", "1");
    line.setAttribute("vector-effect", "non-scaling-stroke");
    line.setAttribute("stroke", "#ccc");
    line.setAttribute("stroke-width", "1");
    line.setAttribute("shape-rendering", "crispEdges");
    g.append(line);
  }
}

export type TimelineProps = {
  children?: React.ReactNode;
};
const TimelineContext = React.createContext<TimelineController | null>(null);
export function useTimeline() {
  return React.useContext(TimelineContext);
}

export default function SvgTimeline({ children }: TimelineProps) {
  const debugRef = React.useRef();

  const timelineRef = React.useRef<HTMLDivElement>(null);

  const [scrollController, setScrollController] = React.useState<HorizontalScrollbarController>(
    null
  );

  const controller = React.useMemo(() => {
    if (!timelineRef.current || !scrollController || !debugRef.current) {
      return null;
    }
    const controller = new TimelineController(timelineRef.current, scrollController);
    controller.renderDebugInfo(debugRef.current);
    return controller;
  }, [timelineRef, debugRef, scrollController]);

  React.useEffect(() => {
    return () => controller?.dispose();
  }, [controller]);

  const debug = window.localStorage.getItem("buildbuddy://debug/flame-chart") === "true";

  return (
    <TimelineContext.Provider value={controller}>
      <div className="timeline" style={{ position: "relative" }}>
        <div className="viewport" ref={timelineRef}>
          <svg className="tracks">
            <g className="grid">
              <g className="gridlines"></g>
              <g className="events">{children}</g>
            </g>
          </svg>
          <pre
            ref={debugRef}
            hidden={!debug}
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
        <HorizontalScrollbar setController={setScrollController} />
      </div>
    </TimelineContext.Provider>
  );
}
