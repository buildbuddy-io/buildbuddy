import React from "react";
import { v4 as uuid } from "uuid";
import Disposer from "../../dispose";
import { EventNotifier } from "../../events";

export class HorizontalScrollbarController {
  private readonly disposer = new Disposer();
  public readonly events = new EventNotifier();

  private track: HTMLDivElement | null = null;
  private thumb: HTMLDivElement | null = null;

  private scrollLeft = 0;
  private scrollLeftMax = 0;

  init(track: HTMLDivElement, scrollingElement: HTMLElement) {
    this.track = track;
    this.thumb = this.track.children[0] as any;
    if (!scrollingElement.id) {
      scrollingElement.id = uuid();
    }
    this.thumb!.setAttribute("aria-controls", scrollingElement.id);

    this.disposer
      .subscribe(this.thumb!, "mousedown", this.onMouseDown.bind(this))
      .subscribe(scrollingElement, "mouseenter", this.onMouseEnterScrollingElement.bind(this))
      .subscribe(scrollingElement, "mouseleave", this.onMouseOutScrollingElement.bind(this))
      .subscribe(scrollingElement, "wheel", this.onWheelScrollingElement.bind(this))
      .subscribe(window, "mousemove", this.onMouseMove.bind(this))
      .subscribe(window, "mouseup", this.onMouseUp.bind(this))
      .subscribe(window, "keydown", this.onKeyDown.bind(this));
  }

  private thumbWidth = 0;
  private scrollableWidth = 0;
  private thumbX = 0;

  update({ scrollLeft, scrollLeftMax }: { scrollLeft: number; scrollLeftMax: number }) {
    if (!this.track) return;

    this.scrollLeft = scrollLeft;
    this.scrollLeftMax = scrollLeftMax;
    const trackClientWidth = this.track.clientWidth;
    this.scrollableWidth = this.scrollLeftMax + trackClientWidth;
    this.thumbWidth = (trackClientWidth / this.scrollableWidth) * trackClientWidth;
    this.thumbX =
      scrollLeftMax === 0
        ? 0
        : (this.scrollLeft / this.scrollLeftMax) * (trackClientWidth - this.thumbWidth);

    this.thumb!.style.left = `${this.thumbX}px`;
    this.thumb!.style.width = `${this.thumbWidth}px`;

    this.track.setAttribute(
      "aria-valuenow",
      String(
        Math.round(
          this.thumbX === 0 ? 0 : (100 * this.thumbX) / (trackClientWidth - this.thumbWidth)
        )
      )
    );
  }

  private isScrolling = false;
  // Single scroll event here avoids creating too many objects.
  private scrollEvent = { delta: 0, animate: false };
  private minMouseX = 0;
  private maxMouseX = 0;
  private mouseX = 0;
  onMouseDown(e: MouseEvent) {
    if (e.buttons & 1) {
      this.isScrolling = true;
      const thumb = this.thumb!.getBoundingClientRect();
      const track = this.track!.getBoundingClientRect();
      this.minMouseX = e.clientX - (thumb.x - track.x);
      this.maxMouseX = e.clientX + (track.x + track.width - (thumb.x + thumb.width));
    }
    this.updateMouse(e);
  }
  onMouseMove(e: MouseEvent) {
    if (!this.isScrolling) return;
    if (!(e.buttons & 1)) {
      this.isScrolling = false;
      return;
    }

    const lastX = this.mouseX;
    this.updateMouse(e);
    const deltaX = this.mouseX - lastX;

    if (deltaX !== 0 && this.track!.clientWidth - this.thumbWidth !== 0) {
      this.scrollEvent.delta =
        (deltaX / (this.track!.clientWidth - this.thumbWidth)) * this.scrollLeftMax;
      this.scrollEvent.animate = false;
      this.events.dispatch("scroll", this.scrollEvent);
    }
  }
  onMouseUp(e: MouseEvent) {
    this.updateMouse(e);
    this.isScrolling = false;
  }
  updateMouse(e: MouseEvent) {
    this.mouseX = Math.min(this.maxMouseX, Math.max(this.minMouseX, e.clientX));
  }

  private isMouseInside = false;
  onMouseEnterScrollingElement(e: MouseEvent) {
    this.isMouseInside = true;
  }
  onMouseOutScrollingElement(e: MouseEvent) {
    this.isMouseInside = false;
  }
  onWheelScrollingElement(e: WheelEvent) {
    if (e.shiftKey) {
      e.preventDefault();
      this.scrollEvent.delta = e.deltaY;
      this.scrollEvent.animate = true;
      this.events.dispatch("scroll", this.scrollEvent);
    }
  }
  onKeyDown(e: KeyboardEvent) {
    if (!this.isMouseInside || e.ctrlKey || e.shiftKey) return;

    if (e.which === 39 || e.which === 37) {
      e.preventDefault();
      const dir = e.which - 38;
      this.scrollEvent.delta = 40 * dir;
      this.scrollEvent.animate = true;
      this.events.dispatch("scroll", this.scrollEvent);
    }
  }

  dispose() {
    this.disposer.dispose();
  }
}

export type HorizontalScrollbarProps = {
  setController: (controller: HorizontalScrollbarController) => void;
};

export function HorizontalScrollbar({ setController }: HorizontalScrollbarProps) {
  const trackRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    if (!setController || !trackRef.current) return;

    const controller = new HorizontalScrollbarController();
    controller.init(trackRef.current, trackRef.current.parentElement!);
    setController(controller);
    return () => controller.dispose();
  }, [setController, trackRef]);

  return (
    <div
      className="hScrollTrack"
      ref={trackRef}
      onDragStart={(e) => e.preventDefault()}
      role="scrollbar"
      // This is set in controller.init
      aria-controls=""
      aria-orientation="horizontal"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={0}
    >
      <div className="hScrollThumb" />
    </div>
  );
}
