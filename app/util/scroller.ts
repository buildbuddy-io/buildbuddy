import { AnimatedValue } from "./animated_value";
import { AnimationLoop } from "./animation_loop";

interface ScrollableElement {
  scrollTop: number;
  readonly scrollHeight: number;
  readonly clientHeight: number;
}

/**
 * Scroller allows controlling scroll position of an element, optionally with smooth
 * scrolling.
 */
export class Scroller {
  private position = new AnimatedValue(0, { min: 0 });
  private animation = new AnimationLoop((dt: number) => this.step(dt));

  constructor(private getElement: () => ScrollableElement | null) {}

  scrollTo(top: number, { animate = true } = {}) {
    this.updateBounds();
    this.position.target = top;
    if (!animate) {
      this.position.value = top;
    }
    this.animation.start();
  }

  getMax(): number {
    this.updateBounds();
    return this.position.max;
  }

  private step(dt: number) {
    this.updateBounds();
    this.position.step(dt, { rate: 0.05, threshold: 1 });
    const el = this.getElement();
    if (el) el.scrollTop = this.position.value;
    if (this.position.isAtTarget) this.animation.stop();
  }

  private updateBounds() {
    const el = this.getElement();
    if (!el) return;
    this.position.max = el.scrollHeight - el.clientHeight;
  }
}
