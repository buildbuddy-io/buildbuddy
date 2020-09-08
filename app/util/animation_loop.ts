/**
 * Utility class for running animations.
 */
export class AnimationLoop {
  private lastTimestamp: number | null = null;

  constructor(private callback: (dt: number) => void, private enabled_ = false) {
    if (enabled_) {
      this.start();
    }
  }

  private loop() {
    if (!this.enabled_ || this.isNextFrameScheduled) return;

    this.callback(this.getTimeSinceLastUpdate());
    this.scheduleNextFrame();
  }

  private isNextFrameScheduled = false;
  private scheduleNextFrame() {
    if (this.isNextFrameScheduled) return;
    this.isNextFrameScheduled = true;
    requestAnimationFrame(() => {
      this.isNextFrameScheduled = false;
      this.loop();
    });
  }

  start() {
    this.enabled_ = true;
    this.loop();
  }

  stop() {
    this.enabled_ = false;
    this.lastTimestamp = null;
  }

  private getTimeSinceLastUpdate() {
    const now = window.performance.now();
    const delta = this.lastTimestamp === null ? 0 : now - this.lastTimestamp;
    this.lastTimestamp = now;
    return delta;
  }

  reset() {
    this.lastTimestamp = null;
  }
}
