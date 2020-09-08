import { TimeDelta } from "./time_delta";

/**
 * Utility class for running animations.
 */
export class AnimationLoop {
  private dt = new TimeDelta();

  constructor(private callback: (dt: number) => void, private enabled_ = false) {
    if (enabled_) {
      this.start();
    }
  }

  private loop() {
    if (!this.enabled_ || this.isNextFrameScheduled) return;

    this.dt.update();
    this.callback(this.dt.get());
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
    this.dt.reset();
  }
}
