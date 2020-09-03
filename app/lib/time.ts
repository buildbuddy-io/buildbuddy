export class TimeDelta {
  lastTimestamp: number | null = null;

  get() {
    const now = window.performance.now();
    const delta = this.lastTimestamp === null ? 0 : now - this.lastTimestamp;
    this.lastTimestamp = now;
    return delta;
  }

  reset() {
    this.lastTimestamp = null;
  }
}
