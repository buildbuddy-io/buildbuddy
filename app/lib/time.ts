export class TimeDelta {
  private lastTimestamp: number | null = null;
  private value: number | null = 0;

  get() {
    return this.value;
  }

  update() {
    const now = window.performance.now();
    this.value = this.lastTimestamp === null ? 0 : now - this.lastTimestamp;
    this.lastTimestamp = now;
    return this.value;
  }

  reset() {
    this.lastTimestamp = null;
    this.value = 0;
  }
}
