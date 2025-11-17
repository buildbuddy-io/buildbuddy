export class TimeDelta {
  private lastTimestamp: number | null = null;
  private value: number = 0;

  get(): number {
    return this.value;
  }

  update(): number {
    const now = window.performance.now();
    this.value = this.lastTimestamp === null ? 0 : now - this.lastTimestamp;
    this.lastTimestamp = now;
    return this.value;
  }

  reset(): void {
    this.lastTimestamp = null;
    this.value = 0;
  }
}
