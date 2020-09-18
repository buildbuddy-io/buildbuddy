import { clamp } from "./math";

/**
 * A value that is animated according to an ease-out curve.
 */
export class AnimatedValue {
  private target_: number;
  private value_: number;
  private min_: number;
  private max_: number;

  constructor(target: number, { min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER } = {}) {
    this.min_ = min;
    this.max_ = max;
    this.target_ = clamp(target, min, max);
    this.value_ = this.target_;
  }

  set target(target: number) {
    this.target_ = clamp(target, this.min_, this.max_);
  }
  get target() {
    return this.target_;
  }

  set value(value: number) {
    this.value_ = clamp(value, this.min_, this.max_);
  }
  get value() {
    return this.value_;
  }

  set min(min: number) {
    // Ensure min <= max
    min = Math.min(this.max_, min);
    this.min_ = min;
    this.value_ = Math.max(min, this.value_);
    this.target_ = Math.max(min, this.target_);
  }
  get min() {
    return this.min_;
  }

  set max(max: number) {
    // Ensure max >= min
    max = Math.max(this.min_, max);
    this.max_ = max;
    this.value_ = Math.min(max, this.value);
    this.target_ = Math.min(max, this.target_);
  }
  get max() {
    return this.max_;
  }

  /**
   * Steps `value` towards `target` according to an "ease-out" curve.
   *
   * Let `distance` be the current distance between `target` and `value`:
   *
   * - `rate` is the fraction of `distance` covered per millisecond in this step.
   *   Note that this varies with `distance`, which results in the ease-out curve.
   *
   * - `threshold` is the minimum distance at which `value` and `target` are considered
   *   visually equal. This allows pausing the animation loop when `value === target`.
   *
   * @param dt step time in milliseconds
   * @param options rate and threshold (optional)
   */
  step(dt: number, { rate = 0.02, threshold = 0.000001 } = {}) {
    const distance = this.target_ - this.value_;
    const stepAmount = distance * rate * dt;

    if (Math.abs(stepAmount) > Math.abs(distance) || Math.abs(this.value_ - this.target_) < threshold) {
      this.value_ = this.target_;
    } else {
      this.value_ = this.value_ + stepAmount;
    }

    this.value_ = clamp(this.value_, this.min_, this.max_);
  }

  toString() {
    return String(this.value_);
  }

  get isAtTarget() {
    return this.value_ === this.target_;
  }

  toJson() {
    return {
      value: this.value_,
      target: this.target_,
      isAtTarget: this.isAtTarget,
      min: this.min_,
      max: this.max_,
    };
  }
}
