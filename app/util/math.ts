export function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

/**
 * Truncates the given value to a maximum number of decimal places past
 * the decimal point.
 *
 * If truncated, the smallest decimal place is rounded.
 *
 * Examples:
 * - truncateDecimals(0.12345678, 2) // returns 0.12
 * - truncateDecimals(1234.5678, 2) // returns 1234.57
 * - truncateDecimals(12345678, 2) // returns 12345678
 */
export function truncateDecimals(value: number, decimalPlaces: number) {
  const decimalShift = Math.pow(10, decimalPlaces);
  return Math.round(value * decimalShift) / decimalShift;
}
