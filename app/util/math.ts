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

/**
 * mod operator that produces only positive numbers when given a positive
 * modulus, making it more sane than `%` for use cases like circular list
 * indexing.
 *
 * Examples:
 * ```
 * mod(8, 9)   // returns 8
 * mod(9, 9)   // returns 0
 * mod(10, 9)  // returns 1
 * mod(-1, 9)  // returns 8
 * ```
 */
export function mod(value: number, modulus: number) {
  return ((value % modulus) + modulus) % modulus;
}
