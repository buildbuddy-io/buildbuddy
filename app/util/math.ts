export function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

export function magnitude(value: number) {
  return Math.pow(10, Math.round(Math.log10(value)));
}
