export function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

export function round(value: number, decimals: number) {
  return Number(Math.round(Number(`${value}e${decimals}`)) + `e-${decimals}`);
}
