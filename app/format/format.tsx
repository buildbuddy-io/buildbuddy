import Long from "long";

export function percent(percent: number | Long) {
  return `${((+percent) * 100).toFixed(0)}%`;
}

export function durationUsec(duration: number | Long) {
  let seconds = (+duration) / 1000000;
  return durationSec(seconds);
}

export function durationSec(duration: number | Long) {
  let seconds = +duration;
  if (seconds > 60 * 60 * 24 * 365) {
    return `${(seconds / (60 * 60 * 24 * 365)).toPrecision(3)} years`;
  }
  if (seconds > 60 * 60 * 24 * 30) {
    return `${(seconds / (60 * 60 * 24 * 30)).toPrecision(3)} months`;
  }
  if (seconds > 60 * 60 * 24 * 7) {
    return `${(seconds / (60 * 60 * 24 * 7)).toPrecision(3)} weeks`;
  }
  if (seconds > 60 * 60 * 24) {
    return `${(seconds / (60 * 60 * 24)).toPrecision(3)} days`;
  }
  if (seconds > 60 * 60) {
    return `${(seconds / (60 * 60)).toPrecision(3)} hours`;
  }
  if (seconds > 60) {
    return `${(seconds / 60).toPrecision(3)} minutes`;
  }
  return `${seconds.toPrecision(3)} seconds`;
}

export function sentenceCase(string: string) {
  return string[0].toUpperCase() + string.slice(1);
}

export default { durationSec, durationUsec, sentenceCase, percent };