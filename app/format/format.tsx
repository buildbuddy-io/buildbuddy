import Long from "long";
import moment from "moment";
import { isSameDay } from "date-fns";

export function percent(percent: number | Long) {
  if (!percent) return "0";
  return `${(+percent * 100).toFixed(0)}`;
}

export function durationUsec(duration: number | Long) {
  let seconds = +duration / 1000000;
  return durationSec(seconds);
}

export function durationMillis(duration: number | Long) {
  let seconds = +duration / 1000;
  return durationSec(seconds);
}

export function durationSec(duration: number | Long) {
  let seconds = +duration;
  if (!seconds || seconds < 0) {
    return "0s";
  }

  if (seconds >= 60 * 60 * 24 * 365) {
    return `${(seconds / (60 * 60 * 24 * 365)).toPrecision(3)} years`;
  }
  if (seconds >= 60 * 60 * 24 * 30) {
    return `${(seconds / (60 * 60 * 24 * 30)).toPrecision(3)} months`;
  }
  if (seconds >= 60 * 60 * 24 * 7) {
    return `${(seconds / (60 * 60 * 24 * 7)).toPrecision(3)} weeks`;
  }
  if (seconds >= 60 * 60 * 24) {
    const hours = seconds / (60 * 60);
    const d = Math.floor(hours / 24);
    const h = Math.floor(hours - d * 24);
    return `${d}d ${h}h`;
  }
  if (seconds >= 60 * 60) {
    const minutes = seconds / 60;
    const h = Math.floor(minutes / 60);
    const m = Math.floor(minutes - h * 60);
    return `${h}h ${m}m`;
  }
  if (seconds >= 60) {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds - m * 60);
    return `${m}m ${s}s`;
  }
  if (seconds >= 1) {
    return `${seconds.toPrecision(3)}s`;
  }
  const milliseconds = seconds * 1000;
  if (milliseconds >= 1) {
    return `${(seconds * 1000).toPrecision(3)}ms`;
  }
  return `${(milliseconds * 1000).toPrecision(3)}µs`;
}

export function compactDurationMillis(duration: number | Long) {
  return compactDurationSec(Number(duration) / 1000);
}

export function compactDurationSec(duration: number | Long) {
  let seconds = +duration;
  if (!seconds || seconds < 0) {
    return "0s";
  }
  if (seconds >= 60 * 60 * 24 * 365) {
    return `${(seconds / (60 * 60 * 24 * 365)).toFixed(0)}y`;
  }
  if (seconds >= 60 * 60 * 24 * 30) {
    return `${(seconds / (60 * 60 * 24 * 30)).toFixed(0)}m`;
  }
  if (seconds >= 60 * 60 * 24 * 7) {
    return `${(seconds / (60 * 60 * 24 * 7)).toFixed(0)}w`;
  }
  if (seconds >= 60 * 60 * 24) {
    return `${(seconds / (60 * 60 * 24)).toFixed(0)}d`;
  }
  if (seconds >= 60 * 60) {
    return `${(seconds / (60 * 60)).toFixed(0)}h`;
  }
  if (seconds >= 60) {
    return `${(seconds / 60).toFixed(0)}m`;
  }
  if (seconds >= 1) {
    return `${seconds.toFixed(0)}s`;
  }
  const ms = seconds * 1000;
  if (ms >= 1) {
    return `${(seconds * 1000).toFixed(0)}ms`;
  }
  return `${(ms * 1000).toFixed(0)}µs`;
}

/**
 * Removes any trailing zeroes after the decimal point,
 * including the decimal point itself if there are only
 * trailing zeroes.
 */
function truncateDecimalZeroes(numString: string): string {
  return numString.replace(/\.(.+?)0+$/, ".$1").replace(/\.0+$/, "");
}

export function bytes(bytes: number | Long) {
  bytes = +bytes;
  if (bytes < 100) {
    return bytes + "B";
  }
  if (bytes < 1e6) {
    return truncateDecimalZeroes((bytes / 1e3).toPrecision(4)) + "KB";
  }
  if (bytes < 1e9) {
    return truncateDecimalZeroes((bytes / 1e6).toPrecision(4)) + "MB";
  }
  if (bytes < 1e12) {
    return truncateDecimalZeroes((bytes / 1e9).toPrecision(4)) + "GB";
  }
  if (bytes < 1e15) {
    return truncateDecimalZeroes((bytes / 1e12).toPrecision(4)) + "TB";
  }
  return truncateDecimalZeroes((bytes / 1e15).toPrecision(4)) + "PB";
}

export function bitsPerSecond(bitsPerSecond: number | Long) {
  bitsPerSecond = Number(bitsPerSecond);
  if (bitsPerSecond < 1e3) {
    return bitsPerSecond + "bps";
  }
  if (bitsPerSecond < 1e6) {
    return truncateDecimalZeroes((bitsPerSecond / 1e3).toPrecision(4)) + "Kbps";
  }
  if (bitsPerSecond < 1e9) {
    return truncateDecimalZeroes((bitsPerSecond / 1e6).toPrecision(4)) + "Mbps";
  }
  if (bitsPerSecond < 1e12) {
    return truncateDecimalZeroes((bitsPerSecond / 1e9).toPrecision(4)) + "Gbps";
  }
  if (bitsPerSecond < 1e15) {
    return truncateDecimalZeroes((bitsPerSecond / 1e12).toPrecision(4)) + "Tbps";
  }
  return truncateDecimalZeroes((bitsPerSecond / 1e15).toPrecision(4)) + "Pbps";
}

export function count(value: number | Long): string {
  value = Number(value);
  if (value < 1e3) {
    return String(value);
  }
  if (value < 1e6) {
    return truncateDecimalZeroes((value / 1e3).toPrecision(4)) + "K";
  }
  if (value < 1e9) {
    return truncateDecimalZeroes((value / 1e6).toPrecision(4)) + "M";
  }
  return truncateDecimalZeroes((value / 1e9).toPrecision(4)) + "B";
}

export function sentenceCase(string: string) {
  if (!string) return "";
  return string[0].toUpperCase() + string.slice(1);
}

export function truncateList(list: string[]) {
  if (list.length > 3) {
    return `${list.slice(0, 3).join(", ")} and ${list.length - 3} more`;
  }
  return list.join(", ");
}

/** Unix epoch expressed in local time. */
export const LOCAL_EPOCH: Date = moment(0).toDate();

export function formatTimestampUsec(timestamp: number | Long) {
  return formatTimestampMillis(+timestamp / 1000);
}

export function formatTimestampMillis(timestamp: number | Long) {
  return `${moment(+timestamp).format("MMMM Do, YYYY")} at ${moment(+timestamp).format("h:mm:ss a")}`;
}

export function formatTimestamp(timestamp: { seconds?: number | Long; nanos?: number | Long }) {
  return `${moment(+timestamp.seconds * 1000).format("MMMM Do, YYYY")} at ${moment(+timestamp.seconds * 1000).format(
    "h:mm:ss"
  )}.${Math.floor(+timestamp.nanos / 1_000_000)
    .toString()
    .padStart(3, "0")} ${moment(+timestamp.seconds * 1000).format("A")}`;
}

export function formatDate(date: Date): string {
  return formatTimestampMillis(date.getTime());
}

const DATE_RANGE_SEPARATOR = "\u2013";

export function formatDateRange(startDate: Date, endDate: Date, { now = new Date() } = {}) {
  let startFormat, endFormat;

  // Special cases for date range picker default options
  if (isSameDay(now, endDate)) {
    if (isSameDay(startDate, LOCAL_EPOCH)) {
      return "All time";
    }
    return `Last ${differenceInCalendarDays(startDate, endDate) + 1} days`;
  }

  if (startDate.getFullYear() === endDate.getFullYear()) {
    startFormat = "MMMM Do";
    endFormat = "MMMM Do, YYYY";
    if (endDate.getFullYear() === now.getFullYear()) {
      endFormat = "MMMM Do";
    }
  } else {
    startFormat = endFormat = "MMMM Do, YYYY";
  }

  let start = moment(startDate).format(startFormat);
  let end = moment(endDate).format(endFormat);

  if (isSameDay(now, startDate)) start = "Today";
  if (isSameDay(now, endDate)) end = "Today";

  if (start === end) return start;

  return `${start} ${DATE_RANGE_SEPARATOR} ${end}`;
}

export function formatGitUrl(url: string) {
  return url
    ?.replace("https://", "")
    .replace("ssh://", "")
    .replace(".git", "")
    .replace("git@", "")
    .replace("github.com/", "")
    .replace("github.com:", "");
}

export function formatCommitHash(commit: string) {
  return commit?.substring(0, 6);
}

export function formatRole(role: string): string | null {
  if (role === "CI_RUNNER") {
    return "Workflow";
  }
  if (role === "CI") {
    return "CI";
  }
  // Don't render unknown roles for now.
  return null;
}

export function formatWithCommas(num: number | Long) {
  return (+num).toLocaleString("en-US");
}

function differenceInCalendarDays(start: Date, end: Date) {
  return moment(end).diff(start, "days");
}

export function colorHash(input: string) {
  let num = 0;
  for (var i = 0; i < input.length; i++) {
    num = input.charCodeAt(i) + ((num << 5) - num);
  }
  return `hsl(${(num % 360000) / 1000}, 50%, 80%)`;
}

export default {
  compactDurationSec,
  durationSec,
  durationMillis,
  durationUsec,
  sentenceCase,
  percent,
  bytes,
  bitsPerSecond,
  count,
  truncateList,
  formatDate,
  formatTimestampUsec,
  formatTimestampMillis,
  formatTimestamp,
  formatGitUrl,
  formatCommitHash,
  formatRole,
  formatWithCommas,
  formatDateRange,
  colorHash,
};
