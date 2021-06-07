import Long from "long";
import moment from "moment";

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
    return "0 s";
  }
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
    return `${(seconds / (60 * 60 * 24)).toPrecision(3)} d`;
  }
  if (seconds > 60 * 60) {
    return `${(seconds / (60 * 60)).toPrecision(3)} h`;
  }
  if (seconds > 60) {
    return `${(seconds / 60).toPrecision(3)} m`;
  }
  return `${seconds.toPrecision(3)} s`;
}

export function compactDurationSec(duration: number | Long) {
  let seconds = +duration;
  if (!seconds || seconds < 0) {
    return "0s";
  }
  if (seconds > 60 * 60 * 24 * 365) {
    return `${(seconds / (60 * 60 * 24 * 365)).toFixed(0)}y`;
  }
  if (seconds > 60 * 60 * 24 * 30) {
    return `${(seconds / (60 * 60 * 24 * 30)).toFixed(0)}m`;
  }
  if (seconds > 60 * 60 * 24 * 7) {
    return `${(seconds / (60 * 60 * 24 * 7)).toFixed(0)}w`;
  }
  if (seconds > 60 * 60 * 24) {
    return `${(seconds / (60 * 60 * 24)).toFixed(0)}d`;
  }
  if (seconds > 60 * 60) {
    return `${(seconds / (60 * 60)).toFixed(0)}h`;
  }
  if (seconds > 60) {
    return `${(seconds / 60).toFixed(0)}m`;
  }
  return `${seconds.toFixed(0)}s`;
}

export function bytes(bytes: number | Long) {
  bytes = +bytes;
  if (bytes < 100) {
    return bytes + "B";
  }
  if (bytes < 1000000) {
    return (bytes / 1000).toFixed(2) + "KB";
  }
  if (bytes < 1000000000) {
    return (bytes / 1000000).toFixed(2) + "MB";
  }
  return (bytes / 1000000000).toFixed(2) + "GB";
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

export function formatTimestampUsec(timestamp: number | Long) {
  return formatTimestampMillis(+timestamp / 1000);
}

export function formatTimestampMillis(timestamp: number | Long) {
  return `${moment(+timestamp).format("MMMM Do, YYYY")} at ${moment(+timestamp).format("h:mm:ss a")}`;
}

export function formatTimestamp(timestamp: { seconds?: number | Long; nanos?: number | Long }) {
  return `${moment(+timestamp.seconds * 1000).format("MMMM Do, YYYY")} at ${moment(+timestamp.seconds * 1000).format(
    "h:mm:ss"
  )}:${Math.floor(+timestamp.nanos / 10000000)
    .toString()
    .padStart(2, "0")} ${moment(+timestamp.seconds * 1000).format("A")}`;
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

export function formatWithCommas(num: number | Long) {
  return (+num).toLocaleString("en-US");
}

export default {
  compactDurationSec,
  durationSec,
  durationMillis,
  durationUsec,
  sentenceCase,
  percent,
  bytes,
  truncateList,
  formatTimestampUsec,
  formatTimestampMillis,
  formatTimestamp,
  formatGitUrl,
  formatCommitHash,
  formatWithCommas,
};
