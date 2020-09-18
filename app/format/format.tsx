import Long from "long";
import moment from "moment";

export function percent(percent: number | Long) {
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

export default {
  durationSec,
  durationMillis,
  durationUsec,
  sentenceCase,
  percent,
  truncateList,
  formatTimestampUsec,
  formatTimestampMillis,
  formatGitUrl,
  formatCommitHash,
};
