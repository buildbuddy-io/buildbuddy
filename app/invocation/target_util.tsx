import { api as api_common } from "../../proto/api/v1/common_ts_proto";
import { durationToMillis } from "../util/proto";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import Long from "long";

export function renderTestSize(size: api_common.v1.TestSize): string {
  const TestSize = api_common.v1.TestSize;
  switch (size) {
    case TestSize.ENORMOUS:
      return "Enormous";
    case TestSize.LARGE:
      return "Large";
    case TestSize.MEDIUM:
      return "Medium";
    case TestSize.SMALL:
      return "Small";
    default:
      return "";
  }
}

export function renderDuration(timing: api_common.v1.Timing): string {
  let ms = 0;
  if (timing.duration) {
    ms = durationToMillis(timing.duration) / 1000;
  }
  return `${ms.toFixed(3)} seconds`;
}

export function getEndTimestamp(timing: api_common.v1.Timing): google_timestamp.protobuf.Timestamp | null {
  if (!timing.startTime || !timing.duration) return null;
  const startNanos = Number(timing.startTime.seconds) * 1e9 + timing.startTime.nanos;
  const endNanos = startNanos + Number(timing.duration.seconds) * 1e9 + timing.duration.nanos;
  const endSeconds = Math.floor(endNanos / 1e9);
  return new google_timestamp.protobuf.Timestamp({
    seconds: Long.fromNumber(endSeconds),
    nanos: endNanos - endSeconds * 1e9,
  });
}
