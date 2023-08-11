import { api as api_common } from "../../proto/api/v1/common_ts_proto";
import { addDurationToTimestamp, durationToMillis } from "../util/proto";
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
