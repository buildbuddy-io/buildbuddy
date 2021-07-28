import { google } from "../../proto/timestamp_ts_proto";

export function dateToTimestamp(date: Date): google.protobuf.Timestamp {
  const timestampMillis = date.getTime();
  return new google.protobuf.Timestamp({
    seconds: Math.floor(timestampMillis / 1e3) as any,
    nanos: (timestampMillis % 1e3) * 1e6,
  });
}
