import { google } from "../../proto/timestamp_ts_proto";

export function dateToTimestamp(date: Date): google.protobuf.Timestamp {
  const timestampSeconds = date.getTime() / 1000;
  const seconds = Math.floor(timestampSeconds);
  const nanos = Math.floor((timestampSeconds - seconds) * 1e9);
  return new google.protobuf.Timestamp({ seconds: seconds as any, nanos });
}
