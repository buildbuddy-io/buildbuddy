import { google } from "../../proto/timestamp_ts_proto";

export function dateToTimestamp(date: Date): google.protobuf.Timestamp {
  const timestampMillis = date.getTime();
  return new google.protobuf.Timestamp({
    seconds: Math.floor(timestampMillis / 1e3) as any,
    nanos: (timestampMillis % 1e3) * 1e6,
  });
}

/** Converts a proto timestamp to a local date. */
export function timestampToDate(timestamp: google.protobuf.ITimestamp) {
  const timestampMillis = Math.floor(((timestamp.seconds as any) + timestamp.nanos / 1e9) * 1e3);
  return new Date(timestampMillis);
}
