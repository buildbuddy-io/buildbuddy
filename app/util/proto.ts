import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { google as google_duration } from "../../proto/duration_ts_proto";

export function dateToTimestamp(date: Date): google_timestamp.protobuf.Timestamp {
  const timestampMillis = date.getTime();
  return new google_timestamp.protobuf.Timestamp({
    seconds: Math.floor(timestampMillis / 1e3) as any,
    nanos: (timestampMillis % 1e3) * 1e6,
  });
}

/** Converts a proto timestamp to a local date. */
export function timestampToDate(timestamp: google_timestamp.protobuf.ITimestamp) {
  const timestampMillis = Math.floor(((timestamp.seconds as any) + timestamp.nanos / 1e9) * 1e3);
  return new Date(timestampMillis);
}

/**
 * Converts a proto timestamp to a number of nanoseconds since the Unix epoch.
 */
export function timestampToUnixNanos(timestamp: google_timestamp.protobuf.ITimestamp): number {
  return Number(timestamp.seconds) * 1e9 + Number(timestamp.nanos);
}

/**
 * Converts a proto duration to a number of nanoseconds.
 */
export function durationToNanos(duration: google_duration.protobuf.IDuration): number {
  return Number(duration.seconds) * 1e9 + Number(duration.nanos);
}
