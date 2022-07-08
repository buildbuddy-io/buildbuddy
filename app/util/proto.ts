import Long from "long";
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
export function timestampToDate(timestamp: google_timestamp.protobuf.ITimestamp): Date {
  const timestampMillis = Math.floor(((timestamp.seconds as any) + timestamp.nanos / 1e9) * 1e3);
  return new Date(timestampMillis);
}

/** Converts a proto duration to a number of millis. */
export function durationToMillis(duration: google_duration.protobuf.IDuration): number {
  const seconds = Number(duration.seconds || 0) + Number(duration.nanos || 0) / 1e9;
  return seconds * 1e3;
}

/** Converts a number of milliseconds to a proto duration. */
export function millisToDuration(ms: number): google_duration.protobuf.IDuration {
  const seconds = ms / 1e3;
  return { seconds: Math.floor(seconds) as any, nanos: (seconds - Math.floor(seconds)) * 1e9 };
}

/**
 * Converts a proto timestamp to a local `Date`, with an optional
 * millis-since-epoch fallback. This is useful for extracting `Date`s from
 * protos that have been migrated to use the `Timestamp` API.
 */
export function timestampToDateWithFallback(
  timestamp: google_timestamp.protobuf.ITimestamp,
  timestampMillisFallback: number | Long
): Date {
  if (timestamp) return timestampToDate(timestamp);

  return new Date(Number(timestampMillisFallback));
}

/**
 * Converts a proto duration to a number of millis, with an optional millis
 * fallback. This is useful for extracting millis from protos that have been
 * migrated to use the `Duration` API.
 */
export function durationToMillisWithFallback(
  duration: google_duration.protobuf.IDuration,
  fallbackMillis: number | Long
): number {
  if (duration) return durationToMillis(duration);

  return Number(fallbackMillis);
}
