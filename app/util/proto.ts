import Long from "long";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { google as google_duration } from "../../proto/duration_ts_proto";

/**
 * Returns a proto enum value from either the name or value. Values can either
 * be strings or numbers.
 *
 * This is useful for parsing form field values such as `<select>` option
 * values, allowing option values to be defined either in terms of the enum name
 * or enum value, and mapping uninitialized selections such as `""` or
 * `undefined` to the 0-value of the enum.
 *
 * If parsing fails, returns the enum's 0-value (if it's defined), otherwise
 * returns undefined.
 */
export function parseEnum<E extends Record<number | string, number | string>>(
  enumClass: E,
  value: string | number | null | undefined
): E[keyof E] {
  const e = enumClass as any;
  const unknownValue = (0 in e ? 0 : undefined) as any;

  // Return "unknown" value for undefined/null
  if (value === undefined || value === null) return unknownValue;

  // Convert numeric strings to numbers, e.g. '2' => 2
  if (!isNaN(Number(value))) {
    value = Number(value);
  }

  // Return numbers directly, except if they're not one of the known enum
  // values.
  if (typeof value === "number") {
    return value in e ? (value as any) : unknownValue;
  }

  // Look up value from string key.
  return e[value] ?? unknownValue;
}

export function dateToTimestamp(date: Date): google_timestamp.protobuf.Timestamp {
  const timestampMillis = date.getTime();
  return new google_timestamp.protobuf.Timestamp({
    seconds: Math.floor(timestampMillis / 1e3) as any,
    nanos: (timestampMillis % 1e3) * 1e6,
  });
}

export function usecToTimestamp(usec: number): google_timestamp.protobuf.Timestamp {
  return new google_timestamp.protobuf.Timestamp({
    seconds: Math.floor(usec / 1e6) as any,
    nanos: Math.floor(usec % 1e6) * 1e3,
  });
}

/** Converts a proto timestamp to a local date. */
export function timestampToDate(timestamp: google_timestamp.protobuf.ITimestamp): Date {
  const timestampMillis = Math.floor((Number(timestamp.seconds || 0) + Number(timestamp.nanos || 0) / 1e9) * 1e3);
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
  return secondsToDuration(seconds);
}

/** Converts a number of seconds to a proto duration. */
export function secondsToDuration(seconds: number): google_duration.protobuf.IDuration {
  return { seconds: Math.floor(seconds) as any, nanos: (seconds - Math.floor(seconds)) * 1e9 };
}

/**
 * Converts a proto timestamp to a local `Date`, with an optional
 * millis-since-epoch fallback. This is useful for extracting `Date`s from
 * protos that have been migrated to use the `Timestamp` API.
 */
export function timestampToDateWithFallback(
  timestamp: google_timestamp.protobuf.ITimestamp | null | undefined,
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
  duration: google_duration.protobuf.IDuration | null | undefined,
  fallbackMillis: number | Long
): number {
  if (duration) return durationToMillis(duration);

  return Number(fallbackMillis);
}

export function addDurationToTimestamp(
  timestamp: google_timestamp.protobuf.ITimestamp,
  duration: google_duration.protobuf.IDuration | null | undefined
) {
  if (!duration) return timestamp;

  const startNanos = Long.fromValue(timestamp.seconds ?? 0)
    .mul(1e9)
    .add(timestamp.nanos ?? 0);
  const endNanos = startNanos.add(Long.fromValue(duration.seconds ?? 0).mul(1e9)).add(timestamp.nanos ?? 0);

  return new google_timestamp.protobuf.Timestamp({
    seconds: endNanos.div(1e9),
    nanos: endNanos.mod(1e9).toNumber(),
  });
}
