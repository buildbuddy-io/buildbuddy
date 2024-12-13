import Long from "long";
import { Reader } from "protobufjs";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { google as google_duration } from "../../proto/duration_ts_proto";

/**
 * Generic interface exposed by message classes.
 *
 * This exposes the static methods from the generated message classes. Note:
 * class objects are not the same as _instances_ of the class:
 *
 * ```
 * // Create an instance of `message FooMsg`:
 * const fooInstance = new FooMsg({ someField: 'someValue' });
 * // Note the types of `fooInstance` and `FooMsg`:
 * let value1: FooMsg = fooInstance;
 * let value2: MessageClass<FooMsg> = FooMsg;
 * ```
 */
export type MessageClass<T> = {
  decode(source: Reader | Uint8Array, length?: number): T;
  getTypeUrl(): string;
};

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
