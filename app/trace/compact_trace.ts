import { StringInterner } from "../util/intern";
import { TypedArrayBuilder } from "../util/typed_arrays";
import type { ProfileInput, ProfileProgressCallback, TraceEvent } from "./trace_events";
import {
  TIME_SERIES_EVENT_ORDER,
  TIME_SERIES_METADATA,
  isNumericTimeSeriesValue,
  normalizeThreadName,
  readProfileEvents,
} from "./trace_events";

/**
 * Trace profile parsed from raw trace events.
 *
 * In order to minimize memory usage (and allow loading larger profiles), the
 * profile does not store the original event objects. Instead, it stores all
 * numeric data using densely packed typed arrays, and stores all string data as
 * interned string IDs.
 */
export class Profile {
  constructor(
    /** Threads containing duration spans. */
    readonly threads: Thread[],
    /** Timeseries events. */
    readonly timeseries: TimeSeries[],
    /** Number of JSON trace events added to the profile builder. */
    readonly eventCount: number,
    /** Max event end time in microseconds. */
    readonly xMax: number
  ) {}
}

/** A thread containing duration spans. */
export class Thread {
  constructor(
    /** Process ID. */
    readonly pid: number,
    /** Thread ID. */
    readonly tid: number,
    /** Display name. */
    readonly name: string,
    /** Interned strings. */
    private readonly strings: StringInterner,
    /** "cat" string IDs. */
    readonly catIDs: Uint32Array,
    /** "name" string IDs. */
    readonly nameIDs: Uint32Array,
    /** "ts" values in microseconds. */
    readonly ts: Float64Array,
    /** "dur" values in microseconds. */
    readonly dur: Float64Array,
    /** Calculated call stack depths. */
    readonly depth: Uint16Array,
    /** "args.target" string IDs, or 0 if missing. */
    readonly targetIDs: Uint32Array,
    /** "args.mnemonic" string IDs, or 0 if missing. */
    readonly mnemonicIDs: Uint32Array,
    /** "out" string IDs, or 0 if missing. */
    readonly outIDs: Uint32Array,
    /** Maximum calculated call stack depth. */
    readonly maxDepth: number
  ) {}

  /** Number of spans in this thread. */
  get length() {
    return this.ts.length;
  }

  /**
   * Returns a full trace event object for the event at the given index. This is
   * only intended to be called for single events of interest (e.g. the hovered
   * event). Avoid calling this method in a loop to collect large amounts of
   * event data - instead, use the individual accessor methods.
   */
  getEvent(index: number): TraceEvent {
    const target = this.getTarget(index);
    const mnemonic = this.getMnemonic(index);
    const args: { [key: string]: any } = {};
    if (target) args.target = target;
    if (mnemonic) args.mnemonic = mnemonic;

    return {
      pid: this.pid,
      tid: this.tid,
      ts: this.ts[index],
      ph: "X",
      cat: this.getCat(index),
      name: this.getName(index),
      dur: this.dur[index],
      tdur: 0,
      tts: 0,
      out: this.getOut(index),
      args,
    };
  }

  /** Returns the event category string. */
  getCat(index: number) {
    return this.strings.get(this.catIDs[index]);
  }

  /** Returns the event name string. */
  getName(index: number) {
    return this.strings.get(this.nameIDs[index]);
  }

  /** Returns the event target string, if present. */
  getTarget(index: number) {
    return this.getOptionalString(this.targetIDs[index]);
  }

  /** Returns the event mnemonic string, if present. */
  getMnemonic(index: number) {
    return this.getOptionalString(this.mnemonicIDs[index]);
  }

  /** Returns the event output string, if present. */
  getOut(index: number) {
    return this.getOptionalString(this.outIDs[index]);
  }

  /** Returns the color key used by the trace viewer. */
  getColorKey(index: number) {
    return `${this.getCat(index)}#${this.getName(index)}`;
  }

  /** Returns whether this event matches the lower-cased trace viewer filter. */
  matchesFilter(index: number, lowerFilter: string) {
    if (!lowerFilter) return true;
    return (
      this.getName(index).toLowerCase().includes(lowerFilter) ||
      this.getCat(index).toLowerCase().includes(lowerFilter) ||
      this.getTarget(index).toLowerCase().includes(lowerFilter) ||
      this.getMnemonic(index).toLowerCase().includes(lowerFilter) ||
      this.getOut(index).toLowerCase().includes(lowerFilter)
    );
  }

  private getOptionalString(id: number) {
    return id ? this.strings.get(id) : "";
  }
}

/** A time series suitable for plotting. */
export class TimeSeries {
  constructor(
    /** Human readable timeseries name, e.g. "action count (local)". */
    readonly name: string,
    /** Args key, e.g. "local action". */
    readonly argsKey: string,
    /** Optional unit label. */
    readonly unit: string | undefined,
    /** Timestamp values in microseconds. */
    readonly ts: Float64Array,
    /** Timeseries values, corresponding to each timestamp. */
    readonly val: Float32Array,
    /** Sort order for known Bazel/executor series. */
    readonly order: number | undefined
  ) {}
}

/** Reads a trace profile from a local file or stream. */
export async function readProfile(input: ProfileInput, progress?: ProfileProgressCallback): Promise<Profile> {
  const builder = new ProfileBuilder();
  await readProfileEvents(
    input,
    (events) => {
      for (const event of events) {
        builder.addEvent(event);
      }
    },
    progress
  );
  return builder.build();
}

/** Mutable builder for a compact trace profile. */
export class ProfileBuilder {
  private strings = new StringInterner();
  private nameIDsByTid = new Map<number, number>();
  private threadBuilders = new Map<number, ThreadBuilder>();
  private timeseriesBuilders = new Map<string, TimeSeriesBuilder>();
  private unknownTimeseriesKeysByName = new Map<string, Set<string>>();
  private eventCount = 0;
  private xMax = 0;

  /** Adds a parsed event from the profile. */
  addEvent(event: TraceEvent) {
    this.eventCount++;

    if (event.name === "thread_name" && event.args?.name && event.tid !== undefined) {
      this.nameIDsByTid.set(event.tid, this.strings.intern(normalizeThreadName(event.args.name)));
      return;
    }

    if (event.ph === "C") {
      this.addTimeseriesEvent(event);
      return;
    }

    if (event.ph !== "X" || event.tid === undefined || event.ts === undefined || !event.dur) {
      return;
    }
    if (event.ts + event.dur < 0) {
      return;
    }

    const builder = this.threadBuilder(event.tid);
    builder.append(event);

    this.xMax = Math.max(this.xMax, event.ts + event.dur);
  }

  /** Builds the finalized profile, including all finalized threads and time series. */
  build(): Profile {
    const threads = Array.from(this.threadBuilders.values())
      .filter((builder) => builder.length)
      .map((builder) => {
        const nameID = this.nameIDsByTid.get(builder.tid);
        return builder.build(nameID !== undefined ? this.strings.get(nameID) : "");
      })
      .sort((a, b) => a.tid - b.tid);

    const timeseries = Array.from(this.timeseriesBuilders.values())
      .map((builder) => builder.build(this.timeseriesDisplayName(builder)))
      .sort((a, b) => {
        if (a.order !== undefined || b.order !== undefined) {
          if (a.order === undefined) return 1;
          if (b.order === undefined) return -1;
          const orderDiff = a.order - b.order;
          if (orderDiff !== 0) return orderDiff;
        }
        const nameDiff = a.name.localeCompare(b.name);
        if (nameDiff !== 0) return nameDiff;
        return a.argsKey.localeCompare(b.argsKey);
      });

    this.strings.freeze();
    return new Profile(threads, timeseries, this.eventCount, this.xMax);
  }

  private threadBuilder(tid: number) {
    let builder = this.threadBuilders.get(tid);
    if (!builder) {
      builder = new ThreadBuilder(tid, this.strings);
      this.threadBuilders.set(tid, builder);
    }
    return builder;
  }

  private addTimeseriesEvent(event: TraceEvent) {
    if (event.ts === undefined) return;

    const knownMetadata = TIME_SERIES_METADATA.get(event.name);
    if (knownMetadata) {
      const order = TIME_SERIES_EVENT_ORDER.get(event.name);
      for (const metadata of knownMetadata) {
        const value = Number(event.args?.[metadata.argKey]);
        if (!Number.isFinite(value)) continue;
        this.timeseriesBuilder(event.name, metadata.argKey, metadata.displayName, metadata.unit, order).push(
          event.ts,
          value
        );
        this.xMax = Math.max(this.xMax, event.ts);
      }
      return;
    }

    if (!event.args) return;
    for (const [argKey, value] of Object.entries(event.args)) {
      if (!isNumericTimeSeriesValue(value)) continue;

      let argKeys = this.unknownTimeseriesKeysByName.get(event.name);
      if (!argKeys) {
        argKeys = new Set<string>();
        this.unknownTimeseriesKeysByName.set(event.name, argKeys);
      }
      argKeys.add(argKey);
      this.timeseriesBuilder(event.name, argKey, undefined, undefined, undefined).push(event.ts, value);
      this.xMax = Math.max(this.xMax, event.ts);
    }
  }

  private timeseriesBuilder(
    traceName: string,
    argsKey: string,
    displayName: string | undefined,
    unit: string | undefined,
    order: number | undefined
  ) {
    const key = `${traceName}\0${argsKey}`;
    let builder = this.timeseriesBuilders.get(key);
    if (!builder) {
      builder = new TimeSeriesBuilder(traceName, argsKey, displayName, unit, order);
      this.timeseriesBuilders.set(key, builder);
    }
    return builder;
  }

  private timeseriesDisplayName(builder: TimeSeriesBuilder) {
    if (builder.displayName) return builder.displayName;
    const unknownArgKeys = this.unknownTimeseriesKeysByName.get(builder.traceName);
    if (!unknownArgKeys || unknownArgKeys.size <= 1) {
      return builder.traceName;
    }
    return `${builder.traceName}: ${builder.argsKey}`;
  }
}

/** Mutable builder for a thread's duration spans. */
export class ThreadBuilder {
  private catIDs = TypedArrayBuilder.of(Uint32Array);
  private nameIDs = TypedArrayBuilder.of(Uint32Array);
  private ts = TypedArrayBuilder.of(Float64Array);
  private dur = TypedArrayBuilder.of(Float64Array);
  private targetIDs = TypedArrayBuilder.of(Uint32Array);
  private mnemonicIDs = TypedArrayBuilder.of(Uint32Array);
  private outIDs = TypedArrayBuilder.of(Uint32Array);
  private lastTs = -Infinity;
  private lastDur = Infinity;
  private needsFullSort = false;
  private needsTimestampTieSort = false;

  pid = 0;

  constructor(
    readonly tid: number,
    private readonly strings: StringInterner
  ) {}

  /** Number of duration events added so far. */
  get length() {
    return this.ts.length;
  }

  /** Adds a duration trace event. */
  append(event: TraceEvent) {
    const ts = event.ts || 0;
    const dur = event.dur || 0;
    if (ts < this.lastTs) {
      this.needsFullSort = true;
    } else if (ts === this.lastTs && dur > this.lastDur) {
      this.needsTimestampTieSort = true;
    }
    this.lastTs = Math.max(this.lastTs, ts);
    this.lastDur = ts === this.lastTs ? dur : Infinity;

    this.pid = event.pid || this.pid;
    this.catIDs.append(this.strings.intern(event.cat));
    this.nameIDs.append(this.strings.intern(event.name));
    this.ts.append(ts);
    this.dur.append(dur);
    const target = event.args?.target;
    const mnemonic = event.args?.mnemonic;
    const out = event.out;
    this.targetIDs.append(target ? this.strings.intern(target) : 0);
    this.mnemonicIDs.append(mnemonic ? this.strings.intern(mnemonic) : 0);
    this.outIDs.append(out ? this.strings.intern(out) : 0);
  }

  /**
   * Builds the finalized thread, compacting builder chunks into final arrays
   * and sorting events if needed.
   */
  build(name: string): Thread {
    const sourceTs = this.ts.toArray();
    const sourceDur = this.dur.toArray();

    const indices = this.sortedIndices(sourceTs, sourceDur);
    const length = sourceTs.length;

    const catIDs = indices ? new Uint32Array(length) : this.catIDs.toArray();
    const nameIDs = indices ? new Uint32Array(length) : this.nameIDs.toArray();
    const ts = indices ? new Float64Array(length) : sourceTs;
    const dur = indices ? new Float64Array(length) : sourceDur;
    const targetIDs = indices ? new Uint32Array(length) : this.targetIDs.toArray();
    const mnemonicIDs = indices ? new Uint32Array(length) : this.mnemonicIDs.toArray();
    const outIDs = indices ? new Uint32Array(length) : this.outIDs.toArray();
    const sourceCatIDs = indices ? this.catIDs.toArray() : catIDs;
    const sourceNameIDs = indices ? this.nameIDs.toArray() : nameIDs;
    const sourceTargetIDs = indices ? this.targetIDs.toArray() : targetIDs;
    const sourceMnemonicIDs = indices ? this.mnemonicIDs.toArray() : mnemonicIDs;
    const sourceOutIDs = indices ? this.outIDs.toArray() : outIDs;
    const depth = new Uint16Array(length);
    let maxDepth = 0;
    const stackEnds: number[] = [];

    for (let i = 0; i < length; i++) {
      const sourceIndex = indices ? indices[i] : i;
      const eventTs = sourceTs[sourceIndex];
      const eventDur = sourceDur[sourceIndex];
      const eventEnd = eventTs + eventDur;

      let topEnd: number | undefined;
      while ((topEnd = stackEnds[stackEnds.length - 1]) !== undefined && (topEnd < eventTs || topEnd < eventEnd)) {
        stackEnds.pop();
      }

      const eventDepth = stackEnds.length;
      maxDepth = Math.max(maxDepth, eventDepth);
      depth[i] = eventDepth;
      stackEnds.push(eventEnd);

      if (indices) {
        catIDs[i] = sourceCatIDs[sourceIndex];
        nameIDs[i] = sourceNameIDs[sourceIndex];
        ts[i] = eventTs;
        dur[i] = eventDur;
        targetIDs[i] = sourceTargetIDs[sourceIndex];
        mnemonicIDs[i] = sourceMnemonicIDs[sourceIndex];
        outIDs[i] = sourceOutIDs[sourceIndex];
      }
    }

    return new Thread(
      this.pid,
      this.tid,
      name,
      this.strings,
      catIDs,
      nameIDs,
      ts,
      dur,
      depth,
      targetIDs,
      mnemonicIDs,
      outIDs,
      maxDepth
    );
  }

  /**
   * Returns source event positions in the order needed for rendering and depth
   * calculation, or null if events were appended in that order already.
   */
  private sortedIndices(ts: Float64Array, dur: Float64Array) {
    if (!this.needsFullSort && !this.needsTimestampTieSort) return null;

    const indices = makeSequentialIndices(this.length);
    if (this.needsFullSort) {
      indices.sort((a, b) => {
        const tsDiff = ts[a] - ts[b];
        if (tsDiff !== 0) return tsDiff;
        return dur[b] - dur[a];
      });
      return indices;
    }

    sortTimestampTies(indices, ts, dur);
    return indices;
  }
}

/** Mutable builder for a time series. */
export class TimeSeriesBuilder {
  private ts = TypedArrayBuilder.of(Float64Array);
  private val = TypedArrayBuilder.of(Float32Array);
  private lastTs = -Infinity;
  private needsSort = false;

  constructor(
    readonly traceName: string,
    readonly argsKey: string,
    readonly displayName: string | undefined,
    readonly unit: string | undefined,
    readonly order: number | undefined
  ) {}

  /** Adds a timestamped value. */
  push(ts: number, val: number) {
    if (ts < this.lastTs) {
      this.needsSort = true;
    }
    this.lastTs = Math.max(this.lastTs, ts);
    this.ts.append(ts);
    this.val.append(val);
  }

  /**
   * Builds the finalized time series, compacting builder chunks into the final
   * arrays and sorting values if needed.
   */
  build(displayName: string): TimeSeries {
    const sourceTs = this.ts.toArray();
    const sourceVal = this.val.toArray();
    if (!this.needsSort) {
      return new TimeSeries(displayName, this.argsKey, this.unit, sourceTs, sourceVal, this.order);
    }

    const indices = makeSequentialIndices(sourceTs.length);
    indices.sort((a, b) => sourceTs[a] - sourceTs[b]);

    const ts = new Float64Array(indices.length);
    const val = new Float32Array(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const sourceIndex = indices[i];
      ts[i] = sourceTs[sourceIndex];
      val[i] = sourceVal[sourceIndex];
    }
    return new TimeSeries(displayName, this.argsKey, this.unit, ts, val, this.order);
  }
}

/**
 * Sorts same-timestamp runs by descending duration. The append path only calls
 * this when timestamps are otherwise in order, so events with distinct
 * timestamps can stay in their original positions.
 */
function sortTimestampTies(indices: number[], ts: Float64Array, dur: Float64Array) {
  let start = 0;
  while (start < indices.length) {
    const timestamp = ts[indices[start]];
    let end = start + 1;
    while (end < indices.length && ts[indices[end]] === timestamp) {
      end++;
    }
    sortDurationDesc(indices, start, end, dur);
    start = end;
  }
}

/**
 * Sorts a contiguous range of source indices by descending duration, preserving
 * the rest of the index array. This is used only within a run of events that
 * share the same timestamp, where longer events must be processed first so
 * depth calculation treats them as parents.
 */
function sortDurationDesc(indices: number[], start: number, end: number, dur: Float64Array) {
  if (end - start < 2) return;
  if (end - start > 32) {
    const sorted = indices.slice(start, end).sort((a, b) => dur[b] - dur[a]);
    for (let i = 0; i < sorted.length; i++) {
      indices[start + i] = sorted[i];
    }
    return;
  }

  for (let i = start + 1; i < end; i++) {
    const index = indices[i];
    const duration = dur[index];
    let j = i - 1;
    while (j >= start && dur[indices[j]] < duration) {
      indices[j + 1] = indices[j];
      j--;
    }
    indices[j + 1] = index;
  }
}

/** Returns an array of [0, 1, 2, ..., length-1] */
function makeSequentialIndices(length: number) {
  const indices = new Array<number>(length);
  for (let i = 0; i < length; i++) {
    indices[i] = i;
  }
  return indices;
}
