/**
 * This file contains utilities for parsing a raw trace file.
 *
 * The trace file format is loosely documented here:
 * https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview#heading=h.yr4qxyxotyw
 */

/** Represents the profile data for an invocation. */
export interface Profile {
  traceEvents: TraceEvent[];
}

/** Represents a trace event in the profile. */
export interface TraceEvent {
  pid: number;
  tid: number;
  ts: number;
  ph: string;
  cat: string;
  name: string;
  dur: number;
  tdur: number;
  tts: number;
  out: string;
  args: { [key: string]: any };
  id?: string;
}

/** TraceEvent augmented with calculated call stack info. */
export type ThreadEvent = TraceEvent & {
  depth: number;
};

/** A list of all the events and their call stack depths within a named thread. */
export type ThreadTimeline = {
  tid: number;
  threadName: string;
  events: ThreadEvent[];
  maxDepth: number;
};

export type TimeSeriesEvent = TraceEvent & {
  value: number | string;
};

export type TimeSeries = {
  name: string;
  events: TimeSeriesEvent[];
};

// Matches strings like "skyframe evaluator 1", "grpc-command-0", etc., splitting the
// non-numeric prefix and numeric suffix into separate match groups.
const NUMBERED_THREAD_NAME_PATTERN = /^(?<prefix>[^\d]+)(?<number>\d+)$/;

// A list of names of events that contain a timestamp and a value in args.
//
// We only render timeseries which are defined in this list. The order of the
// entries in this list determines the order in which the timeseries are
// displayed in the trace viewer panel.
//
// An example event for Bazel CPU usage looks like this: {"name": "CPU usage
// (bazel)", ..., "args": {"cpu": 0.84}}
const TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS_ENTRIES: [string, string][] = [
  // Event names/arg keys from Bazel profiles.
  // These are defined by bazel / not controlled by us.
  ["action count", "action"],
  ["CPU usage (Bazel)", "cpu"],
  ["Memory usage (Bazel)", "memory"],
  ["CPU usage (total)", "system cpu"],
  ["Memory usage (total)", "system memory"],
  ["System load average", "load"],
  ["Network Up usage (total)", "system network up (Mbps)"],
  ["Network Down usage (total)", "system network down (Mbps)"],

  // Event names/arg keys from executor profiles.
  // These are controlled by us, and defined in
  // enterprise/server/execution_service/execution_service.go
  ["CPU usage (cores)", "cpu"],
  ["Memory usage (KB)", "memory"],
  ["Disk read bandwidth (MB/s)", "disk-read-bw"],
  ["Disk read IOPS", "disk-read-iops"],
  ["Disk write bandwidth (MB/s)", "disk-write-bw"],
  ["Disk write IOPS", "disk-write-iops"],
];

const TIME_SERIES_EVENT_ORDER = new Map<string, number>(
  TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS_ENTRIES.map(([eventName, _argKey], index) => [eventName, index])
);

const TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS = new Map(TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS_ENTRIES);

export async function readProfile(
  body: ReadableStream<Uint8Array>,
  progress?: (numBytesLoaded: number) => void
): Promise<Profile> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8");
  let n = 0;
  let buffer = "";
  let profile: Profile | null = null;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    // `stream: true` allows us to handle UTF-8 sequences that cross chunk
    // boundaries (should be relatively rare).
    const text = decoder.decode(value, { stream: true });
    buffer += text;
    n += value.byteLength;
    progress?.(n);
    // Keep accumulating into the buffer until we see the "traceEvents" array.
    // Each entry in this array is newline-delimited (a special property of
    // Google's trace event JSON format).
    if (!profile) {
      const beginMarker = '"traceEvents":[\n';
      const index = buffer.indexOf(beginMarker);
      if (index < 0) continue;

      const before = buffer.substring(0, index + beginMarker.length);
      const after = buffer.substring(before.length);

      const outerJSON = before + "]}";
      profile = JSON.parse(outerJSON) as Profile;
      buffer = after;
    }
    if (profile) {
      buffer = consumeEvents(buffer, profile);
    }
  }
  // Consume last event, which isn't guaranteed to end with ",\n"
  if (buffer) {
    const { chars, suffix } = trailingNonWhitespaceChars(buffer, 2);
    if (chars === "]}") {
      buffer = buffer.substring(0, buffer.length - suffix.length);
    }
    if (buffer) {
      const event = JSON.parse(buffer);
      profile?.traceEvents.push(event);
      buffer = "";
    }
  }

  if (!profile) {
    throw new Error("failed to parse timing profile JSON");
  }
  return profile;
}

/**
 * Returns trailing non-whitespace characters in a string up to a given count.
 * Returns the non-whitespace as `chars` as well as the matched `suffix` from
 * the original string.
 */
function trailingNonWhitespaceChars(text: string, count: number) {
  let out = "";
  let i: number;
  for (i = text.length - 1; i >= 0 && out.length < count; i--) {
    if (text[i].match(/\s/)) {
      continue;
    }
    out = text[i] + out;
  }
  return { chars: out, suffix: text.substring(i + 1) };
}

function consumeEvents(buffer: string, profile: Profile): string {
  // Each event entry looks like "   { ... },\n"
  const parts = buffer.split(",\n");
  const completeEvents = parts.slice(0, parts.length - 1);
  for (const rawEvent of completeEvents) {
    profile!.traceEvents.push(JSON.parse(rawEvent));
  }
  // If there's a partial event at the end, that partial event becomes the new
  // buffer value.
  return parts[parts.length - 1] || "";
}

function eventComparator(a: TraceEvent, b: TraceEvent) {
  // Group by thread ID.
  const threadIdDiff = a.tid - b.tid;
  if (threadIdDiff !== 0) return threadIdDiff;

  // Sort in increasing order of start time.
  const tsDiff = a.ts - b.ts;
  if (tsDiff !== 0) return tsDiff;

  // When two events have the same start time, longer events should come first, since
  // those are considered the parent events of the shorter events and we want to push them
  // to the stack first.
  const durationDiff = b.dur - a.dur;
  return durationDiff;
}

function timeSeriesEventComparator(a: TraceEvent, b: TraceEvent) {
  // Group by name, respecting sort order if defined.
  const orderDiff = (TIME_SERIES_EVENT_ORDER.get(a.name) ?? 0) - (TIME_SERIES_EVENT_ORDER.get(b.name) ?? 0);
  if (orderDiff !== 0) return orderDiff;
  const nameDiff = a.name.localeCompare(b.name);
  if (nameDiff !== 0) return nameDiff;

  // Sort in increasing order of start time.
  const tsDiff = a.ts - b.ts;
  return tsDiff;
}

const threadOrder: { [key: string]: number } = {
  "critical path": 1,
  "main thread": 2,
  "notification thread": 3,
  "skyframe evaluator execution": 4,
  "skyframe evaluator": 5,
  "skyframe evaluator cpu heavy": 6,
  "remote executor": 7,
};

function normalizeThreadName(name: string) {
  return name.toLowerCase().replaceAll("-", " ").trim();
}

function timelineComparator(a: ThreadTimeline, b: ThreadTimeline) {
  // Within numbered thread names (e.g. "skyframe evaluator 0", "grpc-command-0"), sort
  // numerically.
  const matchA = a.threadName.match(NUMBERED_THREAD_NAME_PATTERN)?.groups;
  const matchB = b.threadName.match(NUMBERED_THREAD_NAME_PATTERN)?.groups;
  if (matchA && matchB && matchA["prefix"] === matchB["prefix"]) {
    return Number(matchA["number"]) - Number(matchB["number"]);
  }

  // For known threads, show the most interesting ones first.
  if (
    matchA &&
    matchB &&
    threadOrder[normalizeThreadName(matchA["prefix"])] &&
    threadOrder[normalizeThreadName(matchB["prefix"])]
  ) {
    return threadOrder[normalizeThreadName(matchA["prefix"])] - threadOrder[normalizeThreadName(matchB["prefix"])];
  }

  // Sort other timelines lexicographically by thread name.
  return a.threadName.localeCompare(b.threadName);
}

function getThreadNames(events: TraceEvent[]) {
  const threadNameByTid = new Map<number, string>();
  for (const event of events as ThreadEvent[]) {
    if (event.name === "thread_name") {
      threadNameByTid.set(event.tid, event.args.name);
    }
  }
  return threadNameByTid;
}

function normalizeThreadNames(events: TraceEvent[]) {
  for (const event of events as ThreadEvent[]) {
    if (event.name === "thread_name") {
      // Job threads ("skyframe evaluators") will sometimes have inconsistent dashes.
      if (event.args.name.startsWith("skyframe")) {
        event.args.name = event.args.name.replace(/^skyframe[ \-]evaluator[ \-]/, "skyframe evaluator ");
      }
    }
  }
}

export function buildTimeSeries(events: TraceEvent[]): TimeSeries[] {
  events = events.filter((event) => TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS.has(event.name));
  events.sort(timeSeriesEventComparator);

  const timelines: TimeSeries[] = [];
  let name = null;
  let timeSeries: TimeSeries | null = null;
  for (const event of events as TimeSeriesEvent[]) {
    if (name === null || event.name !== name) {
      // Encountered new type of time series data
      name = event.name;
      timeSeries = {
        name,
        events: [],
      };
      timelines.push(timeSeries);
    }
    for (const key in event.args) {
      if (key == TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS.get(event.name)) {
        event.value = event.args[key];
      }
    }
    timeSeries!.events.push(event);
  }
  return timelines;
}

/**
 * Builds the ThreadTimeline structures given the flat list of trace events
 * from the profile.
 */
export function buildThreadTimelines(events: TraceEvent[], { visibilityThreshold = 0 } = {}): ThreadTimeline[] {
  normalizeThreadNames(events);
  const threadNameByTid = getThreadNames(events);
  const threadNameToTidMap = new Map<string, number>();
  events = events.filter(
    (event) =>
      event.tid !== undefined &&
      event.ts !== undefined &&
      // Some events have negative timestamps -- ignore these for now.
      event.ts + event.dur >= 0 &&
      event.dur &&
      event.dur > visibilityThreshold
  );

  // Merge all of the threads with a single thread name under the same tid.
  for (const [tid, threadName] of threadNameByTid.entries()) {
    threadNameToTidMap.set(threadName, tid);
  }

  for (const event of events) {
    event.tid = threadNameToTidMap.get(threadNameByTid.get(event.tid) || "") ?? -1;
  }

  events.sort(eventComparator);

  const timelines: ThreadTimeline[] = [];
  let tid = null;
  let timeline: ThreadTimeline | null = null;
  let stack: ThreadEvent[] = [];
  for (const event of events as ThreadEvent[]) {
    if (tid === null || event.tid !== tid) {
      // Encountered new thread, and we're done processing events from the previous thread
      // (note that events are sorted by tid first)
      tid = event.tid;
      timeline = {
        tid,
        threadName: "",
        events: [],
        maxDepth: 0,
      };
      timelines.push(timeline);
      stack = [];
    }

    // Traverse up the stack to find the first event that this event fits inside. This works
    // because events are sorted in increasing order of timestamp and ties are broken by sorting
    // in decreasing order of duration.
    //
    // Occasionally, Bazel's profile info will contain an invalid sequence like this:
    //
    // E1: |-------|
    // E2:        |-------|
    //
    // This is probably due to imprecise measurements.
    //
    // To deal with this, we pop the stack when seeing events like E2. This
    // results in some overlap but it's hardly noticeable.
    let top: ThreadEvent;
    while (
      (top = stack[stack.length - 1]) &&
      (top.ts + top.dur < event.ts || top.ts + top.dur < event.ts + event.dur)
    ) {
      stack.pop();
    }
    event.depth = stack.length;
    timeline!.maxDepth = Math.max(event.depth, timeline!.maxDepth);
    timeline!.events.push(event);
    stack.push(event);
  }

  for (const timeline of timelines) {
    timeline.threadName = threadNameByTid.get(timeline.tid) || "";
  }

  timelines.sort(timelineComparator);

  return timelines;
}
