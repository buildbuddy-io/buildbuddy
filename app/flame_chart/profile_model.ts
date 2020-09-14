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

function eventComparator(a: TraceEvent, b: TraceEvent) {
  // Group by thread ID.
  const threadIdDiff = a.tid - b.tid;
  if (threadIdDiff !== 0) return threadIdDiff;

  // Order in increasing order of start time.
  const tsDiff = a.ts - b.ts;
  if (tsDiff !== 0) return tsDiff;

  // When two events have the same start time, longer events should come first, since
  // those are considered the parent events of the shorter events and we want to push them
  // to the stack first.
  const durationDiff = a.dur - b.dur;
  return durationDiff;
}

// Don't show events smaller than 1ms for now.
const EVENT_VISIBILITY_THRESHOLD_MICROSECONDS = 1 * 1000;
// Don't show events that are a tiny fraction of the longest build event time.
// These events probably aren't worth optimizing for most people.
const EVENT_VISIBILITY_THRESHOLD_FRACTION_OF_LONGEST_EVENT = 0.0025;

function getThreadNames(events: TraceEvent[]) {
  const threadNameByTid = new Map<number, string>();
  for (const event of events as ThreadEvent[]) {
    if (event.name === "thread_name") {
      threadNameByTid.set(event.tid, event.args.name);
    }
  }
  return threadNameByTid;
}

/**
 * Builds the ThreadTimeline structures given the flat list of trace events
 * from the profile.
 */
export function buildThreadTimelines(events: TraceEvent[]): ThreadTimeline[] {
  const threadNameByTid = getThreadNames(events);

  const longestEvent = Math.max(...events.map((event) => event.dur || 0));
  const visibilityThreshold = Math.max(
    EVENT_VISIBILITY_THRESHOLD_MICROSECONDS,
    longestEvent * EVENT_VISIBILITY_THRESHOLD_FRACTION_OF_LONGEST_EVENT
  );

  events = events.filter(
    (event) =>
      event.tid !== undefined &&
      event.ts !== undefined &&
      // Some events have negative timestamps -- ignore these for now.
      event.ts + event.dur >= 0 &&
      event.dur &&
      event.dur > visibilityThreshold
  );
  events.sort(eventComparator);

  const timelines: ThreadTimeline[] = [];
  let tid = null;
  let timeline: ThreadTimeline | null = null;
  let stack: ThreadEvent[];
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
    timeline.maxDepth = Math.max(event.depth, timeline.maxDepth);
    timeline.events.push(event);
    stack.push(event);
  }

  for (const timeline of timelines) {
    timeline.threadName = threadNameByTid.get(timeline.tid);
  }

  return timelines;
}
