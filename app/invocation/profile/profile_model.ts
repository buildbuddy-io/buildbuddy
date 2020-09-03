export interface TimelineEvent {
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

export type ThreadTimeline = {
  events: ThreadEvent[];
  maxDepth: number;
};

export type ThreadEvent = TimelineEvent & {
  depth: number;
};

function eventComparator(a: TimelineEvent, b: TimelineEvent) {
  const tid = a.tid !== undefined && b.tid !== undefined ? 0 : a.tid - b.tid;
  if (tid !== 0) return tid;

  const ts = a.ts !== undefined && b.ts !== undefined ? a.ts - b.ts : 0;
  if (ts !== 0) return ts;

  const dur = a.dur !== undefined && b.dur !== undefined ? a.dur - b.dur : 0;
  if (dur !== 0) return dur;

  const cat = a.cat !== undefined && b.cat !== undefined ? a.cat.localeCompare(b.cat) : 0;
  if (cat !== 0) return cat;

  return 0;
}

export function buildThreadTimelines(events: TimelineEvent[]): ThreadTimeline[] {
  events.sort(eventComparator);

  const timelines: ThreadTimeline[] = [];
  let tid = null;
  let timeline: ThreadTimeline | null = null;
  let stack: ThreadEvent[] = [];
  for (const event of events as ThreadEvent[]) {
    if (event.tid === undefined || event.dur === undefined || event.ts === undefined) {
      continue;
    }

    if (tid === null || event.tid !== tid) {
      // Encountered new thread
      // (Note that events are sorted by tid first)
      tid = event.tid;
      timeline = {
        events: [],
        maxDepth: 0,
      };
      timelines.push(timeline);
      stack = [];
    }

    // Traverse up the stack while the current event starts
    // after the current stack frame ends.
    let top = stack[stack.length - 1];
    while (top && top.ts + top.dur <= event.ts) {
      stack.pop();
      top = stack[stack.length - 1];
    }
    event.depth = stack.length;
    timeline.maxDepth = Math.max(event.depth, timeline.maxDepth);
    timeline.events.push(event);
    stack.push(event);
  }
  return timelines;
}
