import { Thread } from "./compact_trace";
import { SectionModel, TrackModel } from "./trace_viewer_model";

function eventEnd(track: TrackModel, eventIndex: number): number {
  return track.thread.ts[eventIndex] + track.thread.dur[eventIndex];
}

function containsEvent(track: TrackModel, outerIndex: number, innerIndex: number): boolean {
  return (
    track.thread.ts[outerIndex] <= track.thread.ts[innerIndex] &&
    eventEnd(track, outerIndex) >= eventEnd(track, innerIndex)
  );
}

/**
 * Returns thread-local indices of the first containing ancestor at each depth.
 * Only ancestors are cached: descendants are checked while rendering so focusing
 * a broad span does not allocate a set containing its entire subtree.
 */
export function collectFocusedAncestorEventIndices(
  section: SectionModel,
  focusedEventIndex: number,
  focusedTrackIndex: number
): Set<number> {
  const ancestors = new Set<number>();
  const tracks = section.tracks ?? [];
  const focusedTrack = tracks[focusedTrackIndex];
  if (!focusedTrack) return ancestors;

  for (let trackIndex = 0; trackIndex < focusedTrackIndex; trackIndex++) {
    const track = tracks[trackIndex];
    if (track.thread !== focusedTrack.thread) continue;
    for (const eventIndex of track.eventIndices) {
      if (track.thread.ts[eventIndex] > track.thread.ts[focusedEventIndex]) break;
      if (containsEvent(track, eventIndex, focusedEventIndex)) {
        ancestors.add(eventIndex);
        break;
      }
    }
  }
  return ancestors;
}

/** Whether a thread-local event is the focus, a cached ancestor, or a fully contained descendant. */
export function isFocusedStackEvent(
  thread: Thread,
  eventIndex: number,
  focusedEventIndex: number,
  ancestors: ReadonlySet<number>
): boolean {
  if (eventIndex === focusedEventIndex || ancestors.has(eventIndex)) return true;
  return (
    thread.depth[eventIndex] > thread.depth[focusedEventIndex] &&
    thread.ts[eventIndex] >= thread.ts[focusedEventIndex] &&
    thread.ts[eventIndex] + thread.dur[eventIndex] <= thread.ts[focusedEventIndex] + thread.dur[focusedEventIndex]
  );
}
