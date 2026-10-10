import { SectionModel } from "./trace_viewer_model";

export type FocusedStack = {
  ancestorEventIndices: Set<number>;
  // Thread-local half-open range containing the focus and its descendants.
  firstEventIndex: number;
  endEventIndex: number;
};

/**
 * Finds the rendered stack using the profile's depth order. At each depth, the
 * latest event before the focus is its active ancestor. The next event at the
 * same or a shallower depth ends its subtree, even when spans cross in time.
 * Binary searches bound navigation work by stack depth, without scanning or
 * storing every descendant.
 */
export function computeFocusedStack(
  section: SectionModel,
  focusedEventIndex: number,
  focusedTrackIndex: number
): FocusedStack | undefined {
  const tracks = section.tracks ?? [];
  const focusedTrack = tracks[focusedTrackIndex];
  if (!focusedTrack) return undefined;
  const stack: FocusedStack = {
    ancestorEventIndices: new Set<number>(),
    firstEventIndex: focusedEventIndex,
    endEventIndex: focusedTrack.thread.length,
  };
  for (let trackIndex = 0; trackIndex <= focusedTrackIndex; trackIndex++) {
    const indices = tracks[trackIndex].eventIndices;
    // Track indices are in thread event order, including ties in timestamps.
    let low = 0;
    let high = indices.length;
    while (low < high) {
      const mid = low + Math.floor((high - low) / 2);
      if (indices[mid] <= focusedEventIndex) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    if (trackIndex < focusedTrackIndex && low > 0) {
      stack.ancestorEventIndices.add(indices[low - 1]);
    }
    if (low < indices.length) {
      stack.endEventIndex = Math.min(stack.endEventIndex, indices[low]);
    }
  }
  return stack;
}

/** Whether a thread-local event belongs to the focused event's rendered stack. */
export function isFocusedStackEvent(eventIndex: number, stack: FocusedStack): boolean {
  return (
    stack.ancestorEventIndices.has(eventIndex) ||
    (eventIndex >= stack.firstEventIndex && eventIndex < stack.endEventIndex)
  );
}
