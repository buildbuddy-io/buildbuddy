import { TraceEvent } from "./trace_events";
import { SectionModel, TrackModel } from "./trace_viewer_model";

function searchableFieldText(value: unknown): string {
  if (value === undefined || value === null) return "";
  return String(value).toLowerCase();
}

export function getTraceEventSearchText(event: TraceEvent): string {
  return [event.name, event.cat, event.args?.target, event.args?.mnemonic, event.out]
    .map(searchableFieldText)
    .join(" ");
}

export function traceEventMatchesFilter(event: TraceEvent, filter: string): boolean {
  return !filter || getTraceEventSearchText(event).includes(filter.toLowerCase());
}

function eventEnd(track: TrackModel, eventIndex: number): number {
  return track.thread.ts[eventIndex] + track.thread.dur[eventIndex];
}

function containsEvent(track: TrackModel, outerIndex: number, innerIndex: number): boolean {
  return (
    track.thread.ts[outerIndex] <= track.thread.ts[innerIndex] &&
    eventEnd(track, outerIndex) >= eventEnd(track, innerIndex)
  );
}

export function collectFocusedTracePathEventIndices(
  section: SectionModel,
  focusedEventIndex: number,
  focusedTrackIndex: number
): Set<number> {
  const focusedPathEventIndices = new Set<number>([focusedEventIndex]);
  const tracks = section.tracks ?? [];
  const focusedTrack = tracks[focusedTrackIndex];
  if (!focusedTrack) return focusedPathEventIndices;

  const thread = focusedTrack.thread;
  const focusedEnd = eventEnd(focusedTrack, focusedEventIndex);

  for (let trackIndex = 0; trackIndex < tracks.length; trackIndex++) {
    if (trackIndex === focusedTrackIndex) continue;

    const track = tracks[trackIndex];
    if (track.thread !== thread) continue;

    for (const eventIndex of track.eventIndices) {
      if (trackIndex < focusedTrackIndex) {
        if (thread.ts[eventIndex] > thread.ts[focusedEventIndex]) break;
        if (containsEvent(track, eventIndex, focusedEventIndex)) {
          focusedPathEventIndices.add(eventIndex);
          break;
        }
        continue;
      }

      if (thread.ts[eventIndex] > focusedEnd) break;
      if (containsEvent(track, focusedEventIndex, eventIndex)) {
        focusedPathEventIndices.add(eventIndex);
      }
    }
  }

  return focusedPathEventIndices;
}
