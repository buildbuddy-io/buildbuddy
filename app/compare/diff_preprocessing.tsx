import Long from "long";
import { invocation as invocation_proto } from "../../proto/invocation_ts_proto";
import { google as google_timestamp } from "../../proto/timestamp_ts_proto";
import { command_line } from "../../proto/command_line_ts_proto";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";

export type PreProcessingOptions = {
  sortEvents?: boolean;
  hideTimingData?: boolean;
  hideInvocationIds?: boolean;
  hideUuids?: boolean;
  hideProgress?: boolean;
};

export function prepareForDiff(
  invocation: invocation_proto.IInvocation,
  { sortEvents, hideTimingData, hideInvocationIds, hideUuids, hideProgress }: PreProcessingOptions = {}
): invocation_proto.IInvocation {
  // Clone the invocation to avoid mutating the original object.
  invocation = invocation_proto.Invocation.fromObject((invocation as invocation_proto.Invocation).toJSON());

  if (hideInvocationIds) {
    delete invocation.invocationId;
  }
  if (sortEvents && invocation.event) {
    sortByProperty(invocation.event, (event: any) => JSON.stringify(event?.buildEvent?.id));
  }
  // Inlined console buffer is deprecated but some older invocations may still
  // have this field. Either way, it is not very useful to show build logs
  // in the diff, so just delete this field unconditionally.
  invocation.consoleBuffer = "";
  if (hideTimingData) {
    invocation.durationUsec = Long.ZERO;
    invocation.createdAtUsec = Long.ZERO;
    invocation.updatedAtUsec = Long.ZERO;
  }

  // Some CommandLine objects are empty for some reason; remove these.
  if (invocation.structuredCommandLine) {
    invocation.structuredCommandLine = invocation.structuredCommandLine.filter(
      (commandLine: any) => commandLine.commandLineLabel
    );
    // Sort structured command lines so canonical always comes before original.
    sortByProperty(invocation.structuredCommandLine, (commandLine) => commandLine.commandLineLabel);
  }

  if (hideTimingData) {
    for (const commandLine of invocation.structuredCommandLine || []) {
      removeTimingData(commandLine);
    }
  }

  const events: invocation_proto.InvocationEvent[] = [];
  for (const event of invocation.event || []) {
    if (sortEvents) {
      event.sequenceNumber = Long.ZERO;
    }

    const buildEvent = event?.buildEvent;
    if (!buildEvent) continue;
    const id = buildEvent?.id;
    if (!id) continue;

    if (buildEvent.configuration?.makeVariable) {
      // The "makeVariable" map sometimes shows the same data but rendered in a different order;
      // sorting the maps solves this problem.
      sortEntriesByKey(buildEvent.configuration.makeVariable);
    }

    if (hideProgress && buildEvent.progress) {
      continue;
    }

    if (hideTimingData) {
      delete event.eventTime;

      if (buildEvent.workspaceStatus) {
        const timestampItem = buildEvent.workspaceStatus.item?.find((item: any) => item.key === "BUILD_TIMESTAMP");
        if (timestampItem) {
          timestampItem.value = "";
        }
      } else if (buildEvent.buildToolLogs) {
        buildEvent.buildToolLogs.log = buildEvent.buildToolLogs.log?.filter(
          (log) => !["elapsed time", "critical path", "process stats"].includes(log.name || "")
        );
      } else if (buildEvent.structuredCommandLine) {
        removeTimingData(buildEvent.structuredCommandLine);
      } else if (buildEvent.buildMetrics) {
        delete buildEvent.buildMetrics.timingMetrics;
        if (buildEvent.buildMetrics.actionSummary) {
          removeTimingDataInActionSummary(buildEvent.buildMetrics.actionSummary);
        }
      } else if (buildEvent.started) {
        buildEvent.started.startTimeMillis = Long.ZERO;
        buildEvent.started.startTime = new google_timestamp.protobuf.Timestamp();
      } else if (buildEvent.finished) {
        buildEvent.finished.finishTimeMillis = Long.ZERO;
        buildEvent.finished.finishTime = new google_timestamp.protobuf.Timestamp();
      }
    }

    if (hideUuids) {
      if (id?.started && buildEvent.started) {
        buildEvent.started.uuid = "";
      }
    }

    events.push(invocation_proto.InvocationEvent.create(event));
  }
  invocation.event = events;
  return invocation;
}

function removeTimingData(commandLine: command_line.ICommandLine) {
  for (const section of commandLine?.sections || []) {
    if (!section?.optionList?.option) continue;
    section.optionList.option = section.optionList.option.filter((option) => option.optionName !== "startup_time");
  }
}

function removeTimingDataInActionSummary(actionSummary: build_event_stream.BuildMetrics.IActionSummary) {
  for (const actionData of actionSummary?.actionData || []) {
    actionData.firstStartedMs = Long.ZERO;
    actionData.lastEndedMs = Long.ZERO;
  }
}

function sortByProperty<T>(items: T[], property: (item: T) => any) {
  items.sort((itemA, itemB) => {
    const a = property(itemA);
    const b = property(itemB);
    return a < b ? -1 : a > b ? 1 : 0;
  });
}

function sortEntriesByKey(object: Record<string, any>) {
  // In ES6, object entries are sorted by the order in which they're inserted.
  // So, deleting all entries and re-inserting them in sorted order effectively
  // sorts the object.
  for (const key of Object.keys(object).sort()) {
    const value = object[key];
    delete object[key];
    object[key] = value;
  }
}
