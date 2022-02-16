import { invocation as invocation_proto } from "../../proto/invocation_ts_proto";
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
  if (sortEvents) {
    sortByProperty(invocation.event, (event: any) => JSON.stringify(event?.buildEvent?.id));
  }
  // Inlined console buffer is deprecated but some older invocations may still
  // have this field. Either way, it is not very useful to show build logs
  // in the diff, so just delete this field unconditionally.
  delete invocation.consoleBuffer;
  if (hideTimingData) {
    delete invocation.durationUsec;
    delete invocation.createdAtUsec;
    delete invocation.updatedAtUsec;
  }

  // Some CommandLine objects are empty for some reason; remove these.
  invocation.structuredCommandLine = invocation.structuredCommandLine.filter(
    (commandLine: any) => commandLine.commandLineLabel
  );
  // Sort structured command lines so canonical always comes before original.
  sortByProperty(invocation.structuredCommandLine, (commandLine) => commandLine.commandLineLabel);

  if (hideTimingData) {
    for (const commandLine of invocation.structuredCommandLine) {
      removeTimingData(commandLine);
    }
  }

  const events: invocation_proto.IInvocationEvent[] = [];
  for (const event of invocation.event) {
    if (sortEvents) {
      delete event.sequenceNumber;
    }

    const buildEvent = event?.buildEvent;
    const id = buildEvent?.id;
    if (!id) continue;

    if (id?.configuration) {
      // The "makeVariable" map sometimes shows the same data but rendered in a different order;
      // sorting the maps solves this problem.
      sortEntriesByKey(buildEvent.configuration.makeVariable);
    }

    if (hideProgress) {
      if (id?.progress) {
        continue;
      }
    }

    if (hideTimingData) {
      delete event.eventTime;

      if (id?.workspaceStatus) {
        const timestampItem = buildEvent.workspaceStatus.item.find((item: any) => item.key === "BUILD_TIMESTAMP");
        if (timestampItem) {
          delete timestampItem.value;
        }
      } else if (id?.buildToolLogs) {
        buildEvent.buildToolLogs.log = event.buildEvent.buildToolLogs.log.filter(
          (log) => !["elapsed time", "critical path", "process stats"].includes(log.name)
        );
      } else if (id?.structuredCommandLine) {
        removeTimingData(buildEvent.structuredCommandLine);
      } else if (id?.buildMetrics) {
        delete buildEvent.buildMetrics.timingMetrics;
        removeTimingDataInActionSummary(buildEvent.buildMetrics.actionSummary);
      } else if (id?.started) {
        delete buildEvent.started.startTimeMillis;
        delete buildEvent.started.startTime;
      } else if (id?.buildFinished) {
        delete buildEvent.finished.finishTimeMillis;
        delete buildEvent.finished.finishTime;
      }
    }

    if (hideUuids) {
      if (id?.started) {
        delete buildEvent.started.uuid;
      }
    }

    events.push(event);
  }
  invocation.event = events;
  return invocation;
}

function removeTimingData(commandLine: command_line.ICommandLine) {
  for (const section of commandLine.sections) {
    if (!section?.optionList?.option) continue;
    section.optionList.option = section.optionList.option.filter((option) => option.optionName !== "startup_time");
  }
}

function removeTimingDataInActionSummary(actionSummary: build_event_stream.BuildMetrics.IActionSummary) {
  for (const actionData of actionSummary?.actionData || []) {
    delete actionData.firstStartedMs;
    delete actionData.lastEndedMs;
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
