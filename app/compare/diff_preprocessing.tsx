import { invocation as invocation_proto } from "../../proto/invocation_ts_proto";
import { command_line } from "../../proto/command_line_ts_proto";

/**
 * A unique token used to identify parts of the diff that were hidden due to denoising.
 */
export const NOISE_REPLACEMENT_TOKEN = `/__NOISE__${Math.random()}/`;

export type PreProcessingOptions = {
  sortEvents?: boolean;
  hideTimingData?: boolean;
  hideConsoleOutput?: boolean;
  hideInvocationIds?: boolean;
  hideUuids?: boolean;
};

export function computeTextForDiff(
  invocation: invocation_proto.IInvocation,
  { sortEvents, hideTimingData, hideConsoleOutput, hideInvocationIds, hideUuids }: PreProcessingOptions = {}
): string {
  const invocationId = invocation.invocationId;
  // Clone the invocation to avoid mutating the original object.
  invocation = invocation_proto.Invocation.fromObject((invocation as invocation_proto.Invocation).toJSON());

  if (sortEvents) {
    sortByProperty(invocation.event, (event: any) => JSON.stringify(event?.buildEvent?.id));
  }
  if (hideConsoleOutput) {
    invocation.consoleBuffer = NOISE_REPLACEMENT_TOKEN;
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

  for (const event of invocation.event) {
    const id = event?.buildEvent?.id;
    if (!id) continue;

    if (id?.configuration) {
      // The "makeVariable" map sometimes shows the same data but rendered in a different order;
      // sorting the maps solves this problem.
      sortEntriesByKey(event.buildEvent.configuration.makeVariable);
      continue;
    }

    if (hideTimingData) {
      if (id?.workspaceStatus) {
        event.buildEvent.workspaceStatus.item.find(
          (item: any) => item.key === "BUILD_TIMESTAMP"
        )!.value = NOISE_REPLACEMENT_TOKEN;
      } else if (id?.buildToolLogs) {
        event.buildEvent.buildToolLogs.log.find(
          (item: any) => item.name === "elapsed time"
        )!.contents = new Uint8Array();
        (event.buildEvent.buildToolLogs.log.find(
          (item: any) => item.name === "critical path"
        ) as any).contents = NOISE_REPLACEMENT_TOKEN;
        (event.buildEvent.buildToolLogs.log.find(
          (item: any) => item.name === "process stats"
        ) as any).contents = NOISE_REPLACEMENT_TOKEN;
      } else if (id?.structuredCommandLine) {
        removeTimingData(event.buildEvent.structuredCommandLine);
      }
    }
    if (hideUuids) {
      if (id?.started) {
        event.buildEvent.started.uuid = NOISE_REPLACEMENT_TOKEN;
      }
    }
  }

  let json = JSON.stringify(invocation, null, 2);
  if (hideTimingData) {
    json = json.replace(
      /"(startTimeMillis|finishTimeMillis|seconds|nanos|createdAtUsec|updatedAtUsec|durationUsec|cpuTimeInMs|wallTimeInMs)": ("?[0-9]+"?)/g,
      `"$1": ${NOISE_REPLACEMENT_TOKEN}`
    );
  }
  if (sortEvents) {
    json = json.replace(/"(sequenceNumber)": ("?[0-9]+"?)/g, `"$1": ${NOISE_REPLACEMENT_TOKEN}`);
  }
  if (hideInvocationIds) {
    json = json.replace(invocationId, NOISE_REPLACEMENT_TOKEN);
  }
  if (hideUuids) {
    json = json.replace(/"(uuid)": ("?[a-z0-9\-]+"?)/g, `"$1": ${NOISE_REPLACEMENT_TOKEN}`);
  }
  return json;
}

function removeTimingData(commandLine: command_line.ICommandLine) {
  for (const section of commandLine.sections) {
    if (!section.optionList) continue;
    for (const option of section.optionList.option) {
      if (option.optionName === "startup_time") {
        option.optionValue = NOISE_REPLACEMENT_TOKEN;
        option.combinedForm = `--startup_time=${NOISE_REPLACEMENT_TOKEN}`;
      }
    }
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
