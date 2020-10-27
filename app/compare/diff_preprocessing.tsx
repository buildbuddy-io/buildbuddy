import { invocation as invocation_proto } from "../../proto/invocation_ts_proto";
import { command_line } from "../../proto/command_line_ts_proto";

/**
 * A unique token used to identify parts of the diff that were hidden due to pre-processing options.
 */
export const HIDDEN_TOKEN = `/__HIDDEN__2540ee7b-c0b0-4ed6-ab90-b68c220cc9ea/`;
/**
 * protobuf.js' JSON representation of HIDDEN_TOKEN when stored in a `bytes` field.
 */
const HIDDEN_TOKEN_BYTES_ENCODING = "AAAAAAAAAAAAAAACBQQAAAAHAAAAAAAAAAQAAAYAAAAJAAAABggAAgIAAAAJAAAA";

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
    invocation.consoleBuffer = HIDDEN_TOKEN;
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
        const timestampItem = event.buildEvent.workspaceStatus.item.find((item: any) => item.key === "BUILD_TIMESTAMP");
        if (timestampItem) {
          timestampItem.value = HIDDEN_TOKEN;
        }
      } else if (id?.buildToolLogs) {
        for (const log of event.buildEvent.buildToolLogs.log) {
          if (["elapsed time", "critical path", "process stats"].includes(log.name)) {
            (log as any).contents = HIDDEN_TOKEN;
          }
        }
      } else if (id?.structuredCommandLine) {
        removeTimingData(event.buildEvent.structuredCommandLine);
      }
    }
    if (hideUuids) {
      if (id?.started) {
        event.buildEvent.started.uuid = HIDDEN_TOKEN;
      }
    }
  }

  let json = JSON.stringify(invocation, null, 2);
  // HIDDEN_TOKEN gets mangled when stored in bytes fields; this replacement effectively de-mangles it.
  json = replaceAll(json, HIDDEN_TOKEN_BYTES_ENCODING, HIDDEN_TOKEN);
  if (hideTimingData) {
    json = replaceAllJsonValues(
      json,
      [
        "startTimeMillis",
        "finishTimeMillis",
        "seconds",
        "nanos",
        "createdAtUsec",
        "updatedAtUsec",
        "durationUsec",
        "cpuTimeInMs",
        "wallTimeInMs",
      ],
      HIDDEN_TOKEN
    );
  }
  if (sortEvents) {
    json = replaceAllJsonValues(json, ["sequenceNumber"], HIDDEN_TOKEN);
  }
  if (hideInvocationIds) {
    json = replaceAll(json, invocationId, HIDDEN_TOKEN);
  }
  if (hideUuids) {
    json = replaceAllJsonValues(json, ["uuid"], HIDDEN_TOKEN);
  }
  return json;
}

function removeTimingData(commandLine: command_line.ICommandLine) {
  for (const section of commandLine.sections) {
    if (!section.optionList) continue;
    for (const option of section.optionList.option) {
      if (option.optionName === "startup_time") {
        option.optionValue = HIDDEN_TOKEN;
        option.combinedForm = `--startup_time=${HIDDEN_TOKEN}`;
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

function replaceAll(str: string, value: string, replacement: string) {
  return str.split(value).join(replacement);
}

function replaceAllJsonValues(json: string, keys: string[], replacement: string) {
  // Regex notes:
  // - The JSON key is captured in capture group $1 and later referenced in the
  //   replacement.
  // - The matched keys are joined with | to match any of the keys.
  // - Quotes around the JSON value (if present) are captured in capture
  //   groups $2 and $3 to preserve quotes around the JSON values if present.
  // - Values are only replaced if they consist of alphanumeric characters or
  //   hyphens ("-")
  const regExp = new RegExp(`"(${keys.join("|")})": ("?)[A-Za-z0-9\\-]+("?)`, "g");
  return json.replace(regExp, `"$1": $2${replacement}$3`);
}
