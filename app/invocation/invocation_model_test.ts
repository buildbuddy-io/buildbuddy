import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import { tools } from "../../proto/spawn_ts_proto";
import InvocationModel from "./invocation_model";

function newInvocationModelWithBuildMetadata(metadata: Record<string, string>) {
  return new InvocationModel(
    new invocation.Invocation({
      event: [
        new invocation.InvocationEvent({
          buildEvent: new build_event_stream.BuildEvent({
            buildMetadata: new build_event_stream.BuildMetadata({ metadata }),
          }),
        }),
      ],
    })
  );
}

function newInvocationModelWithConfigurations(configurations: { cpu?: string; isTool?: boolean }[]) {
  return new InvocationModel(
    new invocation.Invocation({
      event: configurations.map(
        (configuration) =>
          new invocation.InvocationEvent({
            buildEvent: new build_event_stream.BuildEvent({
              configuration: new build_event_stream.Configuration({
                isTool: configuration.isTool,
                makeVariable: configuration.cpu ? { TARGET_CPU: configuration.cpu } : {},
              }),
            }),
          })
      ),
    })
  );
}

describe("InvocationModel.getIsRBEEnabled", () => {
  it("uses REMOTE_EXECUTION_ENABLED build metadata from the BES stream", () => {
    const model = newInvocationModelWithBuildMetadata({ REMOTE_EXECUTION_ENABLED: "yes" });

    expect(model.getIsRBEEnabled()).toBe(true);
  });

  it("prefers REMOTE_EXECUTION_ENABLED build metadata over remote_executor fallback", () => {
    const model = newInvocationModelWithBuildMetadata({ REMOTE_EXECUTION_ENABLED: "off" });
    model.optionsMap.set("remote_executor", "grpcs://remote.buildbuddy.io");

    expect(model.getIsRBEEnabled()).toBe(false);
  });

  it("falls back to remote_executor when REMOTE_EXECUTION_ENABLED is absent", () => {
    const model = new InvocationModel(new invocation.Invocation());
    model.optionsMap.set("remote_executor", "grpcs://remote.buildbuddy.io");

    expect(model.getIsRBEEnabled()).toBe(true);
  });
});

describe("InvocationModel.getCPU", () => {
  it("returns sorted target configuration CPUs and ignores tool configurations", () => {
    const model = newInvocationModelWithConfigurations([
      { cpu: "tool_cpu", isTool: true },
      { cpu: "target_1_cpu" },
      { cpu: "target_2_cpu" },
    ]);

    expect(model.getCPU()).toBe("target_1_cpu, target_2_cpu");
  });

  it("returns Unknown CPU when there are no target configuration CPUs", () => {
    const model = newInvocationModelWithConfigurations([{ cpu: "tool_cpu", isTool: true }]);

    expect(model.getCPU()).toBe("Unknown CPU");
  });
});

describe("InvocationModel.getMode", () => {
  it("uses compilation_mode when present, otherwise defaults to fastbuild", () => {
    const model = new InvocationModel(new invocation.Invocation());

    expect(model.getMode()).toBe("fastbuild");

    model.optionsMap.set("compilation_mode", "opt");

    expect(model.getMode()).toBe("opt");
  });
});

describe("InvocationModel.getExecutionLogIndex", () => {
  it("builds and caches one shared index for the decoded execution log", async () => {
    const model = new InvocationModel(new invocation.Invocation());
    const entries = [
      new tools.protos.ExecLogEntry({
        id: 1,
        file: new tools.protos.ExecLogEntry.File({ path: "input.txt" }),
      }),
    ];
    model.execLogEntryPromise = Promise.resolve(entries);

    const first = await model.getExecutionLogIndex();
    const second = await model.getExecutionLogIndex();

    expect(second).toBe(first);
    expect(first.entries).toBe(entries);
    expect(first.filesById.get(1)?.path).toEqual("input.txt");
  });
});
