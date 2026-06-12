import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
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

describe("InvocationModel.getMode", () => {
  it("uses compilation_mode when present, otherwise defaults to fastbuild", () => {
    const model = new InvocationModel(new invocation.Invocation());

    expect(model.getMode()).toBe("fastbuild");

    model.optionsMap.set("compilation_mode", "opt");

    expect(model.getMode()).toBe("opt");
  });
});
