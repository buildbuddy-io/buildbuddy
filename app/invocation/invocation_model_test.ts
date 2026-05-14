import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";

declare function require(path: string): any;

const testGlobal = globalThis as unknown as { window?: Record<string, unknown> };

testGlobal.window = {
  buildbuddyConfig: {},
  location: {
    host: "localhost",
    pathname: "/",
    search: "",
    hash: "",
  },
};

const InvocationModel = require("./invocation_model").default as typeof import("./invocation_model").default;

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
