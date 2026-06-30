import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { command_line } from "../../proto/command_line_ts_proto";
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

function newInvocationModelWithClientEnv(repoUrl: string, env: Record<string, string>) {
  return new InvocationModel(
    new invocation.Invocation({
      repoUrl,
      structuredCommandLine: [
        new command_line.CommandLine({
          commandLineLabel: "canonical",
          sections: [
            new command_line.CommandLineSection({
              optionList: new command_line.OptionList({
                option: Object.entries(env).map(
                  ([key, value]) =>
                    new command_line.Option({
                      optionName: "client_env",
                      optionValue: `${key}=${value}`,
                    })
                ),
              }),
            }),
          ],
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

describe("InvocationModel.getGithubActionsUrl", () => {
  it("uses GITHUB_REPOSITORY for the actions run URL", () => {
    const model = newInvocationModelWithClientEnv("https://github.com/workspace/repo", {
      GITHUB_REPOSITORY: "actions/repo",
      GITHUB_RUN_ID: "12345",
    });

    expect(model.getGithubActionsUrl()).toBe("https://github.com/actions/repo/actions/runs/12345");
  });

  it("uses GITHUB_SERVER_URL for enterprise actions run URLs", () => {
    const model = newInvocationModelWithClientEnv("https://ghe.example.com/workspace/repo", {
      GITHUB_REPOSITORY: "actions/repo",
      GITHUB_RUN_ID: "12345",
      GITHUB_SERVER_URL: "https://ghe.example.com",
    });

    expect(model.getGithubActionsUrl()).toBe("https://ghe.example.com/actions/repo/actions/runs/12345");
  });
});
