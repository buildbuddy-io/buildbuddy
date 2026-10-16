import Long from "long";
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

describe("InvocationModel regional endpoints", () => {
  const globalPublic = "grpcs://remote.buildbuddy.io";
  const globalOrg = "grpcs://test-org.buildbuddy.io";
  const europePublic = "grpcs://remote.europe.buildbuddy.io";
  const europeOrg = "grpcs://test-org.europe.buildbuddy.io";
  const cases: {
    name: string;
    role: string;
    options: Record<string, string>;
    execution: string;
    cache: string;
    bes: string;
  }[] = [
    ...[
      { name: "global public", endpoint: globalPublic },
      { name: "global organization", endpoint: globalOrg },
      { name: "Europe public", endpoint: europePublic },
      { name: "Europe organization", endpoint: europeOrg },
    ].map(({ name, endpoint }) => ({
      name: `Bazel uses the ${name} endpoint for all services`,
      role: "",
      options: { remote_executor: endpoint, remote_cache: endpoint, bes_backend: endpoint },
      execution: endpoint,
      cache: endpoint,
      bes: endpoint,
    })),
    {
      name: "Bazel executes in Europe with a global cache and build event service",
      role: "",
      options: { remote_executor: europeOrg, remote_cache: globalOrg, bes_backend: globalOrg },
      execution: europeOrg,
      cache: globalOrg,
      bes: globalOrg,
    },
    {
      name: "executor-only Bazel build uses the Europe public endpoint for its cache",
      role: "",
      options: { remote_executor: europePublic },
      execution: europePublic,
      cache: europePublic,
      bes: "",
    },
    {
      name: "workflow runner uses its Europe backend options for all services",
      role: "CI_RUNNER",
      options: { rbe_backend: europePublic, cache_backend: europePublic, bes_backend: europePublic },
      execution: europePublic,
      cache: europePublic,
      bes: europePublic,
    },
    {
      name: "compatibility: workflow runner ignores recorded Bazel flags for its own cache",
      role: "CI_RUNNER",
      options: {
        rbe_backend: europeOrg,
        cache_backend: europeOrg,
        remote_cache: globalOrg,
        remote_executor: globalOrg,
        bes_backend: europeOrg,
      },
      execution: europeOrg,
      cache: europeOrg,
      bes: europeOrg,
    },
    {
      name: "compatibility: hosted runner ignores recorded Bazel flags for its own cache",
      role: "HOSTED_BAZEL",
      options: { rbe_backend: europePublic, remote_cache: globalPublic, remote_executor: globalPublic },
      execution: europePublic,
      cache: europePublic,
      bes: "",
    },
    {
      name: "hosted runner executes in Europe with a global cache backend",
      role: "HOSTED_BAZEL",
      options: { rbe_backend: europeOrg, cache_backend: globalOrg, bes_backend: europeOrg },
      execution: europeOrg,
      cache: globalOrg,
      bes: europeOrg,
    },
    {
      name: "runner backend supplies the global public cache fallback",
      role: "CI_RUNNER",
      options: { rbe_backend: globalPublic },
      execution: globalPublic,
      cache: globalPublic,
      bes: "",
    },
    {
      name: "no configured endpoints",
      role: "",
      options: {},
      execution: "",
      cache: "",
      bes: "",
    },
  ];

  for (const testCase of cases) {
    it(testCase.name, () => {
      const model = new InvocationModel(new invocation.Invocation({ role: testCase.role }));
      for (const [name, endpoint] of Object.entries(testCase.options)) {
        model.optionsMap.set(name, endpoint);
      }
      expect(model.getRemoteExecutorEndpoint()).toBe(testCase.execution);
      expect(model.getCacheEndpoint()).toBe(testCase.cache);
      expect(model.getBESBackendEndpoint()).toBe(testCase.bes);
    });
  }

  it("reads full execution responses from the executor even with a separate cache and artifact prefix", () => {
    const model = new InvocationModel(new invocation.Invocation());
    model.optionsMap.set("remote_executor", europeOrg);
    model.optionsMap.set("remote_cache", globalOrg);
    model.optionsMap.set("remote_bytestream_uri_prefix", "remote.buildbuddy.io/prefix");
    const digest = { hash: "abc", sizeBytes: Long.fromNumber(3) };

    expect(model.getActionCacheURL(digest)).toContain("actioncache://remote.buildbuddy.io/");
    expect(model.getExecuteResponseURL(digest)).toContain("actioncache://test-org.europe.buildbuddy.io/");
    expect(model.getCacheEndpoint()).toBe(globalOrg);
  });
});
