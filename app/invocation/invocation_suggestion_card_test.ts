import React from "react";
import { build_event_stream } from "../../proto/build_event_stream_ts_proto";
import { grp } from "../../proto/group_ts_proto";
import { invocation_status } from "../../proto/invocation_status_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/user";
import capabilities from "../capabilities/capabilities";
import { directReactChildren } from "../util/react";
import InvocationModel from "./invocation_model";
import { getSuggestions, SuggestionLevel } from "./invocation_suggestion_card";

function testUser(): User {
  return {
    selectedGroup: new grp.Group({ suggestionPreference: grp.SuggestionPreference.ENABLED }),
  } as User;
}

function suggestionModel(version?: string, options: Record<string, string> = {}, remote = true) {
  const model = new InvocationModel(
    new invocation.Invocation({
      event: version
        ? [
            new invocation.InvocationEvent({
              buildEvent: new build_event_stream.BuildEvent({
                started: new build_event_stream.BuildStarted({ buildToolVersion: version }),
              }),
            }),
          ]
        : [],
    })
  );
  if (remote) model.optionsMap.set("remote_cache", "grpcs://cache.example.com");
  for (const [name, value] of Object.entries(options)) model.optionsMap.set(name, value);
  return model;
}

function suggestionText(suggestions: ReturnType<typeof getSuggestions>) {
  const text = (node: React.ReactNode): string => {
    if (typeof node === "string" || typeof node === "number") return String(node);
    if (Array.isArray(node)) return node.map(text).join("");
    if (!React.isValidElement<{ children?: React.ReactNode; items?: React.ReactNode[] }>(node)) return "";
    return text(node.props.children) + text(node.props.items || []);
  };
  return suggestions.map((suggestion) => text(suggestion.message)).join("\n");
}

function hasSuggestion(suggestions: ReturnType<typeof getSuggestions>, flag: string) {
  return suggestionText(suggestions).includes(flag);
}

describe("getSuggestions", () => {
  let previousExpandedSuggestionsEnabled: boolean;

  beforeEach(() => {
    previousExpandedSuggestionsEnabled = capabilities.config.expandedSuggestionsEnabled;
    capabilities.config.expandedSuggestionsEnabled = true;
  });

  afterEach(() => {
    capabilities.config.expandedSuggestionsEnabled = previousExpandedSuggestionsEnabled;
  });

  it("shows the disconnected suggestion independently of deadline exceeded errors", () => {
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    expect(suggestions.length).toBe(1);
    expect(suggestions[0].level).toBe(SuggestionLevel.ERROR);
    expect(suggestions[0].reason).toBe("Shown because the build finished with a disconnected status.");

    const messageChildren = directReactChildren(suggestions[0].message);
    expect(messageChildren.some((child) => typeof child === "string" && child.includes("Note:"))).toBe(false);
    expect(
      messageChildren.some(
        (child) =>
          React.isValidElement<{ children?: string }>(child) && child.props.children === "--bes_upload_mode=fully_async"
      )
    ).toBe(false);
  });

  it("includes a BES upload mode note for fully async disconnected invocations", () => {
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.optionsMap.set("bes_upload_mode", "fully_async");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    expect(suggestions.length).toBe(1);

    const messageChildren = directReactChildren(suggestions[0].message);
    expect(messageChildren.some((child) => typeof child === "string" && child.includes("Note:"))).toBe(true);
    expect(
      messageChildren.some(
        (child) =>
          React.isValidElement<{ children?: string }>(child) && child.props.children === "--bes_upload_mode=fully_async"
      )
    ).toBe(true);
  });

  it("includes a BES upload mode note for nowait_for_upload_complete disconnected invocations", () => {
    // Simulate a disconnected non-CI build run with bes_upload_mode set to
    // nowait_for_upload_complete.
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.optionsMap.set("bes_upload_mode", "nowait_for_upload_complete");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    // Expect the generic disconnected card with a note mentioning the async
    // upload mode flag.
    expect(suggestions.length).toBe(1);
    expect(suggestions[0].reason).toBe("Shown because the build finished with a disconnected status.");

    const messageChildren = directReactChildren(suggestions[0].message);
    expect(messageChildren.some((child) => typeof child === "string" && child.includes("Note:"))).toBe(true);
    expect(
      messageChildren.some(
        (child) =>
          React.isValidElement<{ children?: string }>(child) &&
          child.props.children === "--bes_upload_mode=nowait_for_upload_complete"
      )
    ).toBe(true);
  });

  it("attributes the disconnect to the async BES upload mode on CI builds", () => {
    // Simulate a disconnected CI build (CI=true) run with a fully async BES
    // upload mode.
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.optionsMap.set("bes_upload_mode", "fully_async");
    model.clientEnvMap.set("CI", "true");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    // Expect the high-confidence card naming the flag as the likely cause,
    // instead of the generic disconnected card.
    expect(suggestions.length).toBe(1);
    expect(suggestions[0].level).toBe(SuggestionLevel.ERROR);

    const messageChildren = directReactChildren(suggestions[0].message);
    expect(
      messageChildren.some((child) => typeof child === "string" && child.includes("CI runners typically kill"))
    ).toBe(true);
    expect(
      messageChildren.some(
        (child) =>
          React.isValidElement<{ children?: string }>(child) && child.props.children === "--bes_upload_mode=fully_async"
      )
    ).toBe(true);

    const reasonChildren = directReactChildren(suggestions[0].reason);
    expect(reasonChildren.some((child) => typeof child === "string" && child.includes("looks like a CI build"))).toBe(
      true
    );
  });

  it("detects Buildkite builds with nowait_for_upload_complete as CI builds", () => {
    // Simulate a disconnected Buildkite build run with bes_upload_mode set to
    // nowait_for_upload_complete. The CI env var isn't visible, but
    // BUILDKITE_BUILD_URL is.
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.optionsMap.set("bes_upload_mode", "nowait_for_upload_complete");
    model.clientEnvMap.set("BUILDKITE_BUILD_URL", "https://buildkite.com/foo/bar/builds/123");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    // Expect the high-confidence card naming the flag as the likely cause.
    expect(suggestions.length).toBe(1);
    const messageChildren = directReactChildren(suggestions[0].message);
    expect(
      messageChildren.some(
        (child) =>
          React.isValidElement<{ children?: string }>(child) &&
          child.props.children === "--bes_upload_mode=nowait_for_upload_complete"
      )
    ).toBe(true);
  });

  it("does not treat unrelated CI-provider environment variables as CI evidence", () => {
    // Simulate a disconnected local build run with a fully async BES upload
    // mode. GITHUB_TOKEN is set in the local environment (with its value
    // redacted by the server), but no definitive CI variables are set.
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.optionsMap.set("bes_upload_mode", "fully_async");
    model.clientEnvMap.set("GITHUB_TOKEN", "<REDACTED>");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    // Expect the generic disconnected card (with the fully async note), not
    // the CI-specific card.
    expect(suggestions.length).toBe(1);
    expect(suggestions[0].reason).toBe("Shown because the build finished with a disconnected status.");
  });

  it("shows the generic disconnected card for CI builds without an async BES upload mode", () => {
    // Simulate a disconnected CI build that doesn't set bes_upload_mode.
    const model = new InvocationModel(
      new invocation.Invocation({
        invocationStatus: invocation_status.InvocationStatus.DISCONNECTED_INVOCATION_STATUS,
      })
    );
    model.clientEnvMap.set("CI", "true");

    const suggestions = getSuggestions({
      model,
      buildLogs: "ordinary build logs",
      user: testUser(),
    });

    // Expect the generic disconnected card, since the BES upload mode isn't
    // the likely cause here.
    expect(suggestions.length).toBe(1);
    expect(suggestions[0].reason).toBe("Shown because the build finished with a disconnected status.");
  });

  describe("version-gated remote cache suggestions", () => {
    function getRemoteSuggestions(version?: string, options: Record<string, string> = {}, remote = true) {
      return getSuggestions({
        model: suggestionModel(version, options, remote),
        buildLogs: "ordinary build logs",
        user: testUser(),
      });
    }

    it("uses the diagnostic log flags supported by each Bazel release", () => {
      const cases = [
        { version: "0.12.0", grpc: undefined, compact: undefined },
        { version: "0.13.0", grpc: "--experimental_remote_grpc_log=bazel-remote-grpc.log", compact: undefined },
        { version: "6.1.0", grpc: "--experimental_remote_grpc_log=bazel-remote-grpc.log", compact: undefined },
        { version: "6.2.0", grpc: "--remote_grpc_log=bazel-remote-grpc.log", compact: undefined },
        { version: "7.0.0", grpc: "--remote_grpc_log=bazel-remote-grpc.log", compact: undefined },
        {
          version: "7.1.0",
          grpc: "--remote_grpc_log=bazel-remote-grpc.log",
          compact: "--experimental_execution_log_compact_file=execution_log.binpb.zst",
        },
        {
          version: "7.4.0",
          grpc: "--remote_grpc_log=bazel-remote-grpc.log",
          compact: "--execution_log_compact_file=execution_log.binpb.zst",
        },
      ];

      for (const testCase of cases) {
        const text = suggestionText(getRemoteSuggestions(testCase.version));
        expect(text.includes("--experimental_remote_grpc_log=bazel-remote-grpc.log")).toBe(
          testCase.grpc === "--experimental_remote_grpc_log=bazel-remote-grpc.log"
        );
        expect(text.includes("--remote_grpc_log=bazel-remote-grpc.log")).toBe(
          testCase.grpc === "--remote_grpc_log=bazel-remote-grpc.log"
        );
        expect(text.includes("--experimental_execution_log_compact_file=execution_log.binpb.zst")).toBe(
          testCase.compact === "--experimental_execution_log_compact_file=execution_log.binpb.zst"
        );
        expect(text.includes("--execution_log_compact_file=execution_log.binpb.zst")).toBe(
          testCase.compact === "--execution_log_compact_file=execution_log.binpb.zst"
        );
      }
    });

    it("does not recommend diagnostic logs that are already captured or explicitly configured", () => {
      const suggestions = getRemoteSuggestions("7.4.0", {
        remote_grpc_log: "existing.log",
        execution_log_compact_file: "existing.binpb.zst",
      });

      expect(hasSuggestion(suggestions, "remote_grpc_log=bazel-remote-grpc.log")).toBe(false);
      expect(hasSuggestion(suggestions, "execution_log_compact_file=execution_log.binpb.zst")).toBe(false);
    });

    it("does not suggest gRPC logging for HTTP cache endpoints while retaining the local execution log", () => {
      const suggestions = getRemoteSuggestions("7.4.0", { remote_cache: "https://cache.example.com" });

      expect(hasSuggestion(suggestions, "--remote_grpc_log=bazel-remote-grpc.log")).toBe(false);
      expect(hasSuggestion(suggestions, "--execution_log_compact_file=execution_log.binpb.zst")).toBe(true);
    });

    it("gates lost-input rewinding by its supported releases", () => {
      for (const testCase of [
        { version: "8.6.0", supported: false },
        { version: "8.7.0", supported: true },
        { version: "8.8.0", supported: true },
        { version: "9.0.0", supported: false },
        { version: "9.1.0", supported: true },
        { version: "9.2.0", supported: true },
        { version: "10.0.0", supported: false },
      ]) {
        const suggestions = getRemoteSuggestions(testCase.version);
        expect(hasSuggestion(suggestions, "--rewind_lost_inputs")).toBe(testCase.supported);
      }
    });

    it("recommends chunking for supported Bazel releases using a gRPC cache", () => {
      for (const [version, expected] of [
        ["8.6.0", false],
        ["8.7.0", true],
        ["8.8.0", true],
        ["9.0.0", false],
        ["9.1.0", true],
        ["9.2.0", true],
        ["10.0.0", true],
        ["10.0.0-pre.20251105.2", false],
      ] as const) {
        expect(hasSuggestion(getRemoteSuggestions(version), "--experimental_remote_cache_chunking")).toBe(expected);
      }
    });

    it("does not suggest chunking when already configured or without a gRPC cache", () => {
      for (const value of ["1", "0", "false"]) {
        expect(
          hasSuggestion(
            getRemoteSuggestions("9.2.0", { experimental_remote_cache_chunking: value }),
            "--experimental_remote_cache_chunking"
          )
        ).toBe(false);
      }
      expect(
        hasSuggestion(
          getRemoteSuggestions("9.2.0", { remote_cache: "https://cache.example.com" }),
          "--experimental_remote_cache_chunking"
        )
      ).toBe(false);
      expect(hasSuggestion(getRemoteSuggestions("9.2.0", {}, false), "--experimental_remote_cache_chunking")).toBe(
        false
      );
    });

    it("recognizes explicit boolean values and Bazel 10 rewinding defaults", () => {
      for (const version of ["8.8.0", "9.2.0", "10.0.0"]) {
        for (const value of ["0", "false", "no", "f", "n", "FALSE"]) {
          expect(
            hasSuggestion(getRemoteSuggestions(version, { rewind_lost_inputs: value }), "--rewind_lost_inputs")
          ).toBe(true);
        }
        for (const value of ["1", "true", "yes", "t", "y"]) {
          expect(
            hasSuggestion(getRemoteSuggestions(version, { rewind_lost_inputs: value }), "--rewind_lost_inputs")
          ).toBe(false);
        }
      }
    });

    it("does not infer rewinding support or defaults from rolling versions", () => {
      for (const version of ["10.0.0-pre.20251105.2", "10.0.0-pre.20260818.1"]) {
        expect(
          hasSuggestion(getRemoteSuggestions(version, { rewind_lost_inputs: "false" }), "--rewind_lost_inputs")
        ).toBe(false);
      }
    });

    it("only suggests the concurrent changes guard when its effective enum disables it", () => {
      for (const testCase of [
        { value: "off", expected: true },
        { value: "false", expected: true },
        { value: "true", expected: false },
        { value: "lite", expected: false },
      ]) {
        const suggestions = getRemoteSuggestions("8.3.0", { guard_against_concurrent_changes: testCase.value });
        expect(hasSuggestion(suggestions, "--guard_against_concurrent_changes=lite")).toBe(testCase.expected);
      }
    });

    it("only suggests the concurrent changes guard in Bazel 8.3+ when it is explicitly disabled", () => {
      expect(
        hasSuggestion(
          getRemoteSuggestions("8.2.0", { guard_against_concurrent_changes: "off" }),
          "--guard_against_concurrent_changes=lite"
        )
      ).toBe(false);
      expect(hasSuggestion(getRemoteSuggestions("8.3.0"), "--guard_against_concurrent_changes=lite")).toBe(false);
    });

    it("only recommends remote cache async when Bazel 8+ explicitly disables it", () => {
      const cases: Array<{ version: string; options: Record<string, string>; expected: boolean }> = [
        { version: "7.4.0", options: { remote_cache_async: "0" }, expected: false },
        { version: "8.0.0", options: {}, expected: false },
        { version: "8.0.0", options: { remote_cache_async: "0" }, expected: true },
        { version: "9.2.0", options: { remote_cache_async: "false" }, expected: true },
        { version: "9.2.0", options: { remote_cache_async: "no" }, expected: true },
        { version: "9.2.0", options: { remote_cache_async: "true" }, expected: false },
        { version: "9.2.0", options: { experimental_remote_cache_async: "false" }, expected: true },
        { version: "8.0.0", options: { remote_cache_async: "1" }, expected: false },
        { version: "8.0.0", options: { experimental_remote_cache_async: "0" }, expected: true },
      ];
      for (const testCase of cases) {
        const suggestions = getRemoteSuggestions(testCase.version, testCase.options);
        expect(hasSuggestion(suggestions, "--remote_cache_async")).toBe(testCase.expected);
      }
    });

    it("does not recommend rewinding when it is already enabled", () => {
      expect(hasSuggestion(getRemoteSuggestions("8.7.0", { rewind_lost_inputs: "1" }), "--rewind_lost_inputs")).toBe(
        false
      );
    });

    it("does not recommend compact execution logging when a legacy log option or uploaded execution log exists", () => {
      for (const option of ["execution_log_binary_file", "execution_log_json_file"]) {
        expect(
          hasSuggestion(getRemoteSuggestions("7.4.0", { [option]: "existing.log" }), "execution_log_compact_file")
        ).toBe(false);
      }

      const model = suggestionModel("7.4.0");
      model.buildToolLogs = {
        log: [{ name: "execution_log.binpb.zst", uri: "bytestream://cache.example.com/execution_log.binpb.zst" }],
      } as build_event_stream.BuildToolLogs;
      const suggestions = getSuggestions({ model, buildLogs: "ordinary build logs", user: testUser() });
      expect(hasSuggestion(suggestions, "execution_log_compact_file")).toBe(false);
    });

    it("suppresses remote cache suggestions for an unknown version, local builds, and disabled feature gate", () => {
      const original = capabilities.config.expandedSuggestionsEnabled;
      try {
        expect(hasSuggestion(getRemoteSuggestions(), "--remote_grpc_log")).toBe(false);
        expect(hasSuggestion(getRemoteSuggestions("8.7.0", {}, false), "--rewind_lost_inputs")).toBe(false);

        const nonBazelModel = suggestionModel("8.7.0");
        nonBazelModel.invocation.role = "NINJA";
        expect(
          hasSuggestion(
            getSuggestions({ model: nonBazelModel, buildLogs: "ordinary build logs", user: testUser() }),
            "--rewind_lost_inputs"
          )
        ).toBe(false);

        capabilities.config.expandedSuggestionsEnabled = false;
        expect(hasSuggestion(getRemoteSuggestions("8.7.0"), "--rewind_lost_inputs")).toBe(false);
      } finally {
        capabilities.config.expandedSuggestionsEnabled = original;
      }
    });
  });
});
