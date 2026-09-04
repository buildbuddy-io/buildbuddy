import React from "react";
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
});
