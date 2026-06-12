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
});
