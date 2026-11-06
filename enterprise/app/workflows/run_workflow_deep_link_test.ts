import { parseRunRequestFromURL } from "./run_workflow_deep_link";

describe("parseRunRequestFromURL", () => {
  it("parses a valid run-workflow link", () => {
    const result = parseRunRequestFromURL(
      "/workflows/run",
      new URLSearchParams([
        ["repo_url", "git@github.com:buildbuddy-io/buildbuddy.git"],
        ["action_name", "Code review"],
        ["branch", "feature/review"],
        ["commit", "0123456789abcdef0123456789abcdef01234567"],
        ["env_preset", "AGENT_REVIEW"],
      ])
    );

    expect(result).toEqual({
      repoUrl: "https://github.com/buildbuddy-io/buildbuddy",
      actionName: "Code review",
      branch: "feature/review",
      commit: "0123456789abcdef0123456789abcdef01234567",
      env: [{ name: "AGENT_REVIEW_FORCE", value: "1" }],
    });
  });

  it("ignores the normal workflows route", () => {
    expect(parseRunRequestFromURL("/workflows/", new URLSearchParams())).toBeUndefined();
  });

  it("accepts a trailing slash", () => {
    expect(
      parseRunRequestFromURL(
        "/workflows/run/",
        new URLSearchParams({
          repo_url: "https://github.com/foo/bar",
          action_name: "Review",
          branch: "main",
        })
      )
    ).toBeDefined();
  });

  it("accepts either branch or commit", () => {
    const commonParams = {
      repo_url: "https://github.com/foo/bar",
      action_name: "Review",
    };
    expect(
      parseRunRequestFromURL("/workflows/run", new URLSearchParams({ ...commonParams, branch: "main" }))
    ).toBeDefined();
    expect(
      parseRunRequestFromURL(
        "/workflows/run",
        new URLSearchParams({ ...commonParams, commit: "0123456789abcdef0123456789abcdef01234567" })
      )
    ).toBeDefined();
  });

  it("rejects missing and malformed parameters", () => {
    expect(() => parseRunRequestFromURL("/workflows/run", new URLSearchParams())).toThrowError(/repo_url/);
    expect(() =>
      parseRunRequestFromURL(
        "/workflows/run",
        new URLSearchParams({
          repo_url: "https://github.com/foo/bar",
          action_name: "Review",
        })
      )
    ).toThrowError(/branch or commit/);
    expect(() =>
      parseRunRequestFromURL(
        "/workflows/run",
        new URLSearchParams([
          ["repo_url", "https://github.com/foo/bar"],
          ["action_name", "Review"],
          ["branch", "main"],
          ["env_preset", "UNKNOWN"],
        ])
      )
    ).toThrowError(/env_preset/);
    // Presets must be own properties of the preset map.
    expect(() =>
      parseRunRequestFromURL(
        "/workflows/run",
        new URLSearchParams([
          ["repo_url", "https://github.com/foo/bar"],
          ["action_name", "Review"],
          ["branch", "main"],
          ["env_preset", "toString"],
        ])
      )
    ).toThrowError(/env_preset/);
  });

  it("does not allow arbitrary environment variables", () => {
    const result = parseRunRequestFromURL(
      "/workflows/run",
      new URLSearchParams([
        ["repo_url", "https://github.com/foo/bar"],
        ["action_name", "Review"],
        ["branch", "main"],
        ["env", "MALICIOUS=1"],
      ])
    );
    expect(result?.env).toEqual([]);
  });
});
