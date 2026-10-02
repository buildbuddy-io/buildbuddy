import { parseEnvironmentVariablesInput, parseRunRequestFromURL } from "./run_workflow_deep_link";

describe("parseEnvironmentVariablesInput", () => {
  it("preserves commas in environment variable values", () => {
    expect(parseEnvironmentVariablesInput("LIST=a,b, MODE=thorough")).toEqual({
      LIST: "a,b",
      MODE: "thorough",
    });
  });

  it("allows empty values but rejects empty names", () => {
    expect(parseEnvironmentVariablesInput("KEY=")).toEqual({ KEY: "" });
    expect(() => parseEnvironmentVariablesInput(" =hello")).toThrowError(/names must not be empty/);
  });
});

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

    expect(result?.pushedRepoUrl).toBe("https://github.com/buildbuddy-io/buildbuddy");
    expect(result?.targetRepoUrl).toBe("https://github.com/buildbuddy-io/buildbuddy");
    expect(result?.actionNames).toEqual(["Code review"]);
    expect(result?.pushedBranch).toBe("feature/review");
    expect(result?.targetBranch).toBe("feature/review");
    expect(result?.commitSha).toBe("0123456789abcdef0123456789abcdef01234567");
    expect(result?.env).toEqual({ AGENT_REVIEW_FORCE: "1" });
    expect(result?.async).toBeTrue();
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
    expect(result?.env).toEqual({});
  });
});
