import { build } from "../../proto/remote_execution_ts_proto";

export const REDACTED_PLACEHOLDER = "<REDACTED>";

export const DEFAULT_ALLOWED_ENV_VARS = [
  "USER",
  "GITHUB_ACTOR",
  "GITLAB_USER_NAME",
  "BUILDKITE_BUILD_CREATOR",
  "CIRCLE_USERNAME",
  "GITHUB_REPOSITORY",
  "GITHUB_SHA",
  "GITHUB_RUN_ID",
  "BUILDKITE_BUILD_URL",
  "BUILDKITE_JOB_ID",
  "CIRCLE_REPOSITORY_URL",
  "BUILDKITE_REPO",
  "TRAVIS_REPO_SLUG",
  "GIT_REPOSITORY_URL",
  "GIT_URL",
  "CI_REPOSITORY_URL",
  "REPO_URL",
  "CIRCLE_SHA1",
  "BUILDKITE_COMMIT",
  "TRAVIS_COMMIT",
  "BITRISE_GIT_COMMIT",
  "GIT_COMMIT",
  "CI_COMMIT_SHA",
  "COMMIT_SHA",
  "CI",
  "CI_RUNNER",
  "CIRCLE_BRANCH",
  "GITHUB_HEAD_REF",
  "BUILDKITE_BRANCH",
  "BITRISE_GIT_BRANCH",
  "CI_MERGE_REQUEST_SOURCE_BRANCH_NAME",
  "TRAVIS_BRANCH",
  "GIT_BRANCH",
  "CI_COMMIT_BRANCH",
  "GITHUB_REF",
];

export function parseAllowEnvMetadata(metadata: string | undefined): string[] {
  if (!metadata) return [];
  return metadata
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function normalizeAllowList(
  allowEnvMetadata: string | undefined,
  defaultAllowedEnvVars: readonly string[]
): string[] {
  const allowed = new Set<string>();
  for (const envVar of defaultAllowedEnvVars) {
    allowed.add(envVar.toLowerCase());
  }
  for (const envVar of parseAllowEnvMetadata(allowEnvMetadata)) {
    allowed.add(envVar.toLowerCase());
  }
  return Array.from(allowed);
}

function isEnvVarAllowed(variableName: string, allowedLowercase: string[]): boolean {
  const lowercaseName = variableName.toLowerCase();
  for (const entry of allowedLowercase) {
    if (entry === "*") {
      return true;
    }
    if (entry.endsWith("*")) {
      const prefix = entry.slice(0, -1);
      if (lowercaseName.startsWith(prefix)) {
        return true;
      }
      continue;
    }
    if (lowercaseName === entry) {
      return true;
    }
  }
  return false;
}

export function redactCommand(
  command: build.bazel.remote.execution.v2.Command,
  options: {
    allowEnvMetadata?: string;
    defaultAllowedEnvVars?: readonly string[];
  } = {}
): build.bazel.remote.execution.v2.Command {
  const { allowEnvMetadata, defaultAllowedEnvVars = DEFAULT_ALLOWED_ENV_VARS } = options;
  const allowedLowercase = normalizeAllowList(allowEnvMetadata, defaultAllowedEnvVars);
  const redactedCommand = build.bazel.remote.execution.v2.Command.create(command);
  redactedCommand.environmentVariables = (command.environmentVariables ?? []).map((variable) => {
    const clonedVariable =
      build.bazel.remote.execution.v2.Command.EnvironmentVariable.create(variable ?? {});
    const name = clonedVariable.name ?? "";
    const value = clonedVariable.value ?? "";
    if (!name || value === "" || value === REDACTED_PLACEHOLDER) {
      return clonedVariable;
    }
    if (isEnvVarAllowed(name, allowedLowercase)) {
      return clonedVariable;
    }
    clonedVariable.value = REDACTED_PLACEHOLDER;
    return clonedVariable;
  });
  return redactedCommand;
}
