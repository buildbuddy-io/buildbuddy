import { normalizeRepoURL } from "../../../app/util/git";
import { workflow } from "../../../proto/workflow_ts_proto";

const RUN_WORKFLOW_PATH = "/workflows/run";

function requiredParameter(search: URLSearchParams, name: string): string {
  const value = search.get(name)?.trim() ?? "";
  if (!value) {
    throw new Error(`Missing required parameter: ${name}`);
  }
  return value;
}

function validateRepositoryURL(value: string): string {
  const normalized = normalizeRepoURL(value);
  try {
    const parsed = new URL(normalized);
    if (!parsed.hostname || parsed.pathname.split("/").filter(Boolean).length < 2) {
      throw new Error();
    }
  } catch {
    throw new Error("Invalid repo_url parameter");
  }
  return normalized;
}

function parseEnvironmentVariables(values: string[]): Record<string, string> {
  const env: Record<string, string> = {};
  for (const assignment of values) {
    const separatorIndex = assignment.indexOf("=");
    if (separatorIndex < 0) {
      throw new Error("Environment variables must use the format NAME=value");
    }
    const name = assignment.slice(0, separatorIndex).trim();
    if (!name) {
      throw new Error("Environment variable names must not be empty");
    }
    env[name] = assignment.slice(separatorIndex + 1).trim();
  }
  return env;
}

export function parseEnvironmentVariablesInput(value: string): Record<string, string> {
  return Object.fromEntries(
    value
      // A comma starts a new assignment only when followed by another variable name and `=`.
      .split(/,(?=\s*[A-Za-z_][A-Za-z0-9_]*\s*=)/)
      .map((assignment) => assignment.trim())
      .filter(Boolean)
      .map((assignment) => {
        const separatorIndex = assignment.indexOf("=");
        if (separatorIndex < 0) {
          throw new Error("Environment variables must use the format NAME=value");
        }
        const name = assignment.slice(0, separatorIndex).trim();
        if (!name) {
          throw new Error("Environment variable names must not be empty");
        }
        return [name, assignment.slice(separatorIndex + 1).trim()];
      })
  );
}

export function parseRunRequestFromURL(
  path: string,
  search: URLSearchParams
): workflow.ExecuteWorkflowRequest | undefined {
  if (path !== RUN_WORKFLOW_PATH && path !== `${RUN_WORKFLOW_PATH}/`) return undefined;

  const repoURL = validateRepositoryURL(requiredParameter(search, "repo_url"));
  const actionName = requiredParameter(search, "action_name");
  const branch = search.get("branch")?.trim() ?? "";
  const commit = search.get("commit")?.trim() ?? "";

  const env = search
    .getAll("env")
    .map((value) => value.trim())
    .filter(Boolean);

  if (!branch && !commit) {
    throw new Error("At least one of branch or commit must be set");
  }
  const parsedEnv = parseEnvironmentVariables(env);

  return new workflow.ExecuteWorkflowRequest({
    pushedRepoUrl: repoURL,
    targetRepoUrl: repoURL,
    pushedBranch: branch,
    targetBranch: branch,
    commitSha: commit,
    actionNames: [actionName],
    env: parsedEnv,
    async: true,
  });
}
