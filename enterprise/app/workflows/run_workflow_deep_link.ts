import { normalizeRepoURL } from "../../../app/util/git";

const RUN_WORKFLOW_PATH = "/workflows/run";

export type EnvVar = {
  name: string;
  value: string;
};

// Values parsed from a run-workflow link, used to pre-fill the run workflow form.
export type RunWorkflowParams = {
  repoUrl: string;
  actionName: string;
  branch: string;
  commit: string;
  env: EnvVar[];
};

// Links may only select from a fixed set of env var presets, rather than
// setting arbitrary env vars.
const ENV_PRESETS: Record<string, EnvVar[]> = {
  AGENT_REVIEW: [{ name: "AGENT_REVIEW_FORCE", value: "1" }],
};

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

function parseEnvPreset(search: URLSearchParams): EnvVar[] {
  const preset = search.get("env_preset")?.trim() ?? "";
  if (!preset) return [];
  if (!Object.prototype.hasOwnProperty.call(ENV_PRESETS, preset)) {
    throw new Error(`invalid env_preset parameter`);
  }
  return ENV_PRESETS[preset].map((envVar) => ({ ...envVar }));
}

export function parseRunRequestFromURL(path: string, search: URLSearchParams): RunWorkflowParams | undefined {
  if (path !== RUN_WORKFLOW_PATH && path !== `${RUN_WORKFLOW_PATH}/`) return undefined;

  const repoUrl = validateRepositoryURL(requiredParameter(search, "repo_url"));
  const actionName = requiredParameter(search, "action_name");
  const branch = search.get("branch")?.trim() ?? "";
  const commit = search.get("commit")?.trim() ?? "";

  if (!branch && !commit) {
    throw new Error("At least one of branch or commit must be set");
  }
  const env = parseEnvPreset(search);

  return { repoUrl, actionName, branch, commit, env };
}
