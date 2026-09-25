import { git } from "../../proto/git_ts_proto";
import { github } from "../../proto/github_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { runner } from "../../proto/runner_ts_proto";
import error_service from "../errors/error_service";
import InvocationModel from "../invocation/invocation_model";
import rpcService from "../service/rpc_service";

const DEFAULT_CONTAINER_IMAGE = "docker://gcr.io/flame-public/rbe-ubuntu24-04:latest";

export const REMOTE_RUNNER_AGENTS = [
  { id: "claude", name: "Claude", apiKeyEnvVar: "ANTHROPIC_API_KEY" },
  { id: "codex", name: "Codex", apiKeyEnvVar: "CODEX_API_KEY" },
] as const;

export type RemoteRunnerAgent = (typeof REMOTE_RUNNER_AGENTS)[number]["id"];
export const DEFAULT_REMOTE_RUNNER_AGENT: RemoteRunnerAgent = "codex";

export function getRemoteRunnerAgentConfig(agent: RemoteRunnerAgent) {
  return REMOTE_RUNNER_AGENTS.find((config) => config.id === agent)!;
}

// These commands run in the user's repository, where our setup scripts are not available.
// Keep them in sync with enterprise/tools/agents/{claude,codex}/setup.sh.
const SETUP_CLAUDE_COMMAND = `
set -euo pipefail

if [[ -z "\${ANTHROPIC_API_KEY:-}" ]]; then
  echo "ERROR: Add ANTHROPIC_API_KEY as a BuildBuddy secret to use Claude." >&2
  exit 1
fi

case "\${AGENT_AUTO_UPDATE:-true}" in
  true | 1) AUTO_UPDATE=1 ;;
  false | 0) AUTO_UPDATE=0 ;;
  *)
    echo "Error: AGENT_AUTO_UPDATE must be true, false, 1, or 0." >&2
    exit 1
    ;;
esac
AUTO_UPDATE_CHECK_INTERVAL="\${AGENT_AUTO_UPDATE_CHECK_INTERVAL:-86400}"
if [[ ! "$AUTO_UPDATE_CHECK_INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "Error: AGENT_AUTO_UPDATE_CHECK_INTERVAL must be a number of seconds." >&2
  exit 1
fi

INSTALL_STAMP="$HOME/.cache/claude-setup/last-install"

if command -v claude &>/dev/null; then
  if [[ "$AUTO_UPDATE" == 0 ]]; then
    echo "==> claude already installed: $(command -v claude)" >&2
    exit 0
  fi
  # A missing or corrupt stamp counts as stale, so the install below rewrites it.
  last_install="$(cat "$INSTALL_STAMP" 2>/dev/null || true)"
  if [[ "$last_install" =~ ^[0-9]+$ ]] && (( $(date +%s) - last_install < AUTO_UPDATE_CHECK_INTERVAL )); then
    echo "==> claude already installed and updated within the last \${AUTO_UPDATE_CHECK_INTERVAL}s: $(command -v claude)" >&2
    exit 0
  fi
fi

echo "==> Installing latest Claude Code..." >&2
curl -fsSL https://claude.ai/install.sh | bash

# Check for the installer's output rather than using command -v, since during
# an update the previously installed binary is still on PATH.
if [[ ! -f "$HOME/.local/bin/claude" ]]; then
  echo "Error: Claude Code installation failed: $HOME/.local/bin/claude not found." >&2
  exit 1
fi

# Move to a directory already on PATH so callers don't need to modify PATH.
sudo mv "$HOME/.local/bin/claude" /usr/local/bin/claude

mkdir -p "$(dirname "$INSTALL_STAMP")"
date +%s > "$INSTALL_STAMP"

echo "==> Claude Code installed: $(command -v claude) ($(claude --version))" >&2
`;

const SETUP_CODEX_COMMAND = `
set -euo pipefail

if [[ -z "\${CODEX_API_KEY:-}" ]]; then
  echo "ERROR: Add CODEX_API_KEY as a BuildBuddy secret to use Codex." >&2
  exit 1
fi

case "\${AGENT_AUTO_UPDATE:-true}" in
  true | 1) AUTO_UPDATE=1 ;;
  false | 0) AUTO_UPDATE=0 ;;
  *)
    echo "Error: AGENT_AUTO_UPDATE must be true, false, 1, or 0." >&2
    exit 1
    ;;
esac
AUTO_UPDATE_CHECK_INTERVAL="\${AGENT_AUTO_UPDATE_CHECK_INTERVAL:-86400}"
if [[ ! "$AUTO_UPDATE_CHECK_INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "Error: AGENT_AUTO_UPDATE_CHECK_INTERVAL must be a number of seconds." >&2
  exit 1
fi

INSTALL_STAMP="$HOME/.cache/codex-setup/last-install"

if command -v codex &>/dev/null; then
  if [[ "$AUTO_UPDATE" == 0 ]]; then
    echo "==> codex already installed: $(command -v codex)" >&2
    exit 0
  fi
  # A missing or corrupt stamp counts as stale, so the install below rewrites it.
  last_install="$(cat "$INSTALL_STAMP" 2>/dev/null || true)"
  if [[ "$last_install" =~ ^[0-9]+$ ]] && (( $(date +%s) - last_install < AUTO_UPDATE_CHECK_INTERVAL )); then
    echo "==> codex already installed and updated within the last \${AUTO_UPDATE_CHECK_INTERVAL}s: $(command -v codex)" >&2
    exit 0
  fi
fi

echo "==> Installing latest Codex..." >&2
curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh

# Check for the installer's output rather than using command -v, since during
# an update the previously installed binary is still on PATH.
if [[ ! -f "$HOME/.local/bin/codex" ]]; then
  echo "Error: Codex installation failed: $HOME/.local/bin/codex not found." >&2
  exit 1
fi

# Move to a directory already on PATH so callers don't need to modify PATH.
sudo mv "$HOME/.local/bin/codex" /usr/local/bin/codex

mkdir -p "$(dirname "$INSTALL_STAMP")"
date +%s > "$INSTALL_STAMP"

echo "==> Codex installed: $(command -v codex) ($(codex --version))" >&2
`;

const SETUP_AGENT_COMMANDS: Record<RemoteRunnerAgent, string> = {
  claude: SETUP_CLAUDE_COMMAND,
  codex: SETUP_CODEX_COMMAND,
};

export async function supportsRemoteRun(repoUrl: string): Promise<boolean> {
  const rsp = await rpcService.service.getLinkedGitHubRepos(new github.GetLinkedReposRequest());
  return rsp.repos.some((repo) => repo.repoUrl === repoUrl);
}

export function triggerRemoteRun(
  invocationModel: InvocationModel,
  command: string,
  autoOpenChild: boolean,
  platformProps: Map<string, string> | null,
  runnerFlags: string[],
  name: string,
  agent?: RemoteRunnerAgent
) {
  command = command.replaceAll(/--[a-zA-Z_]+='\<REDACTED\>'/g, "");
  let execProps: build.bazel.remote.execution.v2.Platform.Property[] = [];

  if (!platformProps) {
    platformProps = new Map<string, string>();
  }

  if (!platformProps.has("container-image") && platformProps.get("OSFamily") !== "darwin") {
    platformProps.set("container-image", DEFAULT_CONTAINER_IMAGE);
  }

  for (let [key, value] of platformProps) {
    execProps.push(
      new build.bazel.remote.execution.v2.Platform.Property({
        name: key,
        value: value,
      })
    );
  }

  const request = new runner.RunRequest({
    gitRepo: new git.GitRepo({
      repoUrl: invocationModel.getRepo(),
    }),
    repoState: new git.RepoState({
      commitSha: invocationModel.getCommit(),
      branch: invocationModel.getBranchName(),
    }),
    steps: [
      ...(agent
        ? [
            new runner.Step({
              run: SETUP_AGENT_COMMANDS[agent],
            }),
          ]
        : []),
      new runner.Step({
        run: command,
      }),
    ],
    async: true,
    runRemotely: true,
    // In order to increase the odds of hitting a warm snapshot, set the two
    // most common default branch names as fallback keys (as a simplification
    // over fetching the actual default branch name).
    env: {
      GIT_REPO_DEFAULT_BRANCH: "master",
      GIT_BASE_BRANCH: "main",
    },
    execProperties: execProps,
    runnerFlags: runnerFlags,
    name: name,
  });

  return rpcService.service
    .run(request)
    .then((response: runner.RunResponse) => {
      let url = `/invocation/${response.invocationId}?queued=true`;
      if (autoOpenChild) {
        url += "&openChild=true";
      }
      window.open(url, "_blank");
    })
    .catch((error) => {
      error_service.handleError(error);
    });
}

// commandWithRemoteRunnerFlags adds useful flags to bazel commands run on a remote runner
export function commandWithRemoteRunnerFlags(command: string): string {
  // The buildbuddy_ configs point to the corresponding env that created the remote runner
  // action (Ex. If it was created in dev, points to the dev app).
  // These configs are defined in a .bazelrc the ci_runner creates in ci_runner/main.go
  const addlFlags =
    "--remote_cache_compression --config=buildbuddy_bes_backend --config=buildbuddy_bes_results_url --config=buildbuddy_remote_cache";
  command = appendBazelSubCommandArgs(command, addlFlags);
  return command;
}

// appendBazelSubcommandArgs appends bazel arguments to a bazel command
// *before* the arg separator ("--") if it exists, so that the arguments apply
// to the bazel subcommand ("build", "run", etc.) and not the binary being run
// (in the "bazel run" case).
function appendBazelSubCommandArgs(cmd: string, args: string): string {
  if (cmd == "") {
    return "";
  }
  const splitCmd = cmd.split(" -- ", 2);
  splitCmd[0] += " " + args + " ";
  return splitCmd.join(" -- ");
}
