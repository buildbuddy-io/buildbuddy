import { git } from "../../proto/git_ts_proto";
import InvocationModel from "../invocation/invocation_model";
import { runner } from "../../proto/runner_ts_proto";
import rpcService from "../service/rpc_service";
import { github } from "../../proto/github_ts_proto";
import error_service from "../errors/error_service";
import { build } from "../../proto/remote_execution_ts_proto";

export async function supportsRemoteRun(repoUrl: string): Promise<boolean> {
  const rsp = await rpcService.service.getLinkedGitHubRepos(new github.GetLinkedReposRequest());
  return rsp.repoUrls.filter((url) => url === repoUrl).length > 0;
}

export function triggerRemoteRun(
  invocationModel: InvocationModel,
  command: string,
  autoOpenChild: boolean,
  platformProps: Map<string, string> | null
) {
  command = command.replaceAll(/--[a-zA-Z_]+='\<REDACTED\>'/g, "");
  let execProps: build.bazel.remote.execution.v2.Platform.Property[] = [];
  if (platformProps) {
    for (let [key, value] of platformProps) {
      execProps.push(
        new build.bazel.remote.execution.v2.Platform.Property({
          name: key,
          value: value,
        })
      );
    }
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
  });

  rpcService.service
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
