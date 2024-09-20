import { git } from "../../proto/git_ts_proto";
import InvocationModel from "../invocation/invocation_model";
import { runner } from "../../proto/runner_ts_proto";
import rpcService from "../service/rpc_service";
import { github } from "../../proto/github_ts_proto";
import error_service from "../errors/error_service";

export async function supportsRemoteRun(repoUrl: string): Promise<boolean> {
  const rsp = await rpcService.service.getLinkedGitHubRepos(new github.GetLinkedReposRequest());
  return rsp.repoUrls.filter((url) => url === repoUrl).length > 0;
}

export function triggerRemoteRun(invocationModel: InvocationModel, command: string, autoOpenChild: boolean) {
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
