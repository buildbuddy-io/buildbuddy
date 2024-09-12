import { git } from "../../proto/git_ts_proto";
import InvocationModel from "../invocation/invocation_model";
import { runner } from "../../proto/runner_ts_proto";
import rpcService from "../service/rpc_service";
import { github } from "../../proto/github_ts_proto";

export async function supportsRemoteRun(repoUrl: string): Promise<boolean> {
  const rsp = await rpcService.service.getLinkedGitHubRepos(new github.GetLinkedReposRequest());
  return rsp.repoUrls.filter((url) => url === repoUrl).length > 0;
}

export function triggerRemoteRun(invocationModel: InvocationModel, command: string, autoOpenChild: boolean) {
  let request = new runner.RunRequest();
  request.gitRepo = new git.GitRepo();
  request.gitRepo.repoUrl = invocationModel.getRepo();
  let step = new runner.Step();
  step.run = command;
  request.steps = [step];
  let state = new git.RepoState();
  state.commitSha = invocationModel.getCommit();
  state.branch = invocationModel.getBranchName();
  request.repoState = state;
  request.async = true;
  request.runRemotely = true;

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
      alert(error);
    });
}
