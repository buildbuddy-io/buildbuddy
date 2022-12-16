import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import alertService from "../../../app/alert/alert_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { workflow } from "../../../proto/workflow_ts_proto";
import { ArrowRight, Check, ChevronsDown } from "lucide-react";
import { TextLink } from "../../../app/components/link/link";

type GitHubRepoPickerProps = {};

type State = {
  reposResponse?: workflow.IGetReposResponse;
  repoListLimit: number;
  workflowsResponse?: workflow.IGetWorkflowsResponse;
  error?: BuildBuddyError;

  createRequest?: workflow.ICreateWorkflowRequest;
};

const REPO_LIST_DEFAULT_LIMIT = 10;
const REPO_LIST_SHOW_MORE_INCREMENT = 10;

export default class GitHubImport extends React.Component<GitHubRepoPickerProps, State> {
  state: State = {
    repoListLimit: REPO_LIST_DEFAULT_LIMIT,
  };

  componentDidMount() {
    document.title = "Link GitHub repo | BuildBuddy";

    // Fetch workflows and GitHub repos so we know which repos already have workflows
    // created for them.
    rpcService.service
      .getRepos(new workflow.GetReposRequest({ gitProvider: workflow.GitProvider.GITHUB }))
      .then((reposResponse) => this.setState({ reposResponse }))
      .catch((error) => this.setState({ error: BuildBuddyError.parse(error) }));
    rpcService.service
      .getWorkflows(new workflow.GetWorkflowsRequest())
      .then((workflowsResponse) => this.setState({ workflowsResponse }))
      .catch((error) => this.setState({ error: BuildBuddyError.parse(error) }));
  }

  private onClickLinkRepo(url: string) {
    const createRequest = new workflow.CreateWorkflowRequest({
      gitRepo: new workflow.CreateWorkflowRequest.GitRepo({ repoUrl: url }),
    });
    this.setState({ createRequest });
    rpcService.service
      .createWorkflow(createRequest)
      .then(() => {
        alertService.success("Repo linked successfully");
        router.navigateToWorkflows();
      })
      .catch((error) => {
        this.setState({ createRequest: null });
        errorService.handleError(error);
      });
  }

  private onClickShowMore() {
    this.setState({ repoListLimit: this.state.repoListLimit + REPO_LIST_SHOW_MORE_INCREMENT });
  }

  private onClickWorkflowBreadcrumb(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateToWorkflows();
  }

  private onClickAddOther(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateTo("/workflows/new/custom");
  }

  private getImportedRepoUrls(): Set<string> {
    return new Set(this.state.workflowsResponse.workflow.map((workflow) => workflow.repoUrl));
  }

  render() {
    if (this.state.error) {
      return (
        <div className="workflows-github-import">
          <div className="container">
            <div className="card card-failure">
              <div className="content">
                <p>Error: {this.state.error.message}</p>
                {this.state.error.code === "PermissionDenied" && (
                  <p>
                    You may be able to fix this error by{" "}
                    <TextLink href="/settings/org/github">re-linking a GitHub account to your organization</TextLink>.
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>
      );
    }
    if (!this.state.reposResponse || !this.state.workflowsResponse) {
      return <div className="loading"></div>;
    }
    const alreadyCreatedUrls = this.getImportedRepoUrls();
    const isCreating = Boolean(this.state.createRequest);
    return (
      <div className="workflows-github-import">
        <div className="shelf">
          <div className="container">
            <div className="breadcrumbs">
              <span>
                <a href="/workflows/" onClick={this.onClickWorkflowBreadcrumb.bind(this)}>
                  Workflows
                </a>
              </span>
              <span>Link GitHub repo</span>
            </div>
            <div className="title">Link GitHub repo</div>
          </div>
        </div>
        <div className="container content-container">
          <div className="repo-list">
            {this.state.reposResponse.repo.slice(0, this.state.repoListLimit).map((repo) => {
              const [owner, repoName] = parseOwnerRepo(repo.url);
              return (
                <div className="repo-item">
                  <div className="owner-repo">
                    <span>{owner}</span>
                    <span className="repo-owner-separator">/</span>
                    <span className="repo-name">{repoName}</span>
                  </div>
                  {alreadyCreatedUrls.has(repo.url) ? (
                    <div className="created-indicator" title="Already added">
                      <Check className="icon green" />
                    </div>
                  ) : isCreating && this.state.createRequest?.gitRepo?.repoUrl === repo.url ? (
                    <div className="loading create-loading" />
                  ) : (
                    <FilledButton disabled={isCreating} onClick={this.onClickLinkRepo.bind(this, repo.url)}>
                      Link
                    </FilledButton>
                  )}
                </div>
              );
            })}
          </div>
          {this.state.repoListLimit < this.state.reposResponse.repo.length && (
            <div className="show-more-button-container">
              <OutlinedButton className="show-more-button" onClick={this.onClickShowMore.bind(this)}>
                <span>Show more repos</span>
                <ChevronsDown className="show-more-icon" />
              </OutlinedButton>
            </div>
          )}
          <div className="create-other-container">
            <a
              className="create-other clickable"
              href="/workflows/new/custom"
              onClick={this.onClickAddOther.bind(this)}>
              Enter details manually <ArrowRight className="icon" />
            </a>
          </div>
        </div>
      </div>
    );
  }
}

function parseOwnerRepo(url: string): [string, string] {
  return new URL(url).pathname.split("/").slice(1, 3) as [string, string];
}
