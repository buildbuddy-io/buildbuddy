import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { workflow } from "../../../proto/workflow_ts_proto";

type GitHubRepoPickerProps = {};

type State = {
  reposResponse?: workflow.IGetReposResponse;
  repoListLimit: number;
  workflowsResponse?: workflow.IGetWorkflowsResponse;
  error?: BuildBuddyError;

  importRequest?: workflow.ICreateWorkflowRequest;
};

const REPO_LIST_DEFAULT_LIMIT = 6;
const REPO_LIST_SHOW_MORE_INCREMENT = 10;

export default class GitHubImport extends React.Component<GitHubRepoPickerProps, State> {
  state: State = {
    repoListLimit: REPO_LIST_DEFAULT_LIMIT,
  };

  componentDidMount() {
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

  private onClickImport(url: string) {
    const importRequest = new workflow.CreateWorkflowRequest({
      gitRepo: new workflow.CreateWorkflowRequest.GitRepo({ repoUrl: url }),
    });
    this.setState({ importRequest });
    rpcService.service
      .createWorkflow(importRequest)
      .then(() => {
        // TODO: Show "Import succeeded" banner.
        router.navigateToWorkflows();
      })
      .catch((error) => {
        this.setState({ importRequest: null });
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

  private onClickImportOther(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateTo("/workflows/new/custom");
  }

  private getImportedRepoUrls(): Set<string> {
    return new Set(this.state.workflowsResponse.workflow.map((workflow) => workflow.repoUrl));
  }

  render() {
    if (this.state.error) {
      return <div className="error">{this.state.error}</div>;
    }
    if (!this.state.reposResponse || !this.state.workflowsResponse) {
      return <div className="loading"></div>;
    }
    const alreadyImportedUrls = this.getImportedRepoUrls();
    const isImporting = Boolean(this.state.importRequest);
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
              <span>Import GitHub repo</span>
            </div>
            <div className="title">Import GitHub repo</div>
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
                  {alreadyImportedUrls.has(repo.url) ? (
                    <div className="imported-indicator">
                      <img src="/image/check.svg" title="Already imported" alt="" />
                    </div>
                  ) : isImporting && this.state.importRequest?.gitRepo?.repoUrl === repo.url ? (
                    <div className="loading import-loading" />
                  ) : (
                    <FilledButton disabled={isImporting} onClick={this.onClickImport.bind(this, repo.url)}>
                      Import
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
                <img src="/image/chevrons-down.svg" alt="" className="show-more-icon" />
              </OutlinedButton>
            </div>
          )}
          <div className="import-other-container">
            <a
              className="import-other clickable"
              href="/workflows/new/custom"
              onClick={this.onClickImportOther.bind(this)}>
              Import from another Git provider <img src="/image/arrow-right.svg" alt=""></img>
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
