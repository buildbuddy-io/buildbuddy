import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { User } from "../../../app/auth/user";
import errorService from "../../../app/errors/error_service";
import alertService from "../../../app/alert/alert_service";
import router from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { BuildBuddyError } from "../../../app/util/errors";
import { workflow } from "../../../proto/workflow_ts_proto";
import { AlertCircle, ArrowRight, Check, ChevronsDown } from "lucide-react";
import Link, { TextLink } from "../../../app/components/link/link";
import capabilities from "../../../app/capabilities/capabilities";
import { github } from "../../../proto/github_ts_proto";
import Long from "long";
import LinkButton from "../../../app/components/button/link_button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Banner from "../../../app/components/banner/banner";
import Select, { Option } from "../../../app/components/select/select";

type GitHubRepoPickerProps = {
  user: User;
  search: URLSearchParams;
};

type State = {
  reposResponse?: workflow.IGetReposResponse;
  repoListLimit: number;
  workflowsResponse?: workflow.IGetWorkflowsResponse;
  error?: BuildBuddyError;

  selectedOwner?: string;
  searchQuery?: string;
  searchLoading?: boolean;
  searchResponse?: github.SearchReposResponse;

  linkingRepoURL?: string;
};

const REPO_LIST_DEFAULT_LIMIT = 10;
const REPO_LIST_SHOW_MORE_INCREMENT = 50;
const SEARCH_DEBOUNCE_DURATION_MS = 250;

export default class GitHubImport extends React.Component<GitHubRepoPickerProps, State> {
  state: State = {
    repoListLimit: REPO_LIST_DEFAULT_LIMIT,
  };

  componentDidMount() {
    document.title = "Link GitHub repo | BuildBuddy";

    // If the user just installed the app, GitHub would have redirected back
    // here with an installation code in the URL. Handle that now by just
    // retrying the installation, but disallow further redirects.
    if (this.props.search.has("installation_id")) {
      console.debug("Redirected from GitHub with redirect_uri:", window.location.href);
      this.linkWithAppFlow(this.props.search.get("repo_url") || "", this.props.search.get("installation_id")!).finally(
        () => {
          router.replaceParams({});
          this.fetch();
        }
      );
    } else {
      this.fetch();
    }
  }

  componentDidUpdate(prevProps: GitHubRepoPickerProps, prevState: State) {
    if (this.state.selectedOwner !== prevState.selectedOwner) this.fetch();
  }

  private selectedOwner(): string {
    return this.state.selectedOwner || this.state.reposResponse?.owners?.[0] || "";
  }

  private fetch() {
    this.setState({ reposResponse: undefined });
    // Fetch workflows and GitHub repos so we know which repos already have workflows
    // created for them.
    rpcService.service
      .getRepos(new workflow.GetReposRequest({ gitProvider: workflow.GitProvider.GITHUB, owner: this.selectedOwner() }))
      .then((reposResponse) => {
        this.setState({ reposResponse });
      })
      .catch((error) => this.setState({ error: BuildBuddyError.parse(error) }));
    rpcService.service
      .getWorkflows(new workflow.GetWorkflowsRequest())
      .then((workflowsResponse) => this.setState({ workflowsResponse }))
      .catch((error) => this.setState({ error: BuildBuddyError.parse(error) }));
  }

  private onChangeOwner(e: React.ChangeEvent<HTMLSelectElement>) {
    const selectedOwner = e.target.value;
    this.setState({ selectedOwner, searchQuery: "" });
  }

  private linkWithAppFlow(repoUrl: string, installationId = ""): Promise<void> {
    this.setState({ linkingRepoURL: repoUrl });
    return rpcService.service
      .installGitHubApp(
        github.InstallGitHubAppRequest.create({
          repoUrl,
          installationId: Long.fromString(installationId || "0"),
        })
      )
      .then(() => {
        if (repoUrl) {
          alertService.success("Repository linked successfully");
          router.navigateToWorkflows();
        } else {
          alertService.success("GitHub app installation linked successfully");
        }
      })
      .catch((e) => {
        // Handle NotFound by directing to the app installation flow (first-time
        // setup).
        const error = BuildBuddyError.parse(e);
        if (error.code === "NotFound" && !installationId) {
          const redirect = new URL(window.location.href);
          redirect.searchParams.set("repo_url", repoUrl);

          window.location.href = `/auth/github/app/install?${new URLSearchParams({
            redirect_url: String(redirect),
          })}`;
          return;
        }

        errorService.handleError(e);
        throw e;
      })
      .finally(() => this.setState({ linkingRepoURL: undefined }));
  }

  private searchTimeout?: number;
  private onChangeSearch(e: React.ChangeEvent<HTMLInputElement>) {
    const searchQuery = e.target.value;
    this.setState({
      searchQuery,
      searchLoading: Boolean(e.target.value),
    });
    clearTimeout(this.searchTimeout);
    this.searchTimeout = setTimeout(() => this.search(), SEARCH_DEBOUNCE_DURATION_MS);
  }
  private search() {
    const query = this.state.searchQuery;
    if (!query) return;

    rpcService.service
      .searchGitHubRepos(github.SearchReposRequest.create({ query, owner: this.selectedOwner() }))
      .then((searchResponse) => {
        if (query !== this.state.searchQuery) return;
        this.setState({ searchResponse, repoListLimit: REPO_LIST_DEFAULT_LIMIT });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ searchLoading: false }));
  }

  private onClickLinkRepo(url: string) {
    if (capabilities.config.githubAppEnabled) {
      this.linkWithAppFlow(url);
      return;
    }

    const createRequest = new workflow.CreateWorkflowRequest({
      gitRepo: new workflow.CreateWorkflowRequest.GitRepo({ repoUrl: url }),
    });
    this.setState({ linkingRepoURL: url });
    rpcService.service
      .createWorkflow(createRequest)
      .then(() => {
        alertService.success("Repo linked successfully");
        router.navigateToWorkflows();
      })
      .catch((error) => {
        this.setState({ linkingRepoURL: undefined });
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

  private getLinkedRepos(): Map<string, workflow.GetWorkflowsResponse.Workflow> {
    return new Map(this.state.workflowsResponse?.workflow?.map((workflow) => [workflow.repoUrl, workflow]));
  }

  private githubAppSetupUrl(): string {
    // TODO: Have the install endpoint also handle OAuth so that we don't need 2 redirects here.
    const installUrl = `/auth/github/app/install/?${new URLSearchParams({
      redirect_url: window.location.href,
    })}`;
    const signInThenInstallUrl = `/auth/github/app/link/?${new URLSearchParams({
      user_id: this.props.user.displayUser?.userId?.id || "",
      redirect_url: installUrl,
    })}`;
    return signInThenInstallUrl;
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

    const repos =
      (this.state.searchQuery
        ? this.state.searchLoading
          ? []
          : this.state.searchResponse?.repos
        : this.state.reposResponse.repo) || [];
    const alreadyLinkedRepos = this.getLinkedRepos();
    const isLinking = Boolean(this.state.linkingRepoURL);
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
          {capabilities.config.githubAppEnabled && Boolean(this.state.reposResponse?.owners?.length) && (
            <div className="repo-search">
              <Select value={this.selectedOwner()} onChange={this.onChangeOwner.bind(this)} className="owner-picker">
                {this.state.reposResponse?.owners?.map((owner, index) => (
                  <Option key={index} value={owner}>
                    {owner}
                  </Option>
                ))}
              </Select>
              <FilterInput value={this.state.searchQuery} onChange={this.onChangeSearch.bind(this)} />
            </div>
          )}

          {this.state.searchLoading && <div className="loading search-loading" />}

          {!this.state.searchLoading && (
            <>
              <div className="repo-list">
                {repos?.slice(0, this.state.repoListLimit).map((repo) => {
                  const [owner, repoName] = parseOwnerRepo(repo.url);
                  const linkedRepo = alreadyLinkedRepos.get(repo.url);
                  return (
                    <div className="repo-item">
                      <Link className="owner-repo" href={repo.url}>
                        <span>{owner}</span>
                        <span className="repo-owner-separator">/</span>
                        <span className="repo-name">{repoName}</span>
                      </Link>
                      {linkedRepo ? (
                        <>
                          {capabilities.config.githubAppEnabled && linkedRepo.isLegacyWorkflow ? (
                            <div className="created-indicator" title="Workflow linked using legacy OAuth app">
                              {/* TODO: Provide a "migrate" action here instead of an icon */}
                              <AlertCircle className="icon orange" />
                            </div>
                          ) : (
                            <div className="created-indicator" title="Already added">
                              <Check className="icon green" />
                            </div>
                          )}
                        </>
                      ) : this.state.linkingRepoURL === repo.url ? (
                        <div className="loading create-loading" />
                      ) : (
                        <FilledButton disabled={isLinking} onClick={this.onClickLinkRepo.bind(this, repo.url)}>
                          Link
                        </FilledButton>
                      )}
                    </div>
                  );
                })}
              </div>
              {this.state.repoListLimit < (repos.length || 0) && (
                <div className="show-more-button-container">
                  <OutlinedButton className="show-more-button" onClick={this.onClickShowMore.bind(this)}>
                    <span>Show more repos</span>
                    <ChevronsDown className="show-more-icon" />
                  </OutlinedButton>
                </div>
              )}
              {capabilities.config.githubAppEnabled && !Boolean(repos.length) && (
                <>
                  {/* DO NOT MERGE: improve styling and improve the messaging here a bit */}
                  <Banner type="info">
                    <p>
                      {!this.state.reposResponse?.owners?.length && (
                        // TODO: Initate this setup automatically?
                        <>Install BuildBuddy on GitHub to continue.</>
                      )}
                    </p>
                    <LinkButton href={this.githubAppSetupUrl()}>Install & authorize</LinkButton>
                  </Banner>
                </>
              )}
              {capabilities.config.githubAppEnabled && Boolean(repos.length) && (
                <div className="create-other-container">
                  <a className="create-other clickable" href={this.githubAppSetupUrl()}>
                    Don't see a repo here? Manage GitHub app installations <ArrowRight className="icon" />
                  </a>
                </div>
              )}
              <div className="create-other-container">
                <a
                  className="create-other clickable"
                  href="/workflows/new/custom"
                  onClick={this.onClickAddOther.bind(this)}>
                  Enter repository details manually <ArrowRight className="icon" />
                </a>
              </div>
            </>
          )}
        </div>
      </div>
    );
  }
}

function parseOwnerRepo(url: string): [string, string] {
  return new URL(url).pathname.split("/").slice(1, 3) as [string, string];
}
