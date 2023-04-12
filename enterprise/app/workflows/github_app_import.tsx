import { Check, ChevronsDown, ExternalLink } from "lucide-react";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/user";
import Banner from "../../../app/components/banner/banner";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import LinkButton from "../../../app/components/button/link_button";
import { FilterInput } from "../../../app/components/filter_input/filter_input";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { normalizeRepoURL } from "../../../app/util/git";
import { github } from "../../../proto/github_ts_proto";

type GitHubAppImportProps = {
  user: User;
};

type State = {
  installationsResponse: github.GetAppInstallationsResponse | null;
  installationsLoading: boolean;

  accessibleReposResponse: github.GetAccessibleReposResponse | null;
  accessibleReposLoading: boolean;
  repoListLimit: number;
  searchQuery: string;

  linkedReposLoading: boolean;
  linkedReposResponse: github.IGetLinkedReposResponse | null;

  selectedInstallation: github.AppInstallation | null;

  linkRequest: github.ILinkRepoRequest | null;
  linkLoading: boolean;
};

const SEARCH_DEBOUNCE_DURATION_MS = 250;
const REPO_LIST_DEFAULT_LIMIT = 10;
const REPO_LIST_SHOW_MORE_INCREMENT = 10;

/**
 * Displays a page that lets the user link a GitHub repository to BuildBuddy.
 */
export default class GitHubAppImport extends React.Component<GitHubAppImportProps, State> {
  state: State = {
    installationsLoading: false,
    installationsResponse: null,

    accessibleReposResponse: null,
    accessibleReposLoading: false,

    linkedReposLoading: false,
    linkedReposResponse: null,
    repoListLimit: REPO_LIST_DEFAULT_LIMIT,
    searchQuery: "",

    selectedInstallation: null,

    linkLoading: false,
    linkRequest: null,
  };

  private fetchInstallationsRPC?: CancelablePromise;
  private linkedReposRPC?: CancelablePromise;
  private searchRPC?: CancelablePromise;

  componentDidMount() {
    document.title = "Link GitHub repo | BuildBuddy";
    this.fetch();
  }

  componentDidUpdate(prevProps: GitHubAppImportProps) {
    if (this.props.user !== prevProps.user) this.fetch();
  }

  /** Fetches installations from GitHub as well as BB-linked repos. */
  private fetch() {
    this.fetchInstallationsRPC?.cancel();
    this.setState({
      installationsLoading: false,
      installationsResponse: null,
      linkedReposLoading: false,
      linkedReposResponse: null,
    });
    if (!this.props.user.githubToken) return;

    this.setState({ installationsLoading: true });
    this.fetchInstallationsRPC = rpcService.service
      .getGitHubAppInstallations(new github.GetAppInstallationsRequest())
      .then((response) => {
        this.setState({
          installationsResponse: response,
          selectedInstallation:
            response.installations.find(
              (installation) => installation.owner === this.state.selectedInstallation?.owner
            ) || response.installations[0],
        });
        if (response.installations?.length) this.search();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ installationsLoading: false }));

    this.setState({ linkedReposLoading: true });
    this.linkedReposRPC?.cancel();
    this.linkedReposRPC = rpcService.service
      .getLinkedGitHubRepos(github.GetLinkedReposResponse.create())
      .then((response) => this.setState({ linkedReposResponse: response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ linkedReposLoading: false }));
  }

  private search() {
    this.searchRPC?.cancel();
    this.setState({ accessibleReposLoading: false, accessibleReposResponse: null });
    if (!this.props.user.githubToken) return;

    this.setState({ accessibleReposLoading: true });

    this.searchRPC = rpcService.service
      .getAccessibleGitHubRepos(
        github.GetAccessibleReposRequest.create({
          installationId: this.state.selectedInstallation?.installationId,
          query: this.state.searchQuery,
        })
      )
      .then((response) => this.setState({ accessibleReposResponse: response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ accessibleReposLoading: false }));
  }

  private searchTimeout?: number;
  private onChangeSearch(e: React.ChangeEvent<HTMLInputElement>) {
    const searchQuery = e.target.value;
    this.setState({ searchQuery, accessibleReposLoading: true });
    clearTimeout(this.searchTimeout);
    this.searchTimeout = setTimeout(() => this.search(), SEARCH_DEBOUNCE_DURATION_MS);
  }

  private onChangeOwner(e: React.ChangeEvent<HTMLSelectElement>) {
    const owner = e.target.value;
    const selectedInstallation =
      this.state.installationsResponse?.installations?.find((installation) => installation.owner === owner) || null;
    this.setState({ selectedInstallation, searchQuery: "" }, () => this.search());
  }

  private onClickShowMore() {
    this.setState({ repoListLimit: this.state.repoListLimit + REPO_LIST_SHOW_MORE_INCREMENT });
  }

  private onClickWorkflowBreadcrumb(e: React.MouseEvent) {
    e.preventDefault();
    router.navigateToWorkflows();
  }

  private onClickLinkRepo(repoUrl: string) {
    const linkRequest = github.LinkRepoRequest.create({ repoUrl });
    this.setState({ linkRequest, linkLoading: true });
    rpcService.service
      .linkGitHubRepo(linkRequest)
      .then(() => {
        alertService.success("Successfully linked " + repoUrl);
        router.navigateTo("/workflows/");
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ linkRequest: null, linkLoading: false }));
  }
  private getLinkedRepoUrls(): Set<string> {
    return new Set(this.state.linkedReposResponse?.repoUrls || []);
  }

  private githubLinkURL(): string {
    return `/auth/github/app/link/?${new URLSearchParams({
      user_id: this.props.user?.displayUser?.userId?.id || "",
      group_id: this.props.user?.selectedGroup?.id || "",
      redirect_url: window.location.href,
    })}`;
  }
  private appInstallURL(): string {
    return `/auth/github/app/link/?${new URLSearchParams({
      user_id: this.props.user?.displayUser?.userId?.id || "",
      group_id: this.props.user?.selectedGroup?.id || "",
      redirect_url: window.location.href,
      install: "true",
    })}`;
  }

  private renderRepos() {
    if (!this.state.accessibleReposResponse) return null;

    if (!this.state.accessibleReposResponse.repoUrls?.length) {
      return <Banner type="info">No repos were found.</Banner>;
    }

    const alreadyLinkedUrls = this.getLinkedRepoUrls();
    return (
      <>
        <div className="repo-list">
          {this.state.accessibleReposResponse?.repoUrls?.slice(0, this.state.repoListLimit).map((repoUrl) => {
            const [owner, repoName] = parseOwnerRepo(repoUrl);
            return (
              <div className="repo-item">
                <div className="owner-repo" title={repoUrl}>
                  <span>{owner}</span>
                  <span className="repo-owner-separator">/</span>
                  <span className="repo-name">{repoName}</span>
                </div>
                {alreadyLinkedUrls.has(repoUrl) ? (
                  <div className="created-indicator" title="Already added">
                    <Check className="icon green" />
                  </div>
                ) : this.state.linkRequest?.repoUrl === repoUrl ? (
                  <Spinner />
                ) : (
                  <FilledButton disabled={this.state.linkLoading} onClick={this.onClickLinkRepo.bind(this, repoUrl)}>
                    Link
                  </FilledButton>
                )}
              </div>
            );
          })}
        </div>
        {this.state.repoListLimit < (this.state.accessibleReposResponse?.repoUrls?.length || 0) && (
          <div className="show-more-button-container">
            <OutlinedButton className="show-more-button" onClick={this.onClickShowMore.bind(this)}>
              <span>Show more repos</span>
              <ChevronsDown className="show-more-icon" />
            </OutlinedButton>
          </div>
        )}
        <div className="create-other-container">
          <a className="create-other clickable" href={this.appInstallURL()}>
            Don't see a repo in this list? Configure repo permissions <ExternalLink className="icon" />
          </a>
        </div>
      </>
    );
  }

  render() {
    if (this.state.installationsLoading) {
      return <div className="loading"></div>;
    }
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
          {!this.props.user.githubToken && (
            <Banner type="info" className="install-app-banner">
              <div>To get started, link a GitHub account to your BuildBuddy account.</div>
              <LinkButton className="big-button" href={this.githubLinkURL()}>
                Link GitHub account
              </LinkButton>
            </Banner>
          )}
          {this.props.user.githubToken && !this.state.installationsResponse?.installations?.length && (
            <Banner type="info" className="install-app-banner">
              <div>To link a repository, install the BuildBuddy app on GitHub.</div>
              <LinkButton className="big-button" href={this.appInstallURL()}>
                Install
              </LinkButton>
            </Banner>
          )}
          {Boolean(this.state.installationsResponse?.installations?.length) && (
            <>
              <div className="repo-search">
                <Select
                  value={this.state.selectedInstallation?.owner || ""}
                  onChange={this.onChangeOwner.bind(this)}
                  className="owner-picker">
                  {this.state.installationsResponse?.installations?.map((installation) => (
                    <Option key={installation.owner} value={installation.owner}>
                      {installation.owner}
                    </Option>
                  ))}
                </Select>
                <FilterInput value={this.state.searchQuery} onChange={this.onChangeSearch.bind(this)} />
              </div>
              {this.state.accessibleReposLoading || this.state.linkedReposLoading ? (
                <div className="loading loading-slim repos-loading" />
              ) : (
                this.renderRepos()
              )}
            </>
          )}
        </div>
      </div>
    );
  }
}

function parseOwnerRepo(url: string): [string, string] {
  url = normalizeRepoURL(url);
  return new URL(url).pathname.split("/").slice(1, 3) as [string, string];
}
