import React from "react";
import error_service from "../../../app/errors/error_service";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import { repo } from "../../../proto/repo_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { ChevronRightSquare, Github, UserIcon } from "lucide-react";
import auth_service from "../../../app/auth/auth_service";
import { workflow } from "../../../proto/workflow_ts_proto";
import Select from "../../../app/components/select/select";
import Checkbox from "../../../app/components/checkbox/checkbox";
import TextInput from "../../../app/components/input/input";
import { encryptAndUpdate } from "../secrets/secret_util";
import { User } from "../../../app/auth/auth_service";
import { secrets } from "../../../proto/secrets_ts_proto";
import router from "../../../app/router/router";

export interface RepoComponentProps {
  path: string;
  search: URLSearchParams;
  user?: User;
}

interface RepoComponentState {
  selectedInstallationIndex: number;
  githubInstallationsLoading: boolean;
  githubInstallationsResponse: github.GetGithubUserInstallationsResponse | null;
  secretsResponse: secrets.ListSecretsResponse | null;
  isCreating: boolean;
  isDeploying: boolean;

  repoName: string;
  template: string;
  private: boolean;
  secrets: Map<string, string>;

  repoResponse: repo.CreateRepoResponse | null;
  workflowResponse: workflow.ExecuteWorkflowResponse | null;
}

const selectedInstallationIndexLocalStorageKey = "repo-selectedInstallationIndex";
const gcpRefreshTokenKey = "CLOUDSDK_AUTH_REFRESH_TOKEN";
export default class RepoComponent extends React.Component<RepoComponentProps, RepoComponentState> {
  state: RepoComponentState = {
    selectedInstallationIndex: localStorage[selectedInstallationIndexLocalStorageKey] || 0,
    githubInstallationsLoading: true,
    githubInstallationsResponse: null,
    secretsResponse: null,
    isCreating: false,
    isDeploying: false,

    template: this.getTemplate(),
    repoName: this.getRepoName(),
    private: true,
    secrets: new Map<string, string>(),

    repoResponse: this.props.search.get("created")
      ? new repo.CreateRepoResponse({ repoUrl: this.props.search.get("created")! })
      : null,
    workflowResponse: null,
  };

  getTemplate() {
    let paramTemplate = this.props.search.get("template");
    if (paramTemplate) {
      return paramTemplate;
    }
    let referrer = document.referrer;
    if (referrer.startsWith("https://github.com/")) {
      return referrer;
    }
    return "";
  }

  getRepoName() {
    let paramRepoName = this.props.search.get("name");
    if (paramRepoName) {
      return paramRepoName;
    }
    let lastTemplatePath = this.getTemplate().replace(/\/$/, "").split("/").pop();
    if (lastTemplatePath) {
      return lastTemplatePath;
    }
    return "";
  }

  fetchGithubInstallations() {
    if (!this.props.user || !this.props.user.githubLinked) {
      this.setState({ githubInstallationsLoading: false });
      return;
    }
    rpc_service.service
      .getGithubUserInstallations(new github.GetGithubUserInstallationsRequest())
      .then((response) => {
        console.log(response);
        this.setState({ githubInstallationsResponse: response });
      })
      .catch((e) => error_service.handleError(e))
      .finally(() => this.setState({ githubInstallationsLoading: false }));
  }

  fetchSecrets() {
    if (!this.props.user) return;
    rpc_service.service.listSecrets(new secrets.ListSecretsRequest()).then((response) => {
      console.log(response);
      this.setState({ secretsResponse: response });
    });
  }

  componentDidMount() {
    this.fetchGithubInstallations();
    this.fetchSecrets();
  }

  handleInstallationPicked(e: React.ChangeEvent<HTMLSelectElement>) {
    if (e.target.value == "-1") {
      window.location.href = `/auth/github/app/link/?${new URLSearchParams({
        group_id: this.props.user?.selectedGroup.id || "",
        user_id: this.props.user?.displayUser.userId?.id || "",
        redirect_url: window.location.href,
        install: "true",
      })}`;
    }
    let index = Number(e.target.value);
    localStorage[selectedInstallationIndexLocalStorageKey] = index;
    this.setState({ selectedInstallationIndex: index });
  }

  hasPermissions() {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    if (!selectedInstallation) {
      return true;
    }
    return (
      selectedInstallation.permissions?.administration == "write" &&
      selectedInstallation.permissions?.repositoryHooks == "write"
    );
  }

  handleRepoChanged(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ repoName: e.target.value });
  }

  handleTemplateChanged(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ template: e.target.value });
  }

  handlePrivateChanged(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ private: e.target.checked });
  }

  linkInstallation() {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    rpc_service.service
      .linkGitHubAppInstallation(
        github.LinkAppInstallationRequest.create({
          installationId: selectedInstallation?.id,
        })
      )
      .catch((e) => error_service.handleError(e));
  }

  createRepo() {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    let r = new repo.CreateRepoRequest();
    r.name = this.state.repoName;
    r.private = this.state.private;
    r.template = this.state.template;

    if (selectedInstallation?.targetType == "Organization") {
      r.installationId = selectedInstallation.id;
      r.organization = selectedInstallation.login;
    }
    return rpc_service.service.createRepo(r);
  }

  runWorkflow(repo: string) {
    return rpc_service.service.executeWorkflow(
      new workflow.ExecuteWorkflowRequest({
        pushedRepoUrl: repo,
        pushedBranch: "main",
        targetRepoUrl: repo,
        targetBranch: "main",
        clean: false,
        visibility: "",
        async: true,
      })
    );
  }

  getSecrets() {
    let s = this.props.search.get("secret") || this.props.search.get("secrets") || "";
    return s ? s.split(",") : [];
  }

  async handleCreateClicked() {
    this.setState({ isCreating: true });
    try {
      await this.linkInstallation();
      let repoResponse = await this.createRepo();
      router.setQueryParam("created", repoResponse.repoUrl);
      router.setQueryParam("name", this.state.repoName);
      this.setState({ repoResponse: repoResponse });
      if (!this.getSecrets().length) {
        this.handleDeployClicked(repoResponse);
      }
    } catch (e) {
      error_service.handleError(e);
      this.setState({ isCreating: false });
    }
  }

  async saveSecrets() {
    await Promise.all(this.getSecrets().map((s) => encryptAndUpdate(s, this.state.secrets.get(s) || "")));
  }

  async handleDeployClicked(repoResponse: repo.CreateRepoResponse) {
    this.setState({ isDeploying: true });
    try {
      if (this.getSecrets().length) {
        await this.saveSecrets();
      }
      let workflowResponse = await this.runWorkflow(repoResponse.repoUrl);
      this.setState({ isDeploying: false, workflowResponse: workflowResponse });
    } catch (e) {
      error_service.handleError(e);
      this.setState({ isDeploying: false });
    }
  }

  handlePermissionsClicked() {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    window.location.href = selectedInstallation?.url + `/permissions/update`;
  }

  render() {
    if (this.state.githubInstallationsLoading) {
      return (
        <div className="create-repo-page">
          <div className="repo-loading">
            <Spinner />
          </div>
        </div>
      );
    }

    let isGCPDeploy = this.getSecrets().includes(gcpRefreshTokenKey);
    let needsGCPLink =
      isGCPDeploy && !this.state.secretsResponse?.secret.map((s) => s.name).includes(gcpRefreshTokenKey);
    let deployDestination = isGCPDeploy ? " to Google Cloud" : "";
    return (
      <div className="create-repo-page">
        {!this.props.user && (
          <div className="repo-block card login-buttons">
            <div className="repo-title">Get started</div>
            <button
              className="github-button"
              onClick={() =>
                (window.location.href = `/login/github/?${new URLSearchParams({
                  redirect_url: window.location.href,
                  link: "true",
                })}`)
              }>
              <Github /> Continue with Github
            </button>
            <button className="google-button" onClick={() => auth_service.login()}>
              <UserIcon /> Continue with Google
            </button>
          </div>
        )}
        {this.props.user && !this.state.githubInstallationsResponse?.installations && (
          <div className="repo-block card login-buttons">
            <div className="repo-title">Get started</div>
            <button
              className="github-button"
              onClick={() =>
                (window.location.href = `/auth/github/app/link/?${new URLSearchParams({
                  user_id: this.props.user?.displayUser?.userId?.id || "",
                  group_id: this.props.user?.selectedGroup?.id || "",
                  redirect_url: window.location.href,
                  install: "true",
                })}`)
              }>
              <Github /> Link Github
            </button>
          </div>
        )}
        <div
          className={`repo-block card repo-create ${
            this.props.user && this.state.githubInstallationsResponse?.installations ? "" : "disabled"
          }`}>
          <div className="repo-title">Create git repository</div>
          <div className="repo-picker">
            <div>
              <div>Git scope</div>
              <div>
                <Select
                  onChange={this.handleInstallationPicked.bind(this)}
                  value={this.state.selectedInstallationIndex}>
                  {this.state.githubInstallationsResponse?.installations.map((i, index: number) => (
                    <option value={index}>{`${i.login}`}</option>
                  ))}
                  {!this.state.githubInstallationsResponse?.installations && (
                    <option value={-1}>Pick a git scope...</option>
                  )}
                  <option value={-1}>+ Add Github Account</option>
                </Select>
              </div>
            </div>
            <div>
              <div>Repository name</div>
              <div>
                <TextInput value={this.state.repoName} onChange={this.handleRepoChanged.bind(this)} />
              </div>
            </div>
          </div>
          <label className="repo-private">
            <Checkbox checked={this.state.private} onChange={this.handlePrivateChanged.bind(this)} />
            Create private git repository
          </label>
          {!this.hasPermissions() && (
            <button className="permissions-button" onClick={this.handlePermissionsClicked.bind(this)}>
              Grant permissions
            </button>
          )}
          {!this.state.repoResponse && (
            <button
              disabled={
                !this.state.githubInstallationsResponse?.installations ||
                !this.hasPermissions() ||
                this.state.isCreating
              }
              className="create-button"
              onClick={this.handleCreateClicked.bind(this)}>
              {this.state.isCreating ? "Creating..." : "Create repository"}
            </button>
          )}
          {this.state.repoResponse && (
            <div className="view-buttons">
              <a className="view-button" href={this.state.repoResponse.repoUrl} target="_blank">
                View
              </a>
              <a
                className="code-button"
                href={this.state.repoResponse.repoUrl?.replace("github.com", "bgithub.com")}
                target="_blank">
                Code
              </a>
            </div>
          )}
        </div>
        {this.getSecrets().length > 0 && (
          <div
            className={`repo-block card repo-create ${this.props.user && this.state.repoResponse ? "" : "disabled"}`}>
            <div className="repo-title">Configure deployment</div>
            <div className="deployment-configs">
              {this.getSecrets()
                .filter((s) => !this.state.secretsResponse?.secret.map((s) => s.name).includes(s))
                .filter((s) => gcpRefreshTokenKey != s)
                .map((s) => (
                  <div className="deployment-config">
                    <div>{s}</div>
                    <div>
                      <TextInput
                        type="password"
                        placeholder={s}
                        value={this.state.secrets.get(s)}
                        onChange={(e) => {
                          this.state.secrets.set(s, e.target.value);
                          this.forceUpdate();
                        }}
                      />
                    </div>
                  </div>
                ))}
            </div>
            {needsGCPLink && (
              <button
                disabled={!this.state.githubInstallationsResponse?.installations}
                className="create-button"
                onClick={() =>
                  (window.location.href = `/login/?${new URLSearchParams({
                    issuer_url: "https://accounts.google.com",
                    link_gcp_for_group: this.props.user?.selectedGroup.id || "",
                    redirect_url: window.location.href,
                  })}`)
                }>
                Link Google Cloud
              </button>
            )}
            {!this.state.workflowResponse && (
              <button
                disabled={
                  !this.state.githubInstallationsResponse?.installations ||
                  !this.hasPermissions() ||
                  this.state.isDeploying ||
                  needsGCPLink
                }
                className="create-button"
                onClick={() => this.handleDeployClicked(this.state.repoResponse!)}>
                {this.state.isDeploying ? `Deploying${deployDestination}...` : `Deploy${deployDestination}`}
              </button>
            )}
          </div>
        )}
        {this.state.workflowResponse && (
          <div
            className={`repo-block card repo-create ${
              this.props.user && this.state.githubInstallationsResponse?.installations ? "" : "disabled"
            }`}>
            <div className="repo-title">Workflows</div>
            <div className="running-actions">
              {this.state.workflowResponse.actionStatuses.map((s) => (
                <a href={`/invocation/${s.invocationId}?queued=true`} target="_blank">
                  <ChevronRightSquare /> {s.actionName}
                </a>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  }
}
