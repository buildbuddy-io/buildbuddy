import React from "react";
import error_service from "../../../app/errors/error_service";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import { repo } from "../../../proto/repo_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";
import { BookCopy, ChevronRightSquare, FolderInput, Folders, Github } from "lucide-react";
import { workflow } from "../../../proto/workflow_ts_proto";
import Select from "../../../app/components/select/select";
import Checkbox from "../../../app/components/checkbox/checkbox";
import TextInput from "../../../app/components/input/input";
import { encryptAndUpdate } from "../secrets/secret_util";
import auth_service, { User } from "../../../app/auth/auth_service";
import { secrets } from "../../../proto/secrets_ts_proto";
import router from "../../../app/router/router";
import popup from "../../../app/util/popup";
import picker_service from "../../../app/picker/picker_service";
import { GithubIcon } from "../../../app/icons/github";
import { GoogleIcon } from "../../../app/icons/google";
import OrgPicker from "../org_picker/org_picker";

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
  templateDirectory: string;
  destinationDirectory: string;
  private: boolean;
  secrets: Map<string, string>;
  vars: Map<string, string>;

  repoResponse: repo.CreateRepoResponse | null;
  workflowResponse: workflow.ExecuteWorkflowResponse | null;
}

const selectedInstallationIndexLocalStorageKey = "repo-selectedInstallationIndex";
const gcpRefreshTokenKey = "CLOUDSDK_AUTH_REFRESH_TOKEN";
const gcpProjectKey = "CLOUDSDK_CORE_PROJECT";
export default class RepoComponent extends React.Component<RepoComponentProps, RepoComponentState> {
  state: RepoComponentState = {
    selectedInstallationIndex: localStorage[selectedInstallationIndexLocalStorageKey] || 0,
    githubInstallationsLoading: true,
    githubInstallationsResponse: null,
    secretsResponse: null,
    isCreating: false,
    isDeploying: false,

    template: this.getTemplate(),
    templateDirectory: this.getTemplateDirectory(),
    destinationDirectory: this.getDestinationDirectory(),
    repoName: this.getRepoName(),
    private: true,
    secrets: new Map<string, string>(),
    vars: new Map<string, string>(),

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

  getTemplateDirectory() {
    return this.props.search.get("dir") || "";
  }

  getTemplateName() {
    return this.props.search.get("templatename") || "";
  }

  getTemplateUrl() {
    return this.props.search.get("template") || "";
  }

  getTemplateImage() {
    return this.props.search.get("image") || "";
  }

  getDestinationDirectory() {
    return this.props.search.get("destdir") || "";
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
      return Promise.resolve(null);
    }
    return rpc_service.service
      .getGithubUserInstallations(new github.GetGithubUserInstallationsRequest())
      .then((response) => {
        console.log(response);
        this.setState({ githubInstallationsResponse: response });
        return response;
      })
      .finally(() => this.setState({ githubInstallationsLoading: false }));
  }

  fetchSecrets() {
    if (!this.props.user) return;
    return rpc_service.service.listSecrets(new secrets.ListSecretsRequest()).then((response) => {
      console.log(response);
      this.setState({ secretsResponse: response });
    });
  }

  componentDidMount() {
    this.fetchGithubInstallations().catch((e) => {
      // Log the error, but we don't need to show it to the user since
      // when they click the Create repository button, we'll do a GitHub
      // link if installations aren't present.
      console.log(e);
    });
    this.fetchSecrets();
  }

  async loginAndLinkGithub() {
    try {
      if (!this.props.user) {
        await this.loginToGithub();
      }
      await this.linkGithubAccount();
    } catch (e) {
      error_service.handleError(e);
    }
  }

  loginToGithub() {
    return popup
      .open(
        `/login/github/?${new URLSearchParams({
          redirect_url: window.location.href,
          link: "true",
        })}`
      )
      .then(() => auth_service.refreshUser());
  }

  linkGithubAccount() {
    return popup
      .open(
        `/auth/github/app/link/?${new URLSearchParams({
          group_id: this.props.user?.selectedGroup.id || "",
          user_id: this.props.user?.displayUser.userId?.id || "",
          redirect_url: window.location.href,
          install: "true",
        })}`
      )
      .then(() => this.fetchSecrets())
      .then(() => {
        return this.fetchGithubInstallations()?.then((resp) => {
          let installationId = auth_service.getCookie("Github-Linked-Installation-ID");
          let installationIndex = resp?.installations.findIndex((i) => i.id.toString() == installationId);
          if (installationIndex && installationIndex > 0) {
            this.selectInstallation(installationIndex);
          }
          return resp;
        });
      });
  }

  handleInstallationPicked(e: React.ChangeEvent<HTMLSelectElement>) {
    this.selectInstallation(Number(e.target.value));
  }

  selectInstallation(index: number) {
    if (index == -1) {
      return this.loginAndLinkGithub();
    }
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
    return selectedInstallation.permissions?.administration == "write";
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

  createOrUpdateRepo(update?: boolean) {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    let r = new repo.CreateRepoRequest();
    r.name = this.state.repoName;
    r.private = this.state.private;
    r.template = this.state.template;
    r.templateDirectory = this.state.templateDirectory;
    r.destinationDirectory = this.state.destinationDirectory;

    if (selectedInstallation) {
      r.owner = selectedInstallation.login;
      r.installationId = selectedInstallation.id;
      r.installationTargetType = selectedInstallation.targetType;
    }

    if (update) {
      r.skipRepo = true;
      r.skipLink = true;
      r.templateVariables = Object.fromEntries(this.state.vars.entries());
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
        visibility: "",
        async: true,
      })
    );
  }

  getSecrets() {
    let s = this.props.search.get("secret") || this.props.search.get("secrets") || "";
    return s ? s.split(",") : [];
  }

  getVars() {
    let s = this.props.search.get("var") || this.props.search.get("vars") || "";
    return s ? s.split(",") : [];
  }

  getUnsetSecrets() {
    return (
      this.getSecrets()
        .filter((s) => !this.state.secretsResponse?.secret.map((s) => s.name).includes(s))
        // Exclude the GCP refresh token from secrets that need to be set
        // up manually by the user, since we set this up as part of GCP
        // account linking.
        .filter((s) => gcpRefreshTokenKey != s)
        // Exclude the GCP project environment variable since we'll show the user
        // a picker if this isn't set.
        .filter((s) => gcpProjectKey != s)
    );
  }

  async handleCreateClicked() {
    if (!this.state.githubInstallationsResponse?.installations?.length) {
      // TODO(siggisim): Instead of showing link popup again, show an in-app picker if
      // there is more than one github org, or skip the linking if there's exactly one.
      await this.loginAndLinkGithub();
    }

    if (!this.hasPermissions()) {
      // TODO(siggisim): Make sure this permissions thing works well.
      await this.showPermissions();
    }

    this.setState({ isCreating: true });
    try {
      await this.linkInstallation();
      let repoResponse = await this.createOrUpdateRepo();
      if (this.props.search.get("mode") == "code") {
        window.location.href = "/code/" + repoResponse.repoUrl.replaceAll("https://github.com/", "");
        return;
      }
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
    await Promise.all(this.getUnsetSecrets().map((s) => encryptAndUpdate(s, this.state.secrets.get(s) || "")));
  }

  async promptGCPProjectPicker() {
    let picked = await this.showGCPProjectPicker();
    await encryptAndUpdate(gcpProjectKey, picked);
  }
  async showGCPProjectPicker() {
    let resp = await rpc_service.service.getGCPProject({});
    return await picker_service.show({
      placeholder: "Pick a GCP project or search...",
      title: "Projects",
      options: resp.project.map((p) => p.id),
      emptyState: (
        <div className="gcp-picker-empty-state">
          No Google Cloud projects found!
          <br />
          <br />
          <a href="https://console.cloud.google.com/projectcreate" target="_blank">
            Click here to create a Google Cloud project
          </a>
          , and then click refresh below to update this list.
          <br />
          <br />
          <button onClick={this.showGCPProjectPicker.bind(this)}>Refresh</button>
        </div>
      ),
      footer: (
        <div className="gcp-picker-footer">
          Don't want to deploy to any of these projects?
          <br />
          <br />
          <a href="https://console.cloud.google.com/projectcreate" target="_blank">
            Click here to create a Google Cloud project
          </a>
          , and then click refresh below to update this list.
          <br />
          <br />
          <button onClick={this.showGCPProjectPicker.bind(this)}>Refresh</button>
        </div>
      ),
    });
  }

  async handleDeployClicked(repoResponse: repo.CreateRepoResponse) {
    let isGCPDeploy = this.getSecrets().includes(gcpRefreshTokenKey);
    let needsGCPLink =
      isGCPDeploy && !this.state.secretsResponse?.secret.map((s) => s.name).includes(gcpRefreshTokenKey);
    let needsGCPProject = isGCPDeploy && !this.state.secretsResponse?.secret.map((s) => s.name).includes(gcpProjectKey);
    let hasVariables = this.getVars().length > 0;

    this.setState({ isDeploying: true });
    try {
      if (hasVariables) {
        await this.createOrUpdateRepo(true /* update */);
      }
      if (needsGCPLink) {
        await this.linkGoogleCloud().then(() => this.fetchSecrets());
      }
      if (needsGCPProject) {
        await this.promptGCPProjectPicker();
      }
      if (this.getUnsetSecrets().length) {
        await this.saveSecrets();
      }
      let workflowResponse = await this.runWorkflow(repoResponse.repoUrl);
      this.setState({ isDeploying: false, workflowResponse: workflowResponse });
    } catch (e) {
      error_service.handleError(e);
      this.setState({ isDeploying: false });
    }
  }

  showPermissions() {
    let selectedInstallation = this.state.githubInstallationsResponse?.installations[
      this.state.selectedInstallationIndex
    ];
    return popup.open(selectedInstallation?.url + `/permissions/update`).catch((e) => {
      // Log an error here, but let's keep going since sometimes the Github UI
      // doesn't redirect after a permissions change, and the user has to X out
      // of the window.
      console.log(e);
    });
  }

  linkGoogleCloud() {
    return popup.open(
      `/auth/gcp/link/?${new URLSearchParams({
        link_gcp_for_group: this.props.user?.selectedGroup.id || "",
        redirect_url: window.location.href,
      })}`
    );
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
    let deployDestination = isGCPDeploy ? " to Google Cloud" : "";
    return (
      <div className="create-repo-page">
        <OrgPicker user={this.props.user} floating={true} />
        <div className="create-repo-page-details">
          {this.getTemplateName() && (
            <div className="repo-block card template-block">
              <div className="template">
                <div className="template-metadata">
                  <div className="template-name">{this.getTemplateName()}</div>
                  <div className="template-props">
                    {this.getTemplateUrl() && (
                      <div className="template-repo">
                        <Github />
                        <div className="template-repo-name">
                          {this.getTemplateUrl().replaceAll("https://github.com/", "").split("/")[0]}
                        </div>
                      </div>
                    )}
                    {this.getTemplateUrl() && (
                      <div className="template-repo">
                        <BookCopy />
                        <div className="template-repo-name">
                          {this.getTemplateUrl().replaceAll("https://github.com/", "").split("/").pop()}
                        </div>
                      </div>
                    )}
                    {this.getTemplateDirectory() && (
                      <div className="template-repo">
                        <Folders />
                        <div className="template-repo-name">{this.getTemplateDirectory().replaceAll("/", " / ")}</div>
                      </div>
                    )}
                    {this.getDestinationDirectory() && (
                      <div className="template-repo">
                        <FolderInput />
                        <div className="template-repo-name">
                          {this.getDestinationDirectory().replaceAll("/", " / ")}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
                {this.getTemplateImage() && <img className="template-image" src={this.getTemplateImage()} />}
              </div>
            </div>
          )}
        </div>
        <div className={`repo-block card repo-create`}>
          <div className="repo-title">Create repository</div>
          <div className="repo-picker">
            {this.state.githubInstallationsResponse?.installations &&
              this.state.githubInstallationsResponse?.installations.length > 0 && (
                <div>
                  <div>Git scope</div>
                  <div>
                    <Select
                      onChange={this.handleInstallationPicked.bind(this)}
                      value={this.state.selectedInstallationIndex}>
                      {this.state.githubInstallationsResponse?.installations.map((i, index: number) => (
                        <option value={index}>{`${i.login}`}</option>
                      ))}
                      <option value={-1}>+ Add Github Account</option>
                    </Select>
                  </div>
                </div>
              )}
            <div className="repo-form">
              <div className="repo-form-header">
                <div>Repository name</div>
                <div>
                  <label className="repo-private">
                    Private
                    <Checkbox checked={this.state.private} onChange={this.handlePrivateChanged.bind(this)} />
                  </label>
                </div>
              </div>
              <div>
                <TextInput value={this.state.repoName} onChange={this.handleRepoChanged.bind(this)} />
              </div>
            </div>
          </div>
          {!this.state.repoResponse && (
            <button
              disabled={this.state.isCreating}
              className="create-button"
              onClick={this.handleCreateClicked.bind(this)}>
              <GithubIcon /> {this.state.isCreating ? "Creating..." : "Create GitHub repository"}
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
        {(this.getSecrets().length > 0 || this.getVars().length > 0) && !this.state.workflowResponse && (
          <div className={`repo-block card repo-create ${!this.state.repoResponse && "disabled"}`}>
            <div className="repo-title">Configure deployment</div>
            {Boolean(this.getVars().length) && (
              <div className="deployment-configs">
                {this.getVars().map((s) => (
                  <div className="deployment-config">
                    <div>{s}</div>
                    <div>
                      <TextInput
                        placeholder={s}
                        value={this.state.vars.get(s)}
                        onChange={(e) => {
                          this.state.vars.set("$" + s, e.target.value);
                          this.forceUpdate();
                        }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}
            {Boolean(this.getUnsetSecrets().length) && (
              <div className="deployment-configs">
                {this.getUnsetSecrets().map((s) => (
                  <div className="deployment-config">
                    <div>{s}</div>
                    <div>
                      <TextInput
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
            )}
            <button
              disabled={this.state.isDeploying || Boolean(this.state.workflowResponse)}
              className="create-button"
              onClick={() => this.handleDeployClicked(this.state.repoResponse!)}>
              <GoogleIcon />
              {this.state.isDeploying || this.state.workflowResponse
                ? `Deploying${deployDestination}...`
                : `Deploy${deployDestination}`}
            </button>
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
