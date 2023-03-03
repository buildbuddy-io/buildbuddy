import React from "react";
import { CheckCircle, User as UserIcon, ExternalLink } from "lucide-react";
import { Octokit } from "@octokit/rest";
import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/user";
import capabilities from "../../../app/capabilities/capabilities";
import Banner from "../../../app/components/banner/banner";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import Spinner from "../../../app/components/spinner/spinner";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import authService from "../../../app/auth/auth_service";
import { TextLink } from "../../../app/components/link/link";
import { github } from "../../../proto/github_ts_proto";

export interface Props {
  user: User;

  /**
   * Whether this is the user-level GitHub link page.
   * This is only supported when the GitHub App is enabled.
   */
  userLevel?: boolean;
}

export interface State {
  deleteModalVisible?: boolean;
  isDeleting?: boolean;
  isRefreshingUser?: boolean;

  account?: GitHubAccount;
  accountLoading?: boolean;

  installationsResponse?: github.GetGitHubAppInstallationsResponse;
}

type GitHubAccount = {
  name: string;
  login: string;
  avatarUrl: string;
};

export default class GitHubLink extends React.Component<Props, State> {
  state: State = {};

  private accountFetch?: CancelablePromise;

  componentDidMount() {
    if (this.props.userLevel) {
      this.fetchGitHubAccount();
    } else {
      this.fetchInstallations();
    }
  }

  private gitHubLinkUrl(): string {
    const params = new URLSearchParams({
      user_id: this.props.user.displayUser?.userId?.id || "",
      redirect_url: window.location.href,
    });
    if (this.props.userLevel) {
      return `/auth/github/app/link/?${params}`;
    }

    // group_id is only needed for the legacy OAuth-only app.
    params.set("group_id", this.props.user?.selectedGroup?.id);
    return `/auth/github/link/?${params}`;
  }
  private fetchGitHubAccount() {
    this.accountFetch?.cancel();

    if (!this.props.user.githubToken) {
      this.setState({ accountLoading: false, account: undefined });
      return;
    }

    this.setState({ accountLoading: true });
    this.accountFetch = new CancelablePromise(
      new Octokit({ auth: this.props.user.githubToken }).users
        .getAuthenticated()
        .then((response) => {
          this.setState({
            account: {
              name: response.data.name || "",
              login: response.data.login,
              avatarUrl: response.data.avatar_url,
            },
          });
        })
        .finally(() => this.setState({ accountLoading: false }))
    );
  }

  private onRequestCloseDeleteModal() {
    this.setState({ deleteModalVisible: false });
  }
  private onClickUnlinkButton() {
    // TODO(bduffany): Fetch linked workflows and confirm that they should be deleted
    this.setState({ deleteModalVisible: true });
  }
  private onConfirmDelete() {
    this.setState({ isDeleting: true });
    rpcService.service
      .unlinkGitHubAccount(github.UnlinkGitHubAccountRequest.create({ user: this.props.userLevel }))
      .then((response) => {
        if (response.warning.length) {
          alertService.warning("Warnings encountered while deleting GitHub account:\n" + response.warning.join("\n"));
        } else {
          alertService.success("Successfully unlinked GitHub account");
        }
        this.setState({ deleteModalVisible: false });
        this.refreshUser();
      })
      .catch((e: any) => errorService.handleError(e))
      .finally(() => this.setState({ isDeleting: false }));
  }
  private refreshUser() {
    this.setState({ isRefreshingUser: true });
    authService
      .refreshUser()
      .catch((e: any) => errorService.handleError(e))
      .finally(() => this.setState({ isRefreshingUser: false }));
  }
  private fetchInstallations() {
    this.setState({ installationsResponse: undefined });
    rpcService.service
      .getGitHubAppInstallations(github.GetGitHubAppInstallationsRequest.create())
      .then((installationsResponse) => this.setState({ installationsResponse }))
      .catch((e) => errorService.handleError(e));
  }

  private linkInstallation(id: number) {
    // TODO: implement
  }

  render() {
    if (this.state.isRefreshingUser) return <div className="loading" />;

    return (
      <div className="github-account-link">
        <div className="settings-option-title">
          {this.props.userLevel ? "Organization GitHub account link" : "GitHub account link"}
        </div>
        <div className="settings-option-description">
          {this.props.userLevel && (
            <p>
              Linking a GitHub account allows you to configure BuildBuddy integrations for your GitHub repositories
              directly in the BuildBuddy UI.
            </p>
          )}
          {!this.props.userLevel && !capabilities.config.githubAppEnabled && (
            <p>
              Linking a GitHub account to your organization allows BuildBuddy to report commit statuses that appear in
              the GitHub UI. It also enables easier setup for{" "}
              <TextLink href="/workflows">BuildBuddy workflows.</TextLink>
            </p>
          )}
        </div>
        {capabilities.config.githubAppEnabled && !this.props.userLevel && (
          <Banner type="warning" style={{ marginBottom: "16px" }}>
            <p>Organization-linked GitHub accounts are no longer recommended.</p>
            <p>
              Instead, <TextLink href="/settings/personal/github">link a personal GitHub account</TextLink>
              {this.props.user.selectedGroup.githubLinked && (
                <>
                  , then unlink your account below and <TextLink href="/workflows/new">re-link workflows</TextLink>{" "}
                  using the new GitHub app integration
                </>
              )}
              .
            </p>
            {/* TODO: Link to docs so people can refer back to this later. */}
          </Banner>
        )}
        {/* User-level link */}
        {this.props.userLevel && (
          <>
            {this.props.user.githubToken ? (
              <div className="linked-github-account-row">
                {this.state.account ? (
                  <img src={this.state.account.avatarUrl} alt="" className="avatar" />
                ) : (
                  <UserIcon className="icon avatar" />
                )}
                <div>
                  <div>GitHub account linked</div>
                  {this.state.account ? (
                    <div>
                      <b>{this.state.account.login}</b>
                    </div>
                  ) : (
                    <div>{this.state.accountLoading ? "Loading..." : ""}</div>
                  )}
                </div>
                <OutlinedButton className="unlink-button destructive" onClick={this.onClickUnlinkButton.bind(this)}>
                  Unlink
                </OutlinedButton>
              </div>
            ) : (
              <FilledButton className="settings-button settings-link-button">
                <a href={this.gitHubLinkUrl()}>Link GitHub account</a>
              </FilledButton>
            )}
          </>
        )}
        {/* Org-level link */}
        {!this.props.userLevel && (
          <>
            {this.props.user.selectedGroup.githubLinked ? (
              <div className="linked-github-account-row">
                <CheckCircle className="icon green" />
                <div>GitHub account linked</div>
                <OutlinedButton className="unlink-button destructive" onClick={this.onClickUnlinkButton.bind(this)}>
                  Unlink
                </OutlinedButton>
              </div>
            ) : (
              <FilledButton className="settings-button settings-link-button">
                <a href={this.gitHubLinkUrl()}>Link GitHub account</a>
              </FilledButton>
            )}
          </>
        )}
        {/* Org-level GitHub installations */}
        {!this.props.userLevel && (
          <>
            <div className="settings-option-title">GitHub app installations</div>
            {!this.state.installationsResponse && <div className="loading loading-slim"></div>}
            {this.state.installationsResponse && (
              <div className="github-app-installations">
                {this.state.installationsResponse.installations.map((installation) => (
                  <div className="github-app-installation">
                    <div>{installation.owner}</div>
                    {installation.isLinked ? (
                      <div className="linked-message">
                        <div>Linked</div>
                        <CheckCircle className="icon green" />
                      </div>
                    ) : (
                      <OutlinedButton onClick={() => this.linkInstallation(Number(installation.id))}>
                        Link to org
                      </OutlinedButton>
                    )}
                  </div>
                ))}
              </div>
            )}
          </>
        )}
        {this.state.deleteModalVisible && (
          <Modal
            className="github-account-link-delete-modal"
            onRequestClose={this.onRequestCloseDeleteModal.bind(this)}
            isOpen={this.state.deleteModalVisible}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Unlink GitHub account</DialogTitle>
              </DialogHeader>
              <DialogBody>
                {!this.props.userLevel && (
                  <>
                    <p>After unlinking, BuildBuddy will no longer report commit statuses to GitHub.</p>
                    <p>Any workflows linked using this account will also be deleted.</p>
                  </>
                )}
                <p>
                  After unlinking, {this.props.userLevel ? "you" : "the linked account owner"} can also revoke app
                  access via{" "}
                  <TextLink href="https://github.com/settings/applications">GitHub OAuth app settings</TextLink>.
                </p>
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isDeleting && <Spinner />}
                  <OutlinedButton onClick={this.onRequestCloseDeleteModal.bind(this)} disabled={this.state.isDeleting}>
                    Cancel
                  </OutlinedButton>
                  <FilledButton
                    className="destructive"
                    onClick={this.onConfirmDelete.bind(this)}
                    disabled={this.state.isDeleting}>
                    Unlink
                  </FilledButton>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>
        )}
      </div>
    );
  }
}
