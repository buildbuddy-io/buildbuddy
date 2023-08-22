import React from "react";
import { User as UserIcon } from "lucide-react";
import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/user";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
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
}

interface State {
  deleteModalVisible: boolean;
  isDeleting: boolean;
  isRefreshingUser: boolean;

  account: GitHubAccount | null;
  accountLoading: boolean;
}

type GitHubAccount = {
  name: string;
  login: string;
  avatarUrl: string;
};

export default class UserGitHubLink extends React.Component<Props, State> {
  state: State = {
    deleteModalVisible: false,
    isDeleting: false,
    isRefreshingUser: false,

    account: null,
    accountLoading: false,
  };

  private accountFetch?: CancelablePromise;

  componentDidMount() {
    this.fetchGitHubAccount();
  }

  private gitHubLinkUrl(): string {
    const params = new URLSearchParams({
      user_id: this.props.user.displayUser?.userId?.id || "",
      redirect_url: window.location.href,
    });
    return `/auth/github/app/link/?${params}`;
  }
  private fetchGitHubAccount() {
    this.accountFetch?.cancel();

    if (!this.props.user.githubLinked) {
      this.setState({ accountLoading: false, account: null });
      return;
    }

    this.setState({ accountLoading: true });
    this.accountFetch = rpcService.service
      .getGithubUser(new github.GetGithubUserRequest())
      .then((response) => {
        this.setState({
          account: {
            name: response.name || "",
            login: response.login,
            avatarUrl: response.avatarUrl,
          },
        });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ accountLoading: false }));
  }

  private onRequestCloseDeleteModal() {
    this.setState({ deleteModalVisible: false });
  }
  private onClickUnlinkButton() {
    this.setState({ deleteModalVisible: true });
  }
  private onConfirmDelete() {
    this.setState({ isDeleting: true });
    rpcService.service
      .unlinkGitHubAccount(github.UnlinkGitHubAccountRequest.create({ unlinkUserAccount: true }))
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

  render() {
    if (this.state.isRefreshingUser) return <div className="loading" />;

    return (
      <div className="github-account-link">
        <div className="settings-option-title">GitHub account link</div>
        <div className="settings-option-description">
          <p>
            Linking a GitHub account allows you to configure BuildBuddy integrations for your GitHub repositories
            directly in the BuildBuddy UI.
          </p>
        </div>
        {this.props.user.githubLinked ? (
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
                <p>
                  After unlinking, you can also revoke app access via{" "}
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
