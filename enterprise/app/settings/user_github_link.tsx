import { User as UserIcon } from "lucide-react";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import authService from "../../../app/auth/auth_service";
import { User } from "../../../app/auth/user";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import { TextLink } from "../../../app/components/link/link";
import Modal from "../../../app/components/modal/modal";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { linkReadOnlyGitHubAppURL, linkReadWriteGitHubAppURL } from "../../../app/util/github";
import { github } from "../../../proto/github_ts_proto";
import GitHubTooltip from "./github_tooltip";

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
      .unlinkUserGitHubAccount({})
      .then((response) => {
        alertService.success("Successfully unlinked GitHub account");
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
        <div className="settings-option-title github-settings-title">
          GitHub account link
          <GitHubTooltip />
        </div>
        <div className="settings-option-description">
          <p>
            Linking your GitHub account to BuildBuddy lets you manage the BuildBuddy GitHub app directly in the
            BuildBuddy UI.
          </p>
          <p>
            Even if you login to BuildBuddy using your GitHub account, this must be explicitly granted to access
            GitHub-related features.
          </p>
          <p>
            Unlinking your account will prevent you from managing the BuildBuddy GitHub app in our UI, but it will not
            disable the app and related features. To manage the app installation itself, go to the "GitHub
            installations" tab under "Organization settings".
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
          <div className="setup-button-container">
            <FilledButton className="settings-button settings-link-button left-aligned-button">
              <a href={linkReadWriteGitHubAppURL(this.props.user.displayUser?.userId?.id || "", "")}>
                Link GitHub account (full access)
              </a>
            </FilledButton>
            {capabilities.readOnlyGitHubApp && (
              <FilledButton className="settings-button settings-link-button left-aligned-button">
                <a href={linkReadOnlyGitHubAppURL(this.props.user.displayUser?.userId?.id || "", "")}>
                  Link GitHub account (read-only)
                </a>
              </FilledButton>
            )}
          </div>
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
