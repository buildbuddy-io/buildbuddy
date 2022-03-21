import { CheckCircle } from "lucide-react";
import React from "react";
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
import rpcService from "../../../app/service/rpc_service";
import errorService from "../../../app/errors/error_service";
import authService from "../../../app/auth/auth_service";
import { TextLink } from "../../../app/components/link/link";

export interface Props {
  user: User;
}

export interface State {
  deleteModalVisible?: boolean;
  isDeleting?: boolean;
  isRefreshingUser?: boolean;
}

export default class GitHubLink extends React.Component<Props, State> {
  state: State = {};

  private gitHubLinkUrl(): string {
    const params = new URLSearchParams({
      group_id: this.props.user?.selectedGroup?.id,
      user_id: this.props.user?.displayUser.userId.id,
      redirect_url: window.location.href,
    });
    return `/auth/github/link/?${params}`;
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
      .unlinkGitHubAccount({})
      .then((response) => {
        if (response.warning?.length) {
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
            Linking a GitHub account allows BuildBuddy to report commit statuses that appear in the GitHub UI. It also
            enables easier setup for <TextLink href="/workflows">BuildBuddy workflows.</TextLink>
          </p>
        </div>
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
                <p>After unlinking, BuildBuddy will no longer report commit statuses to GitHub.</p>
                <p>Any workflows linked using this account will also be deleted.</p>
                <p>
                  After unlinking, the linked account owner may revoke app access via{" "}
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
