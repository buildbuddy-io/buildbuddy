import { CheckCircle, AlertCircle } from "lucide-react";
import React from "react";
import { github } from "../../../proto/github_ts_proto";
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
import capabilities from "../../../app/capabilities/capabilities";
import Banner from "../../../app/components/banner/banner";
import LinkButton from "../../../app/components/button/link_button";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";

export interface Props {
  user: User;
}

export interface State {
  deleteModalVisible: boolean;
  isDeleting: boolean;
  isRefreshingUser: boolean;

  installationsLoading: boolean;
  installationsResponse: github.GetAppInstallationsResponse | null;

  installationToUnlink: github.AppInstallation | null;
  unlinkInstallationLoading: boolean;
}

export default class GitHubLink extends React.Component<Props, State> {
  state: State = {
    deleteModalVisible: false,
    isDeleting: false,
    isRefreshingUser: false,

    installationsResponse: null,
    installationsLoading: false,

    installationToUnlink: null,
    unlinkInstallationLoading: false,
  };

  componentDidMount() {
    if (capabilities.config.githubAppEnabled) {
      this.fetchInstallations();
    }
  }

  private gitHubLinkUrl(): string {
    const params = new URLSearchParams({
      group_id: this.props.user.selectedGroup.id,
      user_id: this.props.user.displayUser.userId?.id || "",
      redirect_url: window.location.href,
    });
    return `/auth/github/link/?${params}`;
  }

  private fetchInstallations() {
    this.setState({ installationsLoading: true, installationsResponse: null });
    rpcService.service
      .getGitHubAppInstallations(github.GetAppInstallationsRequest.create({}))
      .then((response) => this.setState({ installationsResponse: response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ installationsLoading: false }));
  }
  private getInstallURL() {
    const params = new URLSearchParams({
      group_id: this.props.user.selectedGroup.id,
      user_id: this.props.user.displayUser.userId?.id || "",
      redirect_url: window.location.href,
      install: "true",
    });
    return `/auth/github/app/link/?${params}`;
  }
  private unlinkInstallation(installation: github.AppInstallation) {
    this.setState({ unlinkInstallationLoading: true });
    rpcService.service
      .unlinkGitHubAppInstallation(
        github.UnlinkAppInstallationRequest.create({
          installationId: installation.installationId,
        })
      )
      .then(() => {
        this.setState({ installationToUnlink: null });
        alertService.success(`Successfully unlinked "github.com/${installation.owner}"`);
        this.fetchInstallations();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ unlinkInstallationLoading: false }));
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
      .unlinkGitHubAccount(github.UnlinkGitHubAccountRequest.create({}))
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

  private showUnlinkDialog(installation: github.AppInstallation) {
    this.setState({ installationToUnlink: installation });
  }
  private closeUnlinkDialog() {
    this.setState({ installationToUnlink: null });
  }

  render() {
    if (this.state.isRefreshingUser) return <div className="loading" />;

    return (
      <div className="github-account-link">
        <div className="settings-option-title">GitHub account link</div>
        <div className="settings-option-description">
          {!capabilities.config.githubAppEnabled && (
            <p>
              Linking a GitHub account allows BuildBuddy to report commit statuses that appear in the GitHub UI. It also
              enables easier setup for <TextLink href="/workflows">BuildBuddy workflows.</TextLink>
            </p>
          )}
          {capabilities.config.githubAppEnabled && (
            <Banner type="warning" className="deprecation-notice">
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
            </Banner>
          )}
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
        {capabilities.config.githubAppEnabled && (
          <>
            <div className="settings-option-title">GitHub app link</div>
            <div className="settings-option-description">
              The BuildBuddy GitHub app must be linked to a BuildBuddy organization before it can be used. All app
              installations that you have access to are shown below.
            </div>
            <LinkButton className="big-button" href={this.getInstallURL()}>
              Setup
            </LinkButton>
            {this.state.installationsLoading && <div className="loading loading-slim" />}
            {Boolean(this.state.installationsResponse?.installations?.length) && (
              <div className="github-app-installations">
                {this.state.installationsResponse?.installations.map(
                  (installation) => (
                    console.log(installation),
                    (
                      <div className="github-app-installation">
                        <div className="installation-owner">
                          <TextLink href={`https://github.com/${installation.owner}`}>{installation.owner}</TextLink>
                        </div>
                        <OutlinedButton className="destructive" onClick={() => this.showUnlinkDialog(installation)}>
                          Unlink
                        </OutlinedButton>
                      </div>
                    )
                  )
                )}
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

        {/* Unlink installation dialog */}
        <SimpleModalDialog
          isOpen={this.state.installationToUnlink !== null}
          onRequestClose={() => this.closeUnlinkDialog()}
          title="Unlink GitHub app installation"
          submitLabel="Unlink"
          destructive
          onSubmit={() => this.unlinkInstallation(this.state.installationToUnlink!)}
          loading={this.state.unlinkInstallationLoading}>
          Unlink <b>github.com/{this.state.installationToUnlink?.owner}</b> from BuildBuddy?
        </SimpleModalDialog>
      </div>
    );
  }
}
