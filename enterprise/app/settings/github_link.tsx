import { CheckCircle } from "lucide-react";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import authService from "../../../app/auth/auth_service";
import { User } from "../../../app/auth/user";
import capabilities from "../../../app/capabilities/capabilities";
import Banner from "../../../app/components/banner/banner";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import LinkButton from "../../../app/components/button/link_button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import { TextLink } from "../../../app/components/link/link";
import Modal from "../../../app/components/modal/modal";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";
import GitHubTooltip from "./github_tooltip";

export interface Props {
  user: User;
  path: string;
}

export interface State {
  deleteModalVisible: boolean;
  isDeleting: boolean;
  isRefreshingUser: boolean;

  installationsLoading: boolean;
  installationsResponse: github.GetAppInstallationsResponse | null;

  isUpdatingInstallation: Map<Long, boolean>;

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

    isUpdatingInstallation: new Map<Long, boolean>(),

    installationToUnlink: null,
    unlinkInstallationLoading: false,
  };

  componentDidMount() {
    if (capabilities.config.githubAppEnabled) {
      this.fetchInstallations();
    }
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.user !== prevProps.user || this.props.path !== prevProps.path) {
      if (capabilities.config.githubAppEnabled) {
        this.fetchInstallations();
      }
    }
  }

  private fetchInstallations() {
    this.setState({ installationsLoading: true, installationsResponse: null });
    rpcService.service
      .getGitHubAppInstallations(github.GetAppInstallationsRequest.create({}))
      .then((response) => this.setState({ installationsResponse: response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ installationsLoading: false }));
  }
  // onClickSetupGithubApp redirects to the corresponding app URL if a user has
  // already authorized either the read-write or read-only GitHub app.
  private onClickSetupGithubApp() {
    rpcService.service
      .getGitHubAppInstallPath(new github.GetGithubAppInstallPathRequest())
      .then((response) => {
        const path = `${response.installPath}?${new URLSearchParams({
          group_id: this.props.user.selectedGroup.id,
          user_id: this.props.user.displayUser.userId?.id || "",
          redirect_url: window.location.href,
          install: "true",
        })}`;
        window.location.href = path;
      })
      .catch((e) => errorService.handleError(e));
  }
  private unlinkInstallation(installation: github.AppInstallation) {
    this.setState({ unlinkInstallationLoading: true });
    rpcService.service
      .unlinkGitHubAppInstallation(
        github.UnlinkAppInstallationRequest.create({
          installationId: installation.installationId,
          appId: installation.appId,
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

  private updateInstallationSettings(
    installation: github.AppInstallation,
    idx: number,
    reportCommitStatusesForCIBuilds: boolean
  ) {
    let prevMap = this.state.isUpdatingInstallation;
    prevMap.set(installation.installationId, true);
    this.setState({ isUpdatingInstallation: prevMap });
    rpcService.service
      .updateGitHubAppInstallation(
        github.UpdateGitHubAppInstallationRequest.create({
          appId: installation.appId,
          owner: installation.owner,
          reportCommitStatusesForCiBuilds: reportCommitStatusesForCIBuilds,
        })
      )
      .then(() => {
        if (!this.state.installationsResponse || this.state.installationsResponse.installations.length < idx + 1) {
          throw new Error(`unexpected installation response`);
        }
        this.state.installationsResponse.installations[idx].reportCommitStatusesForCiBuilds =
          reportCommitStatusesForCIBuilds;
        this.setState({ installationsResponse: this.state.installationsResponse });
        alertService.success(`Successfully updated settings for "${installation.owner}"`);
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        let prevMap = this.state.isUpdatingInstallation;
        prevMap.set(installation.installationId, false);
        this.setState({ isUpdatingInstallation: prevMap });
      });
  }

  render() {
    if (this.state.isRefreshingUser) return <div className="loading" />;

    return (
      <div className="github-account-link">
        {(!capabilities.config.githubAppEnabled || this.props.user.selectedGroup.githubLinked) && (
          <>
            <div className="settings-option-title">GitHub account link</div>
            <div className="settings-option-description">
              {!capabilities.config.githubAppEnabled && (
                <p>
                  Linking a GitHub account allows BuildBuddy to report commit statuses that appear in the GitHub UI. It
                  also enables easier setup for <TextLink href="/workflows">BuildBuddy workflows.</TextLink>
                </p>
              )}
              {capabilities.config.githubAppEnabled && (
                <Banner type="warning" className="deprecation-notice">
                  <p>Organization-linked GitHub accounts are no longer recommended.</p>
                  <p>
                    Instead, <TextLink href="/settings/personal/github">link a personal GitHub account</TextLink>
                    {this.props.user.selectedGroup.githubLinked && (
                      <>
                        , then unlink your account below and{" "}
                        <TextLink href="/workflows/new">re-link workflows</TextLink> using the new GitHub app
                        integration
                      </>
                    )}
                    .
                  </p>
                </Banner>
              )}
            </div>
            {this.props.user.selectedGroup.githubLinked && (
              <div className="linked-github-account-row">
                <CheckCircle className="icon green" />
                <div>GitHub account linked</div>
                <OutlinedButton className="unlink-button destructive" onClick={this.onClickUnlinkButton.bind(this)}>
                  Unlink
                </OutlinedButton>
              </div>
            )}
          </>
        )}

        {capabilities.config.githubAppEnabled && (
          <>
            <div className="settings-option-title github-settings-title">
              Manage GitHub app installations
              <GitHubTooltip />
            </div>
            <div className="settings-option-description">
              <p>GitHub app installations that have been imported to BuildBuddy are shown below.</p>
            </div>
            {this.state.installationsLoading && <div className="loading loading-slim" />}
            {Boolean(this.state.installationsResponse?.installations?.length) && (
              <div>
                <div className="settings-option-description">
                  <p>You can also manage installations on GitHub using the "Manage on GitHub" button below.</p>
                </div>
                <div className="setup-button-container">
                  <LinkButton
                    className="big-button left-aligned-button"
                    onClick={this.onClickSetupGithubApp.bind(this)}>
                    Manage on GitHub
                  </LinkButton>
                </div>
                <div>
                  <div className="github-app-installations">
                    {this.state.installationsResponse?.installations.map(
                      (installation, idx) => (
                        console.log(installation),
                        (
                          <div className="github-app-installation">
                            <div className="installation-header">
                              <div className="installation-owner">
                                <TextLink href={`https://github.com/${installation.owner}`}>
                                  {installation.owner}
                                </TextLink>
                              </div>
                              <OutlinedButton
                                className="destructive"
                                onClick={() => this.showUnlinkDialog(installation)}>
                                Unlink
                              </OutlinedButton>
                            </div>
                            <div className="installation-settings">
                              <div className="input-row">
                                <label className="input-label">
                                  <Checkbox
                                    disabled={
                                      this.state.isUpdatingInstallation.get(installation.installationId) === true
                                    }
                                    checked={installation.reportCommitStatusesForCiBuilds}
                                    onChange={(e) =>
                                      this.updateInstallationSettings(installation, idx, e.target.checked)
                                    }
                                  />
                                  <span>Report commit statuses to GitHub</span>
                                </label>
                              </div>
                              <div className="input-description">
                                Applies to BuildBuddy Workflows and builds tagged with{" "}
                                <TextLink href="https://www.buildbuddy.io/docs/guide-metadata#role">ROLE=CI</TextLink>
                              </div>
                            </div>
                          </div>
                        )
                      )
                    )}
                  </div>
                </div>
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
