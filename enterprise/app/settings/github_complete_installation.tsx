import Long from "long";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import authService from "../../../app/auth/auth_service";
import { User } from "../../../app/auth/user";
import Banner from "../../../app/components/banner/banner";
import SimpleModalDialog from "../../../app/components/dialog/simple_modal_dialog";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";

export interface CompleteGitHubAppInstallationProps {
  user: User;
  search: URLSearchParams;
}

interface State {
  isLinkInstallationLoading: boolean;
  isRefreshingUser: boolean;
}

/**
 * Dialog that is shown when the user initiates a BuildBuddy GitHub App
 * installation from GitHub rather than the BuildBuddy UI. The dialog prompts
 * the user to select a group to link the installation to.
 */
export default class CompleteGitHubAppInstallationDialog extends React.Component<
  CompleteGitHubAppInstallationProps,
  State
> {
  state: State = {
    isLinkInstallationLoading: false,
    isRefreshingUser: false,
  };

  private linkInstallation() {
    this.setState({ isLinkInstallationLoading: true });
    rpcService.service
      .linkGitHubAppInstallation(
        github.LinkAppInstallationRequest.create({
          installationId: Long.fromString(this.props.search.get("installation_id") || ""),
        })
      )
      .then(() => {
        alertService.success("Installation linked successfully");
        this.close();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isLinkInstallationLoading: false }));
  }

  private onChangeSelectedGroup(e: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ isRefreshingUser: true });
    const grp = this.props.user.groups.find((g) => g.id === e.target.value);
    // Should not be possible.
    if (!grp) {
      throw new Error("group not found");
    }
    authService
      .setSelectedGroupId(grp.id, grp?.url)
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRefreshingUser: false }));
  }

  private close() {
    if (this.props.user.canCall("unlinkGitHubAccount")) {
      router.navigateTo("/settings/org/github");
    } else {
      router.navigateTo("/settings/org");
    }
  }

  render() {
    return (
      <SimpleModalDialog
        title="Complete GitHub app installation"
        isOpen={true}
        onRequestClose={() => this.close()}
        submitLabel="Link"
        submitDisabled={!this.props.user.canCall("linkGitHubAppInstallation")}
        onSubmit={() => this.linkInstallation()}
        loading={this.state.isLinkInstallationLoading}
        className="github-account-link">
        <p>
          Select the BuildBuddy organization that should own the GitHub app installation for{" "}
          <b>{this.props.search.get("installation_owner")}</b>:
        </p>
        <p className="controls-row">
          <Select
            value={this.props.user.selectedGroup.id}
            onChange={this.onChangeSelectedGroup.bind(this)}
            disabled={this.state.isRefreshingUser}>
            {this.props.user.groups.map((group) => (
              <Option key={group.id} value={group.id}>
                {group.name}
              </Option>
            ))}
          </Select>
          {this.state.isRefreshingUser && <Spinner />}
        </p>
        {!this.props.user.canCall("linkGitHubAppInstallation") && (
          <p>
            <Banner type="error">You are not an admin of this organization.</Banner>
          </p>
        )}
      </SimpleModalDialog>
    );
  }
}
