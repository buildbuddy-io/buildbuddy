import React from "react";
import authService from "../../../app/auth/auth_service";
import FilledButton from "../../../app/components/button/button";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import OrgForm, { FormProps } from "./org_form";
import { Link } from "lucide-react";
import capabilities from "../../../app/capabilities/capabilities";

/**
 * Page that allows editing an org specified in the URL.
 */
export default class EditOrgComponent extends OrgForm<grp.UpdateGroupRequest> {
  componentDidMount() {
    this.populateWithSelectedGroup();
  }

  componentDidUpdate(prevProps: FormProps) {
    if (prevProps.user !== this.props.user) {
      this.populateWithSelectedGroup();
    }
  }

  showAdvancedSettings(): boolean {
    return true;
  }

  private populateWithSelectedGroup() {
    const group = this.props.user.selectedGroup;

    const request = this.newRequest({
      id: group.id,
      name: group.name,
      urlIdentifier: group.urlIdentifier,
      autoPopulateFromOwnedDomain: Boolean(group.ownedDomain),
      sharingEnabled: group.sharingEnabled,
      userOwnedKeysEnabled: group.userOwnedKeysEnabled,
      botSuggestionsEnabled: group.botSuggestionsEnabled,
      codeSearchEnabled: group.codeSearchEnabled,
      developerOrgCreationEnabled: group.developerOrgCreationEnabled,
      useGroupOwnedExecutors: group.useGroupOwnedExecutors,
      suggestionPreference: group.suggestionPreference,
      restrictCleanWorkflowRunsToAdmins: group.restrictCleanWorkflowRunsToAdmins,
    });
    this.setState({ request, initialRequest: this.newRequest(request) });
  }

  newRequest(values?: Record<string, any>) {
    return new grp.UpdateGroupRequest(values);
  }
  async submitRequest() {
    await rpcService.service.updateGroup(this.state.request);
    await authService.refreshUser();
  }

  getInviteLink() {
    if (!this.state.initialRequest.urlIdentifier) {
      return "";
    }
    const port = window.location.port ? ":" + window.location.port : "";
    if (capabilities.config.subdomainsEnabled) {
      return `${this.state.initialRequest.urlIdentifier}.${capabilities.config.domain}${port}/join`;
    }
    return `${window.location.hostname}${port}/join/${this.state.initialRequest.urlIdentifier}`;
  }

  render() {
    const inviteLink = this.getInviteLink();

    return (
      <>
        <form
          autoComplete="off"
          className="organization-form organization-edit-form"
          onSubmit={this.onSubmit.bind(this)}>
          <div className="organization-details-title settings-option-title">Organization details</div>
          {inviteLink && (
            <div className="organization-invite-link-container">
              <Link />
              <div className="invite-link-label">Invite link: </div>
              <div className="invite-link">
                <a href={`//${inviteLink}`} target="_blank">
                  {inviteLink}
                </a>
              </div>
            </div>
          )}
          {this.renderFields()}
          <FilledButton
            disabled={!this.state.dirty || this.state.submitting}
            type="submit"
            className="organization-form-submit-button">
            Save
          </FilledButton>
          {this.renderError()}
          {this.state.submitted && !this.state.dirty && (
            <div className="form-success-message">Successfully saved changes.</div>
          )}
        </form>
      </>
    );
  }
}
