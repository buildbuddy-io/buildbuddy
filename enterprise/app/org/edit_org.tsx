import React from "react";
import authService from "../../../app/auth/auth_service";
import FilledButton from "../../../app/components/button/button";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import OrgForm, { FormProps } from "./org_form";
import { Link } from "lucide-react";

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

  showSuggestionPreference(): boolean {
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
      useGroupOwnedExecutors: group.useGroupOwnedExecutors,
      suggestionPreference: group.suggestionPreference,
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

  render() {
    const inviteLink = this.state.initialRequest.urlIdentifier
      ? `${window.location.hostname}${window.location.port ? ":" + window.location.port : ""}/join/${
          this.state.initialRequest.urlIdentifier
        }`
      : "";

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
