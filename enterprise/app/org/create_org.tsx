import React from "react";
import authService from "../../../app/auth/auth_service";
import FilledButton from "../../../app/components/button/button";
import router, { Path } from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import OrgForm, { getChangedFormState, makeSlug } from "./org_form";

const DEFAULT_VALUES = new grp.CreateGroupRequest({
  sharingEnabled: true,
});

/**
 * Page that allows creating an org (internally called a "group").
 */
export default class CreateOrgComponent extends OrgForm<grp.CreateGroupRequest> {
  newRequest(values?: Record<string, any>) {
    return new grp.CreateGroupRequest({ ...DEFAULT_VALUES, ...values });
  }
  async submitRequest() {
    const { id } = await rpcService.service.createGroup(this.state.request);
    await authService.setSelectedGroupId(id);
    router.navigateTo(Path.settingsPath);
  }

  showSuggestionPreference(): boolean {
    return false;
  }

  onChangeName(e: React.ChangeEvent<HTMLInputElement>) {
    const { name, value } = getChangedFormState(e);
    // Auto-populate the slug field from the opg name if the slug field
    // hasn't yet been touched (we don't want to modify any existing input
    // in that field which may have been intentional).
    if (!this.state.touched.has("urlIdentifier")) {
      this.setFieldValue("urlIdentifier", stripTrailingHyphens(makeSlug((value as string).trim())));
    }
    this.setFieldValue(name, value);
  }

  render() {
    return (
      <div className="organization-page">
        <div className="container">
          <div className="organization-page-title">Create organization</div>
          {this.props.user && !Boolean(this.props.user.groups?.length) && (
            <p className="callout">You are logged in, but not part of any organization. Create one to continue.</p>
          )}
          <form autoComplete="off" className="organization-form" onSubmit={this.onSubmit.bind(this)}>
            {this.renderFields()}
            <FilledButton
              disabled={!this.state.dirty || this.state.submitting || this.state.submitted}
              type="submit"
              className="organization-form-submit-button">
              Create
            </FilledButton>
            {this.renderError()}
            {this.state.submitted && (
              <div className="form-success-message">
                Successfully created <span className="bold">{this.state.request.name}</span>.
              </div>
            )}
          </form>
        </div>
      </div>
    );
  }
}

function stripTrailingHyphens(value: string) {
  return value.replace(/-$/, "");
}
