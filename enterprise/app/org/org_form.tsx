import React from "react";
import capabilities from "../../../app/capabilities/capabilities";
import { User } from "../../../app/auth/auth_service";
import { grp } from "../../../proto/group_ts_proto";
import { BuildBuddyError } from "../../../app/util/errors";
import { AlertCircle } from "lucide-react";
import Select, { Option } from "../../../app/components/select/select";

export type FormProps = {
  user: User;
};

type GroupRequest = Partial<grp.CreateGroupRequest & grp.UpdateGroupRequest>;

export type FormState<T extends GroupRequest> = {
  request: T;
  initialRequest: T;
  // Fields that have received focus at least once.
  touched: Set<string>;
  error?: BuildBuddyError;
  submitting?: boolean;
  submitted?: boolean;
  dirty?: boolean;
};

export default abstract class OrgForm<T extends GroupRequest> extends React.Component<FormProps, FormState<T>> {
  constructor(props: FormProps) {
    super(props);
    const request = this.newRequest();
    this.state = {
      touched: new Set(),
      initialRequest: this.newRequest(request),
      request,
    };
  }

  abstract newRequest(values?: Record<string, any>): T;
  abstract submitRequest(): void;
  abstract showSuggestionPreference(): boolean;

  async onSubmit(e: any) {
    e.preventDefault();

    this.setState({ submitting: true, error: null });
    try {
      await this.submitRequest();
      this.setState({
        submitted: true,
        dirty: false,
        touched: new Set(),
        initialRequest: Object.assign(this.newRequest(), this.state.request),
      });
    } catch (error) {
      this.setState({ error: BuildBuddyError.parse(error) });
    } finally {
      this.setState({ submitting: false });
    }
  }
  onFocus(e: React.FocusEvent) {
    const name = (e.target as HTMLInputElement).name;
    this.setState({ touched: new Set([...this.state.touched, name]) });
  }
  onChange(e: React.ChangeEvent<HTMLInputElement>) {
    const { name, value } = getChangedFormState(e);
    this.setFieldValue(name, value);
  }
  onChangeName(e: React.ChangeEvent<HTMLInputElement>) {
    return this.onChange(e);
  }
  onChangeUrlIdentifier(e: React.ChangeEvent<HTMLInputElement>) {
    const { name, value } = getChangedFormState(e);
    this.setFieldValue(name, makeSlug(value as string));
  }
  onChangeSuggestionPreference(e: React.ChangeEvent<HTMLSelectElement>) {
    const { name, value } = getChangedFormState(e);
    this.setFieldValue(name, Number(value) as grp.SuggestionPreference);
  }

  setFieldValue(name: string, value: any) {
    const request = this.state.request;
    (request as Record<string, any>)[name] = value;

    this.setState({
      request,
      dirty: true,
    });
  }

  renderError() {
    return this.state.error && <div className="form-error">{this.state.error.description}</div>;
  }

  renderFields() {
    const { request, initialRequest } = this.state;
    const domain =
      this.props.user.selectedGroup?.id == (request as grp.UpdateGroupRequest)?.id
        ? this.props.user.selectedGroup?.ownedDomain || getDomainFromEmail(this.props.user.displayUser.email)
        : getDomainFromEmail(this.props.user.displayUser.email);
    return (
      <>
        <div className="form-row stacked">
          <label htmlFor="name" className="input-label">
            Organization name
          </label>
          <input
            autoComplete="off"
            onFocus={this.onFocus.bind(this)}
            onChange={this.onChangeName.bind(this)}
            type="text"
            name="name"
            value={request.name}
          />
        </div>
        <div className="form-row stacked">
          <label htmlFor="urlIdentifier" className="input-label">
            Organization URL
          </label>
          <div className="input-help-text">May contain lowercase letters, numbers, or hyphens (-)</div>
          <div className="url-input-row">
            <span>
              {window.location.hostname}
              {window.location.port && `:${window.location.port}`}/join/
            </span>
            <input
              autoComplete="off"
              onFocus={this.onFocus.bind(this)}
              onChange={this.onChangeUrlIdentifier.bind(this)}
              type="text"
              name="urlIdentifier"
              value={request.urlIdentifier}
            />
          </div>
          {initialRequest.urlIdentifier && initialRequest.urlIdentifier !== request.urlIdentifier && (
            <div className="warning">
              <AlertCircle className="icon red" />
              <div>
                This change will deactivate the old URL. <br />
                Be sure to update any links in docs, bookmarks, etc.
              </div>
            </div>
          )}
        </div>
        {this.showSuggestionPreference() && (
          <div className="form-row stacked">
            <label>Build suggestions</label>
            <div className="input-help-text">Show diagnostics and improvements on builds within this org</div>
            <Select
              name="suggestionPreference"
              value={request.suggestionPreference}
              onChange={this.onChangeSuggestionPreference.bind(this)}>
              <Option value={grp.SuggestionPreference.ENABLED}>Enabled</Option>
              <Option value={grp.SuggestionPreference.ADMINS_ONLY}>Enabled for admins only</Option>
              <Option value={grp.SuggestionPreference.DISABLED}>Disabled</Option>
            </Select>
          </div>
        )}
        <label className="form-row input-label">
          <input
            autoComplete="off"
            onFocus={this.onFocus.bind(this)}
            onChange={this.onChange.bind(this)}
            type="checkbox"
            name="autoPopulateFromOwnedDomain"
            checked={request.autoPopulateFromOwnedDomain}
          />
          <span>
            Automatically add anyone with an <span className="bold">@{domain}</span> email address to this organization
          </span>
        </label>
        {capabilities.invocationSharing && (
          <label className="form-row input-label">
            <input
              autoComplete="off"
              onFocus={this.onFocus.bind(this)}
              onChange={this.onChange.bind(this)}
              type="checkbox"
              name="sharingEnabled"
              checked={request.sharingEnabled}
            />
            <span>Allow members of this org to make builds public (viewable by anyone with a link)</span>
          </label>
        )}
        {capabilities.userOwnedExecutors && (
          <label className="form-row input-label">
            <input
              autoComplete="off"
              onFocus={this.onFocus.bind(this)}
              onChange={this.onChange.bind(this)}
              type="checkbox"
              name="useGroupOwnedExecutors"
              checked={request.useGroupOwnedExecutors}
            />
            <span>Default to self-hosted executors</span>
          </label>
        )}
      </>
    );
  }
}

export function getChangedFormState(changeEvent: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) {
  const input = changeEvent.target;

  const name = input.name;
  const value = input.type === "checkbox" ? (input as HTMLInputElement).checked : input.value;

  return { name, value };
}

function getDomainFromEmail(email: string) {
  return email.split("@").pop();
}

export function makeSlug(value: string) {
  return (
    value
      .toLowerCase()
      // Don't allow hyphens at the start
      .replace(/^\-/g, "")
      // Replace {spaces, '.', '_', '/'} with hyphens
      .replace(/(\s|[\._\/])/g, "-")
      // Prevent multiple consecutive hyphens
      .replace(/\\-{2,}/g, "-")
      // Forbid characters other than a-z, numbers, or hyphen
      .replace(/[^a-z0-9\\-]/g, "")
  );
}
