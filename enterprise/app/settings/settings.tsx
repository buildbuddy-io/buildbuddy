import React from "react";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import ApiKeysComponent from "../api_keys/api_keys";
import EditOrgComponent from "../org/edit_org";

interface Props {
  user: User;
  denseModeEnabled: boolean;
  handleDenseModeToggled: VoidFunction;
}

export default class SettingsComponent extends React.Component {
  props: Props;

  componentWillMount() {
    document.title = `Settings | BuildBuddy`;
  }

  render() {
    return (
      <div className="settings">
        <div className="container">
          <div className="settings-title">Settings</div>
          <div className="settings-section">
            <div className="settings-section-title">Personal Settings</div>
            <div className="settings-section-subtitle">{this.props.user?.displayUser?.name?.full}</div>
            <div className="settings-option">
              <div className="settings-option-title">Dense mode</div>
              <div className="settings-option-description">
                Dense mode packs more information density into the BuildBuddy UI.
              </div>
              {this.props.denseModeEnabled ? (
                <FilledButton className="settings-button" onClick={this.props.handleDenseModeToggled.bind(this)}>
                  Disable dense mode
                </FilledButton>
              ) : (
                <FilledButton className="settings-button" onClick={this.props.handleDenseModeToggled.bind(this)}>
                  Enable dense mode
                </FilledButton>
              )}
            </div>
          </div>
          {capabilities.auth && this.props.user && (
            <div className="settings-section">
              <div className="settings-section-title">Organization Settings</div>
              {/* Don't show the org name subtitle when the "edit org" form is present,
                  since the "name" field plays this role. */}
              {!capabilities.createOrg && (
                <div className="settings-section-subtitle">{this.props.user?.selectedGroupName()}</div>
              )}
              {capabilities.createOrg && <EditOrgComponent user={this.props.user} />}
              {capabilities.github && (
                <div className="settings-option">
                  <div className="settings-option-title">GitHub account link</div>
                  <div className="settings-option-description">
                    Linking a GitHub account allows BuildBuddy to report commit statuses that appear in the GitHub UI.
                  </div>
                  {this.props.user.selectedGroup.githubLinked ? (
                    <FilledButton className="settings-button success">GitHub account linked</FilledButton>
                  ) : (
                    <FilledButton className="settings-button settings-link-button">
                      <a href="/auth/github/link/">Link GitHub account</a>
                    </FilledButton>
                  )}
                </div>
              )}
              {capabilities.manageApiKeys && (
                <div className="settings-option api-keys-section">
                  <div className="settings-option-title">API keys</div>
                  <div className="settings-option-description">
                    API keys grant access to your BuildBuddy organization.
                  </div>
                  <ApiKeysComponent user={this.props.user} />
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    );
  }
}
