import React from "react";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import ApiKeysComponent from "../api_keys/api_keys";
import EditOrgComponent from "../org/edit_org";
import router from "../../../app/router/router";

export interface SettingsProps {
  user: User;
  denseModeEnabled: boolean;
  handleDenseModeToggled: VoidFunction;
  path: string;
}

enum TabId {
  OrgDetails = "org/details",
  OrgGitHub = "org/github",
  OrgApiKeys = "org/api-keys",
  PersonalPreferences = "personal/preferences",
}

const TAB_IDS = new Set<string>(Object.values(TabId));

function isTabId(id: string): id is TabId {
  return TAB_IDS.has(id);
}

const DEFAULT_TAB_ID = TabId.OrgDetails;

export default class SettingsComponent extends React.Component<SettingsProps> {
  componentWillMount() {
    document.title = `Settings | BuildBuddy`;
  }

  private getActiveTabId(): TabId {
    if (this.props.path === "/settings" || this.props.path === "/settings/") {
      return DEFAULT_TAB_ID;
    }
    const path = this.props.path.substring("/settings/".length);
    if (!isTabId(path)) {
      return DEFAULT_TAB_ID;
    }
    return path;
  }

  render() {
    const activeTabId = this.getActiveTabId();

    return (
      <div className="settings">
        <div className="shelf">
          <div className="container">
            <div className="title settings-title">Settings</div>
          </div>
        </div>
        <div className="container">
          <div className="settings-layout">
            <div className="settings-tabs">
              <div className="settings-tab-group-header">
                <div className="settings-tab-group-title">Organization settings</div>
                <div className="settings-tab-group-subtitle">{this.props.user?.selectedGroupName()}</div>
              </div>
              <div className="settings-tab-group">
                <SettingsTab id={TabId.OrgDetails} activeTabId={activeTabId}>
                  Org details
                </SettingsTab>
                <SettingsTab id={TabId.OrgGitHub} activeTabId={activeTabId}>
                  GitHub link
                </SettingsTab>
                <SettingsTab id={TabId.OrgApiKeys} activeTabId={activeTabId}>
                  API keys
                </SettingsTab>
              </div>
              <div className="settings-tab-group-header">
                <div className="settings-tab-group-title">Personal settings</div>
                <div className="settings-tab-group-subtitle">{this.props.user?.displayUser?.name?.full}</div>
              </div>
              <div className="settings-tab-group">
                <SettingsTab id={TabId.PersonalPreferences} activeTabId={activeTabId}>
                  Preferences
                </SettingsTab>
              </div>
            </div>
            <div className="settings-content">
              {activeTabId === "personal/preferences" && (
                <>
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
                </>
              )}
              {capabilities.auth && this.props.user && (
                <>
                  {activeTabId === TabId.OrgDetails && (
                    <>
                      {
                        // Don't show the org name subtitle when the "edit org" form is present,
                        // since the "name" field plays this role.
                      }
                      {!capabilities.createOrg && (
                        <div className="settings-section-subtitle">{this.props.user?.selectedGroupName()}</div>
                      )}
                      {capabilities.createOrg && <EditOrgComponent user={this.props.user} />}
                    </>
                  )}
                  {activeTabId === TabId.OrgGitHub && capabilities.github && (
                    <>
                      <div className="settings-option-title">GitHub account link</div>
                      <div className="settings-option-description">
                        Linking a GitHub account allows BuildBuddy to report commit statuses that appear in the GitHub
                        UI.
                      </div>
                      {this.props.user.selectedGroup.githubLinked ? (
                        <FilledButton className="settings-button success">GitHub account linked</FilledButton>
                      ) : (
                        <FilledButton className="settings-button settings-link-button">
                          <a href="/auth/github/link/">Link GitHub account</a>
                        </FilledButton>
                      )}
                    </>
                  )}
                  {activeTabId === TabId.OrgApiKeys && capabilities.manageApiKeys && (
                    <>
                      <div className="settings-option-title">API keys</div>
                      <div className="settings-option-description">
                        API keys grant access to your BuildBuddy organization.
                      </div>
                      <ApiKeysComponent user={this.props.user} />
                    </>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}

type SettingsTabProps = {
  id: TabId;
  activeTabId: TabId;
};

class SettingsTab extends React.Component<SettingsTabProps> {
  private handleClick(e: React.MouseEvent) {
    e.preventDefault();
    if (this.props.activeTabId === this.props.id) {
      return;
    }
    router.navigateTo((e.target as HTMLAnchorElement).getAttribute("href"));
  }

  render() {
    return (
      <a
        className={`settings-tab ${this.props.activeTabId === this.props.id ? "active-tab" : ""}`}
        href={`/settings/${this.props.id}`}
        onClick={this.handleClick.bind(this)}>
        {this.props.children}
      </a>
    );
  }
}
