import React from "react";
import { AlertCircle } from "lucide-react";
import { User } from "../../../app/auth/auth_service";
import rpc_service from "../../../app/service/rpc_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import ApiKeysComponent from "../api_keys/api_keys";
import EditOrgComponent from "../org/edit_org";
import OrgMembersComponent from "../org/org_members";
import SecretsComponent from "../secrets/secrets";
import router from "../../../app/router/router";
import UserPreferences from "../../../app/preferences/preferences";
import GitHubLink from "./github_link";
import QuotaComponent from "../quota/quota";
import UserGitHubLink from "./user_github_link";
import Banner from "../../../app/components/banner/banner";
import Link from "../../../app/components/link/link";
import CompleteGitHubAppInstallationDialog from "./github_complete_installation";
import EncryptionComponent from "../encryption/encryption";

export interface SettingsProps {
  user: User;
  preferences: UserPreferences;
  path: string;
  search: URLSearchParams;
}

enum TabId {
  OrgDetails = "org/details",
  OrgMembers = "org/members",
  OrgGitHub = "org/github",
  OrgApiKeys = "org/api-keys",
  OrgSecrets = "org/secrets",
  OrgCacheEncryption = "org/cache-encryption",

  PersonalPreferences = "personal/preferences",
  PersonalApiKeys = "personal/api-keys",
  PersonalGitHubLink = "personal/github",

  ServerQuota = "server/quota",
}

const TAB_IDS = new Set<string>(Object.values(TabId));

const CLI_LOGIN_PATH = "/settings/cli-login";

function isTabId(id: string): id is TabId {
  return TAB_IDS.has(id);
}

export default class SettingsComponent extends React.Component<SettingsProps> {
  componentWillMount() {
    document.title = `Settings | BuildBuddy`;

    // Handle the redirect for CLI login.
    if (this.isCLILoginPath()) {
      if (capabilities.config.userOwnedKeysEnabled && this.props.user?.selectedGroup?.userOwnedKeysEnabled) {
        router.replaceURL("/settings/personal/api-keys?cli-login=1");
      } else {
        router.replaceURL("/settings/org/api-keys?cli-login=1");
      }
    }
  }

  private isCLILogin() {
    return this.props.search.get("cli-login") === "1";
  }

  private isCLILoginPath() {
    return this.props.path === CLI_LOGIN_PATH || this.props.path === CLI_LOGIN_PATH + "/";
  }

  private getDefaultTabId(): TabId {
    if (router.canAccessOrgDetailsPage(this.props.user)) {
      return TabId.OrgDetails;
    }
    return TabId.OrgApiKeys;
  }

  private getActiveTabId(): TabId {
    if (this.props.path === "/settings" || this.props.path === "/settings/") {
      return this.getDefaultTabId();
    }
    const path = this.props.path.substring("/settings/".length);
    if (isTabId(path)) {
      return path;
    }
    // If the path is nested under a tab, like "server/quota/edit/:namespace:", return the
    // parent tab "server/quota"
    for (const pathPrefix of Object.values(TabId)) {
      if (path.startsWith(pathPrefix + "/")) {
        return pathPrefix as TabId;
      }
    }
    return this.getDefaultTabId();
  }

  render() {
    if (this.isCLILoginPath()) {
      return null;
    }

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
                <div className="settings-tab-group-subtitle">{this.props.user.selectedGroupName()}</div>
              </div>
              <div className="settings-tab-group">
                {router.canAccessOrgDetailsPage(this.props.user) && (
                  <SettingsTab id={TabId.OrgDetails} activeTabId={activeTabId}>
                    Org details
                  </SettingsTab>
                )}
                {router.canAccessOrgMembersPage(this.props.user) && (
                  <SettingsTab id={TabId.OrgMembers} activeTabId={activeTabId}>
                    Members
                  </SettingsTab>
                )}
                {router.canAccessOrgGitHubLinkPage(this.props.user) &&
                  (capabilities.github || capabilities.config.githubAppEnabled) && (
                    <SettingsTab id={TabId.OrgGitHub} activeTabId={activeTabId}>
                      <span>GitHub link</span>
                      {/* If the user has a group-level GitHub link and the new GitHub App is
                        enabled, show a deprecation alert. */}
                      {capabilities.config.githubAppEnabled && this.props.user.selectedGroup.githubLinked && (
                        <AlertCircle className="icon orange" />
                      )}
                    </SettingsTab>
                  )}
                <SettingsTab id={TabId.OrgApiKeys} activeTabId={activeTabId}>
                  Org API keys
                </SettingsTab>
                {capabilities.config.secretsEnabled && router.canAccessOrgSecretsPage(this.props.user) && (
                  <SettingsTab id={TabId.OrgSecrets} activeTabId={activeTabId}>
                    Secrets
                  </SettingsTab>
                )}
                {capabilities.config.customerManagedEncryptionKeysEnabled &&
                  router.canAccessEncryptionPage(this.props.user) && (
                    <SettingsTab id={TabId.OrgCacheEncryption} activeTabId={activeTabId}>
                      Encryption keys
                    </SettingsTab>
                  )}
              </div>
              <div className="settings-tab-group-header">
                <div className="settings-tab-group-title">Personal settings</div>
                <div className="settings-tab-group-subtitle">{this.props.user.displayUser.name?.full}</div>
              </div>
              <div className="settings-tab-group">
                <SettingsTab id={TabId.PersonalPreferences} activeTabId={activeTabId} debugId="personal-preferences">
                  Preferences
                </SettingsTab>
                {this.props.user?.selectedGroup?.userOwnedKeysEnabled && (
                  <SettingsTab id={TabId.PersonalApiKeys} activeTabId={activeTabId}>
                    Personal API keys
                  </SettingsTab>
                )}
                {capabilities.config.githubAppEnabled && (
                  <SettingsTab id={TabId.PersonalGitHubLink} activeTabId={activeTabId}>
                    GitHub account link
                  </SettingsTab>
                )}
              </div>
              {this.props.user.canCall("getNamespace") && capabilities.config.quotaManagementEnabled && (
                <>
                  <div className="settings-tab-group-header">
                    <div className="settings-tab-group-title">Server settings</div>
                  </div>
                  <div className="settings-tab-group">
                    <SettingsTab id={TabId.ServerQuota} activeTabId={activeTabId}>
                      Quota
                    </SettingsTab>
                  </div>
                </>
              )}
            </div>
            <div className="settings-content">
              {activeTabId === "personal/preferences" && (
                <>
                  <div className="settings-option-title">Dense mode</div>
                  <div className="settings-option-description">
                    Dense mode packs more information density into the BuildBuddy UI.
                  </div>
                  <FilledButton className="settings-button" onClick={() => this.props.preferences.toggleDenseMode()}>
                    {this.props.preferences.denseModeEnabled ? "Disable" : "Enable"} dense mode
                  </FilledButton>
                  <div className="settings-option-title">Log viewer theme</div>
                  <div className="settings-option-description">
                    The log viewer theme allows you to switch between a light and dark log viewer.
                  </div>
                  <FilledButton
                    className="settings-button"
                    onClick={() => this.props.preferences.toggleLightTerminal()}>
                    Switch to {this.props.preferences.lightTerminalEnabled ? "dark" : "light"} log viewer theme
                  </FilledButton>
                  <div className="settings-option-title">Keyboard Shortcuts</div>
                  <div className="settings-option-description">
                    Enables keyboard shortcuts. Hit '?' for help when enabled.
                  </div>
                  <FilledButton
                    className="settings-button"
                    onClick={() => this.props.preferences.toggleKeyboardShortcuts()}
                    debug-id="keyboard-shortcuts-button">
                    {this.props.preferences.keyboardShortcutsEnabled ? "Disable" : "Enable"} keyboard shortcuts
                  </FilledButton>
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
                        <div className="settings-section-subtitle">{this.props.user.selectedGroupName()}</div>
                      )}
                      {capabilities.createOrg && <EditOrgComponent user={this.props.user} />}
                    </>
                  )}
                  {activeTabId === TabId.OrgMembers && capabilities.userManagement && (
                    <>
                      <div className="settings-option-title">Members of {this.props.user.selectedGroupName()}</div>
                      <OrgMembersComponent user={this.props.user} />
                    </>
                  )}
                  {activeTabId === TabId.OrgGitHub &&
                    (capabilities.github || capabilities.config.githubAppEnabled) &&
                    this.props.user.canCall("unlinkGitHubAccount") && (
                      <GitHubLink user={this.props.user} path={this.props.path} />
                    )}
                  {this.props.path === "/settings/org/github/complete-installation" && (
                    <CompleteGitHubAppInstallationDialog user={this.props.user} search={this.props.search} />
                  )}
                  {activeTabId === TabId.PersonalGitHubLink && capabilities.config.githubAppEnabled && (
                    <UserGitHubLink user={this.props.user} />
                  )}
                  {activeTabId === TabId.OrgApiKeys && capabilities.manageApiKeys && (
                    <>
                      <div className="settings-option-title">Org API keys</div>
                      <div className="settings-option-description">
                        API keys grant access to your BuildBuddy organization.
                      </div>
                      {this.isCLILogin() && (
                        <>
                          <div className="settings-option-description">
                            <Banner type="info">
                              {capabilities.config.userOwnedKeysEnabled && (
                                <>
                                  To log in as <b>{this.props.user.displayUser.email}</b>, an organization administrator
                                  must enable user-owned API keys in Org details.{" "}
                                </>
                              )}
                              To log in as the organization <b>{this.props.user.selectedGroupName()}</b>, copy one of
                              the keys below and paste it back into the login prompt.
                            </Banner>
                          </div>
                        </>
                      )}
                      <ApiKeysComponent
                        user={this.props.user}
                        get={rpc_service.service.getApiKeys}
                        create={rpc_service.service.createApiKey}
                        update={rpc_service.service.updateApiKey}
                        delete={rpc_service.service.deleteApiKey}
                      />
                    </>
                  )}
                  {activeTabId === TabId.PersonalApiKeys &&
                    capabilities.manageApiKeys &&
                    this.props.user?.selectedGroup?.userOwnedKeysEnabled && (
                      <>
                        <div className="settings-option-title">Personal API keys</div>
                        <div className="settings-option-description">
                          Personal API keys associate builds with both your individual user account and your
                          organization.
                        </div>
                        {this.isCLILogin() && (
                          <div className="settings-option-description">
                            <Banner type="info">
                              To login, copy one of the API keys below and paste it back into the login prompt.
                            </Banner>
                          </div>
                        )}
                        <ApiKeysComponent
                          user={this.props.user}
                          userOwnedOnly
                          get={rpc_service.service.getUserApiKeys}
                          create={rpc_service.service.createUserApiKey}
                          update={rpc_service.service.updateUserApiKey}
                          delete={rpc_service.service.deleteUserApiKey}
                        />
                      </>
                    )}
                  {activeTabId === TabId.OrgSecrets && capabilities.config.secretsEnabled && (
                    <SecretsComponent path={this.props.path} search={this.props.search} />
                  )}
                  {activeTabId === TabId.ServerQuota && capabilities.config.quotaManagementEnabled && (
                    <QuotaComponent path={this.props.path} search={this.props.search} />
                  )}
                  {activeTabId == TabId.OrgCacheEncryption && (
                    <>
                      <div className="settings-option-title">Encryption keys</div>
                      <div className="settings-option-description">
                        Encryption keys allow control over storage encryption.
                      </div>
                      <EncryptionComponent />
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
  debugId?: string;
};

class SettingsTab extends React.Component<SettingsTabProps> {
  render() {
    return (
      <Link
        className={`settings-tab ${this.props.activeTabId === this.props.id ? "active-tab" : ""}`}
        href={`/settings/${this.props.id}`}
        debug-id={this.props.debugId}>
        {this.props.children}
      </Link>
    );
  }
}
