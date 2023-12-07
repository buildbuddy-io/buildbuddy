import { CheckCircle, Circle, ChevronUp, ChevronDown, LogOut, PlusCircle, ArrowRightCircle } from "lucide-react";
import React from "react";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import router from "../../../app/router/router";

interface Props {
  user?: User;
  floating?: boolean;
}
interface State {
  profileExpanded: boolean;
}

export default class OrgPicker extends React.Component<Props, State> {
  state: State = {
    profileExpanded: false,
  };

  async handleOrgClicked(groupId: string, groupURL: string) {
    if (this.props.user?.selectedGroup?.id === groupId) return;

    await authService.setSelectedGroupId(groupId, groupURL, { reload: true });
  }

  handleCreateOrgClicked(e: React.MouseEvent) {
    router.navigateToCreateOrg();
  }

  handleSearchGroupsClicked() {
    window.dispatchEvent(new CustomEvent("groupSearchClick"));
  }

  handleProfileClicked() {
    this.setState({ profileExpanded: !this.state.profileExpanded });
  }

  render() {
    if (!this.props.user) {
      return (
        <div className={`org-picker-container ${this.props.floating ? "floating" : ""}`}>
          <button
            className="login-button"
            onClick={() =>
              (window.location.href = `/?${new URLSearchParams({
                redirect_url: window.location.href,
              })}`)
            }>
            Login
          </button>
        </div>
      );
    }

    let name = this.props.user?.displayUser?.name?.full || this.props.user?.displayUser?.email;
    return (
      <div
        className={`org-picker-container ${this.state.profileExpanded ? "expanded" : ""} ${
          this.props.floating ? "floating" : ""
        }`}
        debug-id="org-picker">
        {this.state.profileExpanded && (
          <div className="org-picker-expanded-profile">
            <div className="org-picker">
              <div className="org-picker-header">Organization</div>
              <div className="org-list" role="menu">
                {this.props.user?.groups.map((group) => (
                  <div
                    key={group.id}
                    role="menuitem"
                    className={`org-picker-item org-picker-item ${
                      group.id === this.props.user?.selectedGroup.id ? "selected" : ""
                    }`}
                    onClick={this.handleOrgClicked.bind(this, group.id, group.url)}>
                    {group.id === this.props.user?.selectedGroup.id ? (
                      <CheckCircle className="icon" />
                    ) : (
                      <Circle className="icon" />
                    )}
                    <div className="org-picker-item-label">{group.name}</div>
                  </div>
                ))}
              </div>
              {this.props.user && router.canCreateOrg(this.props.user) && (
                <div className="org-picker-item create-organization" onClick={this.handleCreateOrgClicked.bind(this)}>
                  <PlusCircle className="icon" />
                  Create org
                </div>
              )}
              {Boolean(this.props.user?.canImpersonate()) && (
                <div className="org-picker-item admin-only" onClick={this.handleSearchGroupsClicked.bind(this)}>
                  <ArrowRightCircle className="icon" />
                  Go to org
                </div>
              )}
            </div>
            <hr />
            <div className="org-picker-item" debug-id="logout-button" onClick={() => authService.logout()}>
              <LogOut className="icon" /> Logout
            </div>
          </div>
        )}
        {capabilities.auth && this.props.user && (
          <div onClick={this.handleProfileClicked.bind(this)} className="org-picker-profile">
            <img
              className={`org-picker-profile-photo ${
                this.props.user?.displayUser?.profileImageUrl ? "" : "default-photo"
              }`}
              src={this.props.user?.displayUser?.profileImageUrl || "/image/user-regular.svg"}
            />
            <div className="org-picker-profile-name">
              <div className="org-picker-profile-user">{name}</div>
              {name != this.props.user?.selectedGroupName() && (
                <div className="org-picker-profile-org">{this.props.user?.selectedGroupName()}</div>
              )}
            </div>
            {this.state.profileExpanded ? (
              <ChevronDown className="icon org-picker-profile-arrow" />
            ) : (
              <ChevronUp className="icon org-picker-profile-arrow" />
            )}
          </div>
        )}
      </div>
    );
  }
}
