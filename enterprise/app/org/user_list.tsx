import { HelpCircle, ShieldCheck, UserCircle } from "lucide-react";
import React from "react";
import { accountName, User } from "../../../app/auth/user";
import Banner from "../../../app/components/banner/banner";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import { GithubIcon } from "../../../app/icons/github";
import { GoogleIcon } from "../../../app/icons/google";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";

export function iconFromAccountType(accountType: user_id.AccountType | undefined) {
  switch (accountType) {
    case user_id.AccountType.GOOGLE:
      return <GoogleIcon />;
    case user_id.AccountType.GITHUB:
      return <GithubIcon />;
    case user_id.AccountType.SAML:
      return <ShieldCheck />;
    case user_id.AccountType.OIDC:
      return <UserCircle />;
    default:
      return <HelpCircle />;
  }
}

export function getRoleLabel(role: grp.Group.Role): string {
  switch (role) {
    case grp.Group.Role.ADMIN_ROLE:
      return "Admin";
    case grp.Group.Role.DEVELOPER_ROLE:
      return "Developer";
    case grp.Group.Role.WRITER_ROLE:
      return "Writer";
    case grp.Group.Role.READER_ROLE:
      return "Reader";
    default:
      return "";
  }
}

export class UserListUser {
  user: user_id.DisplayUser;
  role?: grp.Group.Role;

  constructor(user: user_id.DisplayUser, role?: grp.Group.Role) {
    this.user = user;
    this.role = role;
  }
}

export type OrgUserListProps = {
  user: User;
  users: UserListUser[];
  buttons?: React.ReactNode[];
  onButtonClick?: (buttonIndex: number, selectedUserIds: Set<string>) => void;
  showRole: boolean;
  readOnly?: boolean;
  maxRows?: number;
};

type State = {
  isSelectingAll?: boolean;
  selectedUserIds: Set<string>;

  showAll: boolean;
};

export default class UserListComponent extends React.Component<OrgUserListProps, State> {
  state: State = {
    selectedUserIds: new Set<string>(),
    showAll: false,
  };

  private onClickRow(userID: string) {
    if (this.props.readOnly) {
      return;
    }
    const clone = new Set(this.state.selectedUserIds);
    if (clone.has(userID)) {
      clone.delete(userID);
    } else {
      clone.add(userID);
    }
    this.setState({
      isSelectingAll: (this.state.isSelectingAll && clone.size > 0) || clone.size === this.props.users?.length,
      selectedUserIds: clone,
    });
  }

  private onClickSelectAllToggle() {
    if (this.state.isSelectingAll) {
      this.setState({
        isSelectingAll: false,
        selectedUserIds: new Set(),
      });
    } else {
      this.setState({
        isSelectingAll: true,
        selectedUserIds: new Set((this.props.users || []).map((member) => member.user?.userId?.id || "")),
      });
    }
  }

  private onClickShowAllMembers() {
    this.setState({ showAll: true });
  }

  private isLoggedInUser(member: grp.GetGroupUsersResponse.IGroupUser) {
    return member?.user?.userId?.id === this.props.user.displayUser?.userId?.id;
  }

  private getSelectedMembers(): grp.GetGroupUsersResponse.IGroupUser[] {
    return (this.props.users || []).filter((member) => this.state.selectedUserIds.has(member.user?.userId?.id || ""));
  }

  render() {
    const isSelectionEmpty = this.state.selectedUserIds.size === 0;

    let users = this.props.users;
    let numOverflow = 0;
    if (this.props.maxRows && users.length > this.props.maxRows && !this.state.showAll) {
      numOverflow = users.length - this.props.maxRows;
      users = users.slice(0, this.props.maxRows);
    }

    return (
      <div className="org-members">
        {!this.props.readOnly && (
          <div className="org-members-list-controls">
            {this.props.user.selectedGroup.externalUserManagement && (
              <div>
                <Banner type="warning" className="user-management-warning">
                  Users are being managed via an external system. All changes must be made there.
                </Banner>
              </div>
            )}
            {!this.props.readOnly && (
              <>
                <CheckboxButton
                  className="select-all-button"
                  checked={this.state.isSelectingAll}
                  onClick={this.onClickSelectAllToggle.bind(this)}
                  checkboxOnLeft>
                  Select all
                </CheckboxButton>
                {(this.props.buttons || []).map((button, index) => (
                  <div key={index} onClick={() => this.props.onButtonClick?.(index, this.state.selectedUserIds)}>
                    {React.isValidElement(button)
                      ? React.cloneElement(button, { disabled: isSelectionEmpty } as any)
                      : button}
                  </div>
                ))}
              </>
            )}
          </div>
        )}
        <div className="org-members-list">
          {users.map((member) => (
            <div
              className={`org-members-list-item ${
                this.state.selectedUserIds.has(member?.user?.userId?.id || "") ? "selected" : ""
              } ${!this.props.readOnly ? "editable" : ""}`}
              onClick={() => this.onClickRow(member?.user?.userId?.id || "")}>
              {!this.props.readOnly && (
                <div>
                  <Checkbox
                    title={`Select ${accountName(member.user)}`}
                    className="org-member-checkbox"
                    checked={this.state.selectedUserIds.has(member?.user?.userId?.id || "")}
                  />
                </div>
              )}
              <div className="org-member-name">
                {accountName(member.user)} {iconFromAccountType(member.user?.accountType)}
              </div>
              {this.props.showRole && (
                <div className="org-member-role">
                  {getRoleLabel(member?.role || 0)} {this.isLoggedInUser(member) && <>(You)</>}
                </div>
              )}
            </div>
          ))}
          {numOverflow > 0 && (
            <div className="org-member-more-rows-link" onClick={this.onClickShowAllMembers.bind(this)}>
              ... and {numOverflow} more
            </div>
          )}
        </div>
      </div>
    );
  }
}
