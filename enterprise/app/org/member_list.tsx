import { HelpCircle, ShieldCheck, UserCircle, Users } from "lucide-react";
import React from "react";
import { accountName, User } from "../../../app/auth/user";
import Banner from "../../../app/components/banner/banner";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import { GithubIcon } from "../../../app/icons/github";
import { GoogleIcon } from "../../../app/icons/google";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";
import { user_list } from "../../../proto/user_list_ts_proto";

export function iconFromAccountType(accountType: user_id.AccountType | undefined): JSX.Element {
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

export class MemberListMember {
  // One of user or userList will be set.
  user?: user_id.DisplayUser;
  userList?: user_list.UserList;
  role?: grp.Group.Role;

  constructor(user?: user_id.DisplayUser, userList?: user_list.UserList, role?: grp.Group.Role) {
    this.user = user;
    this.userList = userList;
    this.role = role;
  }

  id(): string {
    if (this.user) {
      return this.user.userId?.id || "";
    } else if (this.userList) {
      return this.userList.userListId || "";
    }
    return "";
  }

  displayName(): React.ReactNode {
    if (this.user) {
      return accountName(this.user);
    } else if (this.userList) {
      const suffix = this.userList.user.length == 1 ? "user" : "users";
      return (
        <>
          {this.userList.name}
          <span className="org-member-user-list-count">
            ({this.userList.user.length} {suffix})
          </span>
        </>
      );
    } else {
      return "unknown member type";
    }
  }

  icon(): JSX.Element {
    if (this.user) {
      return iconFromAccountType(this.user.accountType);
    } else if (this.userList) {
      return <Users />;
    } else {
      return <HelpCircle />;
    }
  }
}

export type MemberListProps = {
  user: User;
  members: MemberListMember[];
  buttons?: React.ReactNode[];
  onButtonClick?: (buttonIndex: number, selectedMembers: Map<string, MemberListMember>) => void;
  showRole: boolean;
  readOnly?: boolean;
  maxRows?: number;
};

type State = {
  isSelectingAll?: boolean;
  selectedMembers: Map<string, MemberListMember>;

  showAll: boolean;
};

export default class MemberListComponent extends React.Component<MemberListProps, State> {
  state: State = {
    selectedMembers: new Map<string, MemberListMember>(),
    showAll: false,
  };

  private onClickRow(member: MemberListMember): void {
    if (this.props.readOnly) {
      return;
    }
    const id = member.id();
    const clone = new Map(this.state.selectedMembers);
    if (clone.has(id)) {
      clone.delete(id);
    } else {
      clone.set(id, member);
    }
    this.setState({
      isSelectingAll: (this.state.isSelectingAll && clone.size > 0) || clone.size === this.props.members?.length,
      selectedMembers: clone,
    });
  }

  private onClickSelectAllToggle(): void {
    if (this.state.isSelectingAll) {
      this.setState({
        isSelectingAll: false,
        selectedMembers: new Map(),
      });
    } else {
      this.setState({
        isSelectingAll: true,
        selectedMembers: new Map((this.props.members || []).map((member) => [member.id(), member])),
      });
    }
  }

  private onClickShowAllMembers(): void {
    this.setState({ showAll: true });
  }

  private isLoggedInUser(member: grp.GetGroupUsersResponse.IGroupUser): boolean {
    return member?.user?.userId?.id === this.props.user.displayUser?.userId?.id;
  }

  private onButtonClick(buttonIdx: number): void {
    // selectedMembers is stored in the order that the user selected the items.
    // We refilter the original members list here so that we can return
    // the selected items in the same order that they appear in the list.
    this.props.onButtonClick?.(
      buttonIdx,
      new Map(this.props.members.filter((m) => this.state.selectedMembers.has(m.id())).map((u) => [u.id(), u]))
    );
  }

  render(): React.ReactNode {
    const isSelectionEmpty = this.state.selectedMembers.size === 0;

    let users = this.props.members;
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
                  <div key={index} onClick={this.onButtonClick.bind(this, index)}>
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
                this.state.selectedMembers.has(member.id()) ? "selected" : ""
              } ${!this.props.readOnly ? "editable" : ""}`}
              onClick={() => this.onClickRow(member)}>
              {!this.props.readOnly && (
                <div>
                  <Checkbox
                    title={`Select ${member.displayName()}`}
                    className="org-member-checkbox"
                    checked={this.state.selectedMembers.has(member.id())}
                  />
                </div>
              )}
              <div className="org-member-name">
                {member.displayName()} {member.icon()}
              </div>
              {this.props.showRole && (
                <div className="org-member-role">
                  {member.role === undefined && <div className="org-member-role-unassigned">Unassigned</div>}
                  {member.role !== undefined && getRoleLabel(member.role || 0)}{" "}
                  {this.isLoggedInUser(member) && <>(You)</>}
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
