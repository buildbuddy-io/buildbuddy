import React from "react";
import { User } from "../../../app/auth/auth_service";
import Button from "../../../app/components/button/button";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";

export type OrgMembersProps = {
  user: User;
};

type State = {
  loading?: boolean;
  response?: grp.GetGroupUsersResponse;

  isSelectingAll?: boolean;
  selectedUserIds: Set<string>;
};

export default class OrgMembersComponent extends React.Component<OrgMembersProps, State> {
  state: State = {
    loading: true,
    selectedUserIds: new Set<string>(),
  };

  componentDidMount() {
    this.setState({ loading: true });
    rpcService.service
      .getGroupUsers(
        new grp.GetGroupUsersRequest({
          groupId: this.props.user.selectedGroup.id,
          // Only show existing members in this table for now.
          // TODO(bduffany): render 2 separate tables; one for membership
          // requests and one for existing members.
          groupMembershipStatus: [grp.GroupMembershipStatus.MEMBER],
        })
      )
      .then((response) => this.setState({ response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  private onClickRow(userID: string) {
    const clone = new Set(this.state.selectedUserIds);
    if (clone.has(userID)) {
      clone.delete(userID);
    } else {
      clone.add(userID);
    }
    this.setState({
      isSelectingAll: (this.state.isSelectingAll && clone.size > 0) || clone.size === this.state.response.user.length,
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
        selectedUserIds: new Set(this.state.response.user.map((member) => member.user.userId.id)),
      });
    }
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.response) return null;

    const isSelectionEmpty = this.state.selectedUserIds.size === 0;

    return (
      <div className="org-members">
        <div className="org-members-list-controls">
          <CheckboxButton
            className="select-all-button"
            checked={this.state.isSelectingAll}
            onClick={this.onClickSelectAllToggle.bind(this)}
            checkboxOnLeft>
            Select all
          </CheckboxButton>

          <Button disabled={isSelectionEmpty}>Edit role</Button>
          <Button disabled={isSelectionEmpty} className="destructive org-member-remove-button">
            Remove
          </Button>
        </div>
        <div className="org-members-list">
          {this.state.response.user.map((member) => (
            <div
              className={`org-members-list-item ${
                this.state.selectedUserIds.has(member.user.userId.id) ? "selected" : ""
              }`}
              onClick={this.onClickRow.bind(this, member.user.userId.id)}>
              <div>
                <Checkbox
                  title={`Select ${member.user.email}`}
                  className="org-member-checkbox"
                  checked={this.state.selectedUserIds.has(member.user.userId.id)}
                />
              </div>
              <div className="org-member-email">{member.user.email}</div>
              {/* TODO(bduffany): Read from the role field once it's implemented. */}
              <div className="org-member-role">Admin</div>
            </div>
          ))}
        </div>
      </div>
    );
  }
}
