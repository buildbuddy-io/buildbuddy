import React from "react";
import { User } from "../../../app/auth/user";
import Button from "../../../app/components/button/button";
import { TextLink } from "../../../app/components/link/link";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";
import { user_list } from "../../../proto/user_list_ts_proto";
import MemberListComponent, { MemberListMember } from "./member_list";
import MembershipAction = user_list.UpdateUserListMembershipRequest.MembershipAction;

export type OrgUserListProps = {
  user: User;
  userListID: string;
};

type State = {
  loading: boolean;

  userList?: user_list.UserList;
  allUsers?: grp.GetGroupUsersResponse.GroupUser[];
};

export default class OrgUserListComponent extends React.Component<OrgUserListProps, State> {
  state: State = {
    loading: false,
    allUsers: new Array<grp.GetGroupUsersResponse.GroupUser>(),
  };

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
    this.setState({ loading: true });

    const fetchUserList = rpcService.service
      .getUserList(new user_list.GetUserListRequest({ userListId: this.props.userListID }))
      .then((response) => this.setState({ userList: response.userList! }))
      .catch((e) => errorService.handleError(e));

    const fetchGroupUsers = rpcService.service
      .getGroupUsers(
        new grp.GetGroupUsersRequest({
          groupId: this.props.user.selectedGroup.id,
          groupMembershipStatus: [grp.GroupMembershipStatus.MEMBER],
        })
      )
      .then((response) =>
        this.setState({
          loading: false,
          allUsers: response.user,
        })
      )
      .catch((e) => errorService.handleError(e));

    Promise.all([fetchUserList, fetchGroupUsers]).finally(() => this.setState({ loading: false }));
  }

  private newMembershipUpdateRequest(
    userIDs: Set<string>,
    action: user_list.UpdateUserListMembershipRequest.MembershipAction
  ) {
    return new user_list.UpdateUserListMembershipRequest({
      userListId: this.state.userList?.userListId,
      update: Array.from(userIDs).map(
        (userID) =>
          new user_list.UpdateUserListMembershipRequest.Update({
            action: action,
            userId: new user_id.UserId({ id: userID }),
          })
      ),
    });
  }

  private onClickAddUsers(idx: number, selectedUsers: Map<string, MemberListMember>) {
    if (selectedUsers.size == 0) {
      return;
    }

    this.setState({ loading: true });
    rpcService.service
      .updateUserListMembership(this.newMembershipUpdateRequest(new Set(selectedUsers.keys()), MembershipAction.ADD))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.fetch());
  }

  private onClickRemoveUsers(idx: number, selectedUsers: Map<string, MemberListMember>) {
    if (selectedUsers.size == 0) {
      return;
    }

    this.setState({ loading: true });
    rpcService.service
      .updateUserListMembership(this.newMembershipUpdateRequest(new Set(selectedUsers.keys()), MembershipAction.REMOVE))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.fetch());
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }

    if (!this.state.userList || !this.state.allUsers) {
      return <div>IAM Group not found</div>;
    }

    const removeButton = <Button>Remove</Button>;

    const addButton = <Button>Add</Button>;

    const memberIDs = new Set<string>(this.state.userList.user.map((m) => m.userId!.id));

    const members = this.state.userList.user.map((m) => new MemberListMember(m));
    const nonMembers = this.state.allUsers
      ?.filter((gu) => gu.user && !memberIDs.has(gu.user.userId!.id))
      .map((gu) => new MemberListMember(gu.user!, undefined, gu.role));

    return (
      <>
        <div className="org-user-lists">
          <div className="org-user-lists-breadcrumbs">
            <div className="org-user-lists-breadcrumb">
              <TextLink href="/settings/org/user-lists">IAM groups</TextLink>
            </div>
            <div className="org-user-lists-breadcrumb">
              <b>{this.state.userList.name}</b>
            </div>
          </div>
        </div>

        <div className="settings-option-title">Members</div>
        {members.length == 0 && <div>There are no users in the IAM group.</div>}
        {members.length > 0 && (
          <MemberListComponent
            user={this.props.user}
            members={members}
            buttons={[removeButton]}
            showRole={false}
            onButtonClick={this.onClickRemoveUsers.bind(this)}
            readOnly={this.props.user.selectedGroup.externalUserManagement}
          />
        )}
        <div className="settings-option-title">Add users</div>
        {nonMembers?.length == 0 && <div>There are no users that can be added.</div>}
        {nonMembers?.length > 0 && (
          <MemberListComponent
            user={this.props.user}
            members={nonMembers}
            buttons={[addButton]}
            showRole={false}
            onButtonClick={this.onClickAddUsers.bind(this)}
            readOnly={this.props.user.selectedGroup.externalUserManagement}
          />
        )}
      </>
    );
  }
}
