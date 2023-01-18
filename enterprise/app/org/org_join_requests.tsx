import React from "react";
import { User } from "../../../app/auth/auth_service";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";

export interface OrgJoinRequestsComponentProps {
  user: User;
}

interface State {
  users?: grp.GetGroupUsersResponse.IGroupUser[] | null;
  isLoading: boolean;
}

const { ADD, REMOVE } = grp.UpdateGroupUsersRequest.Update.MembershipAction;

export default class OrgJoinRequests extends React.Component<OrgJoinRequestsComponentProps, State> {
  state: State = { isLoading: true };

  componentDidMount() {
    this.getJoinOrgRequests();
  }

  componentDidUpdate(prevProps: OrgJoinRequestsComponentProps) {
    if (prevProps.user.selectedGroup.id !== this.props.user.selectedGroup.id) {
      const _ = this.getJoinOrgRequests();
    }
  }

  private async getJoinOrgRequests() {
    const initialGroupId = this.props.user.selectedGroup.id;

    this.setState({ users: null, isLoading: true });
    const response = await rpcService.service.getGroupUsers(
      new grp.GetGroupUsersRequest({
        groupId: this.props.user.selectedGroup.id,
        groupMembershipStatus: [grp.GroupMembershipStatus.REQUESTED],
      })
    );
    if (this.props.user.selectedGroup.id !== initialGroupId) return;

    this.setState({ isLoading: false, users: response.user });
  }

  private async applyMembershipAction(
    userId: user_id.IUserId,
    membershipAction: grp.UpdateGroupUsersRequest.Update.MembershipAction
  ) {
    const initialGroupId = this.props.user.selectedGroup.id;

    this.setState({ isLoading: true });
    await rpcService.service.updateGroupUsers(
      grp.UpdateGroupUsersRequest.create({
        groupId: this.props.user.selectedGroup.id,
        update: [
          grp.UpdateGroupUsersRequest.Update.create({
            userId: user_id.UserId.create(userId),
            membershipAction,
          }),
        ],
      })
    );
    if (this.props.user.selectedGroup.id !== initialGroupId) return;

    await this.getJoinOrgRequests();
  }

  render() {
    if (!this.state.users?.length) return <></>;

    return (
      <div className="org-join-requests">
        <div className="container narrow">
          <h2 className="org-join-requests-header">New user requests</h2>
          <div className="org-join-requests-grid">
            {this.state.users?.map((groupUser) => (
              <React.Fragment key={groupUser.user?.userId?.id}>
                <div>
                  <div className="email">{groupUser.user?.email}</div>
                  <div className="name">
                    {groupUser.user?.name?.first} {groupUser.user?.name?.last}
                  </div>
                </div>
                <div className="approve-reject-buttons">
                  <FilledButton
                    onClick={() => this.applyMembershipAction(groupUser.user?.userId || {}, ADD)}
                    disabled={this.state.isLoading}>
                    Approve
                  </FilledButton>
                  <OutlinedButton
                    onClick={() => this.applyMembershipAction(groupUser.user?.userId || {}, REMOVE)}
                    disabled={this.state.isLoading}>
                    Reject
                  </OutlinedButton>
                </div>
              </React.Fragment>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
