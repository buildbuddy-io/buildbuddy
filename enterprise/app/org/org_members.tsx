import { CheckCircle, XCircle } from "lucide-react";
import React from "react";
import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import rpcService, { CancelablePromise } from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";
import { user_list } from "../../../proto/user_list_ts_proto";
import * as member_list from "./member_list";
import MemberListComponent, { MemberListMember } from "./member_list";

export type OrgMembersProps = {
  user: User;
};

type State = {
  loading?: boolean;
  response?: grp.GetGroupUsersResponse;
  userLists?: user_list.UserList[];

  selectedMembers: Map<string, member_list.MemberListMember>;

  isEditRoleModalVisible?: boolean;
  roleToApply: grp.Group.Role;
  isRoleUpdateLoading?: boolean;

  isRemoveModalVisible?: boolean;
  isRemoveLoading?: boolean;
};

const DEFAULT_ROLE = grp.Group.Role.DEVELOPER_ROLE;

export default class OrgMembersComponent extends React.Component<OrgMembersProps, State> {
  state: State = {
    loading: true,
    selectedMembers: new Map<string, member_list.MemberListMember>(),
    roleToApply: DEFAULT_ROLE,
  };

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
    this.setState({ loading: true });

    const fetches = new Array<CancelablePromise>();
    fetches.push(
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
    );

    if (capabilities.config.userListsUiEnabled) {
      fetches.push(
        rpcService.service
          .getUserLists(new user_list.GetUserListsRequest())
          .then((response) => this.setState({ userLists: response.userList }))
          .catch((e) => errorService.handleError(e))
      );
    }

    Promise.all(fetches).finally(() => this.setState({ loading: false }));
  }

  // Edit role modal

  private onClickEditRole(selectedMembers: Map<string, member_list.MemberListMember>) {
    const selectedWithRole = Array.from(selectedMembers.values()).filter((u) => u.role !== undefined);

    this.setState({
      isEditRoleModalVisible: true,
      // Set the initially selected role to match the current role of the
      // first user. This is a sensible default when there's only one user
      // selected.
      roleToApply: selectedWithRole[0]?.role || DEFAULT_ROLE,
    });
  }
  private onRequestCloseEditRoleModal() {
    if (this.state.isRoleUpdateLoading) return;

    this.setState({ isEditRoleModalVisible: false });
  }
  private onChangeRoleToApply(event: React.ChangeEvent<HTMLSelectElement>) {
    const roleToApply = Number(event.target.value) as grp.Group.Role;
    this.setState({ roleToApply });
  }
  private onClickApplyRoleEdits() {
    this.setState({ isRoleUpdateLoading: true });

    const req = new grp.UpdateGroupUsersRequest({
      groupId: this.props.user.selectedGroup.id,
    });

    for (let member of this.state.selectedMembers.values()) {
      if (member.user) {
        req.update.push(
          new grp.UpdateGroupUsersRequest.Update({
            userId: new user_id.UserId({ id: member.user.userId?.id }),
            role: this.state.roleToApply,
          })
        );
      } else if (member.userList) {
        let update = new grp.UpdateGroupUsersRequest.Update({
          userListId: member.userList.userListId,
          role: this.state.roleToApply,
        });
        if (member.role === undefined) {
          update.membershipAction = grp.UpdateGroupUsersRequest.Update.MembershipAction.ADD;
        }
        req.update.push(update);
      }
    }

    rpcService.service
      .updateGroupUsers(req)
      .then(() => {
        // After changing your own role within an org, refresh the page to
        // trigger a user refresh and possibly a reroute, in case this settings
        // page is no longer accessible.
        if (Array.from(this.state.selectedMembers.values()).some((m) => this.affectsLoggedInUser(m))) {
          window.location.reload();
          return;
        }
        alertService.success("Changes applied successfully.");
        this.setState({
          isEditRoleModalVisible: false,
          selectedMembers: new Map(),
        });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRoleUpdateLoading: false }));
  }

  // Remove modal

  private onClickRemove() {
    this.setState({ isRemoveModalVisible: true });
  }
  private onRequestCloseRemoveModal() {
    if (this.state.isRemoveLoading) return;

    this.setState({ isRemoveModalVisible: false });
  }
  private onClickConfirmRemove() {
    this.setState({ isRemoveLoading: true });

    const req = new grp.UpdateGroupUsersRequest({
      groupId: this.props.user.selectedGroup.id,
    });

    for (let member of this.state.selectedMembers.values()) {
      if (member.user) {
        req.update.push(
          new grp.UpdateGroupUsersRequest.Update({
            userId: new user_id.UserId({ id: member.user.userId?.id }),
            membershipAction: grp.UpdateGroupUsersRequest.Update.MembershipAction.REMOVE,
          })
        );
      } else if (member.userList && member.role !== undefined) {
        req.update.push(
          new grp.UpdateGroupUsersRequest.Update({
            userListId: member.userList.userListId,
            membershipAction: grp.UpdateGroupUsersRequest.Update.MembershipAction.REMOVE,
          })
        );
      }
    }

    rpcService.service
      .updateGroupUsers(req)
      .then(() => {
        // After removing yourself from an org, refresh the page to trigger
        // group reselection or login page as appropriate.
        if (Array.from(this.state.selectedMembers.values()).some((m) => this.affectsLoggedInUser(m))) {
          window.location.reload();
          return;
        }
        alertService.success("Changes applied successfully.");
        this.setState({
          isRemoveModalVisible: false,
          selectedMembers: new Map(),
        });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRemoveLoading: false }));
  }

  private isLoggedInUser(member: member_list.MemberListMember) {
    return member?.user?.userId?.id === this.props.user.displayUser?.userId?.id;
  }

  private containsLoggedInUser(member: member_list.MemberListMember) {
    return member.userList?.user.some((u) => u.userId?.id === this.props.user.displayUser.userId?.id);
  }

  private affectsLoggedInUser(member: member_list.MemberListMember) {
    return this.isLoggedInUser(member) || this.containsLoggedInUser(member);
  }

  private renderAffectedUsersList({ verb }: { verb: string }) {
    let selectedMembers = Array.from(this.state.selectedMembers.values());
    if (verb == "Removing") {
      selectedMembers = selectedMembers.filter((u) => u.role !== undefined);
    }
    return (
      <>
        <div>
          {verb} <b>{selectedMembers.length}</b> member{selectedMembers.length === 1 ? "" : "s"}:
        </div>
        <div className="affected-users-list">
          {selectedMembers.map((member) => (
            <div className={`affected-users-list-item ${this.affectsLoggedInUser(member) ? "flagged-self-user" : ""}`}>
              {member.displayName()} {member.icon()}
            </div>
          ))}
        </div>
        {selectedMembers.some((member) => this.isLoggedInUser(member)) && (
          <div className="editing-self-warning">
            <b>Warning</b>: Your account is selected.
          </div>
        )}
        {selectedMembers.some((member) => this.containsLoggedInUser(member)) && (
          <div className="editing-self-warning">
            <b>Warning</b>: Your account is a member of a selected group.
          </div>
        )}
      </>
    );
  }

  private renderRoleDescription(role: grp.Group.Role) {
    // TODO: send up role=>capabilities mapping from server, and base these
    // descriptions on that.
    type Capability = {
      description: React.ReactNode;
      read: boolean;
      write: boolean;
    };
    const capabilities: Capability[] = [
      {
        description: "Organization settings and users",
        read: role === grp.Group.Role.ADMIN_ROLE,
        write: role === grp.Group.Role.ADMIN_ROLE,
      },
      {
        description: "Invocations",
        read: true,
        write: true,
      },
      {
        description: "Content-addressable storage (CAS)",
        read: true,
        write: role !== grp.Group.Role.READER_ROLE,
      },
      {
        description: "Action cache (AC)",
        read: true,
        write: role === grp.Group.Role.WRITER_ROLE || role === grp.Group.Role.ADMIN_ROLE,
      },
    ];
    const statusIcon = (ok: boolean) =>
      ok ? <CheckCircle className="icon green" /> : <XCircle className="icon red" />;
    return (
      <>
        <table className="role-capabilities">
          <tr className="role-capability-header">
            <th>Object type</th>
            <th>Read</th>
            <th>Write</th>
          </tr>
          {capabilities.map((capability, i) => (
            <tr key={i} className="role-capability-row">
              <td>{capability.description}</td>
              <td>{statusIcon(capability.read)}</td>
              <td>{statusIcon(capability.write)}</td>
            </tr>
          ))}
        </table>
      </>
    );
  }

  private onClickUserListButton(idx: number, selectedUsers: Map<string, member_list.MemberListMember>) {
    this.setState({ selectedMembers: selectedUsers });
    if (idx == 0) {
      this.onClickEditRole(selectedUsers);
    } else if (idx == 1) {
      this.onClickRemove();
    }
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.response) return null;

    const editRoleButton = <Button>Edit role</Button>;

    const removeButton = <Button className="destructive org-member-remove-button">Remove</Button>;

    // All user list IDs that are already a member of the group and the
    // associated role.
    const userListRole = new Map<string, grp.Group.Role>(
      this.state.response.user.filter((gu) => gu.userList).map((gu) => [gu.userList!.userListId, gu.role])
    );

    const members = new Array<MemberListMember>();

    // Populate user lists at the top of the members list, above users.
    if (capabilities.config.userListsUiEnabled) {
      members.push(
        ...this.state.userLists!.map((ul) => {
          return new MemberListMember(undefined, ul, userListRole.get(ul.userListId));
        })
      );
    }

    // Then add direct user members.
    members.push(
      ...this.state.response.user
        .filter((gu) => gu.user)
        .map((gu) => new MemberListMember(gu.user!, undefined, gu.role))
    );

    return (
      <>
        <MemberListComponent
          user={this.props.user}
          members={members}
          showRole={true}
          buttons={[editRoleButton, removeButton]}
          onButtonClick={this.onClickUserListButton.bind(this)}
          readOnly={this.props.user.selectedGroup.externalUserManagement}
        />
        <div className="org-members">
          {/* Edit role modal */}
          <Modal
            className="org-members-edit-modal"
            isOpen={Boolean(this.state.isEditRoleModalVisible)}
            onRequestClose={this.onRequestCloseEditRoleModal.bind(this)}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Edit role</DialogTitle>
              </DialogHeader>
              <DialogBody className="modal-body">
                {this.renderAffectedUsersList({ verb: "Editing" })}
                <div className="select-role-row">
                  <div>Role</div>
                  <Select value={this.state.roleToApply} onChange={this.onChangeRoleToApply.bind(this)}>
                    {capabilities.config.readerWriterRolesEnabled && (
                      <>
                        <Option value={grp.Group.Role.READER_ROLE}>
                          {member_list.getRoleLabel(grp.Group.Role.READER_ROLE)}
                        </Option>
                      </>
                    )}
                    <Option value={grp.Group.Role.DEVELOPER_ROLE}>
                      {member_list.getRoleLabel(grp.Group.Role.DEVELOPER_ROLE)}
                    </Option>
                    {capabilities.config.readerWriterRolesEnabled && (
                      <>
                        <Option value={grp.Group.Role.WRITER_ROLE}>
                          {member_list.getRoleLabel(grp.Group.Role.WRITER_ROLE)}
                        </Option>
                      </>
                    )}
                    <Option value={grp.Group.Role.ADMIN_ROLE}>
                      {member_list.getRoleLabel(grp.Group.Role.ADMIN_ROLE)}
                    </Option>
                  </Select>
                </div>
                <div className="role-description">{this.renderRoleDescription(this.state.roleToApply)}</div>
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isRoleUpdateLoading && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseEditRoleModal.bind(this)}
                    disabled={this.state.isRoleUpdateLoading}>
                    Cancel
                  </OutlinedButton>
                  <Button onClick={this.onClickApplyRoleEdits.bind(this)} disabled={this.state.isRoleUpdateLoading}>
                    Apply
                  </Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>

          {/* Remove modal */}
          <Modal
            className="org-members-edit-modal"
            isOpen={Boolean(this.state.isRemoveModalVisible)}
            onRequestClose={this.onRequestCloseRemoveModal.bind(this)}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Confirm removal</DialogTitle>
              </DialogHeader>
              <DialogBody className="modal-body">{this.renderAffectedUsersList({ verb: "Removing" })}</DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isRemoveLoading && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseRemoveModal.bind(this)}
                    disabled={this.state.isRemoveLoading}>
                    Cancel
                  </OutlinedButton>
                  <Button
                    className="destructive"
                    onClick={this.onClickConfirmRemove.bind(this)}
                    disabled={this.state.isRemoveLoading}>
                    Remove
                  </Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>
        </div>
      </>
    );
  }
}
