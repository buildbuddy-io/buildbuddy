import React from "react";
import { User } from "../../../app/auth/auth_service";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import CheckboxButton from "../../../app/components/button/checkbox_button";
import Checkbox from "../../../app/components/checkbox/checkbox";
import alertService from "../../../app/alert/alert_service";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import Modal from "../../../app/components/modal/modal";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Select, { Option } from "../../../app/components/select/select";
import { user_id } from "../../../proto/user_id_ts_proto";
import Spinner from "../../../app/components/spinner/spinner";

export type OrgMembersProps = {
  user: User;
};

type State = {
  loading?: boolean;
  response?: grp.GetGroupUsersResponse;

  isSelectingAll?: boolean;
  selectedUserIds: Set<string>;

  isEditRoleModalVisible?: boolean;
  roleToApply?: grp.Group.Role;
  isRoleUpdateLoading?: boolean;

  isRemoveModalVisible?: boolean;
  isRemoveLoading?: boolean;
};

const ROLE_LABELS: Record<grp.Group.Role, string> = {
  [grp.Group.Role.UNKNOWN_ROLE]: "",
  [grp.Group.Role.ADMIN_ROLE]: "Admin",
  [grp.Group.Role.DEVELOPER_ROLE]: "Developer",
};

const DEFAULT_ROLE = grp.Group.Role.DEVELOPER_ROLE;

export default class OrgMembersComponent extends React.Component<OrgMembersProps, State> {
  state: State = {
    loading: true,
    selectedUserIds: new Set<string>(),
    roleToApply: DEFAULT_ROLE,
  };

  componentDidMount() {
    this.fetch();
  }

  private fetch() {
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

  // Edit role modal

  private onClickEditRole() {
    this.setState({
      isEditRoleModalVisible: true,
      // Set the initially selected role to match the current role of the
      // first user. This is a sensible default when there's only one user
      // selected.
      roleToApply: this.getSelectedMembers()[0]?.role || DEFAULT_ROLE,
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
    rpcService.service
      .updateGroupUsers(
        new grp.UpdateGroupUsersRequest({
          groupId: this.props.user.selectedGroup.id,
          update: [...this.state.selectedUserIds].map(
            (id) =>
              new grp.UpdateGroupUsersRequest.Update({
                userId: new user_id.UserId({ id }),
                role: this.state.roleToApply,
              })
          ),
        })
      )
      .then(() => {
        // After changing your own role within an org, refresh the page to
        // trigger a user refresh and possibly a reroute, in case this settings
        // page is no longer accessible.
        if (this.state.selectedUserIds.has(this.props.user.displayUser.userId.id)) {
          window.location.reload();
          return;
        }
        alertService.success("Changes applied successfully.");
        this.setState({
          isEditRoleModalVisible: false,
          selectedUserIds: new Set(),
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
    rpcService.service
      .updateGroupUsers(
        new grp.UpdateGroupUsersRequest({
          groupId: this.props.user.selectedGroup.id,
          update: [...this.state.selectedUserIds].map(
            (id) =>
              new grp.UpdateGroupUsersRequest.Update({
                userId: new user_id.UserId({ id }),
                membershipAction: grp.UpdateGroupUsersRequest.Update.MembershipAction.REMOVE,
              })
          ),
        })
      )
      .then(() => {
        // After removing yourself from an org, refresh the page to trigger
        // group reselection or login page as appropriate.
        if (this.state.selectedUserIds.has(this.props.user.displayUser.userId.id)) {
          window.location.reload();
          return;
        }
        alertService.success("Changes applied successfully.");
        this.setState({
          isRemoveModalVisible: false,
          selectedUserIds: new Set(),
        });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRemoveLoading: false }));
  }

  private isLoggedInUser(member: grp.GetGroupUsersResponse.IGroupUser) {
    return member.user.userId.id === this.props.user.displayUser.userId.id;
  }

  private getSelectedMembers(): grp.GetGroupUsersResponse.IGroupUser[] {
    return this.state.response.user.filter((member) => this.state.selectedUserIds.has(member.user.userId.id));
  }

  private renderAffectedUsersList({ verb }: { verb: string }) {
    const selectedMembers = this.getSelectedMembers();
    return (
      <>
        <div>
          {verb} <b>{selectedMembers.length}</b> user{selectedMembers.length === 1 ? "" : "s"}:
        </div>
        <div className="affected-users-list">
          {selectedMembers.map((member) => (
            <div className={`affected-users-list-item ${this.isLoggedInUser(member) ? "flagged-self-user" : ""}`}>
              {member.user.email}
            </div>
          ))}
        </div>
        {selectedMembers.some((member) => this.isLoggedInUser(member)) && (
          <div className="editing-self-warning">
            <b>Warning</b>: Your account is selected.
          </div>
        )}
      </>
    );
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

          <Button onClick={this.onClickEditRole.bind(this)} disabled={isSelectionEmpty}>
            Edit role
          </Button>
          <Button
            onClick={this.onClickRemove.bind(this)}
            disabled={isSelectionEmpty}
            className="destructive org-member-remove-button">
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
              <div className="org-member-role">{ROLE_LABELS[member.role]}</div>
            </div>
          ))}
        </div>

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
                  <Option value={grp.Group.Role.DEVELOPER_ROLE}>{ROLE_LABELS[grp.Group.Role.DEVELOPER_ROLE]}</Option>
                  <Option value={grp.Group.Role.ADMIN_ROLE}>{ROLE_LABELS[grp.Group.Role.ADMIN_ROLE]}</Option>
                </Select>
              </div>
              <div className="role-description">
                {/* TODO(bduffany): Get role metadata from the server so that we
                   don't have to keep these descriptions in sync. */}
                {this.state.roleToApply === grp.Group.Role.DEVELOPER_ROLE && (
                  <>
                    Developers can view invocations and stats, but do not have access to org settings, usage data, or
                    workflow configuration.
                  </>
                )}
                {this.state.roleToApply === grp.Group.Role.ADMIN_ROLE && (
                  <>
                    Admins have <b>full access</b> to all data within the organization.
                  </>
                )}
              </div>
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
    );
  }
}
