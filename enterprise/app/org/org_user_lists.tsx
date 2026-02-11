import React from "react";

import alertService from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import Banner from "../../../app/components/banner/banner";
import Button, { OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import TextInput from "../../../app/components/input/input";
import Modal from "../../../app/components/modal/modal";
import Select, { Option } from "../../../app/components/select/select";
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import { user_list } from "../../../proto/user_list_ts_proto";
import MemberListComponent, { getRoleLabel, MemberListMember, RoleDescription } from "./member_list";
import OrgUserListComponent from "./org_user_list";

type ChildProps = {
  user: User;
};

type ChildState = {
  loading?: boolean;
  response?: user_list.GetUserListsResponse;
  // Map from user list ID to the role assigned in the current group.
  userListRoles: Map<string, grp.Group.Role>;

  editingList?: boolean;

  // state related to the create user list modal
  isCreateUserListModalVisible?: boolean;
  createUserListRequest?: user_list.CreateUserListRequest;
  isCreateUserListRequestPending?: boolean;

  // state related to the edit role modal
  isEditRoleModalVisible?: boolean;
  editRoleUserList?: user_list.UserList;
  editRoleValue: grp.Group.Role;
  isEditRoleRequestPending?: boolean;

  // state related to the rename user list modal
  isRenameUserListModalVisible?: boolean;
  renameUserList?: user_list.UserList;
  renameNewName?: string;
  isRenameUserListRequestPending?: boolean;

  // state related to the delete user list modal
  isDeleteUserListModalVisible?: boolean;
  userListToBeDeleted?: user_list.UserList;
  isDeleteUserListRequestPending?: boolean;
};

class OrgUserListsChildComponent extends React.Component<ChildProps, ChildState> {
  state: ChildState = {
    loading: true,
    userListRoles: new Map(),
    editRoleValue: grp.Group.Role.UNKNOWN_ROLE,
  };

  private removeButtonRef = React.createRef<HTMLButtonElement>();

  componentDidMount() {
    this.fetch();
  }

  componentDidUpdate(prevProps: OrgUserListsProps, prevState: ChildState) {
    if (!prevState.isDeleteUserListModalVisible && this.state.isDeleteUserListModalVisible) {
      setTimeout(() => {
        this.removeButtonRef.current?.focus();
      }, 0);
    }
  }

  private fetch() {
    this.setState({ loading: true });
    const groupId = this.props.user.selectedGroup.id;
    Promise.all([
      rpcService.service.getUserLists(new user_list.GetUserListsRequest({})),
      rpcService.service.getGroupUsers(
        new grp.GetGroupUsersRequest({
          groupId,
          groupMembershipStatus: [grp.GroupMembershipStatus.MEMBER],
        })
      ),
    ])
      .then(([userListsResponse, groupUsersResponse]) => {
        const userListRoles = new Map<string, grp.Group.Role>();
        for (const gu of groupUsersResponse.user) {
          if (gu.userList) {
            userListRoles.set(gu.userList.userListId, gu.role);
          }
        }
        this.setState({ response: userListsResponse, userListRoles });
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  // Edit role modal

  private onClickOpenCreateGroupModal() {
    this.setState({
      isCreateUserListModalVisible: true,
      createUserListRequest: user_list.CreateUserListRequest.create(),
    });
  }
  private onRequestCloseCreateUserListModal() {
    if (this.state.isCreateUserListRequestPending) return;

    this.setState({ isCreateUserListModalVisible: false });
  }

  private onClickCreateUserList() {
    this.setState({ isCreateUserListRequestPending: true });
    rpcService.service
      .createUserList(this.state.createUserListRequest!)
      .then(() => {
        this.setState({
          isCreateUserListModalVisible: false,
        });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isCreateUserListRequestPending: false }));
  }

  private onClickOpenEditRoleModal(userList: user_list.UserList) {
    this.setState({
      isEditRoleModalVisible: true,
      editRoleUserList: userList,
      editRoleValue: this.state.userListRoles.get(userList.userListId) ?? grp.Group.Role.UNKNOWN_ROLE,
    });
  }
  private onRequestCloseEditRoleModal() {
    if (this.state.isEditRoleRequestPending) return;
    this.setState({ isEditRoleModalVisible: false });
  }
  private onChangeEditRoleValue(event: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ editRoleValue: Number(event.target.value) as grp.Group.Role });
  }
  private onClickApplyRole() {
    const userListId = this.state.editRoleUserList?.userListId;
    if (!userListId) return;
    const hasExistingRole = this.state.userListRoles.has(userListId);
    const isNone = this.state.editRoleValue === grp.Group.Role.UNKNOWN_ROLE;
    if (isNone && !hasExistingRole) {
      this.setState({ isEditRoleModalVisible: false });
      return;
    }
    this.setState({ isEditRoleRequestPending: true });
    const update = new grp.UpdateGroupUsersRequest.Update({ userListId });
    if (isNone) {
      update.membershipAction = grp.UpdateGroupUsersRequest.Update.MembershipAction.REMOVE;
    } else {
      update.role = this.state.editRoleValue;
      if (!hasExistingRole) {
        update.membershipAction = grp.UpdateGroupUsersRequest.Update.MembershipAction.ADD;
      }
    }
    rpcService.service
      .updateGroupUsers(
        new grp.UpdateGroupUsersRequest({
          groupId: this.props.user.selectedGroup.id,
          update: [update],
        })
      )
      .then(() => {
        alertService.success("Role updated successfully.");
        this.setState({ isEditRoleModalVisible: false });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isEditRoleRequestPending: false }));
  }

  // Rename modal

  private onClickOpenRenameModal(userList: user_list.UserList) {
    this.setState({
      isRenameUserListModalVisible: true,
      renameUserList: userList,
      renameNewName: userList.name,
    });
  }
  private onRequestCloseRenameModal() {
    if (this.state.isRenameUserListRequestPending) return;
    this.setState({ isRenameUserListModalVisible: false });
  }
  private onChangeRenameNewName(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ renameNewName: e.target.value });
  }
  private onSubmitRenameForm(e: React.FormEvent) {
    e.preventDefault();
    if (!this.state.isRenameUserListRequestPending) {
      this.onClickRenameUserList();
    }
  }
  private onClickRenameUserList() {
    this.setState({ isRenameUserListRequestPending: true });
    rpcService.service
      .updateUserList(
        user_list.UpdateUserListRequest.create({
          userList: new user_list.UserList({
            userListId: this.state.renameUserList?.userListId,
            name: this.state.renameNewName,
          }),
        })
      )
      .then(() => {
        this.setState({ isRenameUserListModalVisible: false });
        alertService.success("IAM group renamed");
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRenameUserListRequestPending: false }));
  }

  // Remove modal

  private onClickOpenRemoveUserListModal(userList: user_list.UserList) {
    this.setState({
      isDeleteUserListModalVisible: true,
      userListToBeDeleted: userList,
    });
  }
  private onRequestCloseRemoveModal() {
    if (this.state.isDeleteUserListRequestPending) return;

    this.setState({ isDeleteUserListModalVisible: false });
  }
  private onClickRemoveUserList() {
    this.setState({ isDeleteUserListRequestPending: true });
    rpcService.service
      .deleteUserList(
        user_list.DeleteUserListRequest.create({
          userListId: this.state.userListToBeDeleted?.userListId,
        })
      )
      .then(() => {
        this.setState({
          isDeleteUserListModalVisible: false,
        });
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isDeleteUserListRequestPending: false }));
  }

  private onClickEditUserList(userList: user_list.UserList) {
    this.setState({ editingList: true });
    router.navigateToUserList(userList.userListId);
  }

  private onChangeCreateUserListName(e: React.ChangeEvent<HTMLInputElement>) {
    const name = e.target.value;
    this.setState({ createUserListRequest: user_list.CreateUserListRequest.create({ name: name }) });
  }

  render() {
    if (this.state.loading) {
      return <div className="loading" />;
    }
    if (!this.state.response) return null;

    return (
      <>
        <div className="settings-option-title">IAM Groups</div>
        <div className="settings-option-description">
          IAM Groups organize users into lists for easier role management.
        </div>
        <div className="org-user-lists">
          <div className="org-user-lists-list-controls">
            {this.props.user.selectedGroup.externalUserManagement && (
              <div>
                <Banner type="warning" className="user-management-warning">
                  IAM Groups are being managed via an external system. All changes must be made there.
                </Banner>
              </div>
            )}
            {!this.props.user.selectedGroup.externalUserManagement && (
              <>
                <Button className="big-button" onClick={this.onClickOpenCreateGroupModal.bind(this)}>
                  Create new IAM group
                </Button>
              </>
            )}
          </div>
          <div className="org-user-lists-list">
            {this.state.response.userList.map((userList) => (
              <div className="org-user-lists-list-item">
                <div className="org-user-list-name">{userList.name}</div>
                <div className="org-user-list-role">
                  {this.state.userListRoles.has(userList.userListId)
                    ? getRoleLabel(this.state.userListRoles.get(userList.userListId)!)
                    : "No role"}
                </div>
                <div className="org-user-list-members">
                  {userList.user.length > 0 && (
                    <MemberListComponent
                      user={this.props.user}
                      members={userList.user.map((u) => new MemberListMember(u))}
                      showRole={false}
                      readOnly={true}
                      maxRows={10}
                    />
                  )}
                  {userList.user.length == 0 && <div className="org-user-list-empty">no members</div>}
                </div>
                <div className="org-user-list-buttons">
                  <OutlinedButton onClick={this.onClickEditUserList.bind(this, userList)}>Edit members</OutlinedButton>
                  <OutlinedButton onClick={this.onClickOpenEditRoleModal.bind(this, userList)}>
                    Edit role
                  </OutlinedButton>
                  <OutlinedButton onClick={this.onClickOpenRenameModal.bind(this, userList)}>Rename</OutlinedButton>
                  <OutlinedButton
                    onClick={this.onClickOpenRemoveUserListModal.bind(this, userList)}
                    className="destructive">
                    Delete
                  </OutlinedButton>
                </div>
              </div>
            ))}
          </div>

          {/* create group modal */}
          <Modal
            className="org-user-lists-create-modal"
            isOpen={Boolean(this.state.isCreateUserListModalVisible)}
            onRequestClose={this.onRequestCloseCreateUserListModal.bind(this)}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Create group</DialogTitle>
              </DialogHeader>
              <DialogBody className="modal-body">
                <div className="field-container">
                  <label className="note-input-label" htmlFor="name">
                    Name
                  </label>
                  <TextInput
                    name="name"
                    onChange={this.onChangeCreateUserListName.bind(this)}
                    value={this.state.createUserListRequest?.name}
                    autoFocus={true}
                  />
                </div>
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isCreateUserListRequestPending && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseCreateUserListModal.bind(this)}
                    disabled={this.state.isCreateUserListRequestPending}>
                    Cancel
                  </OutlinedButton>
                  <Button
                    onClick={this.onClickCreateUserList.bind(this)}
                    disabled={this.state.isCreateUserListRequestPending}>
                    Create
                  </Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>

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
                <div>
                  Set role for <b>{this.state.editRoleUserList?.name}</b>:
                </div>
                {this.state.editRoleUserList?.user.some(
                  (u) => u.userId?.id === this.props.user.displayUser?.userId?.id
                ) && (
                  <div className="editing-self-warning">
                    <b>Warning</b>: Your account is a member of this group.
                  </div>
                )}
                <div className="select-role-row">
                  <div>Role</div>
                  <Select value={this.state.editRoleValue} onChange={this.onChangeEditRoleValue.bind(this)}>
                    <Option value={grp.Group.Role.UNKNOWN_ROLE}>None</Option>
                    {capabilities.config.readerWriterRolesEnabled && (
                      <Option value={grp.Group.Role.READER_ROLE}>{getRoleLabel(grp.Group.Role.READER_ROLE)}</Option>
                    )}
                    <Option value={grp.Group.Role.DEVELOPER_ROLE}>{getRoleLabel(grp.Group.Role.DEVELOPER_ROLE)}</Option>
                    {capabilities.config.readerWriterRolesEnabled && (
                      <Option value={grp.Group.Role.WRITER_ROLE}>{getRoleLabel(grp.Group.Role.WRITER_ROLE)}</Option>
                    )}
                    <Option value={grp.Group.Role.ADMIN_ROLE}>{getRoleLabel(grp.Group.Role.ADMIN_ROLE)}</Option>
                  </Select>
                </div>
                <div className="role-description">
                  <RoleDescription role={this.state.editRoleValue} />
                </div>
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isEditRoleRequestPending && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseEditRoleModal.bind(this)}
                    disabled={this.state.isEditRoleRequestPending}>
                    Cancel
                  </OutlinedButton>
                  <Button onClick={this.onClickApplyRole.bind(this)} disabled={this.state.isEditRoleRequestPending}>
                    Apply
                  </Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>

          {/* Rename modal */}
          <Modal
            className="org-user-lists-rename-modal"
            isOpen={Boolean(this.state.isRenameUserListModalVisible)}
            onRequestClose={this.onRequestCloseRenameModal.bind(this)}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Rename IAM group</DialogTitle>
              </DialogHeader>
              <DialogBody className="modal-body">
                <form onSubmit={this.onSubmitRenameForm.bind(this)}>
                  <div className="field-container">
                    <label className="note-input-label" htmlFor="name">
                      Name
                    </label>
                    <TextInput
                      name="name"
                      onChange={this.onChangeRenameNewName.bind(this)}
                      value={this.state.renameNewName}
                      autoFocus={true}
                    />
                  </div>
                </form>
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isRenameUserListRequestPending && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseRenameModal.bind(this)}
                    disabled={this.state.isRenameUserListRequestPending}>
                    Cancel
                  </OutlinedButton>
                  <Button
                    onClick={this.onClickRenameUserList.bind(this)}
                    disabled={this.state.isRenameUserListRequestPending}>
                    Rename
                  </Button>
                </DialogFooterButtons>
              </DialogFooter>
            </Dialog>
          </Modal>

          {/* Remove modal */}
          <Modal
            className="org-members-edit-modal"
            isOpen={Boolean(this.state.isDeleteUserListModalVisible)}
            onRequestClose={this.onRequestCloseRemoveModal.bind(this)}>
            <Dialog>
              <DialogHeader>
                <DialogTitle>Confirm deletion</DialogTitle>
              </DialogHeader>
              <DialogBody className="modal-body">
                Are you sure you want to delete the IAM group {this.state.userListToBeDeleted?.name}?
              </DialogBody>
              <DialogFooter>
                <DialogFooterButtons>
                  {this.state.isDeleteUserListRequestPending && <Spinner />}
                  <OutlinedButton
                    onClick={this.onRequestCloseRemoveModal.bind(this)}
                    disabled={this.state.isDeleteUserListRequestPending}>
                    Cancel
                  </OutlinedButton>
                  <Button
                    ref={this.removeButtonRef}
                    className="destructive"
                    onClick={this.onClickRemoveUserList.bind(this)}
                    disabled={this.state.isDeleteUserListRequestPending}>
                    Delete
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

export type OrgUserListsProps = {
  user: User;
};

export default class OrgUserListsComponent extends React.Component<OrgUserListsProps> {
  render() {
    const path = window.location.pathname;
    if (path.startsWith("/settings/org/user-lists/")) {
      return <OrgUserListComponent user={this.props.user} userListID={path.split("/").pop()!} />;
    }

    return <OrgUserListsChildComponent user={this.props.user} />;
  }
}
