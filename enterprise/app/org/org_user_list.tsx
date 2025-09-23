import React from "react";
import alert_service from "../../../app/alert/alert_service";
import { User } from "../../../app/auth/user";
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
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";
import { user_id } from "../../../proto/user_id_ts_proto";
import { user_list } from "../../../proto/user_list_ts_proto";
import UserListComponent, { UserListUser } from "./user_list";
import MembershipAction = user_list.UpdateUserListMembershipRequest.MembershipAction;

export type OrgUserListProps = {
  user: User;
  userListID: string;
};

type State = {
  loading: boolean;

  userList?: user_list.UserList;
  allUsers?: grp.GetGroupUsersResponse.GroupUser[];

  // state related to the rename user list modal
  isRenameUserListModalVisible?: boolean;
  renameNewName?: string;
  isRenameUserListRequestPending?: boolean;
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

  private onClickAddUsers(idx: number, selectedUsers: Set<string>) {
    if (selectedUsers.size == 0) {
      return;
    }

    this.setState({ loading: true });
    rpcService.service
      .updateUserListMembership(this.newMembershipUpdateRequest(selectedUsers, MembershipAction.ADD))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.fetch());
  }

  private onClickRemoveUsers(idx: number, selectedUsers: Set<string>) {
    if (selectedUsers.size == 0) {
      return;
    }

    this.setState({ loading: true });
    rpcService.service
      .updateUserListMembership(this.newMembershipUpdateRequest(selectedUsers, MembershipAction.REMOVE))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.fetch());
  }

  private onClickBackToAllGroups() {
    router.navigateToUserLists();
  }

  // Rename modal.

  private onClickOpenRenameUserListModal() {
    this.setState({
      isRenameUserListModalVisible: true,
      renameNewName: this.state.userList?.name,
    });
  }

  private onRequestCloseRenameModal() {
    if (this.state.isRenameUserListRequestPending) return;

    this.setState({ isRenameUserListModalVisible: false });
  }

  private onChangeRenameUserListName(e: React.ChangeEvent<HTMLInputElement>) {
    const name = e.target.value;
    this.setState({ renameNewName: name });
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
            userListId: this.state.userList?.userListId,
            name: this.state.renameNewName,
          }),
        })
      )
      .then(() => {
        this.setState({
          isRenameUserListModalVisible: false,
        });
        alert_service.success("IAM Group renamed");
        this.fetch();
      })
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ isRenameUserListRequestPending: false }));
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

    const members = this.state.userList.user.map((m) => new UserListUser(m));
    const nonMembers = this.state.allUsers
      ?.filter((gu) => gu.user && !memberIDs.has(gu.user.userId!.id))
      .map((gu) => new UserListUser(gu.user!, gu.role));

    return (
      <>
        <div className="settings-option-title">IAM Group "{this.state.userList.name}"</div>

        <div className="org-user-list-controls">
          <OutlinedButton onClick={this.onClickBackToAllGroups.bind(this)}>Back to all IAM groups</OutlinedButton>
          <OutlinedButton onClick={this.onClickOpenRenameUserListModal.bind(this)}>Rename</OutlinedButton>
        </div>

        <div className="settings-option-title">Members:</div>
        {members.length == 0 && <div>There are no users in the IAM group.</div>}
        {members.length > 0 && (
          <UserListComponent
            user={this.props.user}
            users={members}
            buttons={[removeButton]}
            showRole={false}
            onButtonClick={this.onClickRemoveUsers.bind(this)}
            readOnly={this.props.user.selectedGroup.externalUserManagement}
          />
        )}
        <div className="settings-option-title">Add users:</div>
        {nonMembers?.length == 0 && <div>There are no users that can be added.</div>}
        {nonMembers?.length > 0 && (
          <UserListComponent
            user={this.props.user}
            users={nonMembers}
            buttons={[addButton]}
            showRole={false}
            onButtonClick={this.onClickAddUsers.bind(this)}
            readOnly={this.props.user.selectedGroup.externalUserManagement}
          />
        )}

        {/* rename user list modal */}
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
                    onChange={this.onChangeRenameUserListName.bind(this)}
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
      </>
    );
  }
}
