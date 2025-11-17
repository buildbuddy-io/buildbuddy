import React from "react";

import { User } from "../../../app/auth/auth_service";
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
import Spinner from "../../../app/components/spinner/spinner";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import { user_list } from "../../../proto/user_list_ts_proto";
import MemberListComponent, { MemberListMember } from "./member_list";
import OrgUserListComponent from "./org_user_list";

type ChildProps = {
  user: User;
};

type ChildState = {
  loading?: boolean;
  response?: user_list.GetUserListsResponse;

  editingList?: boolean;

  // state related to the create user list modal
  isCreateUserListModalVisible?: boolean;
  createUserListRequest?: user_list.CreateUserListRequest;
  isCreateUserListRequestPending?: boolean;

  // state related to the delete user list modal
  isDeleteUserListModalVisible?: boolean;
  userListToBeDeleted?: user_list.UserList;
  isDeleteUserListRequestPending?: boolean;
};

class OrgUserListsChildComponent extends React.Component<ChildProps, ChildState> {
  state: ChildState = {
    loading: true,
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
    rpcService.service
      .getUserLists(new user_list.GetUserListsRequest({}))
      .then((response) => this.setState({ response }))
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
                  <OutlinedButton onClick={this.onClickEditUserList.bind(this, userList)}>Edit</OutlinedButton>
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
  render(): React.ReactNode {
    const path = window.location.pathname;
    if (path.startsWith("/settings/org/user-lists/")) {
      return <OrgUserListComponent user={this.props.user} userListID={path.split("/").pop()!} />;
    }

    return <OrgUserListsChildComponent user={this.props.user} />;
  }
}
