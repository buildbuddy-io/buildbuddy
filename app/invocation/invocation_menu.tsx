import { MoreVertical } from "lucide-react";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import FilledButton, { OutlinedButton } from "../components/button/button";
import Spinner from "../components/spinner/spinner";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../components/dialog/dialog";
import Menu, { MenuItem } from "../components/menu/menu";
import Modal from "../components/modal/modal";
import Popup from "../components/popup/popup";
import router from "../router/router";
import rpcService from "../service/rpc_service";
import { BuildBuddyError } from "../util/errors";
import InvocationModel from "./invocation_model";

export interface InvocationMenuComponentProps {
  model: InvocationModel;
  invocationId: string;
  user?: User;
}

interface State {
  isMenuOpen: boolean;
  isDeleteModalOpen: boolean;
  isDeleteModalLoading: boolean;
  deleteModalError?: BuildBuddyError | null;
}

export default class InvocationMenuComponent extends React.Component<InvocationMenuComponentProps, State> {
  state: State = {
    isMenuOpen: false,
    isDeleteModalOpen: false,
    isDeleteModalLoading: false,
  };

  private onClickMenuButton() {
    this.setState({ isMenuOpen: true });
  }

  private onCloseMenu() {
    this.setState({ isMenuOpen: false });
  }

  private onClickDeleteItem() {
    this.setState({ isMenuOpen: false, isDeleteModalOpen: true });
  }

  private onCloseDeleteModal() {
    this.setState({ isDeleteModalOpen: false });
  }

  private async onClickDelete() {
    this.setState({ isDeleteModalLoading: true, deleteModalError: null });
    try {
      await rpcService.service.deleteInvocation(
        new invocation.DeleteInvocationRequest({ invocationId: this.props.invocationId })
      );
      router.navigateHome();
    } catch (e) {
      this.setState({ deleteModalError: BuildBuddyError.parse(e) });
    } finally {
      this.setState({ isDeleteModalLoading: false });
    }
  }

  render() {
    if (!capabilities.deleteInvocation) {
      return <></>;
    }

    const invocation = this.props.model.invocations.find(
      (invocation) => invocation.invocationId === this.props.invocationId
    );
    const hasWritePermissions = this.props.user && invocation && canWrite(this.props.user, invocation);

    return (
      <>
        <div className="invocation-menu-container">
          <OutlinedButton
            onClick={this.onClickMenuButton.bind(this)}
            className="invocation-menu-button"
            title="More invocation options">
            <MoreVertical />
          </OutlinedButton>
          <Popup isOpen={this.state.isMenuOpen} onRequestClose={this.onCloseMenu.bind(this)}>
            <Menu>
              <MenuItem
                disabled={!hasWritePermissions}
                title={hasWritePermissions ? undefined : "You do not have permission to delete this invocation."}
                onClick={this.onClickDeleteItem.bind(this)}>
                Delete invocation
              </MenuItem>
            </Menu>
          </Popup>
        </div>
        <Modal isOpen={this.state.isDeleteModalOpen} onRequestClose={this.onCloseDeleteModal.bind(this)}>
          <Dialog className="invocation-delete-dialog">
            <DialogHeader>
              <DialogTitle>Confirm deletion</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <div>Are you sure you want to delete this invocation? This action cannot be undone.</div>
              {this.state.deleteModalError && (
                <div className="error-description">{this.state.deleteModalError.description}</div>
              )}
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                {this.state.isDeleteModalLoading && <Spinner />}
                <OutlinedButton disabled={this.state.isDeleteModalLoading} onClick={this.onCloseDeleteModal.bind(this)}>
                  Cancel
                </OutlinedButton>
                <FilledButton
                  className="destructive"
                  disabled={this.state.isDeleteModalLoading}
                  onClick={this.onClickDelete.bind(this)}>
                  Delete
                </FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}

function canWrite(user: User, invocation: invocation.Invocation) {
  const acl = invocation.acl;
  if (acl.ownerPermissions.write && acl.userId.id === user.displayUser.userId.id) {
    return true;
  }
  if (acl.groupPermissions.write) {
    return user.groups.some((group) => group.id === acl.groupId);
  }
  return false;
}
