import React from "react";
import alert_service from "../../../app/alert/alert_service";
import authService, { User } from "../../../app/auth/auth_service";
import { FilledButton, OutlinedButton } from "../../../app/components/button/button";
import Dialog, {
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
  DialogHeader,
  DialogTitle,
} from "../../../app/components/dialog/dialog";
import Modal from "../../../app/components/modal/modal";
import Select, { Option } from "../../../app/components/select/select";
import rpc_service from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";

interface GroupStatusProps {
  user: User;
}

interface GroupStatusState {
  isStatusChangeModalOpen: boolean;
  pendingStatus?: grp.Group.GroupStatus;
}

const STATUS_OPTIONS: Array<{ value: grp.Group.GroupStatus; label: string }> = [
  { value: grp.Group.GroupStatus.UNKNOWN_GROUP_STATUS, label: "Unknown" },
  { value: grp.Group.GroupStatus.FREE_TIER_GROUP_STATUS, label: "Free Tier" },
  { value: grp.Group.GroupStatus.ENTERPRISE_GROUP_STATUS, label: "Enterprise" },
  { value: grp.Group.GroupStatus.ENTERPRISE_TRIAL_GROUP_STATUS, label: "Enterprise Trial" },
  { value: grp.Group.GroupStatus.BLOCKED_GROUP_STATUS, label: "Blocked" },
];

const getStatusLabel = (status: grp.Group.GroupStatus): string => {
  const option = STATUS_OPTIONS.find((opt) => opt.value === status);
  return option?.label || "Unknown";
};

export default class GroupStatusComponent extends React.Component<GroupStatusProps, GroupStatusState> {
  state: GroupStatusState = {
    isStatusChangeModalOpen: false,
  };

  private getCurrentStatus(): grp.Group.GroupStatus {
    return this.props.user.selectedGroup?.status || grp.Group.GroupStatus.UNKNOWN_GROUP_STATUS;
  }

  private handleStatusChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const newStatus = parseInt(e.target.value) as grp.Group.GroupStatus;
    if (newStatus === this.getCurrentStatus()) {
      return;
    }
    this.setState({
      isStatusChangeModalOpen: true,
      pendingStatus: newStatus,
    });
  };

  private onCloseStatusChangeModal = () => {
    this.setState({
      isStatusChangeModalOpen: false,
      pendingStatus: undefined,
    });
  };

  private confirmStatusChange = async () => {
    // 0 (UNKNOWN_GROUP_STATUS) is a valid value
    if (this.state.pendingStatus === undefined || this.state.pendingStatus === null) {
      return;
    }

    try {
      const request = grp.SetGroupStatusRequest.create({
        status: this.state.pendingStatus,
      });
      await rpc_service.service.setGroupStatus(request);

      this.setState({
        isStatusChangeModalOpen: false,
        pendingStatus: undefined,
      });

      alert_service.success("Organization status updated");
      await authService.refreshUser();
    } catch (e) {
      alert_service.error("Failed to update organization status: " + e);
      this.onCloseStatusChangeModal();
    }
  };

  render() {
    const currentStatus = this.getCurrentStatus();
    const { pendingStatus, isStatusChangeModalOpen } = this.state;

    return (
      <>
        <div className="settings-option-title">Organization status</div>
        <div className="settings-option-description">
          Change the status of this organization. This action will affect how the organization is treated by the system.
        </div>
        <div className="org-status-selector" style={{ marginTop: "16px", marginBottom: "16px" }}>
          <Select value={currentStatus.toString()} onChange={this.handleStatusChange}>
            {STATUS_OPTIONS.map((option) => (
              <Option key={option.value} value={option.value.toString()}>
                {option.label}
              </Option>
            ))}
          </Select>
        </div>
        <Modal isOpen={isStatusChangeModalOpen} onRequestClose={this.onCloseStatusChangeModal}>
          <Dialog className="status-change-dialog">
            <DialogHeader>
              <DialogTitle>Confirm status change</DialogTitle>
            </DialogHeader>
            <DialogBody>
              <p>
                Are you sure you want to change the organization status from <b>{getStatusLabel(currentStatus)}</b> to{" "}
                <b>{pendingStatus ? getStatusLabel(pendingStatus) : "Unknown"}</b>?
              </p>
              <p>This action will affect how the organization is treated by the system.</p>
            </DialogBody>
            <DialogFooter>
              <DialogFooterButtons>
                <OutlinedButton onClick={this.onCloseStatusChangeModal}>Cancel</OutlinedButton>
                <FilledButton onClick={this.confirmStatusChange}>Confirm</FilledButton>
              </DialogFooterButtons>
            </DialogFooter>
          </Dialog>
        </Modal>
      </>
    );
  }
}
