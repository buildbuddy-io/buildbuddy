import React from "react";
import { build } from "../../proto/remote_execution_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { workflow } from "../../proto/workflow_ts_proto";
import Button, { OutlinedButton } from "../components/button/button";
import { OutlinedButtonGroup } from "../components/button/button_group";
import Modal from "../components/modal/modal";
import Dialog, {
  DialogHeader,
  DialogTitle,
  DialogBody,
  DialogFooter,
  DialogFooterButtons,
} from "../components/dialog/dialog";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup, { PopupContainer } from "../components/popup/popup";
import errorService from "../errors/error_service";
import router from "../router/router";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import InvocationModel from "./invocation_model";
import Spinner from "../components/spinner/spinner";
import { ChevronDown, RefreshCw } from "lucide-react";
import Long from "long";
import { User } from "../auth/user";
import { firecracker } from "../../proto/firecracker_ts_proto";

export interface RemoteBazelUpsellModalProps {
  isOpen: boolean;
  onRequestClose: () => any;
}

export default class RemoteBazelUpsellModal extends React.Component<RemoteBazelUpsellModalProps> {
  private onClickWorkflowUpsell() {
    this.props.onRequestClose;
    window.open("/workflows", "_blank");
  }

  render() {
    return (
      <Modal className="workflow-upsell-modal" isOpen={this.props.isOpen} onRequestClose={this.props.onRequestClose}>
        <Dialog>
          <DialogHeader>
            <DialogTitle>GitHub Sync Required</DialogTitle>
          </DialogHeader>
          <DialogBody>
            {/*TODO(Maggie): Link to a remote bazel blog post*/}
            <p>To use Remote Bazel backed UI features, you must link a GitHub repository.</p>
          </DialogBody>
          <DialogFooter>
            <OutlinedButton onClick={this.props.onRequestClose}>Cancel</OutlinedButton>
            <Button onClick={this.onClickWorkflowUpsell.bind(this)}>Link a Repository</Button>
          </DialogFooter>
        </Dialog>
      </Modal>
    );
  }
}
