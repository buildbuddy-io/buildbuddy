import React from "react";
import Button, { OutlinedButton } from "../components/button/button";
import { OutlinedButtonGroup } from "../components/button/button_group";
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

export interface RemoteBazelRerunButtonProps {
  model: InvocationModel;
}

type State = {
  isMenuOpen: boolean;
  isDialogOpen: boolean;
  isLoading: boolean;
};

export default class RemoteBazelRerunButton extends React.Component<RemoteBazelRerunButtonProps, State> {
  state: State = {
    isMenuOpen: false,
    isDialogOpen: false,
    isLoading: false,
  };

  private inFlightRpc?: CancelablePromise;

  private async onClick() {}

  componentWillUnmount() {
    this.inFlightRpc?.cancel();
  }

  render() {
    return (
      <>
        <PopupContainer>
          <OutlinedButtonGroup>
            <OutlinedButton
              // disabled=false
              // className="workflow-rerun-button"
              onClick={this.onClick.bind(this, /*clean=*/ false)}>
              <span>Show coverage</span>
            </OutlinedButton>
          </OutlinedButtonGroup>
        </PopupContainer>
      </>
    );
  }
}
