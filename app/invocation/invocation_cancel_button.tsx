import React from "react";
import { Slash as SlashIcon } from "lucide-react";
import { OutlinedButton } from "../components/button/button";
import rpcService from "../service/rpc_service";
import { invocation } from "../../proto/invocation_ts_proto";

export interface InvocationCancelButtonComponentProps {
  invocationId: string;
}

export default class InvocationCancelButtonComponent extends React.Component<InvocationCancelButtonComponentProps> {
  private onClick() {
    rpcService.service.cancelInvocation(
      new invocation.CancelInvocationRequest({ invocationId: this.props.invocationId })
    );
  }

  render() {
    return (
      <div className="invocation-cancel-button-container">
        <OutlinedButton onClick={this.onClick.bind(this)}>
          <SlashIcon className="icon" />
          <div>Cancel</div>
        </OutlinedButton>
      </div>
    );
  }
}
