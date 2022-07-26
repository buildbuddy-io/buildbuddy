import React from "react";
import { Slash as SlashIcon } from "lucide-react";
import Spinner from "../components/spinner/spinner";
import { OutlinedButton } from "../components/button/button";
import rpcService from "../service/rpc_service";
import { invocation } from "../../proto/invocation_ts_proto";
import errorService from "../errors/error_service";

export interface InvocationCancelButtonComponentProps {
  invocationId: string;
}

type State = {
  isLoading?: boolean;
};

export default class InvocationCancelButtonComponent extends React.Component<InvocationCancelButtonComponentProps> {
  state: State = {};

  private onClick() {
    this.setState({ isLoading: true });
    rpcService.service
      .cancelInvocation(new invocation.CancelInvocationRequest({ invocationId: this.props.invocationId }))
      .catch((e) => errorService.handleError(e))
      .finally(() => {
        // Delay updating button loading state because after the Cancel RPC completes, can take some time for the
        // invocation to be marked as disconnected
        setTimeout(() => {
          this.setState({ isLoading: false });
        }, 2000);
      });
  }

  render() {
    const isLoading = this.state.isLoading;
    return (
      <div className="invocation-cancel-button-container">
        <OutlinedButton disabled={isLoading} onClick={this.onClick.bind(this)}>
          {isLoading ? <Spinner className="icon" /> : <SlashIcon className="icon" />}
          <div>Cancel</div>
        </OutlinedButton>
      </div>
    );
  }
}
