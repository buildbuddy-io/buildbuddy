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
  cancelled?: boolean;
};

export default class InvocationCancelButtonComponent extends React.Component<InvocationCancelButtonComponentProps> {
  state: State = {};

  private onClick() {
    this.setState({ isLoading: true, cancelled: true });
    rpcService.service
      .cancelExecutions(new invocation.CancelExecutionsRequest({ invocationId: this.props.invocationId }))
      .catch((e) => {
        errorService.handleError(e);
        this.setState({ cancelled: false });
      })
      .finally(() => this.setState({ isLoading: false }));
  }

  render() {
    const isLoading = this.state.isLoading;
    const alreadyCancelled = this.state.cancelled;
    return (
      <div className="invocation-cancel-button-container">
        <OutlinedButton
          disabled={isLoading || alreadyCancelled}
          onClick={this.onClick.bind(this)}
          title={alreadyCancelled ? "Invocation has already been cancelled and is now being cleaned up." : undefined}>
          {isLoading ? <Spinner className="icon" /> : <SlashIcon className="icon" />}
          <div>Cancel</div>
        </OutlinedButton>
      </div>
    );
  }
}
