import React from "react";
import errorService from "../../../app/errors/error_service";
import rpcService from "../../../app/service/rpc_service";
import { usage } from "../../../proto/usage_ts_proto";

export interface UsageProps {}

interface State {
  response?: usage.GetUsageResponse;
  loading?: boolean;
}

export default class UsageComponent extends React.Component<UsageProps, State> {
  state: State = {};

  componentDidMount() {
    document.title = "Usage | BuildBuddy";

    this.setState({ loading: true });
    rpcService.service
      .getUsage(new usage.GetUsageRequest())
      .then((response) => this.setState({ response }))
      .catch((e) => errorService.handleError(e))
      .finally(() => this.setState({ loading: false }));
  }

  render() {
    return (
      <div className="usage-page">
        <div className="container">
          <div className="usage-header">
            <div className="usage-title">Usage</div>
          </div>
          {this.state.loading && <div className="loading" />}
          {/* TODO(bduffany): render the data nicely */}
          {this.state.response && <pre>{JSON.stringify(this.state.response, null, 2)}</pre>}
        </div>
      </div>
    );
  }
}
