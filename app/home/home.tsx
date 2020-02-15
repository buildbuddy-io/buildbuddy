import React from 'react';
import rpcService from 'buildbuddy/app/service/rpc_service'
import { invocation } from 'buildbuddy/proto/invocation_ts_proto';

interface State {
  invocations: invocation.Invocation[];
}

export default class HomeComponent extends React.Component {
  state: State = {
    invocations: []
  };

  componentWillMount() {
    let request = new invocation.GetInvocationRequest()
    rpcService.service.getInvocation(request).then((response) => {
      this.setState({ invocations: response.invocation });
    });
  }

  render() {
    return (
      <div>
        Invocations:
        {this.state.invocations}
      </div>
    );
  }
}
