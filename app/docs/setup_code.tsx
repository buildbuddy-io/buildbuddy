import React from 'react';
import rpcService from '../service/rpc_service';
import { bazel_config } from '../../proto/bazel_config_ts_proto';

interface Props {
  bazelConfigResponse?: bazel_config.GetBazelConfigResponse;
}

interface State {
  bazelConfigResponse: bazel_config.GetBazelConfigResponse;
}

export default class SetupCodeComponent extends React.Component {
  props: Props;
  state: State = {
    bazelConfigResponse: null
  };

  componentWillMount() {
    if (this.props.bazelConfigResponse) {
      return;
    }
    let request = new bazel_config.GetBazelConfigRequest();
    request.host = window.location.host;
    request.protocol = window.location.protocol;
    rpcService.service.getBazelConfig(request).then((response: bazel_config.GetBazelConfigResponse) => {
      this.setState({ ...this.state, bazelConfigResponse: response })
    });

  }

  render() {
    let response = this.props.bazelConfigResponse || this.state.bazelConfigResponse;
    return (
      <code>
        {response?.configOption.find((option: any) => option.flagName == "bes_results_url")?.body}<br />
        {response?.configOption.find((option: any) => option.flagName == "bes_backend")?.body}<br />
      </code>
    );
  }
}
