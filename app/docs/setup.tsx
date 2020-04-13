import React from 'react';
import rpcService from '../service/rpc_service';
import { bazel_config } from '../../proto/bazel_config_ts_proto';
import CacheCodeComponent from './cache_code';
import SetupCodeComponent from './setup_code';

interface Props {
  title?: string;
}

interface State {
  menuExpanded: boolean;
  bazelConfigResponse: bazel_config.GetBazelConfigResponse;
}

export default class SetupComponent extends React.Component {
  props: Props;

  state: State = {
    menuExpanded: false,
    bazelConfigResponse: null
  };

  componentWillMount() {
    document.title = `Setup | Buildbuddy`;
    
    let request = new bazel_config.GetBazelConfigRequest();
    request.host = window.location.host;
    request.protocol = window.location.protocol;
    rpcService.service.getBazelConfig(request).then((response: bazel_config.GetBazelConfigResponse) => {
      console.log(response);
      this.setState({...this.state, bazelConfigResponse: response})
    });
    
  }

  handleMenuClicked() {
    this.setState({ menuExpanded: !this.state.menuExpanded });
  }

  render() {
    return (
      <div className="home">
        <div className="container">
          {!this.props.title && <div className="title">Getting Started with BuildBuddy</div>}
          {this.props.title && <p><b>{this.props.title}</b></p>}
          <p>
            To get started, add the following two lines to your .bazelrc file. If you don't have a .bazelrc file - create one in the same directory as your Bazel WORKSPACE file with the two following lines:
          </p>
          <p>
            <b>.bazelrc</b>
            {this.state.bazelConfigResponse && <SetupCodeComponent bazelConfigResponse={this.state.bazelConfigResponse} />}
          </p>
          <p>
            Once you've added those two lines to your .bazelrc - you'll get a BuildBuddy url printed at the beginning and the end of every Bazel invocation like this:
          </p>
          <p>
            <b>Bazel invocation</b>
            <code>
            $ bazel build //...<br/>
            INFO: Streaming build results to: {window.location.protocol}//{window.location.host}/invocation/7bedd84e-525e-4b93-a5f5-53517d57752b<br/>
            ...
            </code>
          </p>
          <p>
            Now you can command click / double click on these urls to see the results of your build!
          </p>
          <p>
            <b>Optional: Caching, artifact uploads, and profiling</b>
          </p>
          <p>
            If you'd like to enable caching, which makes build artifacts clickable and enables the timing tab - you can add the following additional line to your .bazelrc:
            {this.state.bazelConfigResponse && <CacheCodeComponent bazelConfigResponse={this.state.bazelConfigResponse} />}
          </p>
          <p>
            Feel free to reach out to <a href="mailto:hello@buildbuddy.io">hello@buildbuddy.io</a> if you have any questions or feature requests.
          </p>
        </div>
      </div>
    );
  }
}
