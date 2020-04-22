import React from 'react';
import rpcService from '../service/rpc_service';
import { bazel_config } from '../../proto/bazel_config_ts_proto';
import CacheCodeComponent from './cache_code';
import SetupCodeComponent from './setup_code';
import capabilities from '../capabilities/capabilities';

interface State {
  menuExpanded: boolean;
  bazelConfigResponse: bazel_config.GetBazelConfigResponse;
}

export default class SetupComponent extends React.Component {
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

  render() {
    return (
      <div className="home">
        <div className="container">
          <div className="title">Getting Started with BuildBuddy</div>
            {this.props.children}
            To get started, add the following two lines to your <b>.bazelrc</b> file. If you don't have a .bazelrc file - create one in the same directory as your Bazel WORKSPACE file with the two following lines:

            <h2>.bazelrc</h2>
            {this.state.bazelConfigResponse && <SetupCodeComponent bazelConfigResponse={this.state.bazelConfigResponse} />}
            Once you've added those two lines to your .bazelrc - you'll get a BuildBuddy url printed at the beginning and the end of every Bazel invocation like this:

            <h2>Bazel invocation</h2>
            <code>
            $ bazel build //...<br/>
            INFO: Streaming build results to: {window.location.protocol}//{window.location.host}/invocation/7bedd84e-525e-4b93-a5f5-53517d57752b<br/>
            ...
            </code>
            Now you can command click / double click on these urls to see the results of your build!

            <h2>Optional: Artifact uploads, test logs, and profiling</h2>
            If you'd like to configure a remote cache, which makes build artifacts clickable, enables the timing tab, and viewing of test logs - you can add the following additional line to your <b>.bazelrc</b>:
            {this.state.bazelConfigResponse && <CacheCodeComponent bazelConfigResponse={this.state.bazelConfigResponse} />}

            {!capabilities.enterprise && <div>
              <h2>Enterprise BuildBuddy</h2>
                Want enterprise features like SSO, organization build history, user build history, and more?<br/><br/>
                <b><a target="_blank" href="https://buildbuddy.typeform.com/to/wIXFIA">Click here</a></b> to upgrade to enterprise BuildBuddy.
            </div>}

            <h2>Get in touch!</h2>
            Reach out to <a href="mailto:hello@buildbuddy.io">hello@buildbuddy.io</a> if you have any questions or feature requests!
        </div>
      </div>
    );
  }
}
