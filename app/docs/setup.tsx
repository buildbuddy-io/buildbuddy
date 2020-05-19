import React from 'react';
import rpcService from '../service/rpc_service';
import { bazel_config } from '../../proto/bazel_config_ts_proto';
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
    request.includeCertificate = true;
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
            To get started, add the following lines to your <b>.bazelrc</b> file. If you don't have a .bazelrc file - create one in the same directory as your Bazel WORKSPACE file with the following lines:

            {this.state.bazelConfigResponse && <SetupCodeComponent bazelConfigResponse={this.state.bazelConfigResponse} />}

            <h2>Bazel invocation</h2>
            Once you've added those lines to your .bazelrc - you'll get a BuildBuddy url printed at the beginning and the end of every Bazel invocation like this:
            <code>
            $ bazel build //...<br/>
            INFO: Streaming build results to: {window.location.protocol}//{window.location.host}/invocation/7bedd84e-525e-4b93-a5f5-53517d57752b<br/>
            ...
            </code>
            Now you can command click / double click on these urls to see the results of your build!

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
