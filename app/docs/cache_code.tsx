import React from 'react';
import rpcService from '../service/rpc_service';
import { bazel_config } from '../../proto/bazel_config_ts_proto';

interface Props {
  bazelConfigResponse?: bazel_config.GetBazelConfigResponse;
}

interface State {
  bazelConfigResponse: bazel_config.GetBazelConfigResponse;
}

export default class CacheCodeComponent extends React.Component {
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
      <div>
        <code>
          {response?.configOption.find((option: any) => option.flagName == "remote_cache")?.body || "Caching disabled on this BuildBuddy instance"}
        </code >
        <br />
        <b>Recommended:</b> The following flag is recommended if connecting to a remote (upload speed constrained) BuildBuddy instance. It uploads build artifacts, test logs, and timing information without enabling cache writes:
        <code>
          build --noremote_upload_local_results <span className="comment"># Uploads logs & artifacts without writing to cache</span><br />
        </code>
        <br />
        If you'd like to enable writing to cache instead, this flag can help remove network bottlenecks:
        <code>
          build --remote_download_toplevel <span className="comment"># Helps remove network bottleneck if caching is enabled</span><br />
        </code>
        <br />
        For more information about these flags, check out the <a href="https://docs.bazel.build/versions/master/command-line-reference.html"><u>Bazel's command line reference</u></a>.
      </div>
    );
  }
}
