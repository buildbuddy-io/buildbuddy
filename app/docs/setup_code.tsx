import React from "react";
import rpcService from "../service/rpc_service";
import { bazel_config } from "../../proto/bazel_config_ts_proto";
import capabilities from "../capabilities/capabilities";
import authService, { AuthService, User } from "../auth/auth_service";

interface Props {
  bazelConfigResponse?: bazel_config.GetBazelConfigResponse;
}

interface State {
  bazelConfigResponse: bazel_config.GetBazelConfigResponse;
  user: User;

  auth: string;
  cacheChecked: boolean;
  cache: string;
  executionChecked: boolean;
  separateAuth: boolean;
}

export default class SetupCodeComponent extends React.Component {
  props: Props;
  state: State = {
    bazelConfigResponse: null,
    user: authService.user,

    auth: "key",
    cacheChecked: false,
    cache: "read",
    executionChecked: false,
    separateAuth: false,
  };

  componentWillMount() {
    authService.userStream.subscribe({
      next: (user: User) => this.setState({ ...this.state, user }),
    });

    if (this.props.bazelConfigResponse) {
      return;
    }
    let request = new bazel_config.GetBazelConfigRequest();
    request.host = window.location.host;
    request.protocol = window.location.protocol;
    request.includeCertificate = true;
    rpcService.service.getBazelConfig(request).then((response: bazel_config.GetBazelConfigResponse) => {
      this.setState({ ...this.state, bazelConfigResponse: response });
    });
  }

  handleInputChange(event: any) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.value,
    });
  }

  handleCheckboxChange(event: any) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.checked,
    });
  }

  getResponse() {
    return this.props.bazelConfigResponse || this.state.bazelConfigResponse;
  }

  getResultsUrl() {
    return this.getResponse()?.configOption.find((option: any) => option.flagName == "bes_results_url")?.body;
  }

  getEventStream(otherFile?: boolean) {
    let url = this.getResponse()?.configOption.find((option: any) => option.flagName == "bes_backend")?.body;
    return this.state.auth == "key" && (!this.state.separateAuth || (this.state.separateAuth && otherFile))
      ? url
      : this.stripAPIKey(url);
  }

  getCache(otherFile?: boolean) {
    let url = this.getResponse()?.configOption.find((option: any) => option.flagName == "remote_cache")?.body;
    return this.state.auth == "key" && (!this.state.separateAuth || (this.state.separateAuth && otherFile))
      ? url
      : this.stripAPIKey(url);
  }

  getCacheOptions() {
    if (this.state.cache == "read")
      return (
        <span>
          build --noremote_upload_local_results{" "}
          <span className="comment"># Uploads logs & artifacts without writing to cache</span>
        </span>
      );
    if (this.state.cache == "top")
      return (
        <span>
          build --remote_download_toplevel{" "}
          <span className="comment"># Helps remove network bottleneck if caching is enabled</span>
        </span>
      );
    return "";
  }

  getRemoteExecution(otherFile?: boolean) {
    let url = this.getResponse()?.configOption.find((option: any) => option.flagName == "remote_executor")?.body;
    return this.state.auth == "key" && (!this.state.separateAuth || (this.state.separateAuth && otherFile))
      ? url
      : this.stripAPIKey(url);
  }

  getCredentials() {
    return (
      <div>
        <div>build --tls_client_certificate=buildbuddy-cert.pem</div>
        <div>build --tls_client_key=buildbuddy-key.pem</div>
      </div>
    );
  }

  isAuthEnabled() {
    return !!capabilities.auth && !!this.state.user;
  }

  isCacheEnabled() {
    return !!this.getResponse()?.configOption.find((option: any) => option.flagName == "remote_cache");
  }

  isExecutionEnabled() {
    return (
      this.isAuthenticated() &&
      !!this.getResponse()?.configOption.find((option: any) => option.flagName == "remote_executor")
    );
  }

  isCertEnabled() {
    return this.getResponse()?.certificate?.cert && this.getResponse()?.certificate?.key;
  }

  stripAPIKey(url: string) {
    return url.replace(/\:\/\/.*\@/gi, "://");
  }

  isAuthenticated() {
    return this.isAuthEnabled() && this.state.auth != "none";
  }

  handleCopyClicked(event: any) {
    var copyText = event.target.parentElement.firstChild;
    var input = document.createElement("textarea");
    input.value = copyText.innerText;
    document.body.appendChild(input);
    input.select();
    document.execCommand("copy");
    document.body.removeChild(input);
    event.target.innerText = "Copied!";
  }

  render() {
    return (
      <div className="setup">
        <div className="setup-controls">
          <span className="group-title">Select options</span>
          {this.isAuthEnabled() && (
            <span className="group">
              {capabilities.anonymous && (
                <input
                  id="auth-none"
                  checked={this.state.auth == "none"}
                  onChange={this.handleInputChange.bind(this)}
                  value="none"
                  name="auth"
                  type="radio"
                />
              )}
              {capabilities.anonymous && <label htmlFor="auth-none">No auth</label>}
              <input
                id="auth-key"
                checked={this.state.auth == "key"}
                onChange={this.handleInputChange.bind(this)}
                value="key"
                name="auth"
                type="radio"
              />
              <label htmlFor="auth-key">API Key</label>
              {this.isCertEnabled() && (
                <span className="group-section">
                  <input
                    id="auth-cert"
                    checked={this.state.auth == "cert"}
                    onChange={this.handleInputChange.bind(this)}
                    value="cert"
                    name="auth"
                    type="radio"
                  />
                  <label htmlFor="auth-cert">Certificate</label>
                </span>
              )}
            </span>
          )}

          {this.isAuthenticated() && (
            <span>
              <input
                id="split"
                checked={this.state.separateAuth}
                onChange={this.handleCheckboxChange.bind(this)}
                name="separateAuth"
                type="checkbox"
              />
              <label htmlFor="split">
                <span>Separate auth file</span>
              </label>
            </span>
          )}

          {this.isCacheEnabled() && (
            <span>
              <input
                id="cache"
                checked={this.state.cacheChecked}
                onChange={this.handleCheckboxChange.bind(this)}
                name="cacheChecked"
                type="checkbox"
              />
              <label htmlFor="cache">
                <span>Enable cache</span>
              </label>
            </span>
          )}

          {this.state.cacheChecked && (
            <span className="group">
              <input
                id="cache-full"
                checked={this.state.cache == "full"}
                onChange={this.handleInputChange.bind(this)}
                value="full"
                name="cache"
                type="radio"
              />
              <label htmlFor="cache-full">Full cache</label>
              <input
                id="cache-read"
                checked={this.state.cache == "read"}
                onChange={this.handleInputChange.bind(this)}
                value="read"
                name="cache"
                type="radio"
              />
              <label htmlFor="cache-read">Read only</label>
              <input
                id="cache-top"
                checked={this.state.cache == "top"}
                onChange={this.handleInputChange.bind(this)}
                value="top"
                name="cache"
                type="radio"
              />
              <label htmlFor="cache-top">Top level</label>
            </span>
          )}

          {this.isExecutionEnabled() && (
            <span>
              <input
                id="execution"
                checked={this.state.executionChecked}
                onChange={this.handleCheckboxChange.bind(this)}
                name="executionChecked"
                type="checkbox"
              />
              <label htmlFor="execution">
                <span>Enable remote execution</span>
              </label>
            </span>
          )}
        </div>
        <b>Copy to your .bazelrc</b>
        <code data-header=".bazelrc">
          <div className="contents">
            <div>{this.getResultsUrl()}</div>
            <div>{this.getEventStream()}</div>
            {this.state.cacheChecked && <div>{this.getCache()}</div>}
            {this.state.cacheChecked && <div>{this.getCacheOptions()}</div>}
            {this.state.executionChecked && <div>{this.getRemoteExecution()}</div>}
            {this.state.auth == "cert" && !this.state.separateAuth && <div>{this.getCredentials()}</div>}
          </div>
          <button onClick={this.handleCopyClicked.bind(this)}>Copy</button>
        </code>
        {this.state.separateAuth && this.isAuthenticated() && (
          <div>
            The file below contains your auth credentials - place it in your home directory at{" "}
            <span className="code">~/.bazelrc</span> or place it anywhere you'd like and add{" "}
            <span className="code">try-import /path/to/your/.bazelrc</span> in your primary{" "}
            <span className="code">.bazelrc</span> file.
            <code data-header="~/.bazelrc">
              <div className="contents">
                {this.state.auth != "cert" && <div>{this.getEventStream(true)}</div>}
                {this.state.auth != "cert" && this.state.cacheChecked && <div>{this.getCache(true)}</div>}
                {this.state.auth != "cert" && this.state.executionChecked && <div>{this.getRemoteExecution(true)}</div>}
                {this.state.auth == "cert" && <div>{this.getCredentials()}</div>}
              </div>
              <button onClick={this.handleCopyClicked.bind(this)}>Copy</button>
            </code>
          </div>
        )}
        {this.state.auth == "cert" && (
          <div>
            <div className="downloads">
              {this.getResponse()?.certificate?.cert && (
                <div>
                  <a
                    download="buildbuddy-cert.pem"
                    href={window.URL.createObjectURL(
                      new Blob([this.getResponse()?.certificate?.cert], {
                        type: "text/plain",
                      })
                    )}>
                    Download buildbuddy-cert.pem
                  </a>
                </div>
              )}
              {this.getResponse()?.certificate?.key && (
                <div>
                  <a
                    download="buildbuddy-key.pem"
                    href={window.URL.createObjectURL(
                      new Blob([this.getResponse()?.certificate?.key], {
                        type: "text/plain",
                      })
                    )}>
                    Download buildbuddy-key.pem
                  </a>
                </div>
              )}
            </div>
            To use certificate based auth. Download the two files above and place them in your workspace directory. If
            you place them outside of your workspace - make sure to update the paths in your .bazelrc file to point to
            the correct location.
            <br />
            <br />
            Note: Certificate based auth is only compatible with Bazel version 3.1 and above.
          </div>
        )}
      </div>
    );
  }
}
