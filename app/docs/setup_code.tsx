import React from "react";
import { bazel_config } from "../../proto/bazel_config_ts_proto";
import authService, { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import FilledButton from "../components/button/button";
import Select, { Option } from "../components/select/select";
import rpcService from "../service/rpc_service";
import router from "../router/router";

interface Props {
  bazelConfigResponse?: bazel_config.IGetBazelConfigResponse;
}

interface State {
  bazelConfigResponse: bazel_config.IGetBazelConfigResponse;
  user: User;
  selectedCredentialIndex: number;

  auth: "none" | "cert" | "key";
  separateAuth: boolean;
  cache: "read" | "top" | "full";
  cacheChecked: boolean;
  executionChecked: boolean;
}

export default class SetupCodeComponent extends React.Component<Props, State> {
  state: State = {
    bazelConfigResponse: null,
    selectedCredentialIndex: 0,
    user: authService.user,

    auth: capabilities.auth ? "key" : "none",
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
      this.setConfigResponse(this.props.bazelConfigResponse);
      return;
    }
    let request = new bazel_config.GetBazelConfigRequest();
    request.host = window.location.host;
    request.protocol = window.location.protocol;
    request.includeCertificate = true;
    rpcService.service.getBazelConfig(request).then(this.setConfigResponse.bind(this));
  }

  componentDidUpdate(prevProps: Props) {
    if (this.props.bazelConfigResponse !== prevProps.bazelConfigResponse) {
      this.setConfigResponse(this.props.bazelConfigResponse);
    }
  }

  setConfigResponse(response: bazel_config.IGetBazelConfigResponse) {
    this.setState({ bazelConfigResponse: response, selectedCredentialIndex: 0 });
  }

  getSelectedCredential() {
    const { bazelConfigResponse: response, selectedCredentialIndex: index } = this.state;
    if (!response?.credential) return null;

    return response.credential[index] || null;
  }

  handleInputChange(event: React.ChangeEvent<HTMLInputElement>) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.value,
    } as Record<keyof State, any>);
  }

  handleCheckboxChange(event: React.ChangeEvent<HTMLInputElement>) {
    const target = event.target;
    const name = target.name;
    this.setState({
      [name]: target.checked,
    } as Record<keyof State, any>);
  }

  getResultsUrl() {
    return this.state.bazelConfigResponse?.configOption.find(
      (option: bazel_config.IConfigOption) => option.flagName == "bes_results_url"
    )?.body;
  }

  getEventStream() {
    return this.state.bazelConfigResponse?.configOption.find(
      (option: bazel_config.IConfigOption) => option.flagName == "bes_backend"
    )?.body;
  }

  getCache() {
    return this.state.bazelConfigResponse?.configOption.find(
      (option: bazel_config.IConfigOption) => option.flagName == "remote_cache"
    )?.body;
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

  getRemoteOptions() {
    return <span>build --remote_timeout=3600</span>;
  }

  getRemoteExecution() {
    return this.state.bazelConfigResponse?.configOption?.find(
      (option: bazel_config.IConfigOption) => option.flagName == "remote_executor"
    )?.body;
  }

  getCredentials() {
    if (this.state.auth == "cert") {
      return (
        <div>
          <div>build --tls_client_certificate=buildbuddy-cert.pem</div>
          <div>build --tls_client_key=buildbuddy-key.pem</div>
        </div>
      );
    }

    if (this.state.auth == "key") {
      const selectedCredential = this.getSelectedCredential();
      if (!selectedCredential) return null;

      return (
        <div>
          <div>build --remote_header=x-buildbuddy-api-key={selectedCredential.apiKey.value}</div>
        </div>
      );
    }

    return null;
  }

  isAuthEnabled() {
    return Boolean(capabilities.auth && this.state.user);
  }

  isCacheEnabled() {
    return Boolean(
      this.state.bazelConfigResponse?.configOption.find(
        (option: bazel_config.IConfigOption) => option.flagName == "remote_cache"
      )
    );
  }

  isExecutionEnabled() {
    return (
      (this.isAuthenticated() || !capabilities.auth) &&
      Boolean(
        this.state.bazelConfigResponse?.configOption.find(
          (option: bazel_config.IConfigOption) => option.flagName == "remote_executor"
        )
      )
    );
  }

  isCertEnabled() {
    const certificate = this.getSelectedCredential()?.certificate;
    return Boolean(certificate && certificate.cert && certificate.key);
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

  onChangeCredential(e: React.ChangeEvent<HTMLSelectElement>) {
    this.setState({ selectedCredentialIndex: Number(e.target.value) });
  }

  onClickLink(href: string, e: React.MouseEvent<HTMLAnchorElement>) {
    e.preventDefault();
    router.navigateTo(href);
  }

  renderMissingApiKeysNotice() {
    return (
      <div className="no-api-keys">
        <div className="no-api-keys-content">
          <div>
            {capabilities.anonymous ? (
              <>Looks like your organization doesn't have any API keys that are visible to you.</>
            ) : (
              <>This BuildBuddy installation requires authentication, but no API keys are set up.</>
            )}
            {!this.state.user.canCall("createApiKey") && <> Only organization administrators can create API keys.</>}
          </div>
          {this.state.user.canCall("createApiKey") && (
            <div>
              <FilledButton className="manage-keys-button">
                <a href="/settings/org/api-keys" onClick={this.onClickLink.bind(this, "/settings/org/api-keys")}>
                  Manage keys
                </a>
              </FilledButton>
            </div>
          )}
        </div>
      </div>
    );
  }

  render() {
    if (!this.state.bazelConfigResponse) {
      return <div className="loading"></div>;
    }
    const selectedCredential = this.getSelectedCredential();

    if (!capabilities.anonymous && !selectedCredential) {
      // TODO(bduffany): Ideally we'd hide the entire setup page and direct the user to set up
      // API keys before taking any further steps.
      return <div className="setup">{this.renderMissingApiKeysNotice()}</div>;
    }

    return (
      <div className="setup">
        <div className="setup-step-header">Select options</div>
        <div className="setup-controls">
          {this.isAuthEnabled() && (
            <span className="group-container">
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
            </span>
          )}

          {(this.state.auth === "cert" || this.state.auth === "key") &&
            (this.state.bazelConfigResponse?.credential?.length || 0) > 1 && (
              <span>
                <Select
                  title="Select API key"
                  className="credential-picker"
                  name="selectedCredential"
                  value={this.state.selectedCredentialIndex}
                  onChange={this.onChangeCredential.bind(this)}>
                  {this.state.bazelConfigResponse.credential.map(({ apiKey: { label, value } }, index) => (
                    <Option key={index} value={index}>
                      {label || "Untitled key"} - {value}
                    </Option>
                  ))}
                </Select>
              </span>
            )}

          {this.isAuthenticated() && (
            <span className="group-container">
              <span className="group">
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
            </span>
          )}

          {this.isCacheEnabled() && (
            <span className="group-container">
              <span className="group">
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
            </span>
          )}

          {this.state.cacheChecked && (
            <span className="group-container">
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
            </span>
          )}

          {this.isExecutionEnabled() && (
            <span className="group-container">
              <span className="group">
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
            </span>
          )}
        </div>
        {this.state.executionChecked && (
          <div className="setup-notice">
            <b>Note:</b> You've enabled remote execution. In addition to these .bazelrc flags, you'll also need to
            configure platforms and toolchains which likely involve modifying your WORKSPACE file. See{" "}
            <b>
              <a target="_blank" href="https://www.buildbuddy.io/docs/rbe-setup">
                our guide on configuring platforms and toolchains
              </a>
            </b>{" "}
            for more info.
          </div>
        )}
        {!(this.isAuthenticated() && !selectedCredential) && (
          <>
            <b>Copy to your .bazelrc</b>
            <code data-header=".bazelrc">
              <div className="contents">
                <div>{this.getResultsUrl()}</div>
                <div>{this.getEventStream()}</div>
                {this.state.cacheChecked && <div>{this.getCache()}</div>}
                {this.state.cacheChecked && <div>{this.getCacheOptions()}</div>}
                {(this.state.cacheChecked || this.state.executionChecked) && <div>{this.getRemoteOptions()}</div>}
                {this.state.executionChecked && <div>{this.getRemoteExecution()}</div>}
                {!this.state.separateAuth && <div>{this.getCredentials()}</div>}
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
                    <div>{this.getCredentials()}</div>
                  </div>
                  <button onClick={this.handleCopyClicked.bind(this)}>Copy</button>
                </code>
              </div>
            )}
            {this.state.auth == "cert" && (
              <div>
                <div className="downloads">
                  {selectedCredential?.certificate?.cert && (
                    <div>
                      <a
                        download="buildbuddy-cert.pem"
                        href={window.URL.createObjectURL(
                          new Blob([selectedCredential.certificate.cert], {
                            type: "text/plain",
                          })
                        )}>
                        Download buildbuddy-cert.pem
                      </a>
                    </div>
                  )}
                  {selectedCredential?.certificate?.key && (
                    <div>
                      <a
                        download="buildbuddy-key.pem"
                        href={window.URL.createObjectURL(
                          new Blob([selectedCredential.certificate.key], {
                            type: "text/plain",
                          })
                        )}>
                        Download buildbuddy-key.pem
                      </a>
                    </div>
                  )}
                </div>
                To use certificate based auth, download the two files above and place them in your workspace directory.
                If you place them outside of your workspace, update the paths in your{" "}
                <span className="code">.bazelrc</span> file to point to the correct location.
                <br />
                <br />
                Note: Certificate based auth is only compatible with Bazel version 3.1 and above.
              </div>
            )}
          </>
        )}
        {this.isAuthenticated() && !selectedCredential && this.renderMissingApiKeysNotice()}
      </div>
    );
  }
}
