import React from "react";
import authService from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";

interface State {
  orgName?: string;
}

export default class LoginComponent extends React.Component<{}, State> {
  state: State = {};

  private isJoiningOrg = window.location.pathname.startsWith("/join/");

  componentDidMount() {
    if (this.isJoiningOrg) {
      this.fetchOrgName();
    }
  }

  async fetchOrgName() {
    const urlIdentifier = window.location.pathname.split("/").pop();
    try {
      const { name } = await rpcService.service.getGroup({ urlIdentifier });
      this.setState({ orgName: name });
    } catch (e) {
      // TODO: handle 404 errors better
      router.navigateHome();
    }
  }

  componentWillMount() {
    document.title = `Login | BuildBuddy`;
  }

  handleLoginClicked() {
    authService.login();
  }

  handleSetupClicked() {
    router.navigateTo("/docs/setup");
  }

  render() {
    if (this.isJoiningOrg && !this.state.orgName) {
      return (
        <div className="login">
          <div className="loading" />
        </div>
      );
    }

    return (
      <div className="login">
        <div className="container">
          <div className="login-box">
            <div className={`login-hero ${!this.isJoiningOrg ? "hide-on-mobile" : ""}`}>
              <div className="login-hero-title">
                {!this.isJoiningOrg && (
                  <>
                    Faster Builds.
                    <br />
                    Happier Developers.
                  </>
                )}
                {this.isJoiningOrg && this.state.orgName && <>Join {this.state.orgName} on BuildBuddy</>}
              </div>
              <div className="hide-on-mobile">
                BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to
                build and test software 10x faster.
              </div>
            </div>
            <div className="login-buttons">
              <button onClick={this.handleLoginClicked.bind(this)}>Sign up for BuildBuddy</button>
              <button onClick={this.handleLoginClicked.bind(this)}>Log in to BuildBuddy</button>
              {capabilities.anonymous && (
                <button onClick={this.handleSetupClicked.bind(this)}>Use BuildBuddy without an account</button>
              )}
            </div>
          </div>
          <div className="on-prem">
            Want to run BuildBuddy on-prem?{" "}
            <a href="https://github.com/buildbuddy-io/buildbuddy/blob/master/docs/on-prem.md">Click here</a> for
            instructions.
          </div>
        </div>
      </div>
    );
  }
}
