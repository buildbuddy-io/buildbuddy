import React from "react";
import authService from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import Input from "../../../app/components/input/input";
import Button from "../../../app/components/button/button";
import alertService from "../../../app/alert/alert_service";

interface State {
  orgName?: string;
  showSSO: boolean;
  ssoSlug: string;
  defaultToSSO: boolean;
}

interface Props {
  search: URLSearchParams;
}

export default class LoginComponent extends React.Component<Props, State> {
  state: State = {
    showSSO: false,
    defaultToSSO: false,
    ssoSlug: this.getUrlSlug(),
  };

  ssoSlugButton = React.createRef<HTMLInputElement>();

  componentDidMount() {
    if (this.isJoiningOrg()) {
      this.fetchOrgName();
    }
  }

  isJoiningOrg() {
    return window.location.pathname.startsWith("/join/");
  }

  getUrlSlug() {
    if (this.isJoiningOrg()) {
      return window.location.pathname.split("/").pop();
    }
    return "";
  }

  async fetchOrgName() {
    try {
      const { name, ssoEnabled } = await rpcService.service.getGroup({ urlIdentifier: this.getUrlSlug() });
      this.setState({ orgName: name, defaultToSSO: ssoEnabled });
    } catch (e) {
      // TODO: handle 404 errors better
      router.navigateHome();
    }
  }

  componentWillMount() {
    document.title = `Login | BuildBuddy`;
  }

  handleLoginClicked(event: any) {
    if (this.state.defaultToSSO) {
      this.handleSSOClicked(event);
      return;
    }

    authService.login();
  }

  handleSSOClicked(event: any) {
    event.preventDefault();

    if (!this.state.showSSO && !this.state.ssoSlug) {
      this.setState({ showSSO: true }, () => {
        this.ssoSlugButton.current.focus();
      });
      return;
    }

    if (!this.state.ssoSlug) {
      alertService.error("Enter your organization's url slug to continue");
      return;
    }

    authService.login(this.state.ssoSlug);
  }

  handleSetupClicked() {
    router.navigateTo("/docs/setup");
  }

  onChange(e: React.ChangeEvent<HTMLInputElement>) {
    this.setState({ ssoSlug: e.target.value });
  }

  render() {
    if (this.isJoiningOrg() && !this.state.orgName) {
      return (
        <div className="login">
          <div className="loading" />
        </div>
      );
    }

    let redirecting = this.props.search.has("redirect_url");
    return (
      <div className="login">
        <div className="container">
          <div className="login-box">
            <div className={`login-hero ${!this.isJoiningOrg() ? "hide-on-mobile" : ""}`}>
              <div className="login-hero-title">
                {!redirecting && !this.isJoiningOrg() && (
                  <>
                    Faster Builds.
                    <br />
                    Happier Developers.
                  </>
                )}
                {!redirecting && this.isJoiningOrg() && this.state.orgName && (
                  <>Join {this.state.orgName} on BuildBuddy</>
                )}
                {redirecting && <>Login to continue</>}
              </div>
              <div className="hide-on-mobile">
                BuildBuddy provides enterprise features for Bazel — the open source build system that allows you to
                build and test software 10x faster.
              </div>
            </div>
            <div className="login-buttons">
              <button className="signup-button" onClick={this.handleLoginClicked.bind(this)}>
                Sign up for BuildBuddy
              </button>
              <button className="login-button" onClick={this.handleLoginClicked.bind(this)}>
                Log in to BuildBuddy
              </button>
              {capabilities.anonymous && (
                <button onClick={this.handleSetupClicked.bind(this)}>Use BuildBuddy without an account</button>
              )}
            </div>
          </div>
          <div className="on-prem">
            Want to run BuildBuddy on-prem? <a href="https://docs.buildbuddy.io/docs/on-prem">Click here</a> for
            instructions.
          </div>
          {capabilities.sso && (
            <form className="sso" onSubmit={this.handleSSOClicked.bind(this)}>
              {this.state.showSSO && (
                <>
                  <Input
                    name="ssoSlug"
                    value={this.state.ssoSlug}
                    onChange={this.onChange.bind(this)}
                    placeholder="my-team-slug"
                    ref={this.ssoSlugButton}
                  />
                </>
              )}
              <Button className={this.state.ssoSlug ? "active" : ""}>Log in with SSO</Button>
            </form>
          )}
        </div>
      </div>
    );
  }
}
