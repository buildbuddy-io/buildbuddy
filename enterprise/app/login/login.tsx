import React from "react";
import authService from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import Input from "../../../app/components/input/input";
import alertService from "../../../app/alert/alert_service";
import { grp } from "../../../proto/group_ts_proto";
import { ArrowRight, Lock, User } from "lucide-react";
import popup from "../../../app/util/popup";
import error_service from "../../../app/errors/error_service";
import { GoogleIcon } from "../../../app/icons/google";
import { GithubIcon } from "../../../app/icons/github";

interface State {
  orgName?: string;
  showSSO: boolean;
  ssoSlug?: string;
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
      const { name, ssoEnabled } = await rpcService.service.getGroup(
        grp.GetGroupRequest.create({ urlIdentifier: this.getUrlSlug() })
      );
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
    authService.login();
  }

  handleGithubClicked() {
    const url = "/login/github/";
    if (capabilities.config.popupAuthEnabled) {
      popup
        .open(url)
        .then(() => authService.refreshUser())
        .catch(error_service.handleError);
      return;
    }

    window.location.href = url;
  }

  handleSSOClicked(event: any) {
    event.preventDefault();

    if (!this.state.showSSO && !this.state.ssoSlug) {
      this.setState({ showSSO: true }, () => {
        this.ssoSlugButton.current?.focus();
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

  isGoogleConfigured() {
    return (
      capabilities.config.configuredIssuers.length &&
      capabilities.config.configuredIssuers[0].includes("accounts.google.com")
    );
  }
  isOktaConfigured() {
    return (
      capabilities.config.configuredIssuers.length && capabilities.config.configuredIssuers[0].includes("okta.com")
    );
  }

  render() {
    if (this.isJoiningOrg() && !this.state.orgName) {
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
            <div className="login-buttons">
              {!this.isGoogleConfigured() && !this.isOktaConfigured() && (
                <button debug-id="login-button" className="login-button" onClick={this.handleLoginClicked.bind(this)}>
                  <User /> Continue
                </button>
              )}
              {this.isGoogleConfigured() && (
                <button debug-id="login-button" className="google-button" onClick={this.handleLoginClicked.bind(this)}>
                  <GoogleIcon /> Continue with Google
                </button>
              )}
              {this.isOktaConfigured() && (
                <button debug-id="login-button" className="login-button" onClick={this.handleLoginClicked.bind(this)}>
                  <User /> Continue with Okta
                </button>
              )}
              {capabilities.config.githubAuthEnabled && (
                <button
                  debug-id="github-button"
                  className="github-button"
                  onClick={this.handleGithubClicked.bind(this)}>
                  <GithubIcon /> Continue with GitHub
                </button>
              )}
              {capabilities.sso && (
                <form className="sso" onSubmit={this.handleSSOClicked.bind(this)}>
                  <div className={`sso-prompt ${this.state.showSSO ? "" : "hidden"}`}>
                    <div className="sso-title">Team Slug</div>
                    <Input
                      debug-id="sso-slug"
                      name="ssoSlug"
                      value={this.state.ssoSlug}
                      onChange={this.onChange.bind(this)}
                      placeholder="my-team-slug"
                      ref={this.ssoSlugButton}
                    />
                  </div>
                  <button debug-id="sso-button" className={`sso-button ${this.state.ssoSlug ? "active" : ""}`}>
                    <Lock /> Continue with {this.state.orgName} SSO
                  </button>
                </form>
              )}
              {capabilities.anonymous && (
                <button className="anon-button" onClick={this.handleSetupClicked.bind(this)}>
                  <ArrowRight /> Anonymous mode
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
