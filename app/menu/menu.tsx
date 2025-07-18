import { Menu } from "lucide-react";
import React from "react";
import authService, { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import UserPreferences from "../preferences/preferences";
import router from "../router/router";

interface Props {
  children?: any;
  preferences: UserPreferences;
  user?: User;
  showHamburger: boolean;
  light?: boolean;
}
interface State {
  menuExpanded: boolean;
}

export default class MenuComponent extends React.Component<Props, State> {
  state: State = {
    menuExpanded: false,
  };

  dismissMenu() {
    this.setState({ menuExpanded: false });
  }

  handleShadeClicked() {
    this.dismissMenu();
  }

  handleMenuClicked() {
    this.setState({ menuExpanded: !this.state.menuExpanded });
  }

  handleToggleDenseModeClicked() {
    this.props.preferences.toggleDenseMode();
    this.dismissMenu();
  }

  handleSetupClicked() {
    router.navigateToSetup();
    this.dismissMenu();
  }

  handleLoginClicked() {
    authService.login();
    this.dismissMenu();
  }

  handleLogoutClicked() {
    authService.logout();
    this.dismissMenu();
  }

  render() {
    return (
      <div>
        {this.state.menuExpanded && (
          <div className="side-menu-shade" onClick={this.handleShadeClicked.bind(this)}></div>
        )}
        <div className={`menu ${this.props.light ? "light" : ""}`}>
          <div className="container">
            <div>
              <a href="/">
                <div className="title">
                  <img src={this.props.light ? "/image/logo_dark.svg" : "/image/logo_white.svg"} />
                </div>
              </a>
            </div>
            {this.props.showHamburger && (!capabilities.auth || !this.props.user) && (
              <Menu onClick={this.handleMenuClicked.bind(this)} className="icon white" />
            )}
            {this.props.showHamburger && capabilities.auth && this.props.user && (
              <img
                onClick={this.handleMenuClicked.bind(this)}
                className={`profile-photo ${this.props.user?.displayUser?.profileImageUrl ? "" : "default-photo"}`}
                src={this.props.user?.displayUser?.profileImageUrl || "/image/user-regular.svg"}
              />
            )}
            {this.state.menuExpanded && (
              <div className="side-menu">
                <ul>
                  {this.props.children && <li onClick={this.dismissMenu.bind(this)}>{this.props.children}</li>}
                  <li onClick={this.dismissMenu.bind(this)}>
                    <a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy/issues/new">
                      Report an issue
                    </a>
                  </li>
                  <li onClick={this.dismissMenu.bind(this)}>
                    <a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy">
                      Github repo
                    </a>
                  </li>
                  <li onClick={this.handleToggleDenseModeClicked.bind(this)}>
                    {this.props.preferences.denseModeEnabled ? "Disable" : "Enable"} dense mode
                  </li>
                  <li onClick={this.handleSetupClicked.bind(this)}>Quickstart</li>
                  {!capabilities.enterprise && (
                    <li onClick={this.dismissMenu.bind(this)}>
                      <a target="_blank" href="https://buildbuddy.typeform.com/to/wIXFIA">
                        Upgrade to Enterprise
                      </a>
                    </li>
                  )}
                  <li onClick={this.dismissMenu.bind(this)}>
                    <a href="mailto:hello@buildbuddy.io">Contact us</a>
                  </li>
                  {capabilities.config.communityLinksEnabled && (
                    <li onClick={this.dismissMenu.bind(this)}>
                      <a target="_blank" href="https://community.buildbuddy.io">
                        BuildBuddy Slack
                      </a>
                    </li>
                  )}
                  {capabilities.auth && !this.props.user && <li onClick={this.handleLoginClicked.bind(this)}>Login</li>}
                  {capabilities.auth && this.props.user && (
                    <li onClick={this.handleLogoutClicked.bind(this)}>Logout</li>
                  )}
                </ul>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }
}
