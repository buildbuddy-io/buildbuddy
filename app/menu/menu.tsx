import React from "react";
import authService, { User } from "../auth/auth_service";
import capabilities from "../capabilities/capabilities";
import router, { Path } from "../router/router";

interface Props {
  children?: any;
  denseModeEnabled: boolean;
  handleDenseModeToggled: VoidFunction;
  user: User;
  showHamburger: boolean;
}
interface State {
  menuExpanded: boolean;
}

export default class MenuComponent extends React.Component {
  props: Props;

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
    this.props.handleDenseModeToggled();
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

  handleCreateOrgClicked() {
    router.navigateToCreateOrg();
  }

  render() {
    return (
      <div>
        {this.state.menuExpanded && (
          <div className="side-menu-shade" onClick={this.handleShadeClicked.bind(this)}></div>
        )}
        <div className="menu">
          <div className="container">
            <div>
              <a href="/">
                <div className="title">
                  <img src="/image/logo_white.svg" />
                </div>
              </a>
            </div>
            {this.props.showHamburger && (!capabilities.auth || !this.props.user) && (
              <img onClick={this.handleMenuClicked.bind(this)} className="icon" src="/image/menu.svg" />
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
                  {this.props.user &&
                    !this.props.user?.selectedGroup.ownedDomain &&
                    !this.props.user?.isInDefaultGroup() && (
                      <li onClick={this.dismissMenu.bind(this)}>
                        <div className="clickable" onClick={this.handleCreateOrgClicked.bind(this)}>
                          Create organization
                        </div>
                      </li>
                    )}
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
                    {this.props.denseModeEnabled ? "Disable" : "Enable"} dense mode
                  </li>
                  <li onClick={this.handleSetupClicked.bind(this)}>Setup instructions</li>
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
                  <li onClick={this.dismissMenu.bind(this)}>
                    <a target="_blank" href="https://slack.buildbuddy.io">
                      BuildBuddy Slack
                    </a>
                  </li>
                  {capabilities.auth &&
                    capabilities.github &&
                    this.props.user &&
                    !this.props.user.selectedGroup.githubLinked && (
                      <li onClick={this.dismissMenu.bind(this)}>
                        <a href="/auth/github/link/">Link GitHub Account</a>
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
