import React from 'react';
import authService, { User } from '../auth/auth_service';
import capabilities from '../capabilities/capabilities';

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
    menuExpanded: false
  };

  handleShadeClicked() {
    this.setState({ menuExpanded: false });
  }

  handleMenuClicked() {
    this.setState({ menuExpanded: !this.state.menuExpanded });
  }

  handleToggleDenseModeClicked() {
    this.props.handleDenseModeToggled();
  }

  handleLoginClicked() {
    authService.login();
  }

  handleLogoutClicked() {
    authService.logout();
  }

  render() {
    return (
      <div>
        {this.state.menuExpanded && <div className="side-menu-shade" onClick={this.handleShadeClicked.bind(this)}></div>}
        <div className="menu">
          <div className="container">
            <div>
              <a href="/"><div className="title"><img src="/image/logo_white.svg" /></div></a>
            </div>
            {(this.props.showHamburger && (!capabilities.auth || !this.props.user)) && <img onClick={this.handleMenuClicked.bind(this)} className="icon" src="/image/menu.svg" />}
            {(this.props.showHamburger && capabilities.auth && this.props.user) && <img onClick={this.handleMenuClicked.bind(this)} className={`profile-photo ${this.props.user?.displayUser?.profileImageUrl ? "" : "default-photo"}`} src={this.props.user?.displayUser?.profileImageUrl || "/image/user-regular.svg"} />}
            {this.state.menuExpanded &&
              <div className="side-menu">
                <ul>
                  {this.props.children && <li>{this.props.children}</li>}
                  {this.props.user && !this.props.user?.selectedGroup.ownedDomain && !this.props.user?.isInDefaultGroup() && <li><a target="_blank" href="https://buildbuddy.typeform.com/to/PFjD5A">Create organization</a></li>}
                  <li><a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy/issues/new">Report an issue</a></li>
                  <li><a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy">Github repo</a></li>
                  <li>
                    <a onClick={this.handleToggleDenseModeClicked.bind(this)}>
                      {this.props.denseModeEnabled ? "Disable" : "Enable"} dense mode
                </a>
                  </li>
                  <li><a href="/docs/setup">Setup instructions</a></li>
                  {!capabilities.enterprise && <li><a target="_blank" href="https://buildbuddy.typeform.com/to/wIXFIA">Upgrade to Enterprise</a></li>}
                  <li><a href="mailto:hello@buildbuddy.io">Contact us</a></li>
                  {(capabilities.auth && !this.props.user) && <li onClick={this.handleLoginClicked.bind(this)}>Login</li>}
                  {(capabilities.auth && this.props.user) && <li onClick={this.handleLogoutClicked.bind(this)}>Logout</li>}
                </ul>
              </div>}
          </div>
        </div>
      </div>
    );
  }
}
