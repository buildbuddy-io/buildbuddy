import React from 'react';
import authService, { User } from '../auth/auth_service';
import capabilities from '../capabilities/capabilities';

interface Props {
  denseModeEnabled: boolean;
  handleDenseModeToggled: VoidFunction;
  user: User;
}
interface State {
  menuExpanded: boolean;
}

export default class MenuComponent extends React.Component {
  props: Props;

  state: State = {
    menuExpanded: false
  };

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
        <div className="menu">
          <div className="container">
            <div>
              <a href="/"><div className="title"><img src="/image/logo_white.svg" /></div></a>
            </div>
            {(!capabilities.auth || !this.props.user?.profilePhotoUrl) && <img onClick={this.handleMenuClicked.bind(this)} className="icon" src="/image/menu.svg" />}
            {(capabilities.auth && this.props.user?.profilePhotoUrl) && <img onClick={this.handleMenuClicked.bind(this)} className="profile-photo" src={this.props.user.profilePhotoUrl} />}
            {this.state.menuExpanded &&
              <div className="side-menu">
                <ul>
                  <li><a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy/issues/new">Report an issue</a></li>
                  <li><a target="_blank" href="https://github.com/buildbuddy-io/buildbuddy">Github repo</a></li>
                  <li>
                    <a onClick={this.handleToggleDenseModeClicked.bind(this)}>
                      {this.props.denseModeEnabled ? "Disable" : "Enable"} dense mode
                </a>
                  </li>
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
