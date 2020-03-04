import React from 'react';

interface Props {
  denseModeEnabled: boolean;
  handleDenseModeToggled: VoidFunction;
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

  render() {
    return (
      <div>
        <div className="menu">
          <div className="container">
            <div>
              <a href="/"><div className="title"><img src="/image/logo_white.svg" /></div></a>
            </div>
            <img onClick={this.handleMenuClicked.bind(this)} className="icon" src="/image/menu.svg" />
            {this.state.menuExpanded &&
              <div className="side-menu">
                <ul>
                  <li><a target="_blank" href="https://github.com/tryflame/buildbuddy/issues/new">Report an issue</a></li>
                  <li><a target="_blank" href="https://github.com/tryflame/buildbuddy">Github repo</a></li>
                  <li>
                    <a onClick={this.handleToggleDenseModeClicked.bind(this)}>
                      {this.props.denseModeEnabled ? "Disable" : "Enable"} dense mode
                </a>
                  </li>
                  <li><a href="mailto:help@tryflame.com">Contact us</a></li>
                </ul>
              </div>}
          </div>
        </div>
      </div>
    );
  }
}
