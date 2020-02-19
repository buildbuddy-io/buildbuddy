import React from 'react';

interface State {
  menuExpanded: boolean;
}

export default class MenuComponent extends React.Component {
  state: State = {
    menuExpanded: false
  };

  handleMenuClicked() {
    this.setState({ menuExpanded: !this.state.menuExpanded });
  }

  render() {
    return (
      <div>
        <div className="menu">
          <div className="container">
            <img onClick={this.handleMenuClicked.bind(this)} className="icon" src="/image/menu.svg" />
            <div className="title">BuildBuddy</div>
          </div>
        </div>
        {this.state.menuExpanded &&
          <div className="side-menu">
            <ul>
              <li><a href="/">Home</a></li>
              <li><a href="https://github.com/tryflame/buildbuddy">Github repo</a></li>
              <li><a href="mailto:help@tryflame.com">Contact us</a></li>
            </ul>
          </div>}
      </div>
    );
  }
}
