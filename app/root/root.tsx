import React from 'react';
import MenuComponent from '../menu/menu';
import InvocationComponent from '../invocation/invocation';
import HomeComponent from '../home/home';

const denseModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";

interface State {
  hash: string;
  path: string;
  search: URLSearchParams;
  denseMode: boolean;
}

export default class RootComponent extends React.Component {
  state: State = {
    hash: window.location.hash,
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    denseMode: window.localStorage.getItem(denseModeKey) == denseModeValue || false
  };

  componentWillMount() {
    let pathChangeHandler = this.handlePathChange.bind(this);
    history.pushState = (f => function pushState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.pushState);

    history.replaceState = (f => function replaceState() {
      var ret = f.apply(this, arguments);
      pathChangeHandler();
      return ret;
    })(history.replaceState);

    window.addEventListener('popstate', () => {
      pathChangeHandler();
    });
  }

  handlePathChange() {
    this.setState({
      hash: window.location.hash,
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search)
    });
  }

  getCurrentInvocationId() {
    let invocationPath = "/invocation/"
    if (!this.state.path.startsWith(invocationPath)) {
      return null;
    }
    return this.state.path.replace(invocationPath, "");
  }

  handleToggleDenseClicked() {
    let newDenseMode = !this.state.denseMode;
    this.setState({ ...this.state, denseMode: newDenseMode })
    window.localStorage.setItem(denseModeKey, newDenseMode ? denseModeValue : "")
  }

  render() {
    let invocationId = this.getCurrentInvocationId();
    return (
      <div className={this.state.denseMode ? "dense" : ""}>
        <MenuComponent>
          {this.state.denseMode && <img className="menu-control" src="/image/minimize.svg" onClick={this.handleToggleDenseClicked.bind(this)} />}
          {!this.state.denseMode && <img className="menu-control" src="/image/maximize.svg" onClick={this.handleToggleDenseClicked.bind(this)} />}
        </MenuComponent>
        {invocationId && <InvocationComponent invocationId={invocationId} hash={this.state.hash} search={this.state.search} denseMode={this.state.denseMode} />}
        {!invocationId && <HomeComponent />}
      </div>
    );
  }
}
