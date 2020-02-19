import React from 'react';
import MenuComponent from 'buildbuddy/app/menu/menu';
import InvocationComponent from 'buildbuddy/app/invocation/invocation';
import HomeComponent from 'buildbuddy/app/home/home';

interface State {
  hash: string;
  path: string;
  loading: boolean;
  loggedIn: boolean;
}

export default class RootComponent extends React.Component {
  state: State = {
    hash: window.location.hash,
    path: window.location.pathname,
    loading: true,
    loggedIn: false,
  };

  componentWillMount() {
    window.onpopstate = () => this.handlePathChange();
  }

  handlePathChange() {
    this.setState({
      hash: window.location.hash,
      path: window.location.pathname,
    });
  }

  getCurrentInvocationId() {
    let invocationPath = "/invocation/"
    if (this.state.hash.startsWith("#" + invocationPath)) {
      return this.state.hash.replace("#" + invocationPath, "");
    }
    if (!this.state.path.startsWith(invocationPath)) {
      return null;
    }
    return this.state.path.replace(invocationPath, "");
  }

  render() {
    let invocationId = this.getCurrentInvocationId();
    return (
      <div>
        <MenuComponent />
        {invocationId && <InvocationComponent invocationId={invocationId} hash={this.state.hash} />}
        {!invocationId && <HomeComponent />}
      </div>
    );
  }
}
