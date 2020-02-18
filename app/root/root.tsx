import React from 'react';
import MenuComponent from 'buildbuddy/app/menu/menu';
import InvocationComponent from 'buildbuddy/app/invocation/invocation';

interface State {
  hash: string;
  loading: boolean;
  loggedIn: boolean;
}

export default class RootComponent extends React.Component {
  state: State = {
    hash: window.location.hash,
    loading: true,
    loggedIn: false,
  };

  componentWillMount() {
    window.onhashchange = () => this.handleHashChange();
  }

  handleHashChange() {
    this.setState({
      hash: window.location.hash,
    });
  }

  getCurrentInvocationId() {
    return this.state.hash.replace("#/invocation/", "");
  }

  render() {
    return (
      <div>
        <MenuComponent />
        <InvocationComponent invocationId={this.getCurrentInvocationId()} />
      </div>
    );
  }
}
