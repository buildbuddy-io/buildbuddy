import React from 'react';
import MenuComponent from '../menu/menu';
import InvocationComponent from '../invocation/invocation';
import HomeComponent from '../home/home';
import router, { Capabilities } from '../router/router';

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
    router.register(this.handlePathChange.bind(this), Capabilities.coreCapabilities);
  }

  handlePathChange() {
    this.setState({
      hash: window.location.hash,
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search)
    });
  }

  handleToggleDenseClicked() {
    let newDenseMode = !this.state.denseMode;
    this.setState({ ...this.state, denseMode: newDenseMode })
    window.localStorage.setItem(denseModeKey, newDenseMode ? denseModeValue : "")
  }

  render() {
    let invocationId = router.getInvocationId(this.state.path);
    return (
      <div className={this.state.denseMode ? "dense" : ""}>
        <MenuComponent denseModeEnabled={this.state.denseMode} handleDenseModeToggled={this.handleToggleDenseClicked.bind(this)} />
        {invocationId && <InvocationComponent invocationId={invocationId} hash={this.state.hash} search={this.state.search} denseMode={this.state.denseMode} />}
        {!invocationId && <HomeComponent />}
      </div>
    );
  }
}
