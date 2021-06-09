import React from "react";
import FooterComponent from "../footer/footer";
import MenuComponent from "../menu/menu";
import InvocationComponent from "../invocation/invocation";
import SetupComponent from "../docs/setup";
import capabilities from "../capabilities/capabilities";
import router, { Path } from "../router/router";
import authService from "../auth/auth_service";
import { User } from "../auth/auth_service";
import faviconService from "../favicon/favicon";
import CompareInvocationsComponent from "../compare/compare_invocations";
import AlertComponent from "../alert/alert";

const viewModeKey = "VIEW_MODE";
const denseModeValue = "DENSE";
const comfyModeValue = "COMFY";

declare var window: any;

interface State {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
  denseMode: boolean;
}

export default class RootComponent extends React.Component {
  state: State = {
    user: null,
    hash: window.location.hash,
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    denseMode:
      viewModeKey in window.localStorage
        ? window.localStorage.getItem(viewModeKey) == denseModeValue
        : window.buildbuddyConfig && window.buildbuddyConfig.default_to_dense_mode,
  };

  componentWillMount() {
    capabilities.register("BuildBuddy Community Edition", false, [Path.invocationPath]);
    authService.register();
    router.register(this.handlePathChange.bind(this));
    authService.userStream.subscribe({
      next: (user: User) => this.setState({ ...this.state, user }),
    });
    faviconService.setDefaultFavicon();
  }

  handlePathChange() {
    if (this.state.path != window.location.pathname) {
      faviconService.setDefaultFavicon();
    }
    this.setState({
      hash: window.location.hash,
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search),
    });
    capabilities.didNavigateToPath();
  }

  handleToggleDenseClicked() {
    let newDenseMode = !this.state.denseMode;
    this.setState({ ...this.state, denseMode: newDenseMode });
    window.localStorage.setItem(viewModeKey, newDenseMode ? denseModeValue : comfyModeValue);
  }

  render() {
    let invocationId = router.getInvocationId(this.state.path);
    let compareInvocationIds = router.getInvocationIdsForCompare(this.state.path);
    let showSetup = !invocationId && !compareInvocationIds;
    return (
      <div className={this.state.denseMode ? "dense root" : "root"}>
        <MenuComponent
          user={this.state.user}
          showHamburger={true}
          denseModeEnabled={this.state.denseMode}
          handleDenseModeToggled={this.handleToggleDenseClicked.bind(this)}
        />
        <div className="main">
          <div className="content">
            {invocationId && (
              <InvocationComponent
                invocationId={invocationId}
                key={invocationId}
                hash={this.state.hash}
                search={this.state.search}
                denseMode={this.state.denseMode}
                user={null}
              />
            )}
            {compareInvocationIds && (
              <CompareInvocationsComponent
                invocationAId={compareInvocationIds.a}
                invocationBId={compareInvocationIds.b}
                search={this.state.search}
                user={null}
              />
            )}
            {showSetup && <SetupComponent />}
          </div>
          <FooterComponent />
          <AlertComponent />
        </div>
      </div>
    );
  }
}
