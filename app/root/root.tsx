import React from "react";
import FooterComponent from "../footer/footer";
import MenuComponent from "../menu/menu";
import InvocationComponent from "../invocation/invocation";
import SetupComponent from "../docs/setup";
import capabilities from "../capabilities/capabilities";
import router, { Path } from "../router/router";
import authService from "../auth/auth_service";
import { User } from "../auth/auth_service";
import errorService from "../errors/error_service";
import faviconService from "../favicon/favicon";
import CompareInvocationsComponent from "../compare/compare_invocations";
import AlertComponent from "../alert/alert";
import UserPreferences from "../preferences/preferences";

declare var window: any;

interface State {
  user: User;
  hash: string;
  path: string;
  search: URLSearchParams;
  preferences: UserPreferences;
}

capabilities.register("BuildBuddy Community Edition", false, [Path.invocationPath]);

export default class RootComponent extends React.Component {
  state: State = {
    user: null,
    hash: window.location.hash,
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    preferences: new UserPreferences(this.handlePreferencesChanged.bind(this)),
  };

  componentWillMount() {
    authService.register();
    router.register(this.handlePathChange.bind(this));
    authService.userStream.subscribe({
      next: (user: User) => this.setState({ ...this.state, user }),
    });
    faviconService.setDefaultFavicon();
    window._preferences = this.state.preferences;
  }

  componentDidMount() {
    errorService.register();
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

  handlePreferencesChanged() {
    this.setState({ ...this.state, preferences: this.state.preferences });
  }

  render() {
    let invocationId = router.getInvocationId(this.state.path);
    let compareInvocationIds = router.getInvocationIdsForCompare(this.state.path);
    let showSetup = !invocationId && !compareInvocationIds;
    return (
      <div className={this.state.preferences.denseModeEnabled ? "dense root" : "root"}>
        <MenuComponent user={this.state.user} showHamburger={true} preferences={this.state.preferences} />
        <div className="root-main">
          <div className="content">
            {invocationId && (
              <InvocationComponent
                invocationId={invocationId}
                key={invocationId}
                hash={this.state.hash}
                search={this.state.search}
                preferences={this.state.preferences}
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
