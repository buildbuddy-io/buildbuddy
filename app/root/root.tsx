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
  user?: User;
  tab: string;
  path: string;
  search: URLSearchParams;
  preferences: UserPreferences;
}

capabilities.register("BuildBuddy Community Edition", false, [Path.invocationPath]);

export default class RootComponent extends React.Component {
  state: State = {
    tab: router.getTab(),
    path: window.location.pathname,
    search: new URLSearchParams(window.location.search),
    preferences: new UserPreferences(this.handlePreferencesChanged.bind(this)),
  };

  componentWillMount() {
    authService.register();
    router.register(this.handlePathChange.bind(this));
    authService.userStream.subscribe({
      next: (user?: User) => this.setState({ user }),
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
      tab: router.getTab(),
      path: window.location.pathname,
      search: new URLSearchParams(window.location.search),
    });
    capabilities.didNavigateToPath();
  }

  handlePreferencesChanged() {
    this.forceUpdate();
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
                tab={this.state.tab}
                search={this.state.search}
                preferences={this.state.preferences}
                user={undefined}
              />
            )}
            {compareInvocationIds && (
              <CompareInvocationsComponent
                invocationAId={compareInvocationIds.a}
                invocationBId={compareInvocationIds.b}
                search={this.state.search}
                user={undefined}
              />
            )}
            {showSetup && <SetupComponent user={this.state.user} />}
          </div>
          <FooterComponent />
          <AlertComponent />
        </div>
      </div>
    );
  }
}
