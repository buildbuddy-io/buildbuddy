import React from 'react';
import HomeComponent from 'buildbuddy/app/home/home';

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

  render() {
    var component = <HomeComponent />;

    return (
      <div>
        BuildBuddy
        {component}
      </div>
    );
  }
}
