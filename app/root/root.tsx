import React from 'react';
import MenuComponent from 'buildbuddy/app/menu/menu';
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
    return (
      <div>
        <MenuComponent />
        <HomeComponent />
      </div>
    );
  }
}
