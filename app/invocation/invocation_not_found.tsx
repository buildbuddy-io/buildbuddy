import React from "react";
import authService from "../auth/auth_service";

interface Props {
  invocationId: string;
  authorized: boolean;
}

export default class InvocationNotFoundComponent extends React.Component {
  props: Props;

  handleLoginClicked() {
    authService.login();
  }

  render() {
    return (
      <div className={this.props.authorized ? "state-page" : "login-interstitial"}>
        {this.props.authorized && (
          <div className="shelf">
            <div className="container">
              <div className="breadcrumbs">Invocation {this.props.invocationId}</div>
              <div className="titles">
                <div className="title">Invocation not found!</div>
              </div>
              <div className="details">Double check your invocation URL and try again.</div>
            </div>
          </div>
        )}
        {!this.props.authorized && (
          <div className="container">
            <div className="login-box">
              <div className="login-buttons">
                <h2>Sign in to continue</h2>
                <button onClick={this.handleLoginClicked.bind(this)}>Sign up for BuildBuddy</button>
                <button onClick={this.handleLoginClicked.bind(this)}>Log in to BuildBuddy</button>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  }
}
