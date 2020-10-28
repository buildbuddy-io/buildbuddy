import React from "react";
import authService from "../auth/auth_service";
import { BuildBuddyError } from "../util/errors";

interface Props {
  invocationId: string;
  error: BuildBuddyError | null;
  isAuthenticated: boolean;
}

export default class InvocationNotFoundComponent extends React.Component {
  props: Props;

  handleLoginClicked() {
    authService.login();
  }

  render() {
    if (!this.props.isAuthenticated) {
      return (
        <div className="login-interstitial">
          <div className="container">
            <div className="login-box">
              <div className="login-buttons">
                <h2>Sign in to continue</h2>
                <button onClick={this.handleLoginClicked.bind(this)}>Sign up for BuildBuddy</button>
                <button onClick={this.handleLoginClicked.bind(this)}>Log in to BuildBuddy</button>
              </div>
            </div>
          </div>
        </div>
      );
    }

    if (this.props.isAuthenticated) {
      return (
        <div className="stage-page">
          <div className="shelf">
            <div className="container">
              <div className="breadcrumbs">Invocation {this.props.invocationId}</div>
              {this.props.error?.code === "NotFound" && (
                <>
                  <div className="titles">
                    <div className="title">Invocation not found!</div>
                  </div>
                  <div className="details">Double check your invocation URL and try again.</div>
                </>
              )}
              {this.props.error?.code === "PermissionDenied" && (
                <>
                  <div className="titles">
                    <div className="title">Permission denied</div>
                  </div>
                  <div className="details">You are not authorized to access this invocation.</div>
                </>
              )}
            </div>
          </div>
        </div>
      );
    }
  }
}
