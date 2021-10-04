import React from "react";
import authService, { User } from "../auth/auth_service";
import { BuildBuddyError } from "../util/errors";
import capabilities from "../capabilities/capabilities";

interface Props {
  invocationId: string;
  error: BuildBuddyError | null;
  user?: User;
}

export default class InvocationNotFoundComponent extends React.Component {
  props: Props;

  handleLoginClicked() {
    authService.login();
  }

  render() {
    const invocationExists = this.props.error?.code !== "NotFound";
    const canLogin = capabilities.auth && !this.props.user;

    if (invocationExists && canLogin) {
      window.location.href = "/?" + new URLSearchParams({redirect_url: window.location.href});
    }

    return (
      <div className="state-page">
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
