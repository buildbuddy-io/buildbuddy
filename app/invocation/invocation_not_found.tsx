import React from "react";
import authService, { User } from "../auth/auth_service";
import { BuildBuddyError } from "../util/errors";
import capabilities from "../capabilities/capabilities";
import rpcService from "../service/rpc_service";
import FilledButton from "../components/button/button";
import { invocation } from "../../proto/invocation_ts_proto";
import errorService from "../errors/error_service";

interface Props {
  invocationId: string;
  error: BuildBuddyError | null;
  user?: User;
}

interface State {
  impersonationGroupID?: string;
}

export default class InvocationNotFoundComponent extends React.Component<Props, State> {
  state: State = {};

  canImpersonate() {
    return this.props.user?.canCall("getInvocationOwner");
  }

  isPermissionDenied() {
    return this.props.error?.code === "PermissionDenied";
  }

  componentDidMount() {
    if (!this.isPermissionDenied() || !this.canImpersonate()) {
      return;
    }

    rpcService.service
      .getInvocationOwner(
        new invocation.GetInvocationOwnerRequest({
          invocationId: this.props.invocationId,
        })
      )
      .then((response) => {
        if (!capabilities.config.subdomainsEnabled || capabilities.config.customerSubdomain) {
          this.setState({ impersonationGroupID: response.groupId });
          return;
        }

        if (new URL(response.groupUrl).hostname != window.location.hostname) {
          window.location.href =
            response.groupUrl + window.location.pathname + window.location.search + window.location.hash;
        } else {
          this.setState({ impersonationGroupID: response.groupId });
        }
      })
      .catch((e) => errorService.handleError(BuildBuddyError.parse(e)));
  }

  handleImpersonateClicked() {
    authService.enterImpersonationMode(this.state.impersonationGroupID!);
  }

  render() {
    const invocationExists = this.props.error?.code !== "NotFound";
    const canLogin = capabilities.auth && !this.props.user;

    if (invocationExists && canLogin) {
      window.location.href = "/?" + new URLSearchParams({ redirect_url: window.location.href });
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
            {this.isPermissionDenied() && (
              <>
                <div className="titles">
                  <div className="title">Permission denied</div>
                </div>
                <div className="details">You are not authorized to access this invocation.</div>
                {this.state.impersonationGroupID && (
                  <div>
                    <FilledButton onClick={this.handleImpersonateClicked.bind(this)} className="impersonate-button">
                      Impersonate owner
                    </FilledButton>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    );
  }
}
