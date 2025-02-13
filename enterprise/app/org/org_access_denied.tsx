import { User } from "../../../app/auth/user";
import React from "react";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import authService from "../../../app/auth/auth_service";
import router from "../../../app/router/router";
import { user } from "../../../proto/user_ts_proto";

export type Props = {
  user: User;
};

export default class OrgAccessDeniedComponent extends React.Component<Props> {
  handleImpersonateClicked() {
    const params = new URLSearchParams(window.location.search);
    const sourceUrl = params.get("source_url");
    authService.enterImpersonationMode(this.props.user.subdomainGroupID, { redirectUrl: sourceUrl ?? undefined });
  }

  render() {
    const params = new URLSearchParams(window.location.search);
    const deniedByIpRules = params.get("denied_reason") == user.SelectedGroup.Access.DENIED_BY_IP_RULES.toString();

    return (
      <div className="state-page">
        <div className="shelf">
          <div className="container">
            <div className="titles">
              <div className="title">Access denied</div>
            </div>
            {!deniedByIpRules && <div className="details">You are not authorized to access this organization.</div>}
            {deniedByIpRules && <div className="details">Access blocked by Organization IP Rules.</div>}
            {!deniedByIpRules && (
              <div>
                <FilledButton
                  onClick={() => {
                    window.location.href = "/join/";
                  }}
                  className="request-button">
                  Request to join organization
                </FilledButton>
              </div>
            )}
            {!deniedByIpRules && (
              <div>
                <OutlinedButton
                  onClick={() => {
                    window.location.href = "/logout/";
                  }}
                  className="switch-button">
                  Switch accounts
                </OutlinedButton>
              </div>
            )}
            {this.props.user?.subdomainGroupID && (
              <div>
                <FilledButton onClick={this.handleImpersonateClicked.bind(this)} className="impersonate-button">
                  Impersonate owner
                </FilledButton>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }
}
