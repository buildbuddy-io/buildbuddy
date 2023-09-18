import { User } from "../../../app/auth/user";
import React from "react";
import FilledButton from "../../../app/components/button/button";
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
    if (sourceUrl) {
      router.navigateTo(sourceUrl);
    }
    authService.enterImpersonationMode(this.props.user.subdomainGroupID);
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
            {!deniedByIpRules && <div className="details">You are not authorized to access this site.</div>}
            {deniedByIpRules && <div className="details">Access blocked by Organization IP Rules.</div>}
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
