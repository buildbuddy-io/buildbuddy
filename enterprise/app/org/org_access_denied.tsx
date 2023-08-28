import { User } from "../../../app/auth/user";
import React from "react";
import FilledButton from "../../../app/components/button/button";
import authService from "../../../app/auth/auth_service";
import router from "../../../app/router/router";
import rpcService from "../../../app/service/rpc_service";
import {grp} from "../../../proto/group_ts_proto";

export type Props = {
  user: User;
};

export type State = {
  groupId: string
}

export default class OrgAccessDeniedComponent extends React.Component<Props, State> {
  state: State = {
    groupId: ""
  }

  componentDidMount() {
    rpcService.service.getGroup(grp.GetGroupRequest.create())
        .then((r) => this.setState({groupId: r.id}))
        .catch((e) => this.setState({groupId: ""}))
  }

  handleImpersonateClicked() {
    const params = new URLSearchParams(window.location.search);
    const sourceUrl = params.get("source_url");
    if (sourceUrl) {
      router.navigateTo(sourceUrl);
    }
    authService.enterImpersonationMode(this.state.groupId);
  }

  render() {
    return (
      <div className="state-page">
        <div className="shelf">
          <div className="container">
            <div className="titles">
              <div className="title">Access denied</div>
            </div>
            <div className="details">You are not authorized to access this site.</div>
            {this.props.user?.canImpersonate() && this.state.groupId && (
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
