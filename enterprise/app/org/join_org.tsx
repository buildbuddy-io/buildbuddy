import React from "react";
import router from "../../../app/router/router";
import { grp } from "../../../proto/group_ts_proto";
import authService, { User } from "../../../app/auth/auth_service";
import rpcService from "../../../app/service/rpc_service";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import { BuildBuddyError } from "../../../app/util/errors";

export interface JoinOrgComponentProps {
  user: User;
}

interface State {
  status: "INITIAL_LOAD" | "NOT_FOUND" | "READY" | "ALREADY_EXISTS" | "JOINING_GROUP" | "REQUEST_SUBMITTED";

  error: string;
  org?: grp.GetGroupResponse;
}

export default class JoinOrgComponent extends React.Component<JoinOrgComponentProps, State> {
  state: State = {
    status: "INITIAL_LOAD",
    error: "",
  };

  componentDidMount() {
    this.fetchOrg();
  }

  private async fetchOrg() {
    // URL is expected to look like `/join/$orgName`
    const urlIdentifier = window.location.pathname.split("/").pop();
    try {
      const org = await rpcService.service.getGroup(new grp.GetGroupRequest({ urlIdentifier }));
      this.setState({ status: "READY", org });
    } catch (e) {
      const error = BuildBuddyError.parse(e);
      if (error.code === "NotFound") {
        this.setState({ status: "NOT_FOUND" });
      } else {
        throw e;
      }
    }
  }

  private onNoClicked() {
    router.navigateHome();
  }

  private async onYesClicked(org: grp.GetGroupResponse) {
    this.setState({ status: "JOINING_GROUP" });
    try {
      console.debug("Joining group", org.id);
      await rpcService.service.joinGroup(Object.assign(new grp.JoinGroupRequest(), { id: org.id }));
    } catch (e) {
      const error = BuildBuddyError.parse(e);
      if (error.code === "AlreadyExists") {
        this.setState({ status: "ALREADY_EXISTS", error: error.description });
        return;
      } else {
        throw e;
      }
    }

    if (!this.isInOrgDomain(org)) {
      this.setState({ status: "REQUEST_SUBMITTED" });
      return;
    }

    await authService.setSelectedGroupId(org.id);
    router.navigateHome();
  }

  private async onViewBuildsClicked(org: grp.GetGroupResponse) {
    await authService.setSelectedGroupId(org.id);
    router.navigateHome();
  }

  private isInOrgDomain(org: grp.GetGroupResponse) {
    return org.ownedDomain === getUserEmailDomain(this.props.user);
  }

  render() {
    const status = this.state.status;
    const org = this.state.org;

    if (!org) {
      if (status === "INITIAL_LOAD") {
        return (
          <div className="organization-join-page">
            <div className="loading" />
          </div>
        );
      }

      console.assert(status === "NOT_FOUND");
      return <div className="organization-join-page">The requested organization was not found.</div>;
    }

    switch (status) {
      case "ALREADY_EXISTS":
        return (
          <div className="organization-join-page">
            <img className="illustration" src="/image/join-org-illustration.png"></img>
            <div className="submit-result already-joined">
              <div>{this.state.error}</div>
              {/* TODO: Return a better status code to differentiate already requested vs. already in */}
              {this.state.error.includes("already in") && (
                <div>
                  <FilledButton className="button" onClick={() => this.onViewBuildsClicked(org)}>
                    View builds
                  </FilledButton>
                </div>
              )}
            </div>
          </div>
        );
      case "REQUEST_SUBMITTED":
        return (
          <div className="organization-join-page">
            <img className="illustration" src="/image/join-org-illustration.png"></img>
            <div className="submit-result request-submitted">
              <div>
                Your request to join <span className="org-name">{org.name}</span> has been submitted.
                <br />A member of this organization can approve your request.
              </div>
            </div>
          </div>
        );
      default:
        console.assert(status === "READY" || status === "JOINING_GROUP");
        return (
          <div className="organization-join-page">
            <img className="illustration" src="/image/join-org-illustration.png"></img>
            <div className="title">
              Join <span className="org-name">{org.name}</span> on BuildBuddy{this.isInOrgDomain(org) ? "?" : ""}
            </div>
            <div className="yes-no-buttons">
              <FilledButton
                disabled={status !== "READY"}
                className="yes-no-button yes-button"
                onClick={() => this.onYesClicked(org)}>
                {this.isInOrgDomain(org) ? "OK" : "Request access"}
              </FilledButton>
              <OutlinedButton
                disabled={status !== "READY"}
                className="yes-no-button no-button"
                onClick={() => this.onNoClicked()}>
                No thanks
              </OutlinedButton>
            </div>
          </div>
        );
    }
  }
}

function getUserEmailDomain(user: User) {
  const email = user.displayUser.email;
  return email.split("@").pop();
}
