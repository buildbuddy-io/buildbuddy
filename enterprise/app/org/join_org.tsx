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
  error?: BuildBuddyError;
  org?: grp.GetGroupResponse;
}

export default class JoinOrgComponent extends React.Component<JoinOrgComponentProps, State> {
  state: State = {
    status: "INITIAL_LOAD",
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

  private async onYesClicked() {
    this.setState({ status: "JOINING_GROUP" });
    try {
      console.debug("Joining group", this.state.org.id);
      await rpcService.service.joinGroup(Object.assign(new grp.JoinGroupRequest(), { id: this.state.org.id }));
    } catch (e) {
      const error = BuildBuddyError.parse(e);
      if (error.code === "AlreadyExists") {
        this.setState({ status: "ALREADY_EXISTS", error });
        return;
      } else {
        throw e;
      }
    }

    if (!this.isInOrgDomain()) {
      this.setState({ status: "REQUEST_SUBMITTED" });
      return;
    }

    await authService.setSelectedGroupId(this.state.org.id);
    router.navigateHome();
  }

  private async onViewBuildsClicked() {
    await authService.setSelectedGroupId(this.state.org.id);
    router.navigateHome();
  }

  private isInOrgDomain() {
    return this.state.org.ownedDomain === getUserEmailDomain(this.props.user);
  }

  render() {
    const { status, org } = this.state;

    switch (status) {
      case "INITIAL_LOAD":
        return (
          <div className="organization-join-page">
            <div className="loading" />
          </div>
        );
      case "NOT_FOUND":
        return <div className="organization-join-page">The requested organization was not found.</div>;
      case "ALREADY_EXISTS":
        return (
          <div className="organization-join-page">
            <img className="illustration" src="/image/join-org-illustration.png"></img>
            <div className="submit-result already-joined">
              <div>{this.state.error.description}</div>
              {/* TODO: Return a better status code to differentiate already requested vs. already in */}
              {this.state.error.description.includes("already in") && (
                <div>
                  <FilledButton className="button" onClick={this.onViewBuildsClicked.bind(this)}>
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
                Your request to join <span className="org-name">{this.state.org.name}</span> has been submitted.
                <br />A member of this organization can approve your request.
              </div>
            </div>
          </div>
        );
      default:
        return (
          <div className="organization-join-page">
            <img className="illustration" src="/image/join-org-illustration.png"></img>
            <div className="title">
              Join <span className="org-name">{org.name}</span> on BuildBuddy{this.isInOrgDomain() ? "?" : ""}
            </div>
            <div className="yes-no-buttons">
              <FilledButton
                disabled={status !== "READY"}
                className="yes-no-button yes-button"
                onClick={this.onYesClicked.bind(this)}>
                {this.isInOrgDomain() ? "OK" : "Request access"}
              </FilledButton>
              <OutlinedButton
                disabled={status !== "READY"}
                className="yes-no-button no-button"
                onClick={this.onNoClicked.bind(this)}>
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
