import React from "react";
import alert_service from "../../../app/alert/alert_service";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import errorService from "../../../app/errors/error_service";
import router from "../../../app/router/router";
import rpc_service from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";

interface UsageBasedBillingProps {
  user: User;
  search: URLSearchParams;
}

interface UsageBasedBillingState {
  busy: boolean;
}

export default class UsageBasedBillingComponent extends React.Component<
  UsageBasedBillingProps,
  UsageBasedBillingState
> {
  state: UsageBasedBillingState = { busy: false };

  componentDidMount() {
    // The payment method setup page redirects back here with its session ID.
    const setupSessionId = this.props.search.get("setup_session_id");
    if (!this.enabled() || !setupSessionId) {
      return;
    }
    router.setQueryParam("setup_session_id", undefined);
    this.setState({ busy: true });
    rpc_service.service
      .completeUsageBasedBillingSetup({ setupSessionId })
      .then(() => authService.refreshUser())
      .then(() => alert_service.success("Usage based billing enabled"))
      .catch(errorService.handleError)
      .finally(() => this.setState({ busy: false }));
  }

  private enabled() {
    return (
      capabilities.config.usageBasedBillingEnabled && this.props.user.canCall("createUsageBasedBillingSetupSession")
    );
  }

  private onClickUpgrade = () => {
    this.setState({ busy: true });
    rpc_service.service
      .createUsageBasedBillingSetupSession({})
      .then((response) => {
        window.location.href = response.setupUrl;
      })
      .catch(errorService.handleError)
      .finally(() => this.setState({ busy: false }));
  };

  render() {
    if (!this.enabled() || this.props.user.selectedGroup.status !== grp.Group.GroupStatus.FREE_TIER_GROUP_STATUS) {
      return null;
    }
    return (
      <>
        <div className="settings-option-title">Billing</div>
        <div className="settings-option-description">Attach a billing method to remove free tier usage limits.</div>
        <FilledButton
          className="settings-button"
          onClick={this.onClickUpgrade}
          disabled={this.state.busy}
          debug-id="upgrade-to-usage-based-billing-button">
          {this.state.busy ? "Upgrading..." : "Upgrade to Usage Based Billing"}
        </FilledButton>
      </>
    );
  }
}
