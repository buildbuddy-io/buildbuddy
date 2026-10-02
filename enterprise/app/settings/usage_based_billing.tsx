import React from "react";
import alert_service from "../../../app/alert/alert_service";
import authService, { User } from "../../../app/auth/auth_service";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton from "../../../app/components/button/button";
import { OutlinedLinkButton } from "../../../app/components/button/link_button";
import errorService from "../../../app/errors/error_service";
import rpc_service from "../../../app/service/rpc_service";
import { grp } from "../../../proto/group_ts_proto";

interface UsageBasedBillingProps {
  user: User;
  search: URLSearchParams;
}

interface UsageBasedBillingState {
  completing: boolean;
  upgrading: boolean;
}

const enterpriseQuoteURL = "https://www.buildbuddy.io/request-quote";

export default class UsageBasedBillingComponent extends React.Component<
  UsageBasedBillingProps,
  UsageBasedBillingState
> {
  state: UsageBasedBillingState = {
    completing: false,
    upgrading: false,
  };

  componentDidMount() {
    this.completeBillingSetupFromRedirect();
  }

  componentDidUpdate(prevProps: UsageBasedBillingProps) {
    if (prevProps.search.toString() !== this.props.search.toString()) {
      this.completeBillingSetupFromRedirect();
    }
  }

  private usageBasedBillingEnabled() {
    return (
      capabilities.config.usageBasedBillingEnabled && this.props.user.canCall("createUsageBasedBillingSetupSession")
    );
  }

  private alreadyUsageBased() {
    return this.props.user.selectedGroup.status === grp.Group.GroupStatus.USAGE_BASED_GROUP_STATUS;
  }

  private canUpgradeToUsageBased() {
    return this.props.user.selectedGroup.status === grp.Group.GroupStatus.FREE_TIER_GROUP_STATUS;
  }

  private canRequestEnterpriseQuote() {
    return this.canUpgradeToUsageBased() || this.alreadyUsageBased();
  }

  private clearBillingSetupParams() {
    const url = new URL(window.location.href);
    url.searchParams.delete("usage_billing_setup");
    url.searchParams.delete("setup_session_id");
    url.searchParams.delete("session_id");
    window.history.replaceState({}, document.title, url.pathname + url.search + url.hash);
  }

  private completeBillingSetupFromRedirect() {
    if (!this.usageBasedBillingEnabled() || this.state.completing) {
      return;
    }
    const setupStatus = this.props.search.get("usage_billing_setup");
    if (setupStatus === "cancel") {
      this.clearBillingSetupParams();
      return;
    }
    if (setupStatus !== "success") {
      return;
    }
    const sessionID = this.props.search.get("setup_session_id") || this.props.search.get("session_id");
    if (!sessionID) {
      return;
    }
    this.setState({ completing: true });
    rpc_service.service
      .completeUsageBasedBillingSetup({ setupSessionId: sessionID })
      .then(() => authService.refreshUser())
      .then(() => {
        this.clearBillingSetupParams();
        alert_service.success("Usage based billing enabled");
      })
      .catch(errorService.handleError)
      .finally(() => this.setState({ completing: false }));
  }

  private onClickUpgrade = () => {
    this.setState({ upgrading: true });
    rpc_service.service
      .createUsageBasedBillingSetupSession({})
      .then((response) => {
        window.location.href = response.setupUrl;
      })
      .catch(errorService.handleError)
      .finally(() => this.setState({ upgrading: false }));
  };

  render() {
    if (!this.usageBasedBillingEnabled()) {
      return null;
    }
    const showUsageBasedUpgrade = this.canUpgradeToUsageBased();
    const showEnterpriseQuote = this.canRequestEnterpriseQuote();
    if (!showUsageBasedUpgrade && !showEnterpriseQuote) {
      return null;
    }
    const disabled = this.state.upgrading || this.state.completing;
    return (
      <>
        <div className="settings-option-title">Billing</div>
        <div className="settings-option-description">
          {showUsageBasedUpgrade
            ? "Attach a billing method to remove free tier usage limits."
            : "Request an enterprise quote for custom usage-based billing."}
        </div>
        {showUsageBasedUpgrade && (
          <FilledButton
            className="settings-button"
            onClick={this.onClickUpgrade}
            disabled={disabled}
            debug-id="upgrade-to-usage-based-billing-button">
            {this.state.completing
              ? "Completing upgrade..."
              : this.state.upgrading
                ? "Starting upgrade..."
                : "Upgrade to Usage Based Billing"}
          </FilledButton>
        )}
        {showEnterpriseQuote && (
          <div className="settings-billing-enterprise-quote">
            <OutlinedLinkButton className="settings-button" href={enterpriseQuoteURL} target="_blank" rel="noreferrer">
              Request an Enterprise Quote
            </OutlinedLinkButton>
          </div>
        )}
      </>
    );
  }
}
