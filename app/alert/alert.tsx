import React from "react";
import { Subscription } from "rxjs";
import Banner from "../components/banner/banner";
import alertService, { Alert } from "./alert_service";

interface State {
  alert?: Alert;
  isVisible?: boolean;
}

export default class AlertComponent extends React.Component<{}, State> {
  state: State = {};

  private hideTimeout: any = null;
  private subscription: Subscription = alertService.alerts.subscribe(this.onAlert.bind(this));
  private displayDuration = 0;

  private onAlert(alert: Alert) {
    if (this.hideTimeout) {
      clearTimeout(this.hideTimeout);
    }
    this.setState({ isVisible: true, alert });
    this.displayDuration = displayDurationMs(alert);
    this.hideTimeout = setTimeout(() => this.setState({ isVisible: false }), this.displayDuration);
  }

  // If the user hovers over the banner then they are probably trying to copy & paste
  // the error message. Make sure we don't dismiss while they are doing that.

  private onMouseEnter() {
    clearTimeout(this.hideTimeout);
  }
  private onMouseLeave() {
    this.hideTimeout = setTimeout(() => this.setState({ isVisible: false }), this.displayDuration);
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  render() {
    return (
      <Banner
        type={this.state.alert?.type || "info"}
        className={`alert-banner ${this.state.isVisible ? "visible" : "hidden"}`}
        onMouseEnter={this.onMouseEnter.bind(this)}
        onMouseLeave={this.onMouseLeave.bind(this)}>
        {this.state.alert?.message}
      </Banner>
    );
  }
}

function displayDurationMs(alert: Alert) {
  if (alert.type === "error" || alert.type === "warning") {
    return 8_000;
  }
  return 4_000;
}
