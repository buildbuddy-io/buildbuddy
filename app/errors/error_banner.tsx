import React from "react";
import { Subscription } from "rxjs";
import { BuildBuddyError } from "../util/errors";
import errorService from "./error_service";

interface State {
  error: BuildBuddyError | null;
  isVisible: boolean;
}

const DISPLAY_DURATION_MS = 4000;

export default class ErrorBannerComponent extends React.Component<{}, State> {
  state: State = { isVisible: false, error: null };

  private timeout: any = null;
  private subscription: Subscription;

  constructor(props: {}) {
    super(props);
    this.subscription = errorService.errorStream.subscribe(this.onError.bind(this));
  }

  private onError(error: BuildBuddyError) {
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
    this.setState({ isVisible: true, error });
    setTimeout(() => this.setState({ isVisible: false }), DISPLAY_DURATION_MS);
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  render() {
    return (
      <div className={`error-banner ${this.state.isVisible ? "visible" : "hidden"}`}>
        <img src="/image/x-circle-regular.svg" />
        <span>{this.state.error?.description}</span>
      </div>
    );
  }
}
