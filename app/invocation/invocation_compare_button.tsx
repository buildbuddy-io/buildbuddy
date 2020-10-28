import React from "react";
import { fromEvent, Subscription } from "rxjs";
import { OutlinedButton } from "../components/button/button";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup from "../components/popup/popup";
import router from "../router/router";
import capabilities from "../capabilities/capabilities";

export interface InvocationCompareButtonComponentProps {
  invocationId: string;
}

interface State {
  invocationIdToCompare: string;
  isDropdownOpen: boolean;
}

const INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY = "invocation_id_to_compare";

export default class InvocationCompareButtonComponent extends React.Component<
  InvocationCompareButtonComponentProps,
  State
> {
  state: State = {
    isDropdownOpen: false,
    invocationIdToCompare: localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY] || "",
  };

  private subscription = new Subscription();

  componentDidMount() {
    this.subscription.add(fromEvent(window, "storage").subscribe(this.onStorage.bind(this)));
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  private onStorage() {
    this.setState({ invocationIdToCompare: localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY] || "" });
  }

  private onClick() {
    this.setState({ isDropdownOpen: true });
  }

  private onClickSelectForComparison() {
    localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY] = this.props.invocationId;
    this.setState({ isDropdownOpen: false, invocationIdToCompare: this.props.invocationId });
  }

  private onClickCompareWithSelected() {
    const invocationIdToCompare = this.state.invocationIdToCompare;
    delete localStorage[INVOCATION_ID_TO_COMPARE_LOCALSTORAGE_KEY];
    this.setState({ isDropdownOpen: false, invocationIdToCompare: "" });
    router.navigateTo(`/compare/${invocationIdToCompare}...${this.props.invocationId}`);
  }

  private onRequestCloseDropdown() {
    this.setState({ isDropdownOpen: false });
  }

  render() {
    if (!capabilities.compareInvocations) {
      return <></>;
    }

    return (
      <div className="invocation-compare-button-container">
        <OutlinedButton onClick={this.onClick.bind(this)}>
          <ComparisonBufferIllustration isBuffered={Boolean(this.state.invocationIdToCompare)} />
          <div>Compare</div>
        </OutlinedButton>
        <Popup isOpen={this.state.isDropdownOpen} onRequestClose={this.onRequestCloseDropdown.bind(this)}>
          <Menu>
            <MenuItem onClick={this.onClickSelectForComparison.bind(this)}>Select for comparison</MenuItem>
            <MenuItem
              disabled={
                !this.state.invocationIdToCompare || this.state.invocationIdToCompare === this.props.invocationId
              }
              onClick={this.onClickCompareWithSelected.bind(this)}>
              Compare with selected
            </MenuItem>
          </Menu>
        </Popup>
      </div>
    );
  }
}

function ComparisonBufferIllustration({ isBuffered }: { isBuffered: boolean }) {
  return (
    <div className={`comparison-buffer-illustration ${isBuffered ? "buffered" : ""}`}>
      <div className="comparison-buffer-icon comparison-buffer-icon-a" />
      <div className="comparison-buffer-icon comparison-buffer-icon-b" />
    </div>
  );
}
