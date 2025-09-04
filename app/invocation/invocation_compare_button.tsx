import { MoreVertical } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import capabilities from "../capabilities/capabilities";
import { OutlinedButton } from "../components/button/button";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup from "../components/popup/popup";
import router from "../router/router";
import service, { IdAndModel } from "./invocation_comparison_service";

export interface InvocationCompareButtonComponentProps {
  invocationId: string;
  mini?: boolean;
}

interface State {
  invocationIdToCompare?: string;
  isDropdownOpen: boolean;
}

export default class InvocationCompareButtonComponent extends React.Component<
  InvocationCompareButtonComponentProps,
  State
> {
  state: State = {
    isDropdownOpen: false,
    invocationIdToCompare: service.getComparisonInvocationId(),
  };

  private subscription = new Subscription();

  componentDidMount() {
    this.subscription.add(service.subscribe(this.onInvocationUpdate.bind(this)));
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  private onInvocationUpdate(data: IdAndModel) {
    this.setState({ invocationIdToCompare: data.id });
  }

  private onClick(event: React.MouseEvent<HTMLButtonElement>) {
    this.setState({ isDropdownOpen: true });
    event.preventDefault();
    event.stopPropagation();
  }

  private onClickSelectForComparison(event: React.MouseEvent<HTMLButtonElement>) {
    service.setComparisonInvocation(this.props.invocationId);
    this.setState({ isDropdownOpen: false, invocationIdToCompare: this.props.invocationId });
    event.preventDefault();
    event.stopPropagation();
  }

  private onClickCompareWithSelected(event: React.MouseEvent<HTMLButtonElement>) {
    const invocationIdToCompare = this.state.invocationIdToCompare;
    this.setState({ isDropdownOpen: false, invocationIdToCompare: "" });
    router.navigateTo(`/compare/${invocationIdToCompare}...${this.props.invocationId}`);
    event.preventDefault();
    event.stopPropagation();
  }

  private onRequestCloseDropdown(event: React.MouseEvent<HTMLButtonElement>) {
    this.setState({ isDropdownOpen: false });
    event.preventDefault();
    event.stopPropagation();
  }

  render() {
    if (!capabilities.compareInvocations) {
      return <></>;
    }

    return (
      <div
        className={
          this.props.mini ? "invocation-compare-button-container-mini" : "invocation-compare-button-container"
        }>
        {!this.props.mini && (
          <>
            <OutlinedButton onClick={this.onClick.bind(this)}>
              <ComparisonBufferIllustration isBuffered={Boolean(this.state.invocationIdToCompare)} />
              <div>Compare</div>
            </OutlinedButton>
          </>
        )}
        {this.props.mini && (
          <OutlinedButton className="invocation-menu-button" onClick={this.onClick.bind(this)}>
            <MoreVertical />
          </OutlinedButton>
        )}
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
