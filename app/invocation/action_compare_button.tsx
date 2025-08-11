import React from "react";
import { Subscription } from "rxjs";
import { OutlinedButton } from "../components/button/button";
import Menu, { MenuItem } from "../components/menu/menu";
import Popup from "../components/popup/popup";
import router, { Path } from "../router/router";
import actionComparisonService, { ActionComparisonData } from "./action_comparison_service";

export interface ActionCompareButtonComponentProps {
  invocationId: string;
  actionDigest: string;
}

interface State {
  comparisonActionData?: ActionComparisonData;
  isDropdownOpen: boolean;
}

export default class ActionCompareButtonComponent extends React.Component<ActionCompareButtonComponentProps, State> {
  state: State = {
    isDropdownOpen: false,
    comparisonActionData: actionComparisonService.getComparisonData(),
  };

  private subscription = new Subscription();

  componentDidMount() {
    this.subscription.add(actionComparisonService.subscribe(this.onComparisonDataUpdate.bind(this)));
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  private onComparisonDataUpdate(data: ActionComparisonData) {
    this.setState({ comparisonActionData: data });
  }

  private onClick = () => {
    this.setState({ isDropdownOpen: true });
  };

  private onClickSelectForComparison = () => {
    actionComparisonService.setComparisonAction(this.props.invocationId, this.props.actionDigest);
    this.setState({ isDropdownOpen: false });
  };

  private onClickCompareWithSelected = () => {
    const comparisonData = this.state.comparisonActionData;
    if (!comparisonData?.invocationId || !comparisonData?.actionDigest) {
      return;
    }

    // Build the compare URL
    const comparePath =
      Path.compareActionsPath +
      `${comparisonData.invocationId}:${encodeURIComponent(
        comparisonData.actionDigest
      )}...${this.props.invocationId}:${encodeURIComponent(this.props.actionDigest)}`;

    router.navigateTo(comparePath);

    // Clear the comparison selection
    actionComparisonService.clearComparisonAction();
    this.setState({ isDropdownOpen: false });
  };

  private onRequestCloseDropdown = () => {
    this.setState({ isDropdownOpen: false });
  };

  render() {
    const canCompare = actionComparisonService.canCompareWith(this.props.invocationId, this.props.actionDigest);

    return (
      <div className="invocation-compare-button-container">
        <OutlinedButton onClick={this.onClick}>
          <ComparisonBufferIllustration isBuffered={Boolean(this.state.comparisonActionData?.actionDigest)} />
          <div>Compare</div>
        </OutlinedButton>
        <Popup isOpen={this.state.isDropdownOpen} onRequestClose={this.onRequestCloseDropdown}>
          <Menu>
            <MenuItem onClick={this.onClickSelectForComparison}>Select for comparison</MenuItem>
            <MenuItem disabled={!canCompare} onClick={this.onClickCompareWithSelected}>
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
