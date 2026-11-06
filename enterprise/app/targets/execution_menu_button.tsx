import { MoreVertical } from "lucide-react";
import React from "react";
import { Subscription } from "rxjs";
import Menu, { MenuItem } from "../../../app/components/menu/menu";
import Popup from "../../../app/components/popup/popup";
import actionComparisonService, { ActionComparisonData } from "../../../app/invocation/action_comparison_service";
import router, { Path } from "../../../app/router/router";

interface Props {
  invocationId: string;
  actionDigest: string;
}

interface State {
  comparisonActionData?: ActionComparisonData;
  isDropdownOpen: boolean;
}

/** Returns the path of the page showing the given execution. */
export function getExecutionPath(invocationId: string, actionDigest: string): string {
  return `${Path.invocationPath}${invocationId}?actionDigest=${actionDigest}#action`;
}

/**
 * A compact menu button that fits within a row of the sampled executions
 * table.  It offers the same comparison options as ActionCompareButtonComponent
 * plus a link to the execution's page.
 */
export default class ExecutionMenuButtonComponent extends React.Component<Props, State> {
  state: State = {
    isDropdownOpen: false,
    comparisonActionData: actionComparisonService.getComparisonData(),
  };

  private subscription = new Subscription();

  componentDidMount() {
    this.subscription.add(actionComparisonService.subscribe((data) => this.setState({ comparisonActionData: data })));
  }

  componentWillUnmount() {
    this.subscription.unsubscribe();
  }

  private onClick = (event: React.MouseEvent<HTMLElement>) => {
    this.setState({ isDropdownOpen: true });
    event.stopPropagation();
    event.preventDefault();
  };

  private onClickViewExecution = (event: React.MouseEvent<HTMLElement>) => {
    router.navigateTo(getExecutionPath(this.props.invocationId, this.props.actionDigest));
    this.setState({ isDropdownOpen: false });
    event.stopPropagation();
    event.preventDefault();
  };

  private onClickSelectForComparison = (event: React.MouseEvent<HTMLElement>) => {
    actionComparisonService.setComparisonAction(this.props.invocationId, this.props.actionDigest);
    this.setState({ isDropdownOpen: false });
    event.stopPropagation();
    event.preventDefault();
  };

  private onClickCompareWithSelected = (event: React.MouseEvent<HTMLElement>) => {
    const selected = this.state.comparisonActionData;
    if (!selected?.invocationId || !selected?.actionDigest) {
      return;
    }
    router.navigateToCompareActionsPath(
      selected.invocationId,
      selected.actionDigest,
      this.props.invocationId,
      this.props.actionDigest
    );
    actionComparisonService.clearComparisonAction();
    this.setState({ isDropdownOpen: false });
    event.stopPropagation();
    event.preventDefault();
  };

  private onRequestCloseDropdown = (event: React.MouseEvent<HTMLElement>) => {
    this.setState({ isDropdownOpen: false });
    event.stopPropagation();
    event.preventDefault();
  };

  render() {
    const canCompare = actionComparisonService.canCompareWith(this.props.invocationId, this.props.actionDigest);
    return (
      <div className="execution-menu">
        <button className="execution-menu-button" title="More options" onClick={this.onClick}>
          <MoreVertical />
        </button>
        <Popup isOpen={this.state.isDropdownOpen} onRequestClose={this.onRequestCloseDropdown}>
          <Menu>
            <MenuItem onClick={this.onClickViewExecution}>View execution</MenuItem>
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
