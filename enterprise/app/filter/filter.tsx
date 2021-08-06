import moment from "moment";
import React from "react";
import { DateRangePicker, OnChangeProps, RangeWithKey } from "react-date-range";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Popup from "../../../app/components/popup/popup";
import Radio from "../../../app/components/radio/radio";
import Checkbox from "../../../app/components/checkbox/checkbox";
import { formatDateRange } from "../../../app/format/format";
import router from "../../../app/router/router";
import { invocation } from "../../../proto/invocation_ts_proto";
import {
  parseStatusParam,
  statusToString,
  toStatusParam,
  START_DATE_PARAM_NAME,
  END_DATE_PARAM_NAME,
  ROLE_PARAM_NAME,
  STATUS_PARAM_NAME,
} from "./filter_util";

export interface FilterProps {
  search: URLSearchParams;
}

interface State {
  isDatePickerOpen?: boolean;
  isFilterMenuOpen?: boolean;
}

const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export default class FilterComponent extends React.Component<FilterProps, State> {
  state: State = {};

  private onOpenDatePicker() {
    this.setState({ isDatePickerOpen: true });
  }
  private onCloseDatePicker() {
    this.setState({ isDatePickerOpen: false });
  }
  private onDateChange(range: OnChangeProps) {
    const selection = (range as { selection: RangeWithKey }).selection;
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [START_DATE_PARAM_NAME]: moment(selection.startDate).format(DATE_PARAM_FORMAT),
      [END_DATE_PARAM_NAME]: moment(selection.endDate).format(DATE_PARAM_FORMAT),
    });
  }

  private onOpenFilterMenu() {
    this.setState({ isFilterMenuOpen: true });
  }
  private onCloseFilterMenu() {
    this.setState({ isFilterMenuOpen: false });
  }
  private onClickClearFilters() {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [ROLE_PARAM_NAME]: "",
      [STATUS_PARAM_NAME]: "",
    });
  }

  private onRoleChange(role: string) {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [ROLE_PARAM_NAME]: role,
    });
  }

  private onStatusToggle(status: invocation.OverallStatus, selected: Set<invocation.OverallStatus>) {
    selected = new Set(selected); // clone
    if (selected.has(status)) {
      selected.delete(status);
    } else {
      selected.add(status);
    }
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [STATUS_PARAM_NAME]: toStatusParam(selected),
    });
  }

  private renderStatusCheckbox(
    label: string,
    status: invocation.OverallStatus,
    selected: Set<invocation.OverallStatus>
  ) {
    const name = statusToString(status);
    return (
      <label onClick={this.onStatusToggle.bind(this, status, selected)}>
        <Checkbox checked={selected.has(status)} />
        <span className={`status-badge ${name}`}>{label}</span>
      </label>
    );
  }

  render() {
    const startDateParam = this.props.search.get(START_DATE_PARAM_NAME);
    const endDateParam = this.props.search.get(END_DATE_PARAM_NAME);

    const startDate = (startDateParam ? moment(startDateParam) : moment().subtract(7, "days")).toDate();
    const endDate = (endDateParam ? moment(endDateParam) : moment()).toDate();

    const roleValue = this.props.search.get(ROLE_PARAM_NAME) || "";
    const statusValue = this.props.search.get(STATUS_PARAM_NAME) || "";

    const isFiltering = Boolean(roleValue || statusValue);
    const selectedStatuses = new Set(parseStatusParam(statusValue));

    return (
      <div className={`global-filter ${isFiltering ? "is-filtering" : ""}`}>
        {isFiltering && (
          <FilledButton className="square" title="Clear filters" onClick={this.onClickClearFilters.bind(this)}>
            <img src="/image/x-white.svg" alt="" />
          </FilledButton>
        )}
        <div className="popup-wrapper">
          <OutlinedButton
            className={`filter-menu-button icon-text-button ${isFiltering ? "" : "square"}`}
            onClick={this.onOpenFilterMenu.bind(this)}>
            <img className="subtle-icon" src="/image/filter.svg" alt="" />
            {selectedStatuses.has(invocation.OverallStatus.SUCCESS) && <span className="status-block success" />}
            {selectedStatuses.has(invocation.OverallStatus.FAILURE) && <span className="status-block failure" />}
            {selectedStatuses.has(invocation.OverallStatus.IN_PROGRESS) && (
              <span className="status-block in-progress" />
            )}
            {selectedStatuses.has(invocation.OverallStatus.DISCONNECTED) && (
              <span className="status-block disconnected" />
            )}
            {roleValue === "CI" && <span className="role-badge CI">CI</span>}
            {roleValue === "CI_RUNNER" && <span className="role-badge CI_RUNNER">Workflow</span>}
          </OutlinedButton>
          <Popup
            isOpen={this.state.isFilterMenuOpen}
            onRequestClose={this.onCloseFilterMenu.bind(this)}
            className="filter-menu-popup">
            <div className="option-groups-row">
              <div className="option-group">
                <div className="option-group-title">Role</div>
                <div className="option-group-options">
                  <label onClick={this.onRoleChange.bind(this, "")}>
                    <Radio value="" checked={roleValue === ""} />
                    <span>Any</span>
                  </label>
                  <label onClick={this.onRoleChange.bind(this, "CI")}>
                    <Radio value="CI" checked={roleValue === "CI"} />
                    <span className="role-badge CI">CI</span>
                  </label>
                  <label onClick={this.onRoleChange.bind(this, "CI_RUNNER")}>
                    <Radio value="CI_RUNNER" checked={roleValue === "CI_RUNNER"} />
                    <span className="role-badge CI_RUNNER">Workflow</span>
                  </label>
                </div>
              </div>
              <div className="option-group">
                <div className="option-group-title">Status</div>
                <div className="option-group-options">
                  {this.renderStatusCheckbox("Succeeded", invocation.OverallStatus.SUCCESS, selectedStatuses)}
                  {this.renderStatusCheckbox("Failed", invocation.OverallStatus.FAILURE, selectedStatuses)}
                  {this.renderStatusCheckbox("In progress", invocation.OverallStatus.IN_PROGRESS, selectedStatuses)}
                  {this.renderStatusCheckbox("Disconnected", invocation.OverallStatus.DISCONNECTED, selectedStatuses)}
                </div>
              </div>
            </div>
          </Popup>
        </div>
        <div className="popup-wrapper">
          <OutlinedButton className="date-picker-button icon-text-button" onClick={this.onOpenDatePicker.bind(this)}>
            <img className="subtle-icon" src="/image/calendar.svg" alt="" />
            <span>{formatDateRange(startDate, endDate)}</span>
          </OutlinedButton>
          <Popup
            isOpen={this.state.isDatePickerOpen}
            onRequestClose={this.onCloseDatePicker.bind(this)}
            className="date-picker-popup">
            <DateRangePicker
              ranges={[{ startDate, endDate, key: "selection" }]}
              onChange={this.onDateChange.bind(this)}
              // Disable textbox inputs, like "days from today", or "days until today".
              inputRanges={[]}
            />
          </Popup>
        </div>
      </div>
    );
  }
}
