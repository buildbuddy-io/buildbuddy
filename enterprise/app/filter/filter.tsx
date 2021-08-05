import moment from "moment";
import React from "react";
import { DateRangePicker, defaultInputRanges, OnChangeProps, RangeWithKey } from "react-date-range";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Popup from "../../../app/components/popup/popup";
import Radio from "../../../app/components/radio/radio";
import TextInput from "../../../app/components/input/input";
import Checkbox from "../../../app/components/checkbox/checkbox";
import { formatDateRange } from "../../../app/format/format";
import router from "../../../app/router/router";

export interface FilterProps {
  search: URLSearchParams;
}

interface State {
  isDatePickerOpen?: boolean;
  isFilterMenuOpen?: boolean;
}

export const ROLE_PARAM_NAME = "role";
export const STATUS_PARAM_NAME = "status";
export const START_DATE_PARAM_NAME = "start";
export const END_DATE_PARAM_NAME = "end";

const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export default class FilterComponent extends React.Component<FilterProps, State> {
  state: State = {};

  private onClickClearFilters() {
    router.setQuery({});
  }

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

  private onRoleChange(role: string) {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [ROLE_PARAM_NAME]: role,
    });
  }

  render() {
    const startDateParam = this.props.search.get(START_DATE_PARAM_NAME);
    const endDateParam = this.props.search.get(END_DATE_PARAM_NAME);

    const startDate = (startDateParam ? moment(startDateParam) : moment().subtract(7, "days")).toDate();
    const endDate = (endDateParam ? moment(endDateParam) : moment()).toDate();

    const canClearFilters = Boolean(this.props.search.toString());

    const roleValue = this.props.search.get(ROLE_PARAM_NAME) || "";

    return (
      <div className={`global-filter ${canClearFilters ? "is-filtering" : ""}`}>
        {canClearFilters && (
          <FilledButton className="square" title="Clear filters" onClick={this.onClickClearFilters.bind(this)}>
            <img src="/image/x-white.svg" alt="" />
          </FilledButton>
        )}
        <div className="popup-wrapper">
          <OutlinedButton className="filter-menu-button icon-text-button" onClick={this.onOpenFilterMenu.bind(this)}>
            <img className="subtle-icon" src="/image/filter.svg" alt="" />
            {roleValue === "CI" && <span className="role-badge CI">CI</span>}
            {roleValue === "CI_RUNNER" && <span className="role-badge CI_RUNNER">Workflow</span>}
            {!roleValue && <span className="filter-menu-button-label">Filter...</span>}
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
                  <label>
                    <Checkbox checked />
                    <span className="status-badge succeeded">Succeeded</span>
                  </label>
                  <label>
                    <Checkbox checked />
                    <span className="status-badge failed">Failed</span>
                  </label>
                  <label>
                    <Checkbox checked />
                    <span className="status-badge in-progress">In progress</span>
                  </label>
                  <label>
                    <Checkbox checked />
                    <span className="status-badge disconnected">Disconnected</span>
                  </label>
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
