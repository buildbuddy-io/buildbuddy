import moment from "moment";
import React from "react";
import { DateRangePicker, defaultInputRanges, OnChangeProps, RangeWithKey } from "react-date-range";
import { OutlinedButton } from "../../../app/components/button/button";
import Popup from "../../../app/components/popup/popup";
import { formatDateRange } from "../../../app/format/format";
import router from "../../../app/router/router";

export interface FilterProps {
  search: URLSearchParams;
}

interface State {
  isDatePickerOpen?: boolean;
}

export const START_DATE_PARAM_NAME = "start";
export const END_DATE_PARAM_NAME = "end";
const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export default class FilterComponent extends React.Component<FilterProps, State> {
  state: State = {};

  private onClickClearFilters() {
    router.setQuery(new URLSearchParams());
  }

  private onOpenDatePicker() {
    this.setState({ isDatePickerOpen: true });
  }
  private onCloseDatePicker() {
    this.setState({ isDatePickerOpen: false });
  }
  private onDateChange(range: OnChangeProps) {
    const selection = (range as { selection: RangeWithKey }).selection;
    router.setQuery(
      new URLSearchParams({
        ...Object.fromEntries(this.props.search.entries() || []),
        [START_DATE_PARAM_NAME]: moment(selection.startDate).format(DATE_PARAM_FORMAT),
        [END_DATE_PARAM_NAME]: moment(selection.endDate).format(DATE_PARAM_FORMAT),
      })
    );
  }

  render() {
    const startDateParam = this.props.search.get(START_DATE_PARAM_NAME);
    const endDateParam = this.props.search.get(END_DATE_PARAM_NAME);

    const startDate = (startDateParam ? moment(startDateParam) : moment().subtract(7, "days")).toDate();
    const endDate = (endDateParam ? moment(endDateParam) : moment()).toDate();

    const isFiltering = Boolean(this.props.search.toString());

    return (
      <div className={`global-filter container ${isFiltering ? "is-filtering" : ""}`}>
        {isFiltering ? (
          <OutlinedButton
            className="clear-filters-button"
            title="Clear filters"
            onClick={this.onClickClearFilters.bind(this)}>
            <img src="/image/x.svg" alt="" />
          </OutlinedButton>
        ) : (
          <div className="filter-icon-container">
            <img src="/image/filter.svg" alt="" />
          </div>
        )}
        <div className="date-picker-container">
          <OutlinedButton className="date-picker-button" onClick={this.onOpenDatePicker.bind(this)}>
            <img src="/image/calendar.svg" alt="" />
            <span>{formatDateRange(startDate, endDate)}</span>
          </OutlinedButton>
          <Popup
            isOpen={this.state.isDatePickerOpen}
            onRequestClose={this.onCloseDatePicker.bind(this)}
            className="date-picker-popup"
            anchor="left">
            <DateRangePicker
              ranges={[{ startDate, endDate, key: "selection" }]}
              onChange={this.onDateChange.bind(this)}
              inputRanges={INPUT_RANGES}
            />
          </Popup>
        </div>
      </div>
    );
  }
}

// Exclude "Days from today" label since it's not useful.
const INPUT_RANGES = [defaultInputRanges[0]];
