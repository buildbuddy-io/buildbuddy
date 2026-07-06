import { Calendar } from "lucide-react";
import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import DateRangePicker, { Range } from "../../../app/components/date_range/date_range_picker";
import Popup from "../../../app/components/popup/popup";
import router from "../../../app/router/router";
import { END_DATE_PARAM_NAME, LAST_N_DAYS_PARAM_NAME, START_DATE_PARAM_NAME } from "../../../app/router/router_params";
import { formatDateParam, formatDateRangeFromUrlParams, getDateRangeForPicker } from "./filter_util";

export interface DateRangePickerButtonProps {
  /** URL params holding the current date range selection. */
  search: URLSearchParams;
}

interface State {
  isOpen: boolean;
}

/**
 * DateRangePickerButton renders a button showing the date range selected in
 * the URL, and opens a DateRangePicker popup that updates the URL on change.
 */
export default class DateRangePickerButton extends React.Component<DateRangePickerButtonProps, State> {
  state: State = { isOpen: false };

  private onChange(range: Range) {
    if (range.lastNDays) {
      router.setQuery({
        ...Object.fromEntries(this.props.search.entries()),
        [START_DATE_PARAM_NAME]: "",
        [END_DATE_PARAM_NAME]: "",
        [LAST_N_DAYS_PARAM_NAME]: String(range.lastNDays),
      });
      return;
    }
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [START_DATE_PARAM_NAME]: formatDateParam(range.startDate ?? new Date()),
      [END_DATE_PARAM_NAME]: formatDateParam(range.endDate ?? new Date()),
      [LAST_N_DAYS_PARAM_NAME]: "",
    });
  }

  render() {
    const { startDate, endDate } = getDateRangeForPicker(this.props.search);
    return (
      <div className="popup-wrapper">
        <OutlinedButton onClick={() => this.setState({ isOpen: true })}>
          <Calendar className="icon" />
          <span>{formatDateRangeFromUrlParams(this.props.search)}</span>
        </OutlinedButton>
        <Popup isOpen={this.state.isOpen} onRequestClose={() => this.setState({ isOpen: false })}>
          <DateRangePicker
            // Treat an unset end date as "now" for display purposes only.
            range={{ startDate, endDate: endDate ?? new Date() }}
            onChange={this.onChange.bind(this)}
          />
        </Popup>
      </div>
    );
  }
}
