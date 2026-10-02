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
  /**
   * Names of the params holding the selection, defaulting to the global
   * filter's. With custom names, "last N days" presets are stored as dates.
   */
  paramNames?: { start: string; end: string };
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
    const { paramNames } = this.props;
    if (paramNames) {
      router.setQuery({
        ...Object.fromEntries(this.props.search.entries()),
        [paramNames.start]: formatDateParam(range.startDate ?? new Date()),
        [paramNames.end]: formatDateParam(range.endDate ?? new Date()),
      });
      return;
    }
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

  /** The selection under the global filter's param names, which the date helpers read. */
  private selection(): URLSearchParams {
    const { search, paramNames } = this.props;
    if (!paramNames) return search;
    const selection = new URLSearchParams();
    selection.set(START_DATE_PARAM_NAME, search.get(paramNames.start) ?? "");
    selection.set(END_DATE_PARAM_NAME, search.get(paramNames.end) ?? "");
    return selection;
  }

  render() {
    const selection = this.selection();
    const { startDate, endDate } = getDateRangeForPicker(selection);
    return (
      <div className="popup-wrapper">
        <OutlinedButton onClick={() => this.setState({ isOpen: true })}>
          <Calendar className="icon" />
          <span>{formatDateRangeFromUrlParams(selection)}</span>
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
