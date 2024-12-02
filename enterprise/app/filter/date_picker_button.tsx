import React from "react";
import { OutlinedButton } from "../../../app/components/button/button";
import { DateRangePicker, DateRange, Range, RangeKeyDict } from "react-date-range";
import router from "../../../app/router/router";
import { END_DATE_PARAM_NAME, LAST_N_DAYS_PARAM_NAME, START_DATE_PARAM_NAME } from "../../../app/router/router_params";
import moment from "moment";
import { Calendar } from "lucide-react";
import { formatDateRange } from "../../../app/format/format";
import Popup from "../../../app/components/popup/popup";
import {
  DATE_PARAM_FORMAT,
  DEFAULT_LAST_N_DAYS,
  formatDateRangeFromUrlParams,
  getDateRangeForPicker,
} from "./filter_util";

const LAST_N_DAYS_OPTIONS = [7, 30, 90, 180, 365];

type PresetRange = {
  label: string;
  isSelected?: (range: Range) => boolean;
  range: () => CustomDateRange;
};

/**
 * CustomDateRange is a react-date-range `Range` extended with some custom properties.
 */
type CustomDateRange = Range & {
  /**
   * For the "last {N} days" options, the number of days to look
   * back (relative to today).
   */
  days?: number;
};

interface Props {
  search: URLSearchParams;
}

interface State {
  isOpen: boolean;
}

export default class DatePickerButton extends React.Component<Props, State> {
  state: State = {
    isOpen: false,
  };

  private onOpenDatePicker() {
    this.setState({ isOpen: true });
  }

  private onCloseDatePicker() {
    this.setState({ isOpen: false });
  }

  private onDateChange(range: RangeKeyDict) {
    const selection = range.selection as CustomDateRange;
    if (selection.days) {
      router.setQuery({
        ...Object.fromEntries(this.props.search.entries()),
        [START_DATE_PARAM_NAME]: "",
        [END_DATE_PARAM_NAME]: "",
        [LAST_N_DAYS_PARAM_NAME]: String(selection.days),
      });
      return;
    }
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [START_DATE_PARAM_NAME]: moment(selection.startDate).format(DATE_PARAM_FORMAT),
      [END_DATE_PARAM_NAME]: moment(selection.endDate).format(DATE_PARAM_FORMAT),
      [LAST_N_DAYS_PARAM_NAME]: "",
    });
  }

  render() {
    const { startDate, endDate } = getDateRangeForPicker(this.props.search);

    const isDateRangeSelected =
      this.props.search.get(LAST_N_DAYS_PARAM_NAME) ||
      this.props.search.get(START_DATE_PARAM_NAME) ||
      this.props.search.get(END_DATE_PARAM_NAME);

    const presetDateRanges: PresetRange[] = LAST_N_DAYS_OPTIONS.map((n) => {
      const now = new Date();
      const start = moment(now)
        .add(-n + 1, "days")
        .startOf("day")
        .toDate();
      return {
        label: formatDateRange(start, undefined, { now }),
        isSelected: () =>
          this.props.search.get(LAST_N_DAYS_PARAM_NAME) === String(n) ||
          (!isDateRangeSelected && n === DEFAULT_LAST_N_DAYS),
        range: () => ({
          startDate: start,
          endDate: now,
          days: n,
        }),
      };
    });

    return (
      <div className="popup-wrapper date-picker-button-wrapper">
        <OutlinedButton className="date-picker-button icon-text-button" onClick={this.onOpenDatePicker.bind(this)}>
          <Calendar className="icon" />
          <span>{formatDateRangeFromUrlParams(this.props.search)}</span>
        </OutlinedButton>
        <Popup
          isOpen={this.state.isOpen}
          onRequestClose={this.onCloseDatePicker.bind(this)}
          className="date-picker-popup">
          <DateRangePicker
            // Just for rendering's sake, treat undefined endDate as "now"--this has
            // no impact on the user's actual selection.
            ranges={[{ startDate, endDate: endDate ?? new Date(), key: "selection" }]}
            onChange={this.onDateChange.bind(this)}
            // When showing "All time" we don't want to set the currently
            // visible month to the Unix epoch... so always show the end
            // date when initially rendering the component
            shownDate={endDate}
            // We want our `CustomDateRange` type here, which is compatible
            // with the `StaticRange` type, so the cast to `any` is OK here.
            staticRanges={presetDateRanges as any}
            // Disable textbox inputs, like "days from today", or "days until today".
            inputRanges={[]}
            editableDateInputs
            color="#212121"
            rangeColors={["#212121"]}
            startDatePlaceholder="Start date"
            endDatePlaceholder="End date"
          />
        </Popup>
      </div>
    );
  }
}
