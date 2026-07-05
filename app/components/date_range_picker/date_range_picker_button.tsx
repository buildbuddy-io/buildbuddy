import { Calendar } from "lucide-react";
import React from "react";
import { formatDateRange } from "../../format/format";
import router from "../../router/router";
import { addDays, startOfDay } from "../../util/date";
import { END_DATE_PARAM_NAME, LAST_N_DAYS_PARAM_NAME, START_DATE_PARAM_NAME } from "../../router/router_params";
import { OutlinedButton } from "../button/button";
import Popup from "../popup/popup";
import DateRangePicker, { Range } from "./date_range_picker";

/** The number of days selected when no date range params are set. */
export const DEFAULT_LAST_N_DAYS = 7;

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
    if (range.days) {
      router.setQuery({
        ...Object.fromEntries(this.props.search.entries()),
        [START_DATE_PARAM_NAME]: "",
        [END_DATE_PARAM_NAME]: "",
        [LAST_N_DAYS_PARAM_NAME]: String(range.days),
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

/** Returns the start of the date range selected in the URL. */
export function getStartDate(search: URLSearchParams, now: Date = new Date()): Date {
  const dateString = search.get(START_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      return new Date(dateNumber);
    }
    return parseDateParam(dateString);
  }
  const lastNDays = Number(search.get(LAST_N_DAYS_PARAM_NAME)) || DEFAULT_LAST_N_DAYS;
  return addDays(now, 1 - lastNDays);
}

/**
 * Returns the exclusive end of the date range selected in the URL: the start
 * of the day after the end date named in the URL. Returns undefined if no end
 * date is set.
 */
export function getEndDate(search: URLSearchParams): Date | undefined {
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (!dateString) {
    return undefined;
  }
  const dateNumber = Number(dateString);
  if (Number.isInteger(dateNumber)) {
    return new Date(dateNumber);
  }
  return addDays(parseDateParam(dateString), 1);
}

/** Formats the date range selected in the URL as a human-readable string. */
export function formatDateRangeFromUrlParams(search: URLSearchParams): string {
  let endDate = undefined;
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      endDate = new Date(dateNumber);
    } else {
      endDate = getEndDate(search);
    }
  }
  return formatDateRange(getStartDate(search), endDate);
}

/**
 * Returns the date range to render in the picker. Unlike `getEndDate`, the
 * returned end date is the day named in the URL rather than the start of the
 * day after it, which would be off by one on the calendar.
 */
function getDateRangeForPicker(search: URLSearchParams): { startDate: Date; endDate?: Date } {
  let endDate = undefined;
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      endDate = startOfDay(new Date(dateNumber));
    } else {
      endDate = parseDateParam(dateString);
    }
  }
  return { startDate: getStartDate(search), endDate };
}

/** Parses a "YYYY-MM-DD" date param as local midnight. */
function parseDateParam(value: string): Date {
  const [year, month, day] = value.split("-").map(Number);
  return new Date(year, (month || 1) - 1, day || 1);
}

/** Formats a date as a "YYYY-MM-DD" date param. */
function formatDateParam(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}
