import { addDays, isSameDay, startOfDay, startOfMonth } from "date-fns";
import { ChevronLeft, ChevronRight } from "lucide-react";
import React from "react";
import { formatDateOnly, formatDateRange } from "../../format/format";
import { OutlinedButton } from "../button/button";
import TextInput from "../input/input";
import Select, { Option } from "../select/select";

/** The "last N days" preset options shown in the sidebar. */
const LAST_N_DAYS_OPTIONS = [1, 2, 7, 30, 90, 180, 365];

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

const WEEKDAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/** A date range selected in the picker. */
export interface Range {
  startDate?: Date;
  endDate?: Date;
  /** Set when a "last N days" preset is selected, counting back from today. */
  lastNDays?: number;
}

export interface DateRangePickerProps {
  /** The currently selected date range. */
  range: Range;
  /** Called with the new range when a selection is made. */
  onChange: (range: Range) => void;
}

/** Which end of the range the next calendar click selects. */
type SelectionStep = "start" | "end";

interface State {
  /** The first day of the month shown on the calendar. */
  shownMonth: Date;
  /**
   * Which end of the range the next selection sets. Picking a start date
   * advances this to "end", so that the next pick completes the range.
   */
  step: SelectionStep;
  /** The range highlighted on the calendar while hovering, if any. */
  preview: Range | null;
  /** The day on which an in-progress drag selection started, if any. */
  dragStart: Date | null;
  /** The day the pointer is over during an in-progress drag selection. */
  dragEnd: Date | null;
}

/**
 * DateRangePicker renders a date range picker with a "last N days" preset
 * sidebar, text inputs for the start and end dates, and a month calendar on
 * which a custom range can be selected by clicking or dragging.
 */
export class DateRangePicker extends React.Component<DateRangePickerProps, State> {
  state: State = {
    shownMonth: startOfMonth(this.props.range.endDate ?? new Date()),
    step: "start",
    preview: null,
    dragStart: null,
    dragEnd: null,
  };

  componentDidUpdate(prevProps: DateRangePickerProps) {
    // When the selection changes, show the month containing its end date.
    if (
      prevProps.range.startDate?.getTime() !== this.props.range.startDate?.getTime() ||
      prevProps.range.endDate?.getTime() !== this.props.range.endDate?.getTime()
    ) {
      this.setState({ shownMonth: startOfMonth(this.props.range.endDate ?? new Date()) });
    }
  }

  /** Returns the range that selecting the given day would produce. */
  private nextRange(day: Date): { range: Range; nextStep: SelectionStep } {
    const { startDate } = this.props.range;
    if (this.state.step === "start" || !startDate) {
      return { range: { startDate: day, endDate: day }, nextStep: "end" };
    }
    if (startOfDay(day).getTime() < startOfDay(startDate).getTime()) {
      return { range: { startDate: day, endDate: startDate }, nextStep: "start" };
    }
    return { range: { startDate, endDate: day }, nextStep: "start" };
  }

  private selectDay(day: Date) {
    const { range, nextStep } = this.nextRange(day);
    this.props.onChange(range);
    this.setState({ step: nextStep, preview: null });
  }

  private onDayMouseEnter(day: Date) {
    if (this.state.dragStart) {
      this.setState({ dragEnd: day });
    } else {
      this.setState({ preview: this.nextRange(day).range });
    }
  }

  private onDayMouseUp(day: Date) {
    const { dragStart } = this.state;
    this.setState({ dragStart: null, dragEnd: null });
    if (!dragStart) return;
    if (isSameDay(dragStart, day)) {
      this.selectDay(day);
      return;
    }
    const [startDate, endDate] = day.getTime() < dragStart.getTime() ? [day, dragStart] : [dragStart, day];
    this.props.onChange({ startDate, endDate });
    this.setState({ step: "start", preview: null });
  }

  render() {
    const { range } = this.props;
    const { shownMonth, preview, dragStart, dragEnd } = this.state;
    // While dragging, show the in-progress drag range instead of the current
    // selection, and hide the hover preview.
    let shownRange = range;
    if (dragStart && dragEnd) {
      shownRange =
        dragEnd.getTime() < dragStart.getTime()
          ? { startDate: dragEnd, endDate: dragStart }
          : { startDate: dragStart, endDate: dragEnd };
    }
    const shownPreview = dragStart ? null : preview;
    const now = new Date();
    const years: number[] = [];
    for (let year = now.getFullYear() + 1; year >= now.getFullYear() - 100; year--) {
      years.push(year);
    }
    return (
      <div className="date-range-picker">
        <div className="preset-picker">
          {LAST_N_DAYS_OPTIONS.map((lastNDays) => {
            const startDate = startOfDay(addDays(now, 1 - lastNDays));
            const selected =
              !!range.startDate && isSameDay(range.startDate, startDate) && isSameDay(range.endDate ?? now, now);
            const onSelect = () => this.props.onChange({ startDate, endDate: now, lastNDays });
            return (
              <div
                key={lastNDays}
                role="button"
                tabIndex={0}
                className={`preset-option ${selected ? "selected" : ""}`}
                onClick={onSelect}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onSelect();
                  }
                }}
                onMouseEnter={() => this.setState({ preview: { startDate, endDate: now } })}
                onMouseLeave={() => this.setState({ preview: null })}>
                {formatDateRange(startDate, undefined, { now })}
              </div>
            );
          })}
        </div>
        <div className="calendar-picker">
          <div className="text-inputs">
            <DateInput
              value={range.startDate}
              placeholder="Start date"
              onFocus={() => this.setState({ step: "start" })}
              onCommit={(date) => this.selectDay(date)}
            />
            <DateInput
              value={range.endDate}
              placeholder="End date"
              onFocus={() => this.setState({ step: "end" })}
              onCommit={(date) => this.selectDay(date)}
            />
          </div>
          <div className="calendar-input">
            <div className="month-picker">
              <OutlinedButton
                className="icon-button"
                aria-label="Previous month"
                onClick={() =>
                  this.setState({ shownMonth: new Date(shownMonth.getFullYear(), shownMonth.getMonth() - 1, 1) })
                }>
                <ChevronLeft />
              </OutlinedButton>
              <Select
                aria-label="Month"
                value={shownMonth.getMonth()}
                onChange={(e) =>
                  this.setState({ shownMonth: new Date(shownMonth.getFullYear(), Number(e.target.value), 1) })
                }>
                {MONTH_NAMES.map((name, i) => (
                  <Option key={i} value={i}>
                    {name}
                  </Option>
                ))}
              </Select>
              <Select
                aria-label="Year"
                value={shownMonth.getFullYear()}
                onChange={(e) =>
                  this.setState({ shownMonth: new Date(Number(e.target.value), shownMonth.getMonth(), 1) })
                }>
                {years.map((year) => (
                  <Option key={year} value={year}>
                    {year}
                  </Option>
                ))}
              </Select>
              <OutlinedButton
                className="icon-button"
                aria-label="Next month"
                onClick={() =>
                  this.setState({ shownMonth: new Date(shownMonth.getFullYear(), shownMonth.getMonth() + 1, 1) })
                }>
                <ChevronRight />
              </OutlinedButton>
            </div>
            <div className="range-selection">
              <div className="weekdays">
                {WEEKDAY_NAMES.map((name) => (
                  <span className="weekday" key={name}>
                    {name}
                  </span>
                ))}
              </div>
              <div
                className="days"
                onMouseLeave={() => this.setState({ preview: null, dragStart: null, dragEnd: null })}>
                {calendarDays(shownMonth).map((day) => {
                  const outsideMonth = day.getMonth() !== shownMonth.getMonth();
                  const fill = dayFill(day, shownRange);
                  const previewFill = shownPreview ? dayFill(day, shownPreview) : null;
                  const classes = ["day"];
                  if (outsideMonth) classes.push("outside-month");
                  if (fill) classes.push("in-selection");
                  if (isSameDay(day, now)) classes.push("today");
                  return (
                    <button
                      type="button"
                      key={day.getTime()}
                      className={classes.join(" ")}
                      onMouseDown={(e) =>
                        e.button === 0 && this.setState({ dragStart: day, dragEnd: day, preview: null })
                      }
                      onMouseEnter={() => this.onDayMouseEnter(day)}
                      onMouseUp={() => this.onDayMouseUp(day)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          this.selectDay(day);
                        }
                      }}>
                      {fill && <span className={fillClass("selection", fill)} />}
                      {previewFill && <span className={fillClass("preview", previewFill)} />}
                      <span className="day-number">{day.getDate()}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

interface DateInputProps {
  value?: Date;
  placeholder: string;
  onFocus: () => void;
  onCommit: (date: Date) => void;
}

interface DateInputState {
  text: string;
  dirty: boolean;
  invalid: boolean;
}

/** A text input that displays a date and commits edits on blur or Enter. */
class DateInput extends React.Component<DateInputProps, DateInputState> {
  state: DateInputState = {
    text: this.props.value ? formatDateOnly(this.props.value) : "",
    dirty: false,
    invalid: false,
  };

  componentDidUpdate(prevProps: DateInputProps) {
    if (prevProps.value?.getTime() !== this.props.value?.getTime()) {
      this.setState({
        text: this.props.value ? formatDateOnly(this.props.value) : "",
        dirty: false,
        invalid: false,
      });
    }
  }

  private commit() {
    if (!this.state.dirty || !this.state.text) return;
    const parsed = parseInputDate(this.state.text);
    if (isNaN(parsed.getTime())) {
      this.setState({ invalid: true });
      return;
    }
    this.setState({ dirty: false });
    this.props.onCommit(parsed);
  }

  render() {
    return (
      <TextInput
        className={this.state.invalid ? "invalid" : ""}
        value={this.state.text}
        placeholder={this.props.placeholder}
        onFocus={this.props.onFocus}
        onChange={(e) => this.setState({ text: e.target.value, dirty: true, invalid: false })}
        onKeyDown={(e) => e.key === "Enter" && this.commit()}
        onBlur={() => this.commit()}
      />
    );
  }
}

/** Returns all days shown for a month: full weeks from the week containing the 1st. */
function calendarDays(month: Date): Date[] {
  const firstOfMonth = startOfMonth(month);
  const lastOfMonth = new Date(month.getFullYear(), month.getMonth() + 1, 0);
  const gridStart = addDays(firstOfMonth, -firstOfMonth.getDay());
  const gridEnd = addDays(lastOfMonth, 6 - lastOfMonth.getDay());
  const days: Date[] = [];
  for (let day = gridStart; day.getTime() <= gridEnd.getTime(); day = addDays(day, 1)) {
    days.push(day);
  }
  return days;
}

/** Returns how the range covers the given day, or null if it doesn't. */
function dayFill(day: Date, range: Range): { start: boolean; end: boolean } | null {
  if (!range.startDate || !range.endDate) return null;
  const start = startOfDay(range.startDate).getTime();
  const end = startOfDay(range.endDate).getTime();
  const t = startOfDay(day).getTime();
  if (t < start || t > end) return null;
  return { start: t === start, end: t === end };
}

/** Returns the class name for a selection or preview span, rounding true range ends. */
function fillClass(base: string, fill: { start: boolean; end: boolean }): string {
  return `${base}${fill.start ? " round-left" : ""}${fill.end ? " round-right" : ""}`;
}

/**
 * Parses text typed into a date input. Date-only strings like "2026-07-06"
 * get a time appended so that they are parsed as local time rather than UTC.
 */
function parseInputDate(text: string): Date {
  text = text.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return new Date(text + "T00:00:00");
  }
  return new Date(text);
}

export default DateRangePicker;
