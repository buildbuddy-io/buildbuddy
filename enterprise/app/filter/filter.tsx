import moment from "moment";
import React from "react";
import { DateRangePicker, OnChangeProps, Range } from "react-date-range";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Popup from "../../../app/components/popup/popup";
import Slider from "../../../app/components/slider/slider";
import {
  Filter,
  X,
  Calendar,
  Clock,
  User,
  Github,
  GitBranch,
  GitCommit,
  HardDrive,
  Wrench,
  SortAsc,
  SortDesc,
} from "lucide-react";
import Checkbox from "../../../app/components/checkbox/checkbox";
import Radio from "../../../app/components/radio/radio";
import { compactDurationSec, formatDateRange } from "../../../app/format/format";
import router, {
  START_DATE_PARAM_NAME,
  END_DATE_PARAM_NAME,
  ROLE_PARAM_NAME,
  STATUS_PARAM_NAME,
  LAST_N_DAYS_PARAM_NAME,
  USER_PARAM_NAME,
  REPO_PARAM_NAME,
  BRANCH_PARAM_NAME,
  COMMIT_PARAM_NAME,
  HOST_PARAM_NAME,
  COMMAND_PARAM_NAME,
  MINIMUM_DURATION_PARAM_NAME,
  MAXIMUM_DURATION_PARAM_NAME,
  SORT_BY_PARAM_NAME,
  SORT_ORDER_PARAM_NAME,
} from "../../../app/router/router";
import { invocation } from "../../../proto/invocation_ts_proto";
import {
  parseRoleParam,
  toRoleParam,
  parseStatusParam,
  toStatusParam,
  statusToString,
  getEndDate,
  getStartDate,
  DATE_PARAM_FORMAT,
  DEFAULT_LAST_N_DAYS,
  SortBy,
  SortOrder,
  DURATION_SLIDER_VALUES,
  DURATION_SLIDER_MIN_INDEX,
  DURATION_SLIDER_MIN_VALUE,
  DURATION_SLIDER_MAX_INDEX,
  DURATION_SLIDER_MAX_VALUE,
} from "./filter_util";
import TextInput from "../../../app/components/input/input";

export interface FilterProps {
  search: URLSearchParams;
}

interface State {
  isDatePickerOpen?: boolean;
  isFilterMenuOpen?: boolean;
  isSortMenuOpen?: boolean;

  isAdvancedFilterOpen?: boolean;

  user?: string;
  repo?: string;
  branch?: string;
  commit?: string;
  host?: string;
  command?: string;
  minimumDuration?: number;
  maximumDuration?: number;

  sortBy: string;
  sortOrder: string;
}

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

const LAST_N_DAYS_OPTIONS = [7, 30, 90, 180, 365];

export default class FilterComponent extends React.Component<FilterProps, State> {
  state: State = this.advancedFilterStateFromUrl(this.props.search);

  componentDidUpdate(prevProps: FilterProps) {
    if (this.props.search != prevProps.search) {
      this.setState(this.advancedFilterStateFromUrl(this.props.search));
    }
  }

  advancedFilterStateFromUrl(search: URLSearchParams) {
    return {
      isAdvancedFilterOpen: Boolean(
        search.get(USER_PARAM_NAME) ||
          search.get(REPO_PARAM_NAME) ||
          search.get(BRANCH_PARAM_NAME) ||
          search.get(COMMIT_PARAM_NAME) ||
          search.get(HOST_PARAM_NAME) ||
          search.get(COMMAND_PARAM_NAME) ||
          search.get(MINIMUM_DURATION_PARAM_NAME) ||
          search.get(MAXIMUM_DURATION_PARAM_NAME)
      ),
      user: search.get(USER_PARAM_NAME),
      repo: search.get(REPO_PARAM_NAME),
      branch: search.get(BRANCH_PARAM_NAME),
      commit: search.get(COMMIT_PARAM_NAME),
      host: search.get(HOST_PARAM_NAME),
      command: search.get(COMMAND_PARAM_NAME),
      minimumDuration: Number(search.get(MINIMUM_DURATION_PARAM_NAME)),
      maximumDuration: Number(search.get(MAXIMUM_DURATION_PARAM_NAME)),
      sortBy: search.get(SORT_BY_PARAM_NAME),
      sortOrder: search.get(SORT_ORDER_PARAM_NAME),
    };
  }

  private onOpenDatePicker() {
    this.setState({ isDatePickerOpen: true });
  }
  private onCloseDatePicker() {
    this.setState({ isDatePickerOpen: false });
  }
  private onDateChange(range: OnChangeProps) {
    const selection = (range as { selection: CustomDateRange }).selection;
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
      [USER_PARAM_NAME]: "",
      [REPO_PARAM_NAME]: "",
      [BRANCH_PARAM_NAME]: "",
      [COMMIT_PARAM_NAME]: "",
      [HOST_PARAM_NAME]: "",
      [COMMAND_PARAM_NAME]: "",
      [MINIMUM_DURATION_PARAM_NAME]: "",
      [MAXIMUM_DURATION_PARAM_NAME]: "",
    });
  }

  private onOpenSortMenu() {
    this.setState({ isSortMenuOpen: true });
  }
  private onCloseSortMenu() {
    this.setState({ isSortMenuOpen: false });
  }

  private onRoleToggle(role: string, selected: Set<string>) {
    selected = new Set(selected); // clone
    if (selected.has(role)) {
      selected.delete(role);
    } else {
      selected.add(role);
    }
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [ROLE_PARAM_NAME]: toRoleParam(selected),
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

  private onSortByChange(sortBy: string) {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [SORT_BY_PARAM_NAME]: sortBy,
    });
  }

  private onSortOrderChange(sortOrder: string) {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [SORT_ORDER_PARAM_NAME]: sortOrder,
    });
  }

  private renderRoleCheckbox(label: string, role: string, selected: Set<string>) {
    return (
      <label onClick={this.onRoleToggle.bind(this, role, selected)}>
        <Checkbox checked={selected.has(role)} />
        <span className={`role-badge ${role || "DEFAULT"}`}>{label}</span>
      </label>
    );
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

  private handleFilterApplyClicked() {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [USER_PARAM_NAME]: this.state.user,
      [REPO_PARAM_NAME]: this.state.repo,
      [BRANCH_PARAM_NAME]: this.state.branch,
      [COMMIT_PARAM_NAME]: this.state.commit,
      [HOST_PARAM_NAME]: this.state.host,
      [COMMAND_PARAM_NAME]: this.state.command,
      [MINIMUM_DURATION_PARAM_NAME]: this.state.minimumDuration?.toString(),
      [MAXIMUM_DURATION_PARAM_NAME]: this.state.maximumDuration?.toString(),
    });
  }

  private renderSortByRadio(label: string, sortBy: string, selected: string) {
    return (
      <label onClick={this.onSortByChange.bind(this, sortBy, selected)}>
        <Radio checked={selected === sortBy} />
        <span>{label}</span>
      </label>
    );
  }

  private renderSortOrderRadio(label: string, sortOrder: string, selected: string) {
    return (
      <label onClick={this.onSortOrderChange.bind(this, sortOrder, selected)}>
        <Radio checked={selected === sortOrder} />
        <span>{label}</span>
      </label>
    );
  }

  render() {
    const now = new Date();
    const startDate = getStartDate(this.props.search);
    // Not using `getEndDate` here because it's set to "start of day after the one specified
    // in the URL" which causes an off-by-one error if we were to render that directly in
    // the calendar.
    const endDate = this.props.search.get(END_DATE_PARAM_NAME)
      ? moment(this.props.search.get(END_DATE_PARAM_NAME)).toDate()
      : now;

    const roleValue = this.props.search.get(ROLE_PARAM_NAME) || "";
    const statusValue = this.props.search.get(STATUS_PARAM_NAME) || "";
    const userValue = this.props.search.get(USER_PARAM_NAME) || "";
    const repoValue = this.props.search.get(REPO_PARAM_NAME) || "";
    const branchValue = this.props.search.get(BRANCH_PARAM_NAME) || "";
    const commitValue = this.props.search.get(COMMIT_PARAM_NAME) || "";
    const hostValue = this.props.search.get(HOST_PARAM_NAME) || "";
    const commandValue = this.props.search.get(COMMAND_PARAM_NAME) || "";
    const minimumDurationValue = this.props.search.get(MINIMUM_DURATION_PARAM_NAME) || "";
    const maximumDurationValue = this.props.search.get(MAXIMUM_DURATION_PARAM_NAME) || "";
    const isFiltering = Boolean(
      roleValue ||
        statusValue ||
        userValue ||
        repoValue ||
        branchValue ||
        commitValue ||
        hostValue ||
        commandValue ||
        minimumDurationValue ||
        maximumDurationValue
    );
    const selectedRoles = new Set(parseRoleParam(roleValue));
    const selectedStatuses = new Set(parseStatusParam(statusValue));

    const isDateRangeSelected =
      this.props.search.get(LAST_N_DAYS_PARAM_NAME) ||
      this.props.search.get(START_DATE_PARAM_NAME) ||
      this.props.search.get(END_DATE_PARAM_NAME);

    const presetDateRanges: PresetRange[] = LAST_N_DAYS_OPTIONS.map((n) => {
      const start = moment(now)
        .add(-n + 1, "days")
        .toDate();
      return {
        label: formatDateRange(start, now, { now }),
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

    const sortByValue: SortBy = (this.props.search.get(SORT_BY_PARAM_NAME) as SortBy) || "";
    const sortOrderValue: SortOrder = (this.props.search.get(SORT_ORDER_PARAM_NAME) as SortOrder) || "desc";

    return (
      <div className={`global-filter ${isFiltering ? "is-filtering" : ""}`}>
        {isFiltering && (
          <FilledButton className="square" title="Clear filters" onClick={this.onClickClearFilters.bind(this)}>
            <X className="icon white" />
          </FilledButton>
        )}
        <div className="popup-wrapper">
          <OutlinedButton
            className={`filter-menu-button icon-text-button ${isFiltering ? "" : "square"}`}
            onClick={this.onOpenFilterMenu.bind(this)}>
            <Filter className="icon" />
            {selectedStatuses.has(invocation.OverallStatus.SUCCESS) && <span className="status-block success" />}
            {selectedStatuses.has(invocation.OverallStatus.FAILURE) && <span className="status-block failure" />}
            {selectedStatuses.has(invocation.OverallStatus.IN_PROGRESS) && (
              <span className="status-block in-progress" />
            )}
            {selectedStatuses.has(invocation.OverallStatus.DISCONNECTED) && (
              <span className="status-block disconnected" />
            )}
            {selectedRoles.has("") && <span className="role-badge DEFAULT">Default</span>}
            {selectedRoles.has("CI") && <span className="role-badge CI">CI</span>}
            {selectedRoles.has("CI_RUNNER") && <span className="role-badge CI_RUNNER">Workflow</span>}
            {userValue && (
              <span className="advanced-badge">
                <User /> {userValue}
              </span>
            )}
            {repoValue && (
              <span className="advanced-badge">
                <Github /> {repoValue}
              </span>
            )}
            {branchValue && (
              <span className="advanced-badge">
                <GitBranch /> {branchValue}
              </span>
            )}
            {commitValue && (
              <span className="advanced-badge">
                <GitCommit /> {commitValue}
              </span>
            )}
            {hostValue && (
              <span className="advanced-badge">
                <HardDrive /> {hostValue}
              </span>
            )}
            {commandValue && (
              <span className="advanced-badge">
                <Wrench /> {commandValue}
              </span>
            )}
            {(minimumDurationValue || maximumDurationValue) && (
              <span className="advanced-badge">
                <Clock /> {compactDurationSec(Number(minimumDurationValue))} -{" "}
                {compactDurationSec(Number(maximumDurationValue))}
              </span>
            )}
          </OutlinedButton>
          <Popup
            isOpen={this.state.isFilterMenuOpen}
            onRequestClose={this.onCloseFilterMenu.bind(this)}
            className="filter-menu-popup">
            <div className="option-groups-row">
              <div className="option-group">
                <div className="option-group-title">Role</div>
                <div className="option-group-options">
                  {this.renderRoleCheckbox("Default", "", selectedRoles)}
                  {this.renderRoleCheckbox("CI", "CI", selectedRoles)}
                  {this.renderRoleCheckbox("Workflow", "CI_RUNNER", selectedRoles)}
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
            <div
              className="filter-menu-advanced-filter-toggle"
              onClick={() => this.setState({ isAdvancedFilterOpen: !this.state.isAdvancedFilterOpen })}>
              {this.state.isAdvancedFilterOpen ? "Hide advanced filters" : "Show advanced filters"}
            </div>
            {this.state.isAdvancedFilterOpen && (
              <form className="option-groups-row">
                <div className="option-group">
                  <div className="option-group-title">User</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. tylerw"}
                      value={this.state.user}
                      onChange={(e) => this.setState({ user: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Repo</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. https://github.com/buildbuddy-io/buildbuddy"}
                      value={this.state.repo}
                      onChange={(e) => this.setState({ repo: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Branch</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. main"}
                      value={this.state.branch}
                      onChange={(e) => this.setState({ branch: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Commit</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. 115a0cdbe816b8cb80089dd200247752fef723fe"}
                      value={this.state.commit}
                      onChange={(e) => this.setState({ commit: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Host</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. lunchbox"}
                      value={this.state.host}
                      onChange={(e) => this.setState({ host: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Command</div>
                  <div className="option-group-input">
                    <TextInput
                      placeholder={"e.g. test"}
                      value={this.state.command}
                      onChange={(e) => this.setState({ command: e.target.value })}
                    />
                  </div>
                  <div className="option-group-title">Duration</div>
                  <div className="option-group-input">
                    <Slider
                      value={[
                        DURATION_SLIDER_VALUES.indexOf(this.state.minimumDuration || DURATION_SLIDER_MIN_VALUE),
                        DURATION_SLIDER_VALUES.indexOf(this.state.maximumDuration || DURATION_SLIDER_MAX_VALUE),
                      ]}
                      renderThumb={(props, state) => (
                        <div {...props}>
                          <div className="slider-thumb-circle"></div>
                          <div className="slider-thumb-value">
                            {compactDurationSec(DURATION_SLIDER_VALUES[state.valueNow])}
                          </div>
                        </div>
                      )}
                      min={DURATION_SLIDER_MIN_INDEX}
                      max={DURATION_SLIDER_MAX_INDEX}
                      pearling
                      minDistance={1}
                      onChange={(e) =>
                        this.setState({
                          minimumDuration: DURATION_SLIDER_VALUES[e[0]],
                          maximumDuration: DURATION_SLIDER_VALUES[e[1]],
                        })
                      }
                    />
                  </div>
                  <div className="option-group-input">
                    <FilledButton onClick={this.handleFilterApplyClicked.bind(this)}>Apply</FilledButton>
                  </div>
                </div>
              </form>
            )}
          </Popup>
        </div>
        <div className="popup-wrapper">
          <OutlinedButton
            className={`sort-button icon-text-button ${sortByValue !== "" ? "" : "square"}`}
            onClick={this.onOpenSortMenu.bind(this)}>
            {sortOrderValue === "asc" && <SortAsc className="icon" />}
            {sortOrderValue === "desc" && <SortDesc className="icon" />}
            {sortByValue !== "" && (
              <span>
                {sortByValue === "start-time" && "Start time"}
                {sortByValue === "end-time" && "End time"}
                {sortByValue === "duration" && "Duration"}
                {sortByValue === "ac-hit-ratio" && "AC hit ratio"}
                {sortByValue === "cas-hit-ratio" && "CAS hit ratio"}
                {sortByValue === "cache-down" && "Cache download"}
                {sortByValue === "cache-up" && "Cache upload"}
                {sortByValue === "cache-xfer" && "Cache transfer"}
              </span>
            )}
          </OutlinedButton>
          <Popup
            isOpen={this.state.isSortMenuOpen}
            onRequestClose={this.onCloseSortMenu.bind(this)}
            className="filter-menu-popup">
            <div className="option-groups-row">
              <div className="option-group">
                <div className="option-group-title">Sort By</div>
                <div className="option-group-options">
                  {this.renderSortByRadio("Start time", "start-time", sortByValue)}
                  {this.renderSortByRadio("End time", "end-time", sortByValue)}
                  {this.renderSortByRadio("Duration", "duration", sortByValue)}
                  {this.renderSortByRadio("Action cache hit ratio", "ac-hit-ratio", sortByValue)}
                  {this.renderSortByRadio("CAS hit ratio", "cas-hit-ratio", sortByValue)}
                  {this.renderSortByRadio("Cache download", "cache-down", sortByValue)}
                  {this.renderSortByRadio("Cache upload", "cache-up", sortByValue)}
                  {this.renderSortByRadio("Cache transfer", "cache-xfer", sortByValue)}
                </div>
              </div>
              <div className="option-group">
                <div className="option-group-title">Sort Order</div>
                <div className="option-group-options">
                  {this.renderSortOrderRadio("Ascending", "asc", sortOrderValue)}
                  {this.renderSortOrderRadio("Descending", "desc", sortOrderValue)}
                </div>
              </div>
            </div>
          </Popup>
        </div>
        <div className="popup-wrapper">
          <OutlinedButton className="date-picker-button icon-text-button" onClick={this.onOpenDatePicker.bind(this)}>
            <Calendar className="icon" />
            <span>{formatDateRange(startDate, endDate)}</span>
          </OutlinedButton>
          <Popup
            isOpen={this.state.isDatePickerOpen}
            onRequestClose={this.onCloseDatePicker.bind(this)}
            className="date-picker-popup">
            <DateRangePicker
              ranges={[{ startDate, endDate, key: "selection" }]}
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
              color="#263238"
              rangeColors={["#263238"]}
              startDatePlaceholder="Start date"
              endDatePlaceholder="End date"
            />
          </Popup>
        </div>
      </div>
    );
  }
}
