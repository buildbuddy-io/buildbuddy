import React from "react";
import capabilities from "../../../app/capabilities/capabilities";
import FilledButton, { OutlinedButton } from "../../../app/components/button/button";
import Popup from "../../../app/components/popup/popup";
import Slider from "../../../app/components/slider/slider";
import {
  Filter,
  X,
  Clock,
  User,
  Github,
  GitBranch,
  GitCommit,
  HardDrive,
  LayoutGrid,
  Wrench,
  Tag,
  SortAsc,
  SortDesc,
  Cloud,
  Sparkles,
} from "lucide-react";
import Checkbox from "../../../app/components/checkbox/checkbox";
import Radio from "../../../app/components/radio/radio";
import { compactDurationSec } from "../../../app/format/format";
import router from "../../../app/router/router";
import {
  DIMENSION_PARAM_NAME,
  GENERIC_FILTER_PARAM_NAME,
  ROLE_PARAM_NAME,
  STATUS_PARAM_NAME,
  USER_PARAM_NAME,
  REPO_PARAM_NAME,
  BRANCH_PARAM_NAME,
  COMMIT_PARAM_NAME,
  HOST_PARAM_NAME,
  COMMAND_PARAM_NAME,
  PATTERN_PARAM_NAME,
  TAG_PARAM_NAME,
  MINIMUM_DURATION_PARAM_NAME,
  MAXIMUM_DURATION_PARAM_NAME,
  SORT_BY_PARAM_NAME,
  SORT_ORDER_PARAM_NAME,
  DEFAULT_SORT_BY_VALUE,
  DEFAULT_SORT_ORDER_VALUE,
} from "../../../app/router/router_params";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import {
  parseRoleParam,
  toRoleParam,
  parseStatusParam,
  toStatusParam,
  statusToString,
  isAnyNonDateFilterSet,
  SortBy,
  SortOrder,
  DURATION_SLIDER_VALUES,
  DURATION_SLIDER_MIN_INDEX,
  DURATION_SLIDER_MIN_VALUE,
  DURATION_SLIDER_MAX_INDEX,
  DURATION_SLIDER_MAX_VALUE,
  getFiltersFromDimensionParam,
  getDimensionName,
} from "./filter_util";
import TextInput from "../../../app/components/input/input";
import DatePickerButton from "./date_picker_button";

export interface FilterProps {
  search: URLSearchParams;
}

interface State {
  isDatePickerOpen: boolean;
  isFilterMenuOpen: boolean;
  isSortMenuOpen: boolean;

  isAdvancedFilterOpen: boolean;

  user?: string;
  repo?: string;
  branch?: string;
  commit?: string;
  host?: string;
  command?: string;
  pattern?: string;
  tag?: string;
  minimumDuration?: number;
  maximumDuration?: number;

  genericFilterString?: string;

  sortBy?: SortBy;
  sortOrder?: SortOrder;
}

export default class FilterComponent extends React.Component<FilterProps, State> {
  state: State = this.newFilterState(this.props.search);

  componentDidUpdate(prevProps: FilterProps) {
    if (this.props.search != prevProps.search) {
      this.setState(this.updateFilterState(this.props.search));
    }
  }

  isAdvancedFilterOpen(search: URLSearchParams): boolean {
    return Boolean(
      search.get(USER_PARAM_NAME) ||
        search.get(REPO_PARAM_NAME) ||
        search.get(BRANCH_PARAM_NAME) ||
        search.get(COMMIT_PARAM_NAME) ||
        search.get(HOST_PARAM_NAME) ||
        search.get(COMMAND_PARAM_NAME) ||
        (capabilities.config.patternFilterEnabled && search.get(PATTERN_PARAM_NAME)) ||
        search.get(GENERIC_FILTER_PARAM_NAME) ||
        (capabilities.config.tagsUiEnabled && search.get(TAG_PARAM_NAME)) ||
        search.get(MINIMUM_DURATION_PARAM_NAME) ||
        search.get(MAXIMUM_DURATION_PARAM_NAME) ||
        search.get(GENERIC_FILTER_PARAM_NAME)
    );
  }

  newFilterState(search: URLSearchParams): State {
    return {
      isDatePickerOpen: false,
      isFilterMenuOpen: false,
      isSortMenuOpen: false,
      isAdvancedFilterOpen: this.isAdvancedFilterOpen(search),
      user: search.get(USER_PARAM_NAME) || undefined,
      repo: search.get(REPO_PARAM_NAME) || undefined,
      branch: search.get(BRANCH_PARAM_NAME) || undefined,
      commit: search.get(COMMIT_PARAM_NAME) || undefined,
      host: search.get(HOST_PARAM_NAME) || undefined,
      command: search.get(COMMAND_PARAM_NAME) || undefined,
      pattern: (capabilities.config.patternFilterEnabled && search.get(PATTERN_PARAM_NAME)) || undefined,
      tag: (capabilities.config.tagsUiEnabled && search.get(TAG_PARAM_NAME)) || undefined,
      minimumDuration: Number(search.get(MINIMUM_DURATION_PARAM_NAME)) || undefined,
      maximumDuration: Number(search.get(MAXIMUM_DURATION_PARAM_NAME)) || undefined,
      sortBy: (search.get(SORT_BY_PARAM_NAME) as SortBy) || undefined,
      sortOrder: (search.get(SORT_ORDER_PARAM_NAME) as SortOrder) || undefined,
      genericFilterString: search.get(GENERIC_FILTER_PARAM_NAME) || undefined,
    };
  }

  updateFilterState(search: URLSearchParams) {
    return {
      isAdvancedFilterOpen: this.isAdvancedFilterOpen(search),
      user: search.get(USER_PARAM_NAME) || undefined,
      repo: search.get(REPO_PARAM_NAME) || undefined,
      branch: search.get(BRANCH_PARAM_NAME) || undefined,
      commit: search.get(COMMIT_PARAM_NAME) || undefined,
      host: search.get(HOST_PARAM_NAME) || undefined,
      command: search.get(COMMAND_PARAM_NAME) || undefined,
      pattern: (capabilities.config.patternFilterEnabled && search.get(PATTERN_PARAM_NAME)) || undefined,
      tag: (capabilities.config.tagsUiEnabled && search.get(TAG_PARAM_NAME)) || undefined,
      minimumDuration: Number(search.get(MINIMUM_DURATION_PARAM_NAME)) || undefined,
      maximumDuration: Number(search.get(MAXIMUM_DURATION_PARAM_NAME)) || undefined,
      sortBy: (search.get(SORT_BY_PARAM_NAME) as SortBy) || undefined,
      sortOrder: (search.get(SORT_ORDER_PARAM_NAME) as SortOrder) || undefined,
      genericFilterString: search.get(GENERIC_FILTER_PARAM_NAME) || undefined,
    };
  }

  private onOpenFilterMenu() {
    this.setState({ isFilterMenuOpen: true });
  }
  private onCloseFilterMenu() {
    this.setState({ isFilterMenuOpen: false });
  }
  private onClickClearFiltersAndSort() {
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
      [PATTERN_PARAM_NAME]: "",
      [TAG_PARAM_NAME]: "",
      [DIMENSION_PARAM_NAME]: "",
      [GENERIC_FILTER_PARAM_NAME]: "",
      [MINIMUM_DURATION_PARAM_NAME]: "",
      [MAXIMUM_DURATION_PARAM_NAME]: "",
      [SORT_BY_PARAM_NAME]: "",
      [SORT_ORDER_PARAM_NAME]: "",
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

  private onStatusToggle(status: invocation_status.OverallStatus, selected: Set<invocation_status.OverallStatus>) {
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
      [SORT_BY_PARAM_NAME]: sortBy === DEFAULT_SORT_BY_VALUE ? "" : sortBy,
    });
  }

  private onSortOrderChange(sortOrder: string) {
    router.setQuery({
      ...Object.fromEntries(this.props.search.entries()),
      [SORT_ORDER_PARAM_NAME]: sortOrder === DEFAULT_SORT_ORDER_VALUE ? "" : sortOrder,
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
    status: invocation_status.OverallStatus,
    selected: Set<invocation_status.OverallStatus>
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
      [USER_PARAM_NAME]: this.state.user || "",
      [REPO_PARAM_NAME]: this.state.repo || "",
      [BRANCH_PARAM_NAME]: this.state.branch || "",
      [COMMIT_PARAM_NAME]: this.state.commit || "",
      [HOST_PARAM_NAME]: this.state.host || "",
      [COMMAND_PARAM_NAME]: this.state.command || "",
      [PATTERN_PARAM_NAME]: this.state.pattern || "",
      [TAG_PARAM_NAME]: this.state.tag || "",
      [MINIMUM_DURATION_PARAM_NAME]: this.state.minimumDuration?.toString() || "",
      [MAXIMUM_DURATION_PARAM_NAME]: this.state.maximumDuration?.toString() || "",
      [GENERIC_FILTER_PARAM_NAME]: this.state.genericFilterString || "",
    });
  }

  private renderSortByRadio(label: string, sortBy: string, selected: string) {
    return (
      <label onClick={this.onSortByChange.bind(this, sortBy)}>
        <Radio checked={selected === sortBy} />
        <span>{label}</span>
      </label>
    );
  }

  private renderSortOrderRadio(label: string, sortOrder: string, selected: string) {
    return (
      <label onClick={this.onSortOrderChange.bind(this, sortOrder)}>
        <Radio checked={selected === sortOrder} />
        <span>{label}</span>
      </label>
    );
  }

  render() {
    const roleValue = this.props.search.get(ROLE_PARAM_NAME) || "";
    const statusValue = this.props.search.get(STATUS_PARAM_NAME) || "";
    const userValue = this.props.search.get(USER_PARAM_NAME) || "";
    const repoValue = this.props.search.get(REPO_PARAM_NAME) || "";
    const branchValue = this.props.search.get(BRANCH_PARAM_NAME) || "";
    const commitValue = this.props.search.get(COMMIT_PARAM_NAME) || "";
    const hostValue = this.props.search.get(HOST_PARAM_NAME) || "";
    const commandValue = this.props.search.get(COMMAND_PARAM_NAME) || "";
    const patternValue = (capabilities.config.patternFilterEnabled && this.props.search.get(PATTERN_PARAM_NAME)) || "";
    const tagValue = (capabilities.config.tagsUiEnabled && this.props.search.get(TAG_PARAM_NAME)) || "";
    const minimumDurationValue = this.props.search.get(MINIMUM_DURATION_PARAM_NAME) || "";
    const maximumDurationValue = this.props.search.get(MAXIMUM_DURATION_PARAM_NAME) || "";
    const isFiltering = isAnyNonDateFilterSet(this.props.search);
    const isSorting = Boolean(
      this.props.search.get(SORT_BY_PARAM_NAME) || this.props.search.get(SORT_ORDER_PARAM_NAME)
    );
    const dimensions = getFiltersFromDimensionParam(this.props.search.get(DIMENSION_PARAM_NAME) ?? "");
    const selectedRoles = new Set(parseRoleParam(roleValue));
    const selectedStatuses = new Set(parseStatusParam(statusValue));
    const genericFilterString = this.props.search.get(GENERIC_FILTER_PARAM_NAME) || "";

    const sortByValue: SortBy = (this.props.search.get(SORT_BY_PARAM_NAME) || DEFAULT_SORT_BY_VALUE) as SortBy;
    const sortOrderValue: SortOrder = (this.props.search.get(SORT_ORDER_PARAM_NAME) ||
      DEFAULT_SORT_ORDER_VALUE) as SortOrder;

    return (
      <div className={`global-filter ${isFiltering ? "is-filtering" : ""}`}>
        {(isFiltering || isSorting) && (
          <FilledButton className="square" title="Clear filters" onClick={this.onClickClearFiltersAndSort.bind(this)}>
            <X className="icon white" />
          </FilledButton>
        )}
        <div className="popup-wrapper">
          <OutlinedButton
            className={`filter-menu-button icon-text-button ${isFiltering ? "" : "square"}`}
            onClick={this.onOpenFilterMenu.bind(this)}>
            <Filter className="icon" />
            {selectedStatuses.has(invocation_status.OverallStatus.SUCCESS) && <span className="status-block success" />}
            {selectedStatuses.has(invocation_status.OverallStatus.FAILURE) && <span className="status-block failure" />}
            {selectedStatuses.has(invocation_status.OverallStatus.IN_PROGRESS) && (
              <span className="status-block in-progress" />
            )}
            {selectedStatuses.has(invocation_status.OverallStatus.DISCONNECTED) && (
              <span className="status-block disconnected" />
            )}
            {selectedRoles.has("") && <span className="role-badge DEFAULT">Default</span>}
            {selectedRoles.has("CI") && <span className="role-badge CI">CI</span>}
            {selectedRoles.has("CI_RUNNER") && <span className="role-badge CI_RUNNER">Workflow</span>}
            {selectedRoles.has("HOSTED_BAZEL") && <span className="role-badge HOSTED_BAZEL">Remote Bazel</span>}
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
            {capabilities.config.patternFilterEnabled && patternValue && (
              <span className="advanced-badge">
                <LayoutGrid /> {patternValue}
              </span>
            )}
            {capabilities.config.tagsUiEnabled && tagValue && (
              <span className="advanced-badge">
                <Tag /> {tagValue}
              </span>
            )}
            {(minimumDurationValue || maximumDurationValue) && (
              <span className="advanced-badge">
                <Clock /> {compactDurationSec(Number(minimumDurationValue))} -{" "}
                {compactDurationSec(Number(maximumDurationValue))}
              </span>
            )}
            {genericFilterString && (
              <span className="advanced-badge">
                <Sparkles /> {genericFilterString}
              </span>
            )}
            {dimensions.map(
              (v) =>
                v.dimension && (
                  <span className="advanced-badge" title={getDimensionName(v.dimension)}>
                    {getDimensionIcon(v.dimension)} {v.value}
                  </span>
                )
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
                  {this.renderRoleCheckbox("Remote Bazel", "HOSTED_BAZEL", selectedRoles)}
                </div>
              </div>
              <div className="option-group">
                <div className="option-group-title">Status</div>
                <div className="option-group-options">
                  {this.renderStatusCheckbox("Succeeded", invocation_status.OverallStatus.SUCCESS, selectedStatuses)}
                  {this.renderStatusCheckbox("Failed", invocation_status.OverallStatus.FAILURE, selectedStatuses)}
                  {this.renderStatusCheckbox(
                    "In progress",
                    invocation_status.OverallStatus.IN_PROGRESS,
                    selectedStatuses
                  )}
                  {this.renderStatusCheckbox(
                    "Disconnected",
                    invocation_status.OverallStatus.DISCONNECTED,
                    selectedStatuses
                  )}
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
                      placeholder={"e.g. github.com/buildbuddy-io/buildbuddy"}
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

                  {capabilities.config.patternFilterEnabled && (
                    <>
                      <div className="option-group-title">Pattern</div>
                      <div className="option-group-input">
                        <TextInput
                          placeholder={"e.g. //foo/..."}
                          value={this.state.pattern}
                          onChange={(e) => this.setState({ pattern: e.target.value })}
                        />
                      </div>
                    </>
                  )}
                  {capabilities.config.tagsUiEnabled && (
                    <>
                      <div className="option-group-title">Tag</div>
                      <div className="option-group-input">
                        <TextInput
                          placeholder={"e.g. coverage-build"}
                          value={this.state.tag}
                          onChange={(e) => this.setState({ tag: e.target.value })}
                        />
                      </div>
                    </>
                  )}
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
                  {genericFilterString && (
                    <>
                      <div className="option-group-title">Advanced</div>
                      <div className="option-group-input">
                        <TextInput
                          placeholder={"e.g., branch:main -command:test"}
                          value={this.state.genericFilterString}
                          onChange={(e) => this.setState({ genericFilterString: e.target.value })}
                        />
                      </div>
                    </>
                  )}
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
            className={`sort-button icon-text-button ${sortByValue !== DEFAULT_SORT_BY_VALUE ? "" : "square"}`}
            onClick={this.onOpenSortMenu.bind(this)}>
            {sortOrderValue === "asc" && <SortAsc className="icon" />}
            {sortOrderValue === "desc" && <SortDesc className="icon" />}
            {sortByValue !== DEFAULT_SORT_BY_VALUE && (
              <span>
                {sortByValue === "start-time" && "Start time"}
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
                  {this.renderSortByRadio("End time (Default)", "end-time", sortByValue)}
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
                  {this.renderSortOrderRadio("Descending (Default)", "desc", sortOrderValue)}
                </div>
              </div>
            </div>
          </Popup>
        </div>
        <DatePickerButton search={this.props.search}></DatePickerButton>
      </div>
    );
  }
}

function getDimensionIcon(f: stat_filter.Dimension) {
  if (f.execution) {
    switch (f.execution) {
      case stat_filter.ExecutionDimensionType.WORKER_EXECUTION_DIMENSION:
        return <Cloud />;
    }
  } else if (f.invocation) {
    switch (f.invocation) {
      case stat_filter.InvocationDimensionType.BRANCH_INVOCATION_DIMENSION:
        return <GitBranch />;
    }
  }
  return undefined;
}
