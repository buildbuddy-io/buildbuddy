import capabilities from "../../../app/capabilities/capabilities";
import { differenceInCalendarDays, durationMillis, formatDateRange } from "../../../app/format/format";
import * as proto from "../../../app/util/proto";
import { google as google_duration } from "../../../proto/duration_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import moment from "moment";
import {
  DIMENSION_PARAM_NAME,
  GENERIC_FILTER_PARAM_NAME,
  ROLE_PARAM_NAME,
  START_DATE_PARAM_NAME,
  END_DATE_PARAM_NAME,
  STATUS_PARAM_NAME,
  LAST_N_DAYS_PARAM_NAME,
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
} from "../../../app/router/router_params";
import Long from "long";

// URL param value representing the empty role (""), which is the default.
const DEFAULT_ROLE_PARAM_VALUE = "DEFAULT";

export const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export const DEFAULT_LAST_N_DAYS = 7;

export type SortBy =
  | "start-time"
  | "end-time"
  | "duration"
  | "ac-hit-ratio"
  | "cas-hit-ratio"
  | "cache-down"
  | "cache-up"
  | "cache-xfer";
export type SortOrder = "asc" | "desc";

export interface ProtoFilterParams {
  role?: string[];
  status?: invocation_status.OverallStatus[];
  updatedAfter?: google_timestamp.protobuf.Timestamp;
  updatedBefore?: google_timestamp.protobuf.Timestamp;

  user?: string;
  repo?: string;
  branch?: string;
  commit?: string;
  host?: string;
  command?: string;
  pattern?: string;
  tags?: string[];
  minimumDuration?: google_duration.protobuf.Duration;
  maximumDuration?: google_duration.protobuf.Duration;
  dimensionFilters?: stat_filter.DimensionFilter[];
  genericFilters?: stat_filter.GenericFilter[];

  sortBy?: SortBy;
  sortOrder?: SortOrder;
}

export const LAST_N_DAYS_OPTIONS = [7, 30, 90, 180, 365];

function splitAndTrimTags(param: string | null): string[] {
  if (!param) {
    return [];
  }
  return param
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s);
}

function parseDimensionType(stringValue: string): stat_filter.Dimension | undefined {
  if (stringValue.length < 2) {
    return undefined;
  }
  const type = stringValue[0];
  const enumValue = Number.parseInt(stringValue.substring(1));
  if (!Number.isInteger(enumValue) || enumValue === 0) {
    return undefined;
  }
  if (type == "e" && Object.values(stat_filter.ExecutionDimensionType).find((v) => v === enumValue)) {
    return new stat_filter.Dimension({
      execution: enumValue,
    });
  } else if (type == "i" && Object.values(stat_filter.InvocationDimensionType).find((v) => v === enumValue)) {
    return new stat_filter.Dimension({
      invocation: enumValue,
    });
  }
  return undefined;
}

export function getDimensionParamFromFilters(filters: stat_filter.DimensionFilter[]): string {
  const filterStrings = filters
    .map((f) => {
      if (!(f.dimension?.execution || f.dimension?.invocation) || !f.value) {
        return undefined;
      }
      let dimensionName = "";
      if (f.dimension.execution) {
        dimensionName = "e" + f.dimension.execution.toString();
      } else {
        dimensionName = "i" + f.dimension.invocation!.toString();
      }
      return dimensionName + "|" + f.value.length + "|" + f.value;
    })
    .filter((f) => f !== undefined);

  return filterStrings.join("|");
}

// Parses a set of DimensionFilters from the supplied URL param string.  Each
// entry is formatted as "dimension|value_length|value" and entry is separated
// from the next by another pipe.
// dimension: [ei][0-9]+ identifies either the [e]xecution or [i]nvocation
// field of the Dimension field in the DimensionFilter.
// value_length: the length of the value field because (rather than escaping)
// value: the actual string value to match for the dimension.
// So, for example, e1|5|abcdef|i1|4|main represents:
//     * ExecutionDimensionType.WORKER_EXECUTION_DIMENSION == abcdef
//     * InvocationDimensionType.BRANCH_INVOCATION_DIMENSION == main
export function getFiltersFromDimensionParam(dimensionParamValue: string): stat_filter.DimensionFilter[] {
  const filters: stat_filter.DimensionFilter[] = [];
  while (dimensionParamValue.length > 0) {
    let separatorIndex = dimensionParamValue.indexOf("|");
    if (separatorIndex == -1 || separatorIndex + 1 === dimensionParamValue.length) {
      break;
    }
    const dimension = parseDimensionType(dimensionParamValue.substring(0, separatorIndex));
    if (!dimension) {
      // The param is malformed or we don't recognize the param ID. Give up.
      break;
    }
    dimensionParamValue = dimensionParamValue.substring(separatorIndex + 1);
    separatorIndex = dimensionParamValue.indexOf("|");
    if (separatorIndex == -1 || separatorIndex + 1 === dimensionParamValue.length) {
      break;
    }

    const dimensionValueLength = Number.parseInt(dimensionParamValue.substring(0, separatorIndex));
    dimensionParamValue = dimensionParamValue.substring(separatorIndex + 1);

    if (!Number.isInteger(dimensionValueLength) || dimensionParamValue.length < dimensionValueLength) {
      break;
    }

    const value = dimensionParamValue.substring(0, dimensionValueLength);
    filters.push(new stat_filter.DimensionFilter({ dimension, value }));

    dimensionParamValue = dimensionParamValue.substring(dimensionValueLength + 1);
  }
  return filters;
}

const PARAM_NAME_REGEX = /^[a-zA-Z_]+\s*[:<>]/;

// TODO(jdhollen): reflect or use a genrule to create these lists.
const INT_TYPES: stat_filter.FilterType[] = [
  stat_filter.FilterType.INVOCATION_DURATION_USEC_FILTER_TYPE,
  stat_filter.FilterType.INVOCATION_CREATED_AT_USEC_FILTER_TYPE,
  stat_filter.FilterType.INVOCATION_UPDATED_AT_USEC_FILTER_TYPE,
  stat_filter.FilterType.EXECUTION_CREATED_AT_USEC_FILTER_TYPE,
  stat_filter.FilterType.EXECUTION_UPDATED_AT_USEC_FILTER_TYPE,
];

const STRING_TYPES: stat_filter.FilterType[] = [
  stat_filter.FilterType.REPO_URL_FILTER_TYPE,
  stat_filter.FilterType.USER_FILTER_TYPE,
  stat_filter.FilterType.COMMAND_FILTER_TYPE,
  stat_filter.FilterType.PATTERN_FILTER_TYPE,
  stat_filter.FilterType.HOST_FILTER_TYPE,
  stat_filter.FilterType.COMMIT_SHA_FILTER_TYPE,
  stat_filter.FilterType.BRANCH_FILTER_TYPE,
  stat_filter.FilterType.WORKER_FILTER_TYPE,
  stat_filter.FilterType.ROLE_FILTER_TYPE,
];

const STRING_ARRAY_TYPES: stat_filter.FilterType[] = [stat_filter.FilterType.TAG_FILTER_TYPE];

function getType(stringRep: string): stat_filter.FilterType | undefined {
  switch (stringRep) {
    case "status":
      return stat_filter.FilterType.INVOCATION_STATUS_FILTER_TYPE;
    case "repo":
      return stat_filter.FilterType.REPO_URL_FILTER_TYPE;
    case "user":
      return stat_filter.FilterType.USER_FILTER_TYPE;
    case "command":
      return stat_filter.FilterType.COMMAND_FILTER_TYPE;
    case "pattern":
      return stat_filter.FilterType.PATTERN_FILTER_TYPE;
    case "host":
      return stat_filter.FilterType.HOST_FILTER_TYPE;
    case "commit":
      return stat_filter.FilterType.COMMIT_SHA_FILTER_TYPE;
    case "branch":
      return stat_filter.FilterType.BRANCH_FILTER_TYPE;
    case "executor":
      return stat_filter.FilterType.WORKER_FILTER_TYPE;
    case "role":
      return stat_filter.FilterType.ROLE_FILTER_TYPE;
    case "inv_duration":
      return stat_filter.FilterType.INVOCATION_DURATION_USEC_FILTER_TYPE;
    // TODO(jdhollen): should invocation and execution be treated as
    // separate? seems like yes, since we could filter executions by
    // invocation duration, etc., but probably requires more thought.
    // TODO(jdhollen): support date strings for these.
    case "inv_created":
      return stat_filter.FilterType.INVOCATION_CREATED_AT_USEC_FILTER_TYPE;
    case "inv_updated":
      return stat_filter.FilterType.INVOCATION_UPDATED_AT_USEC_FILTER_TYPE;
    case "exec_created":
      return stat_filter.FilterType.EXECUTION_CREATED_AT_USEC_FILTER_TYPE;
    case "exec_updated":
      return stat_filter.FilterType.EXECUTION_UPDATED_AT_USEC_FILTER_TYPE;
    case "tag":
      return stat_filter.FilterType.TAG_FILTER_TYPE;
  }
  return undefined;
}

function userInputToOverallStatus(input: string): invocation_status.OverallStatus | undefined {
  // Convert to UPPER_CASE so that we can match by enum name.
  input = input.toUpperCase().replace(/-/g, "_");
  const entry = Object.entries(invocation_status.OverallStatus).find((e) => e[0] === input);
  if (!entry) {
    return undefined;
  }
  const numericValue = Number.parseInt(input);
  // If the input value is a number, then it must be an OverallStatus.
  if (Number.isInteger(numericValue)) {
    return numericValue || undefined;
  }
  // If the input value is a string, then the value of the entry is the enum itself.
  return (entry[1] as invocation_status.OverallStatus) || undefined;
}

function getOperand(
  filterType: stat_filter.FilterType | undefined,
  stringRep: string
): stat_filter.FilterOperand | undefined {
  if (stringRep === ">") {
    return stat_filter.FilterOperand.GREATER_THAN_OPERAND;
  } else if (stringRep === "<") {
    return stat_filter.FilterOperand.LESS_THAN_OPERAND;
  } else if (stringRep === ":" || stringRep === "=") {
    return filterType && STRING_ARRAY_TYPES.includes(filterType)
      ? stat_filter.FilterOperand.ARRAY_CONTAINS_OPERAND
      : stat_filter.FilterOperand.IN_OPERAND;
  }
  return undefined;
}

const QUOTED_STRING_MATCHER = /^"(?:[^"\\]|\\.)*"/;
const PLAIN_ARG_MATCHER = /^[^\s]+/;

function getValues(
  type: stat_filter.FilterType | undefined,
  stringRep: string
): [stat_filter.FilterValue | undefined, string] {
  let values: string[] = [];
  let remainder = "";
  if (stringRep.startsWith("(")) {
    // TODO(jdhollen): support unquoted values here, too.
    stringRep = stringRep.substring(1).trimStart();
    while (stringRep.length > 0) {
      const v = stringRep.match(QUOTED_STRING_MATCHER);
      if (v && v.length > 0) {
        values.push(v[0].substring(1, v[0].length - 1));
        stringRep = stringRep.substring(v[0].length);
      } else {
        return [undefined, ""];
      }

      stringRep = stringRep.trimStart();
      if (stringRep.length === 0) {
        // Unexpected end of list.
        return [undefined, ""];
      }
      if (stringRep[0] === ")") {
        stringRep = stringRep.substring(1);
        break;
      } else if (stringRep[0] === ",") {
        stringRep = stringRep.substring(1).trimStart();
      } else {
        return [undefined, ""];
      }
    }
    remainder = stringRep;
  } else if (stringRep.startsWith('"')) {
    const v = stringRep.match(QUOTED_STRING_MATCHER);
    if (v && v.length > 0) {
      values = [v[0].substring(1, v[0].length - 1)];
      remainder = stringRep.substring(v[0].length);
    } else {
      return [undefined, ""];
    }
  } else {
    // just a word, match up to next whitespace.
    const v = stringRep.match(PLAIN_ARG_MATCHER);
    if (v && v.length > 0) {
      values = [v[0]];
      remainder = stringRep.substring(v[0].length);
    } else {
      return [undefined, ""];
    }
  }

  if (type && INT_TYPES.indexOf(type) >= 0) {
    // TODO(jdhollen): decide on proper behavior for non-int values here,
    // currently just dropping them.
    const fvs = values
      .map((v) => Number.parseInt(v))
      .filter(Number.isInteger)
      .map((value) => Long.fromValue(value));
    return [new stat_filter.FilterValue({ intValue: fvs }), remainder];
  } else if (type === stat_filter.FilterType.INVOCATION_STATUS_FILTER_TYPE /* STATUS_FILTER_CATEGORY */) {
    // Casting because typescript doesn't understand the filter() call.
    const fvs = values
      .map(userInputToOverallStatus)
      .filter((v) => v !== undefined) as invocation_status.OverallStatus[];
    if (fvs.length < 1) {
      return [undefined, ""];
    }
    return [new stat_filter.FilterValue({ statusValue: fvs }), remainder];
  } else {
    return [new stat_filter.FilterValue({ stringValue: values }), remainder];
  }
}

export function getFiltersFromGenericFilterParam(userQuery: string): stat_filter.GenericFilter[] {
  const out: stat_filter.GenericFilter[] = [];
  while (userQuery.length > 0) {
    userQuery = userQuery.trimStart();
    const negate = userQuery.startsWith("-");
    if (negate) {
      userQuery = userQuery.substring(1);
    }

    const nextParam = userQuery.match(PARAM_NAME_REGEX) ?? [];
    let value: stat_filter.FilterValue | undefined;
    let type: stat_filter.FilterType | undefined;
    let operand: stat_filter.FilterOperand | undefined;
    if (nextParam.length > 0 && nextParam[0]) {
      type = getType(nextParam[0].substring(0, nextParam[0].length - 1).trimEnd());
      operand = getOperand(type, nextParam[0][nextParam[0].length - 1]);
      userQuery = userQuery.substring(nextParam[0].length).trimStart();
    } else {
      type = stat_filter.FilterType.TEXT_MATCH_FILTER_TYPE;
      operand = stat_filter.FilterOperand.TEXT_MATCH_OPERAND;
    }

    [value, userQuery] = getValues(type, userQuery);
    if (!type || !operand || !value) {
      continue;
    }
    out.push(new stat_filter.GenericFilter({ type, operand, value, negate }));
  }

  return out;
}

export function getProtoFilterParams(search: URLSearchParams, now?: moment.Moment): ProtoFilterParams {
  const endDate = getEndDate(search);
  return {
    role: parseRoleParam(search.get(ROLE_PARAM_NAME)),
    status: parseStatusParam(search.get(STATUS_PARAM_NAME)),
    updatedAfter: proto.dateToTimestamp(getStartDate(search, now)),
    updatedBefore: endDate ? proto.dateToTimestamp(endDate) : undefined,

    user: search.get(USER_PARAM_NAME) || undefined,
    repo: search.get(REPO_PARAM_NAME) || undefined,
    branch: search.get(BRANCH_PARAM_NAME) || undefined,
    commit: search.get(COMMIT_PARAM_NAME) || undefined,
    host: search.get(HOST_PARAM_NAME) || undefined,
    command: search.get(COMMAND_PARAM_NAME) || undefined,
    pattern: (capabilities.config.patternFilterEnabled && search.get(PATTERN_PARAM_NAME)) || undefined,
    tags: (capabilities.config.tagsUiEnabled && splitAndTrimTags(search.get(TAG_PARAM_NAME))) || undefined,
    minimumDuration: parseDuration(search.get(MINIMUM_DURATION_PARAM_NAME)),
    maximumDuration: parseDuration(search.get(MAXIMUM_DURATION_PARAM_NAME)),

    sortBy: search.get(SORT_BY_PARAM_NAME) as SortBy,
    sortOrder: search.get(SORT_ORDER_PARAM_NAME) as SortOrder,
    dimensionFilters: getFiltersFromDimensionParam(search.get(DIMENSION_PARAM_NAME) ?? ""),
    genericFilters: getFiltersFromGenericFilterParam(search.get(GENERIC_FILTER_PARAM_NAME) ?? ""),
  };
}

export function getDimensionName(d: stat_filter.Dimension): string {
  if (d.execution) {
    switch (d.execution) {
      case stat_filter.ExecutionDimensionType.WORKER_EXECUTION_DIMENSION:
        return "Worker";
      case stat_filter.ExecutionDimensionType.TARGET_LABEL_EXECUTION_DIMENSION:
        return "Target";
      case stat_filter.ExecutionDimensionType.ACTION_MNEMONIC_EXECUTION_DIMENSION:
        return "Mnemonic";
    }
  } else if (d.invocation) {
    switch (d.invocation) {
      case stat_filter.InvocationDimensionType.BRANCH_INVOCATION_DIMENSION:
        return "Branch";
    }
  }
  return "";
}

export function getDefaultStartDate(now?: moment.Moment): Date {
  return (now ? moment(now) : moment())
    .add(-DEFAULT_LAST_N_DAYS + 1, "days")
    .startOf("day")
    .toDate();
}

export function getStartDate(search: URLSearchParams, now?: moment.Moment): Date {
  const dateString = search.get(START_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      return new Date(dateNumber);
    }
    return moment(dateString).toDate();
  }
  if (search.get(LAST_N_DAYS_PARAM_NAME)) {
    return (now ? moment(now) : moment())
      .add(-Number(search.get(LAST_N_DAYS_PARAM_NAME)) + 1, "days")
      .startOf("day")
      .toDate();
  }
  return getDefaultStartDate(now);
}

export function getDateRangeForPicker(search: URLSearchParams): { startDate: Date; endDate?: Date } {
  // Not using `getEndDate` here because it's set to "start of day after the one specified
  // in the URL" which causes an off-by-one error if we were to render that directly in
  // the calendar.
  let endDate = undefined;
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      endDate = moment
        .unix(dateNumber / 1000)
        .startOf("day")
        .toDate();
    } else {
      endDate = moment(dateString).toDate();
    }
  }
  return { startDate: getStartDate(search), endDate };
}

function getDateRangeForStringFromUrlParams(search: URLSearchParams): { startDate: Date; endDate?: Date } {
  // Not using `getEndDate` here because it's set to "start of day after the one specified
  // in the URL" which causes an off-by-one error if we were to render that directly in
  // the calendar.
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
  return { startDate: getStartDate(search), endDate };
}

export function getEndDate(search: URLSearchParams): Date | undefined {
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (!dateString) {
    return undefined;
  }
  const dateNumber = Number(dateString);
  if (Number.isInteger(dateNumber)) {
    return new Date(dateNumber);
  }
  return moment(search.get(END_DATE_PARAM_NAME)).add(1, "days").toDate();
}

const STATUS_TO_STRING = Object.fromEntries(
  Object.entries(invocation_status.OverallStatus).map(([k, v]) => [v, k.toLowerCase().replace(/_/g, "-")])
);

export function statusToString(status: invocation_status.OverallStatus) {
  return STATUS_TO_STRING[status];
}

export function statusFromString(value: string) {
  return invocation_status.OverallStatus[
    value.toUpperCase().replace(/-/g, "_") as any
  ] as unknown as invocation_status.OverallStatus;
}

export function parseRoleParam(paramValue: string | null): string[] {
  if (!paramValue) return [];
  return paramValue.split(" ").map((role) => (role === DEFAULT_ROLE_PARAM_VALUE ? "" : role));
}

export function toRoleParam(roles: Iterable<string>): string {
  return [...roles]
    .map((role) => (role === "" ? DEFAULT_ROLE_PARAM_VALUE : role))
    .sort()
    .join(" ");
}

export function parseStatusParam(paramValue: string | null): invocation_status.OverallStatus[] {
  if (!paramValue) return [];
  return paramValue.split(" ").map((name) => statusFromString(name));
}

export function toStatusParam(statuses: Iterable<invocation_status.OverallStatus>): string {
  return [...statuses]
    .map(statusToString)
    .sort((a, b) => statusFromString(a) - statusFromString(b))
    .join(" ");
}

export function isExecutionMetric(m: stat_filter.Metric): boolean {
  return m.execution !== null && m.execution !== undefined;
}

export function formatDateRangeDurationFromSearchParams(search: URLSearchParams): string {
  const startDate = getStartDate(search);
  const endDate = getEndDate(search) ?? moment(new Date()).add(1, "days").startOf("day").toDate();
  const deltaMillis = endDate.getTime() - startDate.getTime();

  if (
    startDate.getMinutes() != 0 ||
    endDate?.getMinutes() != 0 ||
    startDate.getHours() != 0 ||
    endDate?.getHours() != 0
  ) {
    return durationMillis(deltaMillis);
  }

  const diff = differenceInCalendarDays(startDate, endDate ?? new Date());
  return diff == 1 ? "day" : `${diff} days`;
}

export function formatDateRangeFromUrlParams(search: URLSearchParams): string {
  const { startDate, endDate } = getDateRangeForStringFromUrlParams(search);
  return formatDateRange(startDate, endDate);
}

export function isAnyDimensionFilterSet(param: string): boolean {
  return getFiltersFromDimensionParam(param).length > 0;
}

export function isAnyGenericFilterSet(param: string): boolean {
  return getFiltersFromGenericFilterParam(param).length > 0;
}

export function isAnyNonDateFilterSet(search: URLSearchParams): boolean {
  return Boolean(
    search.get(ROLE_PARAM_NAME) ||
      search.get(STATUS_PARAM_NAME) ||
      search.get(USER_PARAM_NAME) ||
      search.get(REPO_PARAM_NAME) ||
      search.get(BRANCH_PARAM_NAME) ||
      search.get(COMMIT_PARAM_NAME) ||
      search.get(HOST_PARAM_NAME) ||
      search.get(COMMAND_PARAM_NAME) ||
      search.get(PATTERN_PARAM_NAME) ||
      (capabilities.config.tagsUiEnabled && search.get(TAG_PARAM_NAME)) ||
      search.get(MINIMUM_DURATION_PARAM_NAME) ||
      search.get(MAXIMUM_DURATION_PARAM_NAME) ||
      isAnyDimensionFilterSet(search.get(DIMENSION_PARAM_NAME) ?? "") ||
      isAnyGenericFilterSet(search.get(GENERIC_FILTER_PARAM_NAME) ?? "")
  );
}

function parseDuration(value: string | null): google_duration.protobuf.Duration | undefined {
  if (!value) {
    return undefined;
  }
  return google_duration.protobuf.Duration.create(proto.secondsToDuration(Number(value)));
}

/** Duration slider values, in seconds. **/
export const DURATION_SLIDER_VALUES = [
  0,
  1,
  2,
  3,
  4,
  5,
  10,
  15,
  20,
  25,
  30,
  40,
  50,
  60,
  2 * 60,
  3 * 60,
  4 * 60,
  5 * 60,
  10 * 60,
  15 * 60,
  20 * 60,
  25 * 60,
  30 * 60,
  40 * 60,
  50 * 60,
  60 * 60,
  2 * 60 * 60,
  3 * 60 * 60,
  4 * 60 * 60,
  5 * 60 * 60,
  6 * 60 * 60,
  7 * 60 * 60,
  8 * 60 * 60,
  9 * 60 * 60,
  10 * 60 * 60,
  11 * 60 * 60,
  12 * 60 * 60,
  24 * 60 * 60,
];
export const DURATION_SLIDER_MIN_INDEX = 0;
export const DURATION_SLIDER_MIN_VALUE = DURATION_SLIDER_VALUES[DURATION_SLIDER_MIN_INDEX];
export const DURATION_SLIDER_MAX_INDEX = DURATION_SLIDER_VALUES.length - 1;
export const DURATION_SLIDER_MAX_VALUE = DURATION_SLIDER_VALUES[DURATION_SLIDER_MAX_INDEX];
