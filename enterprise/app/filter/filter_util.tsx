import capabilities from "../../../app/capabilities/capabilities";
import { differenceInCalendarDays, formatDateRange, formatPreviousDateRange } from "../../../app/format/format";
import * as proto from "../../../app/util/proto";
import { google as google_duration } from "../../../proto/duration_ts_proto";
import { google as google_timestamp } from "../../../proto/timestamp_ts_proto";
import { invocation_status } from "../../../proto/invocation_status_ts_proto";
import { stat_filter } from "../../../proto/stat_filter_ts_proto";
import moment from "moment";
import {
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

// URL param value representing the empty role (""), which is the default.
const DEFAULT_ROLE_PARAM_VALUE = "DEFAULT";

export const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export const DEFAULT_LAST_N_DAYS = 30;

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
  };
}

export function getDefaultStartDate(now?: moment.Moment): Date {
  return (now ? moment(now) : moment()).add(-DEFAULT_LAST_N_DAYS + 1, "days").toDate();
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
    return (now ? moment(now) : moment()).add(-Number(search.get(LAST_N_DAYS_PARAM_NAME)) + 1, "days").toDate();
  }
  return getDefaultStartDate(now);
}

export function getDisplayDateRange(search: URLSearchParams): { startDate: Date; endDate: Date } {
  // Not using `getEndDate` here because it's set to "start of day after the one specified
  // in the URL" which causes an off-by-one error if we were to render that directly in
  // the calendar.
  let endDate = new Date();
  const dateString = search.get(END_DATE_PARAM_NAME);
  if (dateString) {
    const dateNumber = Number(dateString);
    if (Number.isInteger(dateNumber)) {
      endDate = new Date(dateNumber);
    } else {
      endDate = moment(dateString).toDate();
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
  return (invocation_status.OverallStatus[
    value.toUpperCase().replace(/-/g, "_") as any
  ] as unknown) as invocation_status.OverallStatus;
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

export function formatPreviousDateRangeFromSearchParams(search: URLSearchParams): string {
  const { startDate, endDate } = getDisplayDateRange(search);
  return formatPreviousDateRange(startDate, endDate);
}

export function getDayCountStringFromSearchParams(search: URLSearchParams): string {
  const { startDate, endDate } = getDisplayDateRange(search);
  const diff = differenceInCalendarDays(startDate, endDate) + 1;
  return diff == 1 ? "1 day" : `${diff} days`;
}

export function formatDateRangeFromSearchParams(search: URLSearchParams): string {
  const { startDate, endDate } = getDisplayDateRange(search);
  return formatDateRange(startDate, endDate);
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
      search.get(MAXIMUM_DURATION_PARAM_NAME)
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
