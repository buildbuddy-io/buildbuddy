import * as proto from "../../../app/util/proto";
import { google } from "../../../proto/timestamp_ts_proto";
import { invocation } from "../../../proto/invocation_ts_proto";
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
  MINIMUM_DURATION_PARAM_NAME,
  MAXIMUM_DURATION_PARAM_NAME,
  SORT_BY_PARAM_NAME,
  SORT_ORDER_PARAM_NAME,
} from "../../../app/router/router";

// URL param value representing the empty role (""), which is the default.
const DEFAULT_ROLE_PARAM_VALUE = "DEFAULT";

export const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export const DEFAULT_LAST_N_DAYS = 30;

export type SortBy =
  | ""
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
  status?: invocation.OverallStatus[];
  updatedAfter?: google.protobuf.Timestamp;
  updatedBefore?: google.protobuf.Timestamp;

  user: string;
  repo: string;
  branch: string;
  commit: string;
  host: string;
  command: string;
  minimumDuration?: string;
  maximumDuration?: string;

  sortBy?: SortBy;
  sortOrder?: SortOrder;
}

export const LAST_N_DAYS_OPTIONS = [7, 30, 90, 180, 365];

export function getProtoFilterParams(search: URLSearchParams): ProtoFilterParams {
  const endDate = getEndDate(search);
  return {
    role: parseRoleParam(search.get(ROLE_PARAM_NAME)),
    status: parseStatusParam(search.get(STATUS_PARAM_NAME)),
    updatedAfter: proto.dateToTimestamp(getStartDate(search)),
    updatedBefore: endDate ? proto.dateToTimestamp(endDate) : undefined,

    user: search.get(USER_PARAM_NAME),
    repo: search.get(REPO_PARAM_NAME),
    branch: search.get(BRANCH_PARAM_NAME),
    commit: search.get(COMMIT_PARAM_NAME),
    host: search.get(HOST_PARAM_NAME),
    command: search.get(COMMAND_PARAM_NAME),
    minimumDuration: search.get(MINIMUM_DURATION_PARAM_NAME),
    maximumDuration: search.get(MAXIMUM_DURATION_PARAM_NAME),

    sortBy: search.get(SORT_BY_PARAM_NAME) as SortBy,
    sortOrder: search.get(SORT_ORDER_PARAM_NAME) as SortOrder,
  };
}

export function getDefaultStartDate(): Date {
  return moment()
    .add(-DEFAULT_LAST_N_DAYS + 1, "days")
    .toDate();
}

export function getStartDate(search: URLSearchParams): Date {
  if (search.get(START_DATE_PARAM_NAME)) {
    return moment(search.get(START_DATE_PARAM_NAME)).toDate();
  }
  if (search.get(LAST_N_DAYS_PARAM_NAME)) {
    return moment()
      .add(-Number(search.get(LAST_N_DAYS_PARAM_NAME)) + 1, "days")
      .toDate();
  }
  return getDefaultStartDate();
}

export function getEndDate(search: URLSearchParams): Date {
  if (!search.get(END_DATE_PARAM_NAME)) {
    return undefined;
  }
  return moment(search.get(END_DATE_PARAM_NAME)).add(1, "days").toDate();
}

const STATUS_TO_STRING = Object.fromEntries(
  Object.entries(invocation.OverallStatus).map(([k, v]) => [v, k.toLowerCase().replace(/_/g, "-")])
);

export function statusToString(status: invocation.OverallStatus) {
  return STATUS_TO_STRING[status];
}

export function statusFromString(value: string) {
  return (invocation.OverallStatus[
    value.toUpperCase().replace(/-/g, "_") as any
  ] as unknown) as invocation.OverallStatus;
}

export function parseRoleParam(paramValue?: string): string[] {
  if (!paramValue) return [];
  return paramValue.split(" ").map((role) => (role === DEFAULT_ROLE_PARAM_VALUE ? "" : role));
}

export function toRoleParam(roles: Iterable<string>): string {
  return [...roles]
    .map((role) => (role === "" ? DEFAULT_ROLE_PARAM_VALUE : role))
    .sort()
    .join(" ");
}

export function parseStatusParam(paramValue?: string): invocation.OverallStatus[] {
  if (!paramValue) return [];
  return paramValue.split(" ").map((name) => statusFromString(name));
}

export function toStatusParam(statuses: Iterable<invocation.OverallStatus>): string {
  return [...statuses]
    .map(statusToString)
    .sort((a, b) => statusFromString(a) - statusFromString(b))
    .join(" ");
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
