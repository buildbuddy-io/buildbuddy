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

class BiMap {
  forwardMap: Map<number, string>;
  reverseMap: Map<string, number>;
  constructor(map: Map<number, string>) {
     this.forwardMap = new Map<number, string>();
     this.reverseMap = new Map<string, number>();
     map.forEach((value: string, key: number) => {
      this.forwardMap[key] = value
      this.reverseMap[value] = key
  });
  }
  get(key: number): string { return this.forwardMap[key]; }
  revGet(key: string): number { return this.reverseMap[key]; }
}

export class DurationSlider {
  // Map of linear slider values (1, 2, 3, ...) to sort-of logarithmic scale duration display values.
  static values = new BiMap(new Map([
    [0, '0s'],
    [1, '1s'],
    [2, '2s'],
    [3, '3s'],
    [4, '4s'],
    [5, '5s'],
    [6, '10s'],
    [7, '15s'],
    [8, '20s'],
    [9, '25s'],
    [10, '30s'],
    [11, '40s'],
    [12, '45s'],
    [13, '50s'],
    [14, '1m'],
    [15, '2m'],
    [16, '3m'],
    [17, '4m'],
    [18, '5m'],
    [19, '10m'],
    [20, '15m'],
    [21, '20m'],
    [22, '25m'],
    [23, '30m'],
    [24, '40m'],
    [25, '50m'],
    [26, '1h'],
    [27, '2h'],
    [28, '3h'],
    [29, '4h'],
    [30, '5h'],
    [31, '6h'],
    [32, '7h'],
    [33, '8h'],
    [34, '9h'],
    [35, '10h'],
    [36, '11h'],
    [37, '12h'],
  ]))

  static minValue(): number {
    return 0
  }

  static minDisplayValue(): string {
    return this.toDisplayValue(this.minValue())
  }

  static maxValue(): number {
    return 37
  }

  static maxDisplayValue(): string {
    return this.toDisplayValue(this.maxValue())
  }

  static toDisplayValue(value: number): string {
    return this.values.get(value)
  }

  static fromDisplayValue(value: string): number {
    return this.values.revGet(value)
  }

  static toMillis(value: string): number {
    if (value.charAt(value.length-1) == 's') {
      return Number(value.replace('s', '')) * 1000
    }
    if (value.charAt(value.length-1) == 'm') {
      return Number(value.replace('m', '')) * 60 * 1000
    }
    if (value.charAt(value.length-1) == 'h') {
      return Number(value.replace('h', '')) * 60 * 60 * 1000
    }
    return -1
  }
}