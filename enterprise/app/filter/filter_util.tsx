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
} from "../../../app/router/router";

export const DATE_PARAM_FORMAT = "YYYY-MM-DD";

export const DEFAULT_LAST_N_DAYS = 30;

export interface ProtoFilterParams {
  role?: string;
  updatedAfter?: google.protobuf.Timestamp;
  updatedBefore?: google.protobuf.Timestamp;
  status?: invocation.OverallStatus[];
}

export const LAST_N_DAYS_OPTIONS = [7, 30, 90, 180, 365];

export function getProtoFilterParams(search: URLSearchParams): ProtoFilterParams {
  const endDate = getEndDate(search);
  return {
    role: search.get(ROLE_PARAM_NAME),
    updatedAfter: proto.dateToTimestamp(getStartDate(search)),
    updatedBefore: endDate ? proto.dateToTimestamp(endDate) : undefined,
    status: parseStatusParam(search.get(STATUS_PARAM_NAME)),
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
