import * as proto from "../../../app/util/proto";
import { google } from "../../../proto/timestamp_ts_proto";
import { invocation } from "../../../proto/invocation_ts_proto";
import moment from "moment";

export const ROLE_PARAM_NAME = "role";
export const STATUS_PARAM_NAME = "status";
export const START_DATE_PARAM_NAME = "start";
export const END_DATE_PARAM_NAME = "end";

export interface ProtoFilterParams {
  role?: string;
  updatedAfter?: google.protobuf.Timestamp;
  updatedBefore?: google.protobuf.Timestamp;
  status?: invocation.OverallStatus[];
}

export function getProtoFilterParams(search: URLSearchParams): ProtoFilterParams {
  return {
    role: search.get(ROLE_PARAM_NAME),
    updatedAfter: parseStartOfDay(search.get(START_DATE_PARAM_NAME)),
    updatedBefore: parseStartOfDay(search.get(END_DATE_PARAM_NAME), /*offsetDays=*/ +1),
    status: parseStatusParam(search.get(STATUS_PARAM_NAME)),
  };
}

export const ALL_STATUSES = [
  invocation.OverallStatus.SUCCESS,
  invocation.OverallStatus.FAILURE,
  invocation.OverallStatus.IN_PROGRESS,
  invocation.OverallStatus.DISCONNECTED,
];

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

function parseStartOfDay(value: string, offsetDays = 0): google.protobuf.Timestamp {
  if (!value) return undefined;

  return proto.dateToTimestamp(moment(value).add(offsetDays, "days").toDate());
}
