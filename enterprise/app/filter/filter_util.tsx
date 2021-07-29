import * as proto from "../../../app/util/proto";
import { google } from "../../../proto/timestamp_ts_proto";
import moment from "moment";
import { START_DATE_PARAM_NAME, END_DATE_PARAM_NAME } from "./filter";

export interface ProtoFilterParams {
  updatedAfter?: google.protobuf.Timestamp;
  updatedBefore?: google.protobuf.Timestamp;
}

export function getProtoFilterParams(search: URLSearchParams): ProtoFilterParams {
  return {
    updatedAfter: parseStartOfDay(search.get(START_DATE_PARAM_NAME)),
    updatedBefore: parseStartOfDay(search.get(END_DATE_PARAM_NAME), /*offsetDays=*/ +1),
  };
}

function parseStartOfDay(value: string, offsetDays = 0): google.protobuf.Timestamp {
  if (!value) return undefined;

  return proto.dateToTimestamp(moment(value).add(offsetDays, "days").toDate());
}
