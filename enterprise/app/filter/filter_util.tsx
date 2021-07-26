import * as proto from "../../../app/util/proto";
import { google } from "../../../proto/timestamp_ts_proto";
import { START_DATE_PARAM_NAME, END_DATE_PARAM_NAME } from "./filter";
import moment from "moment";
import { startOfDay, addDays } from "date-fns";

type ProtoFilterParams = {
  startTimestamp: google.protobuf.Timestamp;
  endTimestamp: google.protobuf.Timestamp;
};

export function getProtoFilterParams(search: URLSearchParams): ProtoFilterParams {
  let startTimestamp: google.protobuf.Timestamp;
  const startParamValue = search.get(START_DATE_PARAM_NAME);
  if (startParamValue) {
    startTimestamp = proto.dateToTimestamp(startOfDay(new Date(startParamValue)));
  }

  let endTimestamp: google.protobuf.Timestamp;
  const endParamValue = search.get(END_DATE_PARAM_NAME);
  if (endParamValue) {
    const startOfNextDay = startOfDay(addDays(new Date(endParamValue), 1));
    endTimestamp = proto.dateToTimestamp(startOfNextDay);
  }

  return { startTimestamp, endTimestamp };
}
