import { Observable } from "rxjs";
import { handle } from "./stream";

import { eventlog } from "../../proto/eventlog_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";

// Note: For now, this is manually kept in sync with proto/buildbuddy_stream_service.proto
export class BuildBuddyStreamService {
  getInvocationEvents(request: invocation.IGetInvocationEventsRequest): Observable<invocation.IInvocationEvent> {
    return handle("GetInvocationEvents", request, invocation.GetInvocationEventsRequest, invocation.InvocationEvent);
  }

  getInvocationStateChanges(
    request: invocation.IGetInvocationStateChangesRequest
  ): Observable<invocation.IGetInvocationStateChangesResponse> {
    return handle(
      "GetInvocationStateChanges",
      request,
      invocation.GetInvocationStateChangesRequest,
      invocation.GetInvocationStateChangesResponse
    );
  }

  getInvocationLog(request: eventlog.IGetEventLogChunkRequest): Observable<eventlog.IGetEventLogChunkResponse> {
    return handle("GetInvocationLog", request, eventlog.GetEventLogChunkRequest, eventlog.GetEventLogChunkResponse);
  }
}
