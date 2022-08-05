import { CancelablePromise } from "../util/async";
import { invocation } from "../../proto/invocation_ts_proto";
import rpcService from "../service/rpc_service";

export function fetchEvents(invocationId: string): CancelablePromise<invocation.IInvocationEvent[]> {
  const events: invocation.IInvocationEvent[] = [];
  return new CancelablePromise(
    new Promise((resolve, reject) => {
      rpcService.streamService.getInvocationEvents({ invocationId }).subscribe({
        next: (event) => events.push(event),
        complete: () => resolve(events),
        error: (e) => reject(e),
      });
    })
  );
}
