import { Subject, Subscription, from } from "rxjs";
import { eventlog } from "../../proto/eventlog_ts_proto";
import errorService from "../errors/error_service";
import rpcService, { Cancelable } from "../service/rpc_service";
import { streamWithRetry } from "../util/rpc";
import capabilities from "../capabilities/capabilities";

const POLL_TAIL_INTERVAL_MS = 3_000;
// How many lines to request from the server on each chunk request.
const MIN_LINES = 100_000;

/**
 * InvocationLogsModel holds the invocation log content for chunkstore-enabled
 * invocations, and handles fetching log chunks from chunkstore.
 */
export default class InvocationLogsModel {
  /** Streams an event whenever the state of the model changes. */
  readonly onChange: Subject<undefined> = new Subject<undefined>();

  private logs = "";
  // Length of the log prefix which has already been persisted. The remainder of
  // the log is considered "live" and may be updated on subsequent fetches.
  private stableLogLength = 0;

  // Polling-based state
  private responseSubscription?: Subscription;
  private pollTailTimeout?: number;

  // Server-stream based state
  private stream?: Cancelable;

  constructor(private invocationId: string) {}

  startFetching() {
    this.stopFetching();
    if (capabilities.config.streamingHttpEnabled && capabilities.config.invocationLogStreamingEnabled) {
      this.streamLogs();
    } else {
      this.fetchTail();
    }
  }

  stopFetching() {
    if (this.pollTailTimeout !== undefined) {
      window.clearTimeout(this.pollTailTimeout);
    }
    this.responseSubscription?.unsubscribe();
    this.responseSubscription = undefined;

    this.stream?.cancel();
    this.stream = undefined;
  }

  getLogs(): string {
    return this.logs;
  }

  isFetching(): boolean {
    return Boolean(this.responseSubscription);
  }

  private streamLogs() {
    let chunkId = "";
    this.stream = streamWithRetry(
      rpcService.service.getEventLog,
      () => {
        return new eventlog.GetEventLogChunkRequest({
          invocationId: this.invocationId,
          chunkId,
          minLines: MIN_LINES,
        });
      },
      {
        next: (response) => {
          this.handleResponse(response);
          // Save the response chunk ID - if the stream is retried then we'll
          // resume starting from this chunk.
          chunkId = response.nextChunkId;
        },
        error: (e) => {
          errorService.handleError(e, { ignoreErrorCodes: ["NotFound", "PermissionDenied", "Unauthenticated"] });
        },
        complete: () => {},
      }
    );
  }

  private fetchTail(chunkId = "") {
    this.responseSubscription = from<Promise<eventlog.GetEventLogChunkResponse>>(
      rpcService.service.getEventLogChunk(
        new eventlog.GetEventLogChunkRequest({
          invocationId: this.invocationId,
          chunkId,
          minLines: MIN_LINES,
        })
      )
    ).subscribe({
      next: (response) => {
        this.handleResponse(response);
        // Unchanged next chunk ID means the invocation is still in progress and
        // we should continue polling that chunk.
        if (response.nextChunkId === chunkId) {
          this.pollTailTimeout = window.setTimeout(() => this.fetchTail(chunkId), POLL_TAIL_INTERVAL_MS);
          return;
        }
        // New next chunk ID means we successfully fetched the requested
        // chunk, and more may be available. Try fetching it immediately.
        this.fetchTail(response.nextChunkId);
      },
      error: (e) =>
        errorService.handleError(e, { ignoreErrorCodes: ["NotFound", "PermissionDenied", "Unauthenticated"] }),
    });
  }

  private handleResponse(response: eventlog.GetEventLogChunkResponse) {
    this.logs = this.logs.slice(0, this.stableLogLength);
    this.logs = this.logs + new TextDecoder().decode(response.buffer || new Uint8Array());
    if (!response.live) {
      this.stableLogLength = this.logs.length;
    }

    // Empty next chunk ID means the invocation is complete and we've reached
    // the end of the log.
    if (!response.nextChunkId) {
      this.responseSubscription = undefined;
      // Notify of change to `isFetching` state.
      this.onChange.next();
      return;
    }

    if (response.buffer?.length) {
      // Notify of change to `logs` state.
      this.onChange.next();
    }
  }
}
