import { Subject, Subscription, from } from "rxjs";
import { eventlog } from "../../proto/eventlog_ts_proto";
import capabilities from "../capabilities/capabilities";
import errorService from "../errors/error_service";
import rpcService, { Cancelable, CancelablePromise, ExtendedBuildBuddyService } from "../service/rpc_service";
import { streamWithRetry } from "../util/rpc";

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
  // Whether we've received an empty nextChunkId, indicating the log stream is complete.
  private complete = false;

  // Polling-based state
  private responseSubscription?: Subscription;
  private responseRPC?: CancelablePromise<eventlog.GetEventLogChunkResponse>;
  private pollTailTimeout?: number;

  // Server-stream based state
  private stream?: Cancelable;
  // Match the service selected by a same-region BES endpoint so learning the
  // endpoint does not unnecessarily cancel and replay the current log stream.
  private service: ExtendedBuildBuddyService = rpcService.getRegionalServiceOrDefault(window.location.origin);
  private fetchingRequested = false;
  private fetchGeneration = 0;

  constructor(
    private invocationId: string,
    private logType: eventlog.LogType = eventlog.LogType.BUILD_LOG
  ) {}

  startFetching() {
    this.stopFetching();
    this.fetchingRequested = true;
    this.complete = false;
    if (capabilities.config.streamingHttpEnabled && capabilities.config.invocationLogStreamingEnabled) {
      this.streamLogs();
    } else {
      this.fetchTail();
    }
  }

  /** Switch live log reads to the BES region once the invocation identifies it. */
  setService(service: ExtendedBuildBuddyService) {
    if (service === this.service) return;
    const restart = this.fetchingRequested;
    this.stopFetching();
    this.service = service;
    // Restart at chunk zero. Reusing a prefix while replaying chunks would append
    // the persisted portion twice, and the old region's live tail may be stale.
    this.logs = "";
    this.stableLogLength = 0;
    this.complete = false;
    if (restart) this.startFetching();
    this.onChange.next();
  }

  stopFetching() {
    this.fetchingRequested = false;
    this.fetchGeneration++;
    if (this.pollTailTimeout !== undefined) {
      window.clearTimeout(this.pollTailTimeout);
      this.pollTailTimeout = undefined;
    }
    this.responseRPC?.cancel();
    this.responseRPC = undefined;
    this.responseSubscription?.unsubscribe();
    this.responseSubscription = undefined;

    this.stream?.cancel();
    this.stream = undefined;
  }

  getLogs(): string {
    return this.logs;
  }

  isFetching(): boolean {
    return Boolean(this.responseSubscription || (this.stream && !this.complete));
  }

  isComplete(): boolean {
    return this.complete;
  }

  private streamLogs() {
    let chunkId = "";
    const generation = this.fetchGeneration;
    this.stream = streamWithRetry(
      this.service.getEventLog,
      () => {
        return new eventlog.GetEventLogChunkRequest({
          invocationId: this.invocationId,
          chunkId,
          minLines: MIN_LINES,
          type: this.logType,
        });
      },
      {
        next: (response) => {
          if (generation !== this.fetchGeneration) return;
          this.handleResponse(response);
          // Save the response chunk ID - if the stream is retried then we'll
          // resume starting from this chunk.
          chunkId = response.nextChunkId;
        },
        error: (e) => {
          if (generation !== this.fetchGeneration) return;
          this.stream = undefined;
          this.onChange.next();
          errorService.handleError(e, { ignoreErrorCodes: ["NotFound", "PermissionDenied", "Unauthenticated"] });
        },
        complete: () => {
          if (generation !== this.fetchGeneration) return;
          this.stream = undefined;
          this.onChange.next();
        },
      }
    );
  }

  private fetchTail(chunkId = "") {
    const generation = this.fetchGeneration;
    this.responseRPC = this.service.getEventLogChunk(
      new eventlog.GetEventLogChunkRequest({
        invocationId: this.invocationId,
        chunkId,
        minLines: MIN_LINES,
        type: this.logType,
      })
    );
    this.responseSubscription = from<Promise<eventlog.GetEventLogChunkResponse>>(this.responseRPC).subscribe({
      next: (response) => {
        if (generation !== this.fetchGeneration) return;
        this.handleResponse(response);
        if (response.nextChunkId === "") {
          return;
        }
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
      error: (e) => {
        if (generation !== this.fetchGeneration) return;
        errorService.handleError(e, { ignoreErrorCodes: ["NotFound", "PermissionDenied", "Unauthenticated"] });
      },
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
      this.complete = true;
      this.responseSubscription = undefined;
      // Notify of change to `isFetching` and `isComplete` state.
      this.onChange.next();
      return;
    }

    if (response.buffer?.length) {
      // Notify of change to `logs` state.
      this.onChange.next();
    }
  }
}
