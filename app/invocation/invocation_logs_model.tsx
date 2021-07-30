import { Subject, Subscription, from } from "rxjs";
import { eventlog } from "../../proto/eventlog_ts_proto";
import errorService from "../errors/error_service";
import rpcService from "../service/rpc_service";

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
  private responseSubscription: Subscription;
  private pollTailTimeout: number | null = null;

  constructor(private invocationId: string) {}

  startFetching() {
    this.fetchTail();
  }

  stopFetching() {
    window.clearTimeout(this.pollTailTimeout);
    this.responseSubscription?.unsubscribe();
    this.responseSubscription = null;
  }

  getLogs(): string {
    return this.logs;
  }

  isFetching(): boolean {
    return Boolean(this.responseSubscription);
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
        this.logs = this.logs + String.fromCharCode(...(response.buffer || []));

        // Empty next chunk ID means the invocation is complete and we've reached
        // the end of the log.
        if (!response.nextChunkId) {
          this.responseSubscription = null;
          // Notify of change to `isFetching` state.
          this.onChange.next();
          return;
        }

        if (response.buffer?.length) {
          // Notify of change to `logs` state.
          this.onChange.next();
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
      error: (e) => errorService.handleError(e),
    });
  }
}
