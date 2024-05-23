import { Cancelable, ServerStream } from "../service/rpc_service";
import { ServerStreamHandler, ServerStreamingRpcMethod } from "../service/rpc_service";
import { FetchError, GRPCStatusError } from "./errors";
import { google as google_code } from "../../proto/grpc_code_ts_proto";

export const Code = google_code.rpc.Code;

/**
 * Performs a server-streaming RPC, automatically retrying certain types of
 * errors.
 *
 * @param method A reference to the RPC method, like `fooService.getBarStream`
 * @param requestArg Either a fixed request proto or a function that returns
 * one. The function is useful particularly when the stream is seekable - during
 * retry attempts, you can have this function return a request with the desired
 * seek position.
 * @param handler The stream handler. The error handler will not be called if
 * there is a transient error - instead, the stream will be transparently
 * reconnected using the provided request or request function.
 * @param options Options for customizing retry behavior.
 */
export function streamWithRetry<Request, Response>(
  method: ServerStreamingRpcMethod<Request, Response>,
  requestArg: Request | (() => Request),
  handler: ServerStreamHandler<Response>,
  { maxRetries = 5, shouldRetry = defaultRetryPredicate, retryDelayMs = backoff() }: RetryOptions = {}
): Cancelable {
  let retryTimeout: number | null = null;
  let retryAttempt = 1;
  let lastErrorTimestamp = 0;
  let stream: ServerStream<Response> | null = null;
  const attemptCall = () => {
    const request = typeof requestArg === "function" ? (requestArg as Function)() : requestArg;
    stream = method(request, {
      next: (response) => {
        // The max retry count applies to each reconnect attempt across the
        // entire lifecycle of the stream. So if we successfully get a message
        // on the stream, reset the retry attempt number.
        retryAttempt = 1;
        handler.next(response);
      },
      complete: handler.complete,
      error: (error: any) => {
        // If it's been a long time since the last error, we most likely were
        // able to reconnect successfully but just didn't get any messages on
        // the stream to let us know that we successfully reconnected. So we
        // reset the retry attempt number in this case too.
        const now = Date.now();
        if (now - lastErrorTimestamp > 30_000) {
          retryAttempt = 1;
        }
        lastErrorTimestamp = Date.now();

        if (retryAttempt > maxRetries || !shouldRetry(error)) {
          return handler.error(error);
        }

        const delay = retryDelayMs(retryAttempt);
        retryAttempt++;
        retryTimeout = window.setTimeout(() => attemptCall(), delay);
      },
    });
  };

  attemptCall();

  return {
    cancel: () => {
      if (retryTimeout !== null) {
        clearTimeout(retryTimeout);
      }
      stream?.cancel();
    },
  };
}

export type RetryOptions = {
  /** Max number of retry attempts after an error occurs. */
  maxRetries?: number;

  /** Given a stream error, returns whether it is retryable. */
  shouldRetry?: (error: any) => boolean;

  /**
   * Returns the delay for the given retry attempt. Retry attempts are numbered
   * starting from 1. The retry attempt number will be reset to 1 after
   * successfully reconnecting.
   */
  retryDelayMs?: (retryAttempt: number) => number;
};

/**
 * Default implementation that determines whether an error is retryable - by
 * default we retry network errors as well as gRPC errors with Unavailable
 * status.
 */
export function defaultRetryPredicate(e: any): boolean {
  return e instanceof FetchError || (e instanceof GRPCStatusError && e.status.code == Code.UNAVAILABLE);
}

export type BackoffConfig = {
  /** Delay is multiplied by this much on each round of backoff. */
  multiplier?: number;

  /** Randomization factor. Must be between 0 and 1. */
  jitter?: number;

  /** Initial backoff delay in milliseconds. */
  baseDelayMs?: number;

  /** Max backoff delay in milliseconds. */
  maxDelayMs?: number;
};

function backoff({ multiplier = 1.6, jitter = 0.2, baseDelayMs = 250, maxDelayMs = 60_000 }: BackoffConfig = {}) {
  return (retryAttempt: number): number => {
    let delay = baseDelayMs * Math.pow(multiplier, retryAttempt - 1);
    delay *= 1 + jitter * (Math.random() * 2 - 1);
    return Math.min(maxDelayMs, delay);
  };
}
