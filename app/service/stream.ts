import { Writer } from "protobufjs";
import { Observable } from "rxjs";
import { MAX_VARINT_LENGTH_64, readVarint } from "../util/varint";

export interface MessageClass<T> {
  encode(message: T): Writer;
  decode(message: Uint8Array): T;
}

/** Returns whether the given error may be retried. */
export function isRetryable(error: any) {
  return error instanceof FetchError;
}

/**
 * FetchError indicates a failure to connect to or receive data from an RPC endpoint.
 *
 * These errors should be retried by the client in order to resume the stream.
 */
export class FetchError {
  constructor(private wrappedError: any) {}

  toString() {
    return String(this.wrappedError);
  }
}

/** Handles a streaming RPC request. */
export function handle<RequestType, ResponseType>(
  rpcName: string,
  request: RequestType,
  requestClass: MessageClass<RequestType>,
  responseClass: MessageClass<ResponseType>
): Observable<ResponseType> {
  return new Observable<ResponseType>((subscriber) => {
    const url = `${window.location.origin}/rpc/BuildBuddyStreamService/${rpcName}`;
    let responses: ResponseType[] = [];
    let buffer = new Uint8Array();
    let nextLength: number | null = null;
    let consumingError = false;
    /**
     * Parses as much of the currently fetched buffer as possible into
     * response protos.
     */
    function consumeBuffer(lastCall = false) {
      while (buffer.byteLength > 0) {
        // If we don't know the length of the next proto to consume, look for an encoded length.
        if (nextLength === null) {
          // Don't read the length yet if the current buffer might contain a
          // partial chunk of a varint sequence.
          if (!lastCall && buffer.byteLength < MAX_VARINT_LENGTH_64) return;

          // TODO: Is it possible we might consume a partial varint here?
          const [length, numBytesRead] = readVarint(buffer);
          // We never expect file sizes that are larger than Number.MAX_SAFE_INTEGER,
          // so the conversion from Long to Number here is OK.
          nextLength = length.toNumber();
          buffer = buffer.subarray(numBytesRead);
          continue;
        }

        // A length of -1 signals that the next varint length and following
        // bytes encode an error message.
        if (nextLength === -1) {
          consumingError = true;
          nextLength = null;
          continue;
        }

        // If this is the last call and we don't have the expected number of
        // bytes yet, we won't be able to parse the proto, so give a nicer
        // error in this case.
        if (lastCall && buffer.byteLength < nextLength) {
          throw new Error(
            `Event stream is unexpectedly truncated: expected ${nextLength} bytes, but only ${buffer.byteLength} are available`
          );
        }

        // If our buffer has at least the expected number of bytes, consume
        // those bytes, either as a proto response or an error.
        if (buffer.byteLength >= nextLength) {
          const bytes = buffer.subarray(0, nextLength);
          if (consumingError) {
            const errorText = new TextDecoder().decode(bytes);
            throw new Error(errorText);
          }
          const response = responseClass.decode(bytes);
          responses.push(response);
          buffer = buffer.subarray(bytes.byteLength);
          nextLength = null;
          continue;
        }
        // We couldn't consume a proto length varint or bytes; nothing left to do.
        break;
      }
      for (const event of responses) {
        subscriber?.next(event);
      }
      responses = [];
    }

    // Start a worker to stream the HTTP response and post the raw data back to
    // us for parsing. This approach is used because parsing and downloading in
    // the same thread significantly reduces throughput.
    const worker = startFetchWorker(url, request, requestClass);
    function end(error?: any) {
      if (error) {
        subscriber.error(error);
      }
      subscriber.complete();
      worker.terminate();
      subscriber = null;
    }
    worker.onmessage = (event: MessageEvent) => {
      if (!subscriber) return;

      const message = event.data as Message;
      switch (message.type) {
        case "data":
          buffer = concatChunks([buffer, message.value]);
          try {
            consumeBuffer();
          } catch (e) {
            end(e);
            return;
          }
          break;
        case "end":
          try {
            consumeBuffer(/*lastCall=*/ true);
          } catch (e) {
            end(e);
            return;
          }
          end();
          return;
        case "error":
          end(new FetchError(message.error));
          return;
        case "http_error":
          let errorMessage = message.error.responseBody;
          if (!errorMessage) {
            if (message.error.responseBodyError) {
              errorMessage = `(error reading response body: ${message.error.responseBodyError})`;
            } else {
              errorMessage = "(empty response body)";
            }
          }
          end(
            new FetchError(
              `HTTP ${message.error.status}: ${
                message.error.responseBody || message.error.responseBodyError || "[empty response body]"
              }`
            )
          );
          return;
      }
    };
  });
}

type DataMessage = {
  type: "data";
  value: Uint8Array;
};

type EndMessage = {
  type: "end";
};

type ErrorMessage = {
  type: "error";
  error: any;
};

type HTTPErrorMessage = {
  type: "http_error";
  error: {
    status: number;
    responseBody?: string;
    responseBodyError?: any;
  };
};

type Message = ErrorMessage | HTTPErrorMessage | DataMessage | EndMessage;

function startFetchWorker<T>(url: string, request: T, requestClass: MessageClass<T>) {
  const requestBytes = requestClass.encode(request).finish();
  const source = `
  (async () => {
    const url = ${JSON.stringify(url)};
    const requestBody = new Uint8Array([${requestBytes}]);
    let response;
    try {
      response = await fetch(url, {
        credentials: "include",
        method: "POST",
        body: requestBody
      });
    } catch (e) {
      globalThis.postMessage({
        type: 'error',
        error: e,
      });
      return;
    }
    if (response.status >= 400) {
      let responseBody, responseBodyError;
      try {
        responseBody = await response.text();
      } catch (e) {
        responseBodyError = e;
      }
      globalThis.postMessage({
        type: 'http_error',
        error: {
          status: response.status,
          responseBody,
          responseBodyError,
        }
      })
      return;
    }
    const reader = response.body.getReader();
    while (true) {
      let value, done, error;
      try {
        ({ value, done } = await reader.read());
      } catch (e) {
        error = e;
      }
      globalThis.postMessage({
        type: error ? 'error' : (done ? 'end' : 'data'),
        value,
        error,
      });
      if (done || error) return;
    }
  })();
  `;
  return new Worker(URL.createObjectURL(new Blob([source], { type: "application/javascript" })));
}

function concatChunks(chunks: Uint8Array[]) {
  const out = new Uint8Array(chunks.reduce((acc, value) => acc + value.length, 0));
  let offset = 0;
  for (let chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
