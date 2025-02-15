import { io, $stream } from "../proto/databuddy_ts_proto";
import { CancelablePromise } from "../../../../app/util/async";
import * as protobufjs from "protobufjs";
import { FetchError, GRPCStatusError, HTTPStatusError, parseGRPCStatus } from "../../../../app/util/errors";
import { google as google_status } from "../../../../proto/grpc_status_ts_proto";
import { google as google_code } from "../../../../proto/grpc_code_ts_proto";
import { BrowserHeaders } from "browser-headers";
import React from "react";

/** Return type for unary RPCs. */
export { CancelablePromise } from "../../../../app/util/async";

/** Return type for server-streaming RPCs. */
export type ServerStream<T> = $stream.ServerStream<T>;

/** Return type common to both unary and server-streaming RPCs. */
export type Cancelable = CancelablePromise<any> | ServerStream<any>;

/** Stream handler params passed when calling a server-streaming RPC. */
export type ServerStreamHandler<T> = $stream.ServerStreamHandler<T>;

/**
 * ExtendedQueries is an extended version of Queries with
 * the following differences:
 *
 * - All RPC methods return a `CancelablePromise` instead of a `Promise`.
 */
export type ExtendedQueries = CancelableService<io.buildbuddy.databuddy.Queries>;

/**
 * QueriesRpcName is a union type consisting of all Queries
 * RPC names (in `camelCase`).
 */
export type QueriesRpcName = RpcMethodNames<io.buildbuddy.databuddy.Queries>;

export type FetchResponseType = "arraybuffer" | "stream" | "text" | "";

/**
 * Lowest-level fetch method. Ensures that headers are set correctly,
 * and handles returning the correct type of response based on the given
 * response type.
 */
async function doFetch<T extends FetchResponseType>(
  url: string,
  responseType: T,
  init: RequestInit = {}
): Promise<FetchPromiseType<T>> {
  const headers = new Headers(init.headers);
  let response: Response;
  try {
    response = await fetch(url, { ...init, headers });
  } catch (e) {
    throw new FetchError(e);
  }
  if (response.status < 200 || response.status >= 400) {
    // Read error message from response body
    let body = "";
    try {
      body = await response.text();
    } catch (e) {
      // If we failed to read the response body then ignore the status code
      // and return a generic network error. It may be possible to retry
      // this error to get the complete error message.
      throw new FetchError(e);
    }
    throw new HTTPStatusError(response.status, body);
  }
  switch (responseType) {
    case "arraybuffer":
      try {
        return (await response.arrayBuffer()) as FetchPromiseType<T>;
      } catch (e) {
        throw new FetchError(e);
      }
    case "stream":
      return response as FetchPromiseType<T>;
    default:
      try {
        return (await response.text()) as FetchPromiseType<T>;
      } catch (e) {
        throw new FetchError(e);
      }
  }
}

async function rpcImpl(
  method: { name: string; serverStreaming?: boolean },
  requestData: Uint8Array,
  callback: (error: any, data?: Uint8Array) => void,
  streamParams?: $stream.StreamingRPCParams
): Promise<void> {
  const url = `/rpc/io.buildbuddy.databuddy.Queries/${method.name}`;
  const init: RequestInit = { method: "POST", body: requestData };
  init.headers = { "Content-Type": "application/proto" };
  // Set the signal to allow canceling the underlying fetch, if applicable.
  if (streamParams?.signal) {
    init.signal = streamParams.signal;
  }

  init.headers = { "Content-Type": "application/proto+prefixed" };
  init.body = lengthPrefixMessage(requestData);
  try {
    const response = await doFetch(url, "stream", init);
    if (response.headers.has("grpc-status")) {
      const status = statusFromHeaders(response.headers);
      if (status.code !== google_code.rpc.Code.OK) {
        throw new GRPCStatusError(status);
      }
    } else if (response.body) {
      await readLengthPrefixedStream(response.body.getReader(), (messageBytes) => {
        callback(null, messageBytes);
      });
    }
    streamParams?.complete?.();
  } catch (e) {
    // If we successfully read the HTTP response but it returned an error
    // code, try to parse it as a gRPC error.
    const grpcStatus = e instanceof HTTPStatusError ? parseGRPCStatus(e.body) : null;
    if (grpcStatus) {
      callback(new GRPCStatusError(grpcStatus));
    } else {
      callback(e);
    }
  }
}

function getExtendedService(service: io.buildbuddy.databuddy.Queries): ExtendedQueries {
  const extendedService = Object.create(service);
  for (const rpcName of getRpcMethodNames(io.buildbuddy.databuddy.Queries)) {
    const originalMethod = (service as any)[rpcName] as BaseRpcMethod<any, any>;
    const method = (request: Record<string, any>, subscriber?: any) => {
      if (originalMethod.serverStreaming) {
        // ServerStream method already supports cancel function.
        return originalMethod.call(service, request, subscriber);
      } else {
        // Wrap with our CancelablePromise util.
        // TODO: add codegen support to allow canceling the underlying fetch
        // for unary RPCs.
        return new CancelablePromise(originalMethod.call(service, request));
      }
    };
    // Preserve generated metadata attributes attached to each method.
    for (const name of ["name", "serverStreaming"] as const) {
      Object.defineProperty(method, name, { value: originalMethod[name] });
    }
    extendedService[rpcName] = method;
  }
  return extendedService;
}

// GRPC over HTTP requires protobuf messages to be sent in a series of `Length-Prefixed-Message`s
// Here's what a Length-Prefixed-Message looks like:
// 		Length-Prefixed-Message → Compressed-Flag Message-Length Message
// 		Compressed-Flag → 0 / 1 # encoded as 1 byte unsigned integer
// 		Message-Length → {length of Message} # encoded as 4 byte unsigned integer (big endian)
// 		Message → *{binary octet}
// For more info, see: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
function lengthPrefixMessage(requestData: Uint8Array) {
  const frame = new ArrayBuffer(requestData.byteLength + 5);
  new DataView(frame, 1, 4).setUint32(0, requestData.length, false /* big endian */);
  new Uint8Array(frame, 5).set(requestData);
  return new Uint8Array(frame);
}

/**
 * Reads length-prefixed message payloads from the stream and invokes the given
 * callback for each one.
 *
 * The returned promise completes when all messages are successfully read from
 * the stream. It resolves with an error if reading from the stream fails or if
 * it contains malformed data.
 */
async function readLengthPrefixedStream(reader: ReadableStreamDefaultReader, callback: (b: Uint8Array) => void) {
  const bufferedStream = new BufferedStream(reader);

  // Reusable buffer for the flags + length header.
  const headerBytes = new Uint8Array(5);
  const readHeader = async () => {
    const n = await bufferedStream.read(headerBytes);
    if (n === 0) {
      return null; // no more data
    }
    if (n !== headerBytes.length) {
      throw new Error("unexpected data on stream while reading length prefix");
    }
    return {
      flags: headerBytes[0],
      payloadLength: new DataView(headerBytes.buffer, 1, 4).getUint32(0, /*littleEndian=*/ false),
    };
  };

  const readPayload = async (payloadLength: number) => {
    const messageBytes = new Uint8Array(payloadLength);
    const n = await bufferedStream.read(messageBytes);
    if (n < messageBytes.length) {
      throw new Error("stream ended unexpectedly while reading payload");
    }
    return messageBytes;
  };

  while (true) {
    const header = await readHeader();
    if (!header) {
      // No more data, and we haven't read the status.
      // Just assume OK status for now.
      return;
    }

    if ((header.flags & 0x80) === 0x80) {
      // This value indicates that there is no more data and the gRPC status
      // payload will follow.
      const encodedTrailers = await readPayload(header.payloadLength);
      const status = statusFromHeaders(decodeTrailers(encodedTrailers));
      if (status.code === google_code.rpc.Code.OK) {
        return; // OK status - don't throw an error.
      }
      throw new GRPCStatusError(status);
    }

    const message = await readPayload(header.payloadLength);
    callback(message);
  }
}

export const queriesService = getExtendedService(new io.buildbuddy.databuddy.Queries(rpcImpl));

function statusFromHeaders(input: Headers | string): google_status.rpc.Status {
  const headers = new BrowserHeaders(input);
  const code = Number(headers.get("grpc-status")?.[0] ?? undefined);
  const message = headers.get("grpc-message")?.[0] ?? undefined;
  return new google_status.rpc.Status({
    code: isNaN(code) ? google_code.rpc.Code.UNKNOWN : code,
    message: message || "unknown error",
  });
}

/**
 * Provides buffering for a ReadableStream.
 */
class BufferedStream {
  private chunks: Uint8Array[] = [];
  private len = 0;
  private done = false;

  constructor(private reader: ReadableStreamDefaultReader) {}

  /**
   * Tries to fill the given buffer with data from the stream, and returns the
   * number of bytes that were successfully read. It returns a value less than
   * the buffer length only if the stream ends while reading.
   */
  async read(out: Uint8Array): Promise<number> {
    // Read chunks until either we can fill the buffer or the stream has no more
    // data.
    while (this.len < out.length && !this.done) {
      const { value, done } = await this.reader.read();
      this.done = done;
      if (value) {
        this.chunks.push(value);
        this.len += value.length ?? 0;
      }
    }
    // Consume chunk data until either we fill the buffer, or the stream is done
    // and we don't have any buffered data left.
    let n = 0;
    while (n < out.length && this.chunks.length) {
      const remainder = out.length - n;
      let data: Uint8Array;
      if (remainder >= this.chunks[0].length) {
        // Consume the full chunk.
        data = this.chunks[0];
        this.chunks.shift();
      } else {
        // Consume a partial chunk.
        data = this.chunks[0].subarray(0, remainder);
        this.chunks[0] = this.chunks[0].subarray(remainder);
      }
      out.set(data, n);
      n += data.length;
    }
    this.len -= n;
    return n;
  }
}

const isAllowedControlChar = (char: number) => char === 0x9 || char === 0xa || char === 0xd;

function isValidHeaderAscii(val: number): boolean {
  return isAllowedControlChar(val) || (val >= 0x20 && val <= 0x7e);
}

function decodeTrailers(bytes: Uint8Array): string {
  for (let i = 0; i !== bytes.length; ++i) {
    if (!isValidHeaderAscii(bytes[i])) {
      throw new Error("gRPC status trailers returned by server are not valid ASCII");
    }
  }
  return new TextDecoder("ASCII").decode(bytes);
}

function getRpcMethodNames(serviceClass: Function) {
  return new Set(Object.keys(serviceClass.prototype).filter((key) => key !== "constructor"));
}

/**
 * Type of a unary RPC method on the originally generated service type,
 * before wrapping with our ExtendedQueries functionality.
 */
type BaseUnaryRpcMethod<Request, Response> = ((request: Request) => Promise<Response>) & {
  name: string;
  serverStreaming: false;
};

/**
 * Type of a unary RPC method on the ExtendedQueries.
 */
export type UnaryRpcMethod<Request, Response> = (request: Request) => CancelablePromise<Response>;

/**
 * Type of a server-streaming RPC method.
 */
export type ServerStreamingRpcMethod<Request, Response> = ((
  request: Request,
  handler: $stream.ServerStreamHandler<Response>
) => $stream.ServerStream<Response>) & {
  name: string;
  serverStreaming: true;
};

export type RpcMethod<Request, Response> =
  | UnaryRpcMethod<Request, Response>
  | ServerStreamingRpcMethod<Request, Response>;

type BaseRpcMethod<Request, Response> =
  | BaseUnaryRpcMethod<Request, Response>
  | ServerStreamingRpcMethod<Request, Response>;

type RpcMethodNames<Service extends protobufjs.rpc.Service> = keyof Omit<Service, keyof protobufjs.rpc.Service>;

/**
 * Utility type that adapts a generated service class so that
 * `CancelablePromise` is returned from all unary RPC methods instead of
 * `Promise`.
 */
type CancelableService<Service extends protobufjs.rpc.Service> = protobufjs.rpc.Service & {
  // Loop over all methods in the service, except for the ones inherited from the base
  // service (we don't want to modify those at all).
  [MethodName in RpcMethodNames<Service>]: Service[MethodName] extends BaseUnaryRpcMethod<infer Request, infer Response>
    ? /* Unary RPC: transform the generated method's return type from Promise to CancelablePromise. */
      UnaryRpcMethod<Request, Response>
    : /* Server-streaming RPC: keep the original method as-is. */
      Service[MethodName];
};

type FetchPromiseType<T extends FetchResponseType> = T extends ""
  ? string
  : T extends "text"
    ? string
    : T extends "arraybuffer"
      ? ArrayBuffer
      : T extends "stream"
        ? Response
        : never;

export function useRPC<RequestType, ResponseType>(
  method: (request: RequestType) => CancelablePromise<ResponseType>,
  request: RequestType | null
) {
  const [loading, setLoading] = React.useState(!!request);
  const [error, setError] = React.useState<any>(null);
  const [response, setResponse] = React.useState<ResponseType | null>(null);

  React.useEffect(() => {
    if (!request) {
      setLoading(false);
      setError(null);
      setResponse(null);
      return;
    }
    const rpc = method(request);
    rpc
      .then((response) => setResponse(response))
      .catch((e) => setError(e))
      .finally(() => setLoading(false));
    return () => rpc.cancel();
  }, [method, JSON.stringify(request)]);

  return { loading, response, error };
}
