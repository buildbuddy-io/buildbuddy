import { Subject } from "rxjs";
import { buildbuddy, $stream } from "../../proto/buildbuddy_service_ts_proto";
import { context } from "../../proto/context_ts_proto";
import { CancelablePromise } from "../util/async";
import * as protobufjs from "protobufjs";
import capabilities from "../capabilities/capabilities";
import { FetchError, GRPCStatusError, HTTPStatusError, parseGRPCStatus } from "../util/errors";
import { google as google_status } from "../../proto/grpc_status_ts_proto";

/** Return type for unary RPCs. */
export { CancelablePromise } from "../util/async";

/** Return type for server-streaming RPCs. */
export type ServerStream<T> = $stream.ServerStream<T>;

/** Return type common to both unary and server-streaming RPCs. */
export type Cancelable = CancelablePromise<any> | ServerStream<any>;

/** Stream handler params passed when calling a server-streaming RPC. */
export type ServerStreamHandler<T> = $stream.ServerStreamHandler<T>;

/**
 * ExtendedBuildBuddyService is an extended version of BuildBuddyService with
 * the following differences:
 *
 * - The `requestContext` field is automatically set on each request.
 * - All RPC methods return a `CancelablePromise` instead of a `Promise`.
 *
 * TODO(bduffany): allow customizing the codegen to provide this extended functionality
 * instead of trying to transform the service types / classes like this.
 */
type ExtendedBuildBuddyService = CancelableService<buildbuddy.service.BuildBuddyService>;

/**
 * BuildBuddyServiceRpcName is a union type consisting of all BuildBuddyService
 * RPC names (in `camelCase`).
 */
export type BuildBuddyServiceRpcName = RpcMethodNames<buildbuddy.service.BuildBuddyService>;

export type FileEncoding = "gzip" | "zstd" | "";

export type FetchResponseType = "arraybuffer" | "stream" | "text" | "";

/**
 * Optional parameters for bytestream downloads.
 *
 * init:
 *     RequestInit for the fetch call that powers this download.
 * filename:
 *     the file will be downloaded with this filename rather than the digest.
 * zip:
 *     a serialized, base64-encoded zip.ManifestEntry that instructs which
 *     sub-file in a zip file should be extracted and downloaded (the file
 *     referenced by the digest must be a valid zip file).
 */
export type BytestreamFileOptions = {
  init?: RequestInit;
  filename?: string;
  zip?: string;
};

// When streaming HTTP is enabled, use more structured gRPC errors, since we
// need to be able to classify errors accurately in order to know whether
// they can be retried or not.
// TODO: enable these unconditionally after testing.
const structuredErrors = capabilities.config.streamingHttpEnabled;

class RpcService {
  service: ExtendedBuildBuddyService;
  regionalServices = new Map<string, ExtendedBuildBuddyService>();
  events: Subject<string>;
  requestContext = new context.RequestContext({
    timezoneOffsetMinutes: new Date().getTimezoneOffset(),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });

  constructor() {
    this.service = this.getExtendedService(new buildbuddy.service.BuildBuddyService(this.rpc.bind(this, "")));
    this.events = new Subject();

    if (capabilities.config.regions) {
      for (let r of capabilities.config.regions) {
        this.regionalServices.set(
          r.name,
          this.getExtendedService(new buildbuddy.service.BuildBuddyService(this.rpc.bind(this, r.server)))
        );
      }
    }

    (window as any)._rpcService = this;
  }

  debuggingEnabled(): boolean {
    const url = new URL(window.location.href);
    let sp = url.searchParams.get("debug");
    if (sp === "1" || sp === "true" || sp === "True") {
      return true;
    }
    return false;
  }

  getDownloadUrl(params: Record<string, string>): string {
    const encodedRequestContext = uint8ArrayToBase64(context.RequestContext.encode(this.requestContext).finish());
    return `/file/download?${new URLSearchParams({
      ...params,
      request_context: encodedRequestContext,
    })}`;
  }

  getBytestreamUrl(bytestreamURL: string, invocationId: string, { filename = "", zip = "" } = {}): string {
    const params: Record<string, string> = {
      bytestream_url: bytestreamURL,
      invocation_id: invocationId,
    };
    if (filename) params.filename = filename;
    if (zip) params.z = zip;
    return this.getDownloadUrl(params);
  }

  downloadLog(invocationId: string, attempt: number) {
    const params: Record<string, string> = {
      invocation_id: invocationId,
      attempt: attempt.toString(),
      artifact: "buildlog",
    };
    window.open(this.getDownloadUrl(params));
  }

  downloadBytestreamFile(filename: string, bytestreamURL: string, invocationId: string) {
    window.open(this.getBytestreamUrl(bytestreamURL, invocationId, { filename }));
  }

  downloadBytestreamZipFile(filename: string, bytestreamURL: string, zip: string, invocationId: string) {
    window.open(this.getBytestreamUrl(bytestreamURL, invocationId, { filename, zip }));
  }

  /**
   * Fetches a bytestream resource.
   *
   * If the resource is known to already be stored in compressed form,
   * storedEncoding can be specified to prevent the server from
   * double-compressing (since it gzips all resources by default).
   */
  fetchBytestreamFile<T extends FetchResponseType = "text">(
    bytestreamURL: string,
    invocationId: string,
    responseType?: T,
    options: BytestreamFileOptions = {}
  ): Promise<FetchPromiseType<T>> {
    return this.fetch(
      this.getBytestreamUrl(bytestreamURL, invocationId, options),
      (responseType || "") as FetchResponseType,
      options.init ?? {}
    ) as Promise<FetchPromiseType<T>>;
  }

  /**
   * Lowest-level fetch method. Ensures that tracing headers are set correctly,
   * and handles returning the correct type of response based on the given
   * response type.
   */
  async fetch<T extends FetchResponseType>(
    url: string,
    responseType: T,
    init: RequestInit = {}
  ): Promise<FetchPromiseType<T>> {
    const headers = new Headers(init.headers);
    if (this.debuggingEnabled()) {
      headers.set("x-buildbuddy-trace", "force");
    }
    let response: Response;
    try {
      response = await fetch(url, { ...init, headers });
    } catch (e) {
      throw structuredErrors ? new FetchError(e) : `connection error: ${e}`;
    }
    if (response.status < 200 || response.status >= 400) {
      // Read error message from response body
      let body = "";
      try {
        body = await response.text();
      } catch (e) {
        if (structuredErrors) {
          // If we failed to read the response body then ignore the status code
          // and return a generic network error. It may be possible to retry
          // this error to get the complete error message.
          throw new FetchError(e);
        }
        body = `unknown (failed to read response body: ${e})`;
      }

      throw structuredErrors ? new HTTPStatusError(response.status, body) : `failed to fetch: ${body}`;
    }
    switch (responseType) {
      case "arraybuffer":
        try {
          return (await response.arrayBuffer()) as FetchPromiseType<T>;
        } catch (e) {
          throw structuredErrors ? new FetchError(e) : e;
        }
      case "stream":
        return response.body as FetchPromiseType<T>;
      default:
        try {
          return (await response.text()) as FetchPromiseType<T>;
        } catch (e) {
          throw structuredErrors ? new FetchError(e) : e;
        }
    }
  }

  async rpc(
    server: string,
    method: { name: string; serverStreaming?: boolean },
    requestData: Uint8Array,
    callback: (error: any, data?: Uint8Array) => void,
    streamParams?: $stream.StreamingRPCParams
  ): Promise<void> {
    const url = `${server || ""}/rpc/BuildBuddyService/${method.name}`;
    const init: RequestInit = { method: "POST", body: requestData };
    if (capabilities.config.regions?.map((r) => r.server).includes(server)) {
      init.credentials = "include";
    }
    init.headers = { "Content-Type": "application/proto" };
    // Set the signal to allow canceling the underlying fetch, if applicable.
    if (streamParams?.signal) {
      init.signal = streamParams.signal;
    }

    if (method.serverStreaming && !capabilities.config.streamingHttpEnabled) {
      console.error("Attempted to call server-streaming RPC, but streaming HTTP is disabled");
      return;
    }

    if (capabilities.config.streamingHttpEnabled) {
      init.headers["Content-Type"] = "application/proto+prefixed";
      init.body = lengthPrefixMessage(requestData);
      try {
        const response = await this.fetch(url, "stream", init);
        await readLengthPrefixedStream(response!.getReader(), (messageBytes) => {
          this.events.next(method.name);
          callback(null, messageBytes);
        });
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
      return;
    }

    try {
      const arrayBuffer = await this.fetch(url, "arraybuffer", init);
      callback(null, new Uint8Array(arrayBuffer));
      this.events.next(method.name);
    } catch (e) {
      console.error("RPC failed:", e);
      callback(new Error(String(e)));
    }
  }

  private getExtendedService(service: buildbuddy.service.BuildBuddyService): ExtendedBuildBuddyService {
    const extendedService = Object.create(service);
    for (const rpcName of getRpcMethodNames(buildbuddy.service.BuildBuddyService)) {
      const method = (request: Record<string, any>, subscriber?: any) => {
        if (this.requestContext && !request.requestContext) {
          request.requestContext = this.requestContext;
        }
        const originalMethod = (service as any)[rpcName] as BaseRpcMethod<any, any>;
        if (originalMethod.serverStreaming) {
          // ServerStream method already supports cancel function.
          return originalMethod.call(service, request, subscriber);
        } else {
          // Wrap with our CancelablePromise util.
          return new CancelablePromise(originalMethod.call(service, request));
        }
      };
      extendedService[rpcName] = method;
    }
    return extendedService;
  }
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

  // Reusable buffer for the length prefix header.
  const header = new Uint8Array(5);
  const readLength = async () => {
    const n = await bufferedStream.read(header);
    if (n === 0) {
      return null; // no more data
    }
    if (n !== header.length) {
      throw new Error("unexpected data on stream while reading length prefix");
    }
    // Return the length. Note, byte 0 is the compression flag and is unused.
    return new DataView(header.buffer, 1, 4).getUint32(0, /*littleEndian=*/ false);
  };

  const readMessage = async (messageLength: number) => {
    const messageBytes = new Uint8Array(messageLength);
    const n = await bufferedStream.read(messageBytes);
    if (n < messageBytes.length) {
      throw new Error("stream ended unexpectedly while reading message payload");
    }
    return messageBytes;
  };

  const readStatus = async () => {
    const length = await readLength();
    if (length === null) {
      throw new Error("stream ended unexpectedly while reading error status header");
    }
    const message = await readMessage(length);
    return google_status.rpc.Status.decode(message);
  };

  while (true) {
    const messageLength = await readLength();
    if (messageLength === null) {
      return; // no more data
    }

    if (messageLength === 0xffffffff) {
      // This value indicates that an error occurred mid-stream. We expect to
      // see a length-prefixed gRPC status in this case.
      const status = await readStatus();
      throw new GRPCStatusError(status);
    }

    const message = await readMessage(messageLength);
    callback(message);
  }
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

function uint8ArrayToBase64(array: Uint8Array): string {
  const str = array.reduce((str, b) => str + String.fromCharCode(b), "");
  return btoa(str);
}

function getRpcMethodNames(serviceClass: Function) {
  return new Set(Object.keys(serviceClass.prototype).filter((key) => key !== "constructor"));
}

/**
 * Type of a unary RPC method on the originally generated service type,
 * before wrapping with our ExtendedBuildBuddyService functionality.
 */
type BaseUnaryRpcMethod<Request, Response> = ((request: Request) => Promise<Response>) & {
  name: string;
  serverStreaming: false;
};

/**
 * Type of a unary RPC method on the ExtendedBuildBuddyService.
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
type CancelableService<Service extends protobufjs.rpc.Service> = protobufjs.rpc.Service &
  {
    // Loop over all methods in the service, except for the ones inherited from the base
    // service (we don't want to modify those at all).
    [MethodName in RpcMethodNames<Service>]: Service[MethodName] extends BaseUnaryRpcMethod<
      infer Request,
      infer Response
    >
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
  ? ReadableStream<Uint8Array> | null
  : never;

export default new RpcService();
