import { Subject } from "rxjs";
import { buildbuddy } from "../../proto/buildbuddy_service_ts_proto";
import { context } from "../../proto/context_ts_proto";
import { CancelablePromise } from "../util/async";
import * as protobufjs from "protobufjs";
import capabilities from "../capabilities/capabilities";

export { CancelablePromise } from "../util/async";

/**
 * ExtendedBuildBuddyService is an extended version of BuildBuddyService with
 * the following differences:
 *
 * - The `requestContext` field is automatically set on each request.
 * - All RPC methods return a `CancelablePromise` instead of a `Promise`.
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
      throw `connection error: ${e}`;
    }
    if (response.status < 200 || response.status >= 400) {
      // Read error message from response body
      let message = "";
      try {
        message = await response.text();
      } catch (e) {
        message = `unknown (failed to read response body: ${e})`;
      }
      throw `failed to fetch: ${message}`;
    }
    switch (responseType) {
      case "arraybuffer":
        return (await response.arrayBuffer()) as FetchPromiseType<T>;
      case "stream":
        return response.body as FetchPromiseType<T>;
      default:
        return (await response.text()) as FetchPromiseType<T>;
    }
  }

  async rpc(server: string, method: any, requestData: any, callback: any) {
    const url = `${server || ""}/rpc/BuildBuddyService/${method.name}`;
    const init: RequestInit = { method: "POST", body: requestData };
    if (capabilities.config.regions?.map((r) => r.server).includes(server)) {
      init.credentials = "include";
    }
    init.headers = { "Content-Type": "application/proto" };
    try {
      if (capabilities.config.streamingHttpEnabled) {
        init.headers["Content-Type"] = "application/proto+prefixed";
        init.body = lengthPrefixMessage(requestData);

        const reader = (await this.fetch(url, "stream", init))?.getReader();
        transformLengthPrefixedReaderStreamToProtoMessageBytes(reader, (error, bytes) => {
          this.events.next(method.name);
          callback(error, bytes);
        });
      }

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
      extendedService[rpcName] = (request: Record<string, any>) => {
        if (this.requestContext && !request.requestContext) {
          request.requestContext = this.requestContext;
        }
        return new CancelablePromise((service as any)[rpcName](request));
      };
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

interface StreamState {
  buffer: Uint8Array;
  bufferedLength: number;
  messageExpectedLength: number;
  leftovers: Value;
}

interface Value {
  value: Uint8Array;
  done: boolean;
}

async function transformLengthPrefixedReaderStreamToProtoMessageBytes(
  reader: ReadableStreamDefaultReader | undefined,
  callback: (e: Error | null, b: Uint8Array) => void
) {
  let streamState: StreamState = {
    buffer: new Uint8Array(),
    bufferedLength: 0,
    messageExpectedLength: 0,
    leftovers: { value: new Uint8Array(), done: false },
  };
  while (true) {
    // Start with any leftovers from the last turn.
    let value: Value = streamState.leftovers;

    // If we don't have an expected message length, we can consume the length prefix to get it.
    if (streamState.messageExpectedLength == 0) {
      value = consumeLengthPrefix(streamState, await readAtLeastNBytes(value, reader, 5));
    }

    // If the stream closed with no more data, we're done.
    if (streamDone(value)) {
      break;
    }

    // Read the rest of the bytes into the buffer until it's full, saving any leftovers.
    value = consumeMessageChunk(
      streamState,
      await readAtLeastNBytes(value, reader, streamState.messageExpectedLength - streamState.bufferedLength)
    );

    // We've got a full message, flush it to the callback so the full proto can be parsed.
    if (streamState.bufferedLength >= streamState.messageExpectedLength) {
      callback(null, streamState.buffer);
    }

    // If the stream closed with no more data, we're done.
    if (streamDone(value)) {
      break;
    }

    // Now that we've flushed the buffer, reset it.
    streamState.buffer = new Uint8Array();
    streamState.bufferedLength = 0;
    streamState.messageExpectedLength = 0;
  }
}

function consumeLengthPrefix(streamState: StreamState, value: Value) {
  if (streamDone(value)) {
    return value;
  }

  streamState.messageExpectedLength = new DataView(value.value.buffer, 1, 4).getUint32(0, false /* big endian */);
  streamState.buffer = new Uint8Array(streamState.messageExpectedLength);
  value.value = value.value.slice(5);
  return value;
}

function consumeMessageChunk(streamState: StreamState, value: Value) {
  if (streamDone(value)) {
    return value;
  }

  // If we got more bytes than we need to fill the rest of the buffer, we've got leftovers.
  streamState.leftovers = {
    value:
      value.value.length > streamState.messageExpectedLength - streamState.bufferedLength
        ? value.value.slice(streamState.messageExpectedLength - streamState.bufferedLength)
        : new Uint8Array(),
    done: value.done,
  };

  // Fill up the buffer as much as we can with the bytes we were given.
  streamState.buffer.set(
    value.value.slice(0, streamState.messageExpectedLength - streamState.bufferedLength),
    streamState.bufferedLength
  );
  streamState.bufferedLength += value.value.length;

  return { value: value.value.slice(streamState.messageExpectedLength - streamState.bufferedLength), done: value.done };
}

async function readAtLeastNBytes(value: Value, reader: ReadableStreamDefaultReader | undefined, n: number) {
  let done = false;
  while (value.value.length < n) {
    let r = await reader?.read();
    if (r?.value?.length) {
      value.value = mergeBuffers(value.value, r.value);
    }

    if (r?.done) {
      done = true;
      if (value.value.length < n && value.value.length > 0) {
        throw new Error(
          `Tried to read ${n} bytes from stream, but stream finished early with only ${value.value.length} bytes.`
        );
      }
      break;
    }
    if (!r?.value) {
      continue;
    }
  }
  return { value: value.value, done };
}

function mergeBuffers(a: Uint8Array, b: Uint8Array) {
  let merged = new Uint8Array(a.length + b.length);
  merged.set(a, 0);
  merged.set(b, a.length);
  return merged;
}

function streamDone(value: Value) {
  return value.done && value.value.length == 0;
}

function uint8ArrayToBase64(array: Uint8Array): string {
  const str = array.reduce((str, b) => str + String.fromCharCode(b), "");
  return btoa(str);
}

function getRpcMethodNames(serviceClass: Function) {
  return new Set(Object.keys(serviceClass.prototype).filter((key) => key !== "constructor"));
}

type Rpc<Request, Response> = (request: Request) => Promise<Response>;

export type CancelableRpc<Request, Response> = (request: Request) => CancelablePromise<Response>;

type RpcMethodNames<Service> = keyof Omit<Service, keyof protobufjs.rpc.Service>;

/**
 * Utility type that adapts a `PromiseBasedService` so that `CancelablePromise` is
 * returned from all methods, instead of `Promise`.
 */
type CancelableService<Service extends protobufjs.rpc.Service> = protobufjs.rpc.Service &
  {
    // Loop over all methods in the service, except for the ones inherited from the base
    // service (we don't want to modify those at all).
    [MethodName in RpcMethodNames<Service>]: Service[MethodName] extends Rpc<infer Request, infer Response>
      ? CancelableRpc<Request, Response>
      : never;
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
