import * as protobufjs from "protobufjs";
import { Subject } from "rxjs";
import { $stream, buildbuddy } from "../../proto/buildbuddy_service_ts_proto";
import { context } from "../../proto/context_ts_proto";
import { google as google_code } from "../../proto/grpc_code_ts_proto";
import capabilities from "../capabilities/capabilities";
import { CancelablePromise } from "../util/async";
import { FetchError, GRPCStatusError, HTTPStatusError, parseGRPCStatus } from "../util/errors";
import { lengthPrefixMessage, readLengthPrefixedStream, statusFromHeaders } from "./length_prefixed";

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
export type ExtendedBuildBuddyService = CancelableService<buildbuddy.service.BuildBuddyService>;

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

const SUBDOMAIN_REGEX = /^[a-zA-Z0-9-]+$/;

class RpcService {
  service: ExtendedBuildBuddyService;
  regionalServices = new Map<string, ExtendedBuildBuddyService>();
  events: Subject<string>;
  requestContext = new context.RequestContext({
    timezoneOffsetMinutes: new Date().getTimezoneOffset(),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    appBundleHash: capabilities.config.appBundleHash,
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

    (globalThis as any)._rpcService = this;
  }

  debuggingEnabled(): boolean {
    const url = new URL(window.location.href);
    let sp = url.searchParams.get("debug");
    if (sp === "1" || sp === "true" || sp === "True") {
      return true;
    }
    return false;
  }

  getRegionalServiceOrDefault(server: string): ExtendedBuildBuddyService {
    if (!capabilities.config.regions) {
      return this.service;
    }

    // grpcs uses https, let's just treat them as equivalent to make matching easier..
    server = server.replace("grpcs://", "https://");

    let bestMatch = this.service;
    let bestMatchDepth = 0;
    for (let i = 0; i < capabilities.config.regions.length; i++) {
      const region = capabilities.config.regions[i];
      if (region.server === server) {
        return this.regionalServices.get(region.name) ?? this.service;
      }
      const chunks = region.subdomains.split("*");
      // Only one wildcard is allowed for the subdomain.
      if (chunks.length != 2) {
        continue;
      }

      // Trim the http:// prefix bit and the top level domain suffix.
      if (!server.startsWith(chunks[0]) || !server.endsWith(chunks[1])) {
        continue;
      }
      const subdomain = server.substring(0, server.length - chunks[1].length).substring(chunks[0].length);
      // Make sure the subdomain doesn't have any non alphanumeric or dash characters.
      if (subdomain.match(SUBDOMAIN_REGEX)) {
        const domainSuffixDepth = chunks[1].split(".").length;
        if (domainSuffixDepth > bestMatchDepth) {
          bestMatchDepth = domainSuffixDepth;
          bestMatch = this.regionalServices.get(region.name) ?? this.service;
        }
      }
    }
    return bestMatch;
  }

  /**
   * Temporarily re-scopes the requestContext to use the given group ID as the
   * selected group ID.
   *
   * The returned function must be called to restore the original group ID.
   */
  overrideGroupId(groupId: string): () => void {
    const originalGroupId = this.requestContext.groupId;
    this.requestContext.groupId = groupId;
    return () => {
      this.requestContext.groupId = originalGroupId;
    };
  }

  getDownloadUrl(params: Record<string, string>, view = false): string {
    const encodedRequestContext = uint8ArrayToBase64(context.RequestContext.encode(this.requestContext).finish());
    return `/file/${view ? "view" : "download"}?${new URLSearchParams({
      ...params,
      request_context: encodedRequestContext,
    })}`;
  }

  getBytestreamUrl(
    bytestreamURL: string,
    invocationId: string,
    { filename = "", zip = "", view = false } = {}
  ): string {
    const params: Record<string, string> = {
      bytestream_url: bytestreamURL,
      invocation_id: invocationId,
    };
    if (filename) params.filename = filename;
    if (zip) params.z = zip;
    return this.getDownloadUrl(params, view);
  }

  downloadBuildLog(invocationId: string, attempt: number) {
    const params: Record<string, string> = {
      invocation_id: invocationId,
      attempt: attempt.toString(),
      artifact: "buildlog",
    };
    window.open(this.getDownloadUrl(params));
  }

  downloadRunLog(invocationId: string) {
    const params: Record<string, string> = {
      invocation_id: invocationId,
      artifact: "runlog",
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
   * Fetches a bytestream resource from the /file/download endpoint.
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
  ): CancelablePromise<FetchPromiseType<T>> {
    return this.fetchFile(this.getBytestreamUrl(bytestreamURL, invocationId, options), responseType, options.init);
  }

  /**
   * Performs a cancelable fetch request to the /file/download endpoint.
   *
   * If the server returns a status code in the response other than 2XX, the
   * returned promise rejects with a HTTPStatusError, which contains the
   * response code and body.
   *
   * Canceling the returned promise prevents it from completing and also cancels
   * the underlying network request.
   */
  fetchFile<T extends FetchResponseType = "text">(
    url: string,
    responseType?: T,
    init?: RequestInit
  ): CancelablePromise<FetchPromiseType<T>> {
    const controller = new AbortController();
    return new CancelablePromise(
      this.fetch(url, (responseType || "") as FetchResponseType, {
        ...(init ?? {}),
        signal: controller.signal,
      }) as Promise<FetchPromiseType<T>>,
      {
        oncancelled: () => {
          // In addition to preventing the promise from completing, cancel the
          // underlying network request.
          controller.abort();
        },
      }
    );
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
        return response as FetchPromiseType<T>;
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
    // Protobufjs returns ArrayBuffer-backed Uint8Arrays, which are valid fetch request bodies.
    // TODO: Update protobufjs to return Uint8Array<ArrayBuffer> so this assertion is unnecessary.
    const init: RequestInit = { method: "POST", body: requestData as Uint8Array<ArrayBuffer> };
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
        if (response.headers.has("grpc-status")) {
          const status = statusFromHeaders(response.headers);
          if (status.code !== google_code.rpc.Code.OK) {
            throw new GRPCStatusError(status);
          }
        } else if (response.body) {
          await readLengthPrefixedStream(response.body.getReader(), (messageBytes) => {
            this.events.next(method.name);
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
      const originalMethod = (service as any)[rpcName] as BaseRpcMethod<any, any>;
      const method = (request: Record<string, any>, subscriber?: any) => {
        if (this.requestContext && !request.requestContext) {
          request.requestContext = this.requestContext;
        }
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

export default new RpcService();
