import { Subject } from "rxjs";
import { buildbuddy } from "../../proto/buildbuddy_service_ts_proto";
import { context } from "../../proto/context_ts_proto";
import { CancelablePromise } from "../util/async";
import * as protobufjs from "protobufjs";

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

class RpcService {
  service: ExtendedBuildBuddyService;
  events: Subject<string>;
  requestContext = new context.RequestContext({
    timezoneOffsetMinutes: new Date().getTimezoneOffset(),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });

  constructor() {
    this.service = this.getExtendedService(new buildbuddy.service.BuildBuddyService(this.rpc.bind(this)));
    this.events = new Subject();

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
    return `/file/download?${new URLSearchParams(params)}`;
  }

  getBytestreamUrl(bytestreamURL: string, invocationId: string, { filename = "", zip = "" } = {}): string {
    const encodedRequestContext = uint8ArrayToBase64(context.RequestContext.encode(this.requestContext).finish());
    const params: Record<string, string> = {
      bytestream_url: bytestreamURL,
      invocation_id: invocationId,
      request_context: encodedRequestContext,
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

  fetchBytestreamFile(
    bytestreamURL: string,
    invocationId: string,
    responseType?: "arraybuffer" | "json" | "text" | undefined
  ) {
    return this.fetchFile(this.getBytestreamUrl(bytestreamURL, invocationId), responseType || "");
  }

  fetchFile(fileURL: string, responseType: "arraybuffer" | "json" | "text" | "") {
    return new Promise((resolve, reject) => {
      var request = new XMLHttpRequest();
      request.responseType = responseType;
      request.open("GET", fileURL, true);
      request.onload = function () {
        if (this.status >= 200 && this.status < 400) {
          resolve(this.response);
        } else {
          let message: String;
          if (this.response instanceof ArrayBuffer) {
            message = new TextDecoder().decode(this.response);
          } else {
            message = String(this.response);
          }
          reject("Error loading file: " + message);
        }
      };
      request.onerror = function () {
        reject("Error loading file (unknown error)");
      };
      request.send();
    });
  }

  rpc(method: any, requestData: any, callback: any) {
    var request = new XMLHttpRequest();
    request.open("POST", `/rpc/BuildBuddyService/${method.name}`, true);
    if (this.debuggingEnabled()) {
      request.setRequestHeader("x-buildbuddy-trace", "force");
    }

    request.setRequestHeader("Content-Type", method.contentType || "application/proto");
    request.responseType = "arraybuffer";
    request.onload = () => {
      if (request.status >= 200 && request.status < 400) {
        callback(null, new Uint8Array(request.response));
        this.events.next(method.name);
        console.log(`Emitting event [${method.name}]`);
      } else {
        callback(new Error(`${new TextDecoder("utf-8").decode(new Uint8Array(request.response))}`));
      }
    };

    request.onerror = () => {
      callback(new Error("Connection error"));
    };

    request.send(requestData);
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

export default new RpcService();
