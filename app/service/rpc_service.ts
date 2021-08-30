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

class RpcService {
  service: ExtendedBuildBuddyService;
  events: Subject<string>;
  requestContext = new context.RequestContext({
    timezoneOffsetMinutes: new Date().getTimezoneOffset(),
  });

  constructor() {
    this.service = this.getExtendedService(new buildbuddy.service.BuildBuddyService(this.rpc.bind(this)));
    this.events = new Subject();

    (window as any)._rpcService = this;
  }

  getBytestreamFileUrl(filename: string, bytestreamURL: string, invocationId: string): string {
    return `/file/download?${new URLSearchParams({
      filename,
      bytestream_url: bytestreamURL,
      invocation_id: invocationId,
    })}`;
  }

  downloadBytestreamFile(filename: string, bytestreamURL: string, invocationId: string) {
    window.open(this.getBytestreamFileUrl(filename, bytestreamURL, invocationId));
  }

  fetchBytestreamFile(
    bytestreamURL: string,
    invocationId: string,
    responseType?: "arraybuffer" | "json" | "text" | undefined
  ) {
    return this.fetchFile(
      `/file/download?${new URLSearchParams({
        bytestream_url: bytestreamURL,
        invocation_id: invocationId,
      })}`,
      responseType || ""
    );
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
          reject("Error loading file");
        }
      };
      request.onerror = function () {
        reject("Error loading file");
      };
      request.send();
    });
  }

  rpc(method: any, requestData: any, callback: any) {
    var request = new XMLHttpRequest();
    request.open("POST", `/rpc/BuildBuddyService/${method.name}`, true);

    request.setRequestHeader("Content-Type", method.contentType || "application/proto");
    request.responseType = "arraybuffer";
    request.onload = () => {
      if (request.status >= 200 && request.status < 400) {
        callback(null, new Uint8Array(request.response));
        this.events.next(method.name);
        console.log(`Emitting event [${method.name}]`);
      } else {
        callback(`Error: ${new TextDecoder("utf-8").decode(new Uint8Array(request.response))}`);
      }
    };

    request.onerror = () => {
      callback("Error: Connection error");
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

function getRpcMethodNames(serviceClass: Function) {
  return new Set(Object.keys(serviceClass.prototype).filter((key) => key !== "constructor"));
}

type Rpc<Request, Response> = (request: Request) => Promise<Response>;

type CancelableRpc<Request, Response> = (request: Request) => CancelablePromise<Response>;

/**
 * Utility type that adapts a `PromiseBasedService` so that `CancelablePromise` is
 * returned from all methods, instead of `Promise`.
 */
type CancelableService<Service extends protobufjs.rpc.Service> = protobufjs.rpc.Service &
  {
    // Loop over all methods in the service, except for the ones inherited from the base
    // service (we don't want to modify those at all).
    [MethodName in keyof Omit<Service, keyof protobufjs.rpc.Service>]: Service[MethodName] extends Rpc<
      infer Request,
      infer Response
    >
      ? CancelableRpc<Request, Response>
      : never;
  };

export default new RpcService();
