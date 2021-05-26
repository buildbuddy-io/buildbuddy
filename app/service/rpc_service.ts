import { Subject } from "rxjs";
import { buildbuddy } from "../../proto/buildbuddy_service_ts_proto";
import { context } from "../../proto/context_ts_proto";

class RpcService {
  service: buildbuddy.service.BuildBuddyService;
  events: Subject<string>;
  requestContext: context.RequestContext;

  constructor() {
    this.service = this.autoAttachRequestContext(new buildbuddy.service.BuildBuddyService(this.rpc.bind(this)));
    this.events = new Subject();

    (window as any)._rpcService = this;
  }

  getBytestreamFileUrl(filename: string, bytestreamURL: string, invocationId: string): string {
    return `/file/download?filename=${encodeURI(filename)}&bytestream_url=${encodeURIComponent(
      bytestreamURL
    )}&invocation_id=${invocationId}`;
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
      `/file/download?bytestream_url=${encodeURIComponent(bytestreamURL)}&invocation_id=${invocationId}`,
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

  private autoAttachRequestContext(
    service: buildbuddy.service.BuildBuddyService
  ): buildbuddy.service.BuildBuddyService {
    const extendedService = Object.create(service);
    for (const rpcName of getRpcMethodNames(buildbuddy.service.BuildBuddyService)) {
      extendedService[rpcName] = (request: Record<string, any>) => {
        if (this.requestContext && !request.requestContext) {
          request.requestContext = this.requestContext;
        }
        return (service as any)[rpcName](request);
      };
    }
    return extendedService;
  }
}

function getRpcMethodNames(serviceClass: Function) {
  return new Set(Object.keys(serviceClass.prototype).filter((key) => key !== "constructor"));
}

export default new RpcService();
