import { buildbuddy } from "../../proto/buildbuddy_service_ts_proto";
import { Subject } from "rxjs";

class RpcService {
  service: buildbuddy.service.BuildBuddyService;
  events: Subject<string>;

  constructor() {
    this.service = new buildbuddy.service.BuildBuddyService(this.rpc.bind(this));
    this.events = new Subject();
  }

  downloadBytestreamFile(filename: string, bytestreamURL: string, invocationId: string) {
    window.open(
      `/file/download?filename=${encodeURI(filename)}&bytestream_url=${encodeURIComponent(
        bytestreamURL
      )}&invocation_id=${invocationId}`
    );
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

    request.setRequestHeader("Content-Type", "application/proto");
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
}

export default new RpcService();
