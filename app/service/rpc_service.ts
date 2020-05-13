import { buildbuddy } from '../../proto/buildbuddy_service_ts_proto';
import events from 'fbemitter';

class RpcService {
  service: buildbuddy.service.BuildBuddyService;
  events: events.EventEmitter;

  constructor() {
    this.service = new buildbuddy.service.BuildBuddyService(this.rpc.bind(this));
    this.events = new events.EventEmitter();
  }

  fetchBytestreamFile(bytestreamURL: string) {
    return this.fetchFile("/file/download?bytestream_url=" + encodeURIComponent(bytestreamURL));
  }

  fetchFile(fileURL: string) {
    return new Promise((resolve, reject) => {
      var request = new XMLHttpRequest();
      request.open('GET', fileURL, true);
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
    request.open('POST', `/rpc/BuildBuddyService/${method.name}`, true);

    request.setRequestHeader('Content-Type', 'application/proto');
    request.responseType = 'arraybuffer';
    request.onload = () => {
      if (request.status >= 200 && request.status < 400) {
        callback(null, new Uint8Array(request.response));
        this.events.emit(method.name, 'completed');
        console.log(`Emitting event [${method.name}]`);
      } else {
        callback(`Error: ${new TextDecoder("utf-8").decode(new Uint8Array(request.response))}`);
      }
    };

    request.onerror = () => {
      callback('Error: Connection error');
    };

    request.send(requestData);
  };
}

export default new RpcService();
