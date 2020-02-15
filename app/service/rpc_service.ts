import { service } from 'buildbuddy/proto/service_ts_proto';
import events from 'fbemitter';

class RpcService {
  service: service.BuildBuddyService;
  events: events.EventEmitter;

  constructor() {
    this.service = new service.BuildBuddyService(this.rpc.bind(this));
    this.events = new events.EventEmitter();
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
        callback(`Error: ${request.responseText}`);
      }
    };

    request.onerror = () => {
      callback('Error: Connection error');
    };
  };
}

export default new RpcService();
