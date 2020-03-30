import { buildbuddy } from '../../proto/buildbuddy_service_ts_proto';
import events from 'fbemitter';
import auth_service from '../auth/auth_service';

class RpcService {
  service: buildbuddy.service.BuildBuddyService;
  events: events.EventEmitter;

  constructor() {
    this.service = new buildbuddy.service.BuildBuddyService(this.rpc.bind(this));
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
        callback(`Error: ${new TextDecoder("utf-8").decode(new Uint8Array(request.response))}`);
      }
    };

    request.onerror = () => {
      callback('Error: Connection error');
    };

    // Make sure we get a fresh user so the id token isn't expired.
    auth_service.userManager.getUser().then((user) => {
      console.log("User", user);
      if (user) {
        request.setRequestHeader('Authorization', user.id_token);
        request.setRequestHeader('X-Authorization-Authority', auth_service.userManager.settings.authority);
      }
      request.send(requestData);
    }).catch((error) => {
      console.error("Authentication error", error)
    });
  };
}

export default new RpcService();
