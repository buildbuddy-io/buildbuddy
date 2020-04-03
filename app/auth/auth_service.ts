import events from 'fbemitter';
import rpcService from '../service/rpc_service';
import { user } from '../../proto/user_ts_proto';
import capabilities from '../capabilities/capabilities'

export class AuthService {
  user: user.DisplayUser = null;
  userStream = new events.EventEmitter();

  static userEventName = "user";

  constructor() {
  }

  register() {
    if (!capabilities.auth) return;
    let request = new user.GetUserRequest();
    rpcService.service.getUser(request).then((response: user.GetUserResponse) => {
      this.emitUser(response.displayUser as user.DisplayUser);
    }).catch((error: any) => {
      console.log(error);
      // TODO(siggisim): make this more robust.
      if (error.includes("User not found")) {
        this.createUser();
      }
    });
  }

  createUser() {
    let request = new user.CreateUserRequest();
    rpcService.service.createUser(request).then((response: user.CreateUserResponse) => {
      this.emitUser(response.displayUser as user.DisplayUser);
    }).catch((error: any) => {
      console.log(error);
      // TODO(siggisim): figure out what we should do in this case.
    });

  }

  emitUser(displayUser: user.DisplayUser) {
    console.log("User", displayUser);
    this.userStream.emit(AuthService.userEventName, displayUser);
  }

  login() {
    window.location.href = `/login/?redirect_url=${encodeURIComponent(window.location.href)}&issuer_url=${encodeURIComponent(capabilities.auth)}`;
  }

  logout() {
    window.location.href = `/logout/`;
  }
}

export default new AuthService();
