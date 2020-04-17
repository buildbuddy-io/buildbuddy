import events from 'fbemitter';
import rpcService from '../service/rpc_service';
import { user } from '../../proto/user_ts_proto';
import { grp } from '../../proto/group_ts_proto';
import { context } from '../../proto/context_ts_proto';
import capabilities from '../capabilities/capabilities'

export class User {
  displayUser: user.DisplayUser;
  groups: grp.Group[];
  selectedGroup: grp.Group;

  selectedGroupName() {
    if (this.selectedGroup?.name == "DEFAULT_GROUP") return "Organization";
    return this.selectedGroup?.name;
  }

  isInDefaultGroup() {
    return this.selectedGroup?.id == "GR0000000000000000000";
  }
}

export class AuthService {
  user: User = null;
  userStream = new events.EventEmitter();

  static userEventName = "user";

  constructor() {
  }

  register() {
    if (!capabilities.auth) return;
    let request = new user.GetUserRequest();
    rpcService.service.getUser(request).then((response: user.GetUserResponse) => {
      this.emitUser(this.userFromResponse(response));
    }).catch((error: any) => {
      console.log(error);
      // TODO(siggisim): make this more robust.
      if (error.includes("not found")) {
        this.createUser();
      } else {
        this.emitUser(null);
      }
    });
  }

  createUser() {
    let request = new user.CreateUserRequest();
    rpcService.service.createUser(request).then((response: user.CreateUserResponse) => {
      this.register();
    }).catch((error: any) => {
      console.log(error);
      this.emitUser(null);
      // TODO(siggisim): figure out what we should do in this case.
    });
  }

  userFromResponse(response: user.GetUserResponse) {
    let user = new User();
    user.displayUser = response.displayUser as user.DisplayUser;
    user.groups = response.userGroup as grp.Group[];
    if (user.groups.length > 0) {
      user.selectedGroup = user.groups[0];
    }
    return user;
  }

  emitUser(user: User) {
    console.log("User", user);
    this.userStream.emit(AuthService.userEventName, user);
  }

  getRequestContext() {
    let cookieName = "userId";
    let match = document.cookie.match('(^|[^;]+)\\s*' + cookieName + '\\s*=\\s*([^;]+)');
    let userIdFromCookie = match ? match.pop() : '';
    let requestContext = new context.RequestContext();
    requestContext.userId = new user.UserId();
    requestContext.userId.id = userIdFromCookie;
    return requestContext;
  }

  login() {
    window.location.href = `/login/?redirect_url=${encodeURIComponent(window.location.href)}&issuer_url=${encodeURIComponent(capabilities.auth)}`;
  }

  logout() {
    window.location.href = `/logout/`;
  }
}

export default new AuthService();
