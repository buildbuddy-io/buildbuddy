import { Subject } from "rxjs";
import { context } from "../../proto/context_ts_proto";
import { grp } from "../../proto/group_ts_proto";
import { user_id } from "../../proto/user_id_ts_proto";
import { user } from "../../proto/user_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService from "../service/rpc_service";
import errorService from "../errors/error_service";
import { BuildBuddyError } from "../../app/util/errors";

const SELECTED_GROUP_ID_LOCAL_STORAGE_KEY = "selected_group_id";

export class User {
  displayUser: user_id.DisplayUser;
  groups: grp.Group[];
  selectedGroup: grp.Group;

  selectedGroupName() {
    if (this.selectedGroup?.name == "DEFAULT_GROUP") return "Organization";
    return this.selectedGroup?.name?.trim();
  }

  isInDefaultGroup() {
    return this.selectedGroup?.id == "GR0000000000000000000";
  }
}

export class AuthService {
  user: User = null;
  userStream = new Subject<User>();

  static userEventName = "user";

  register() {
    if (!capabilities.auth) return;
    let request = new user.GetUserRequest();
    rpcService.service
      .getUser(request)
      .then((response: user.GetUserResponse) => {
        this.emitUser(this.userFromResponse(response));
      })
      .catch((error: any) => {
        // TODO(siggisim): Remove "not found" string matching after the next release.
        if (BuildBuddyError.parse(error).code == "Unauthenticated" || error.includes("not found")) {
          this.createUser();
        } else {
          this.onUserRpcError(error);
        }
      });
  }

  refreshUser() {
    return rpcService.service
      .getUser(new user.GetUserRequest())
      .then((response: user.GetUserResponse) => {
        this.emitUser(this.userFromResponse(response));
      })
      .catch((error: any) => {
        this.onUserRpcError(error);
      });
  }

  createUser() {
    let request = new user.CreateUserRequest();
    rpcService.service
      .createUser(request)
      .then((response: user.CreateUserResponse) => {
        this.refreshUser();
      })
      .catch((error: any) => {
        // TODO(siggisim): Remove "No user token" string matching after the next release.
        if (BuildBuddyError.parse(error).code == "Unauthenticated" || error.includes("No user token")) {
          console.log("User was not created because no auth cookie was set, this is normal.");
          this.emitUser(null);
        } else {
          this.onUserRpcError(error);
        }
      });
  }

  onUserRpcError(error: any) {
    errorService.handleError(error);
    this.emitUser(null);
  }

  userFromResponse(response: user.GetUserResponse) {
    let user = new User();
    user.displayUser = response.displayUser as user_id.DisplayUser;
    user.groups = response.userGroup as grp.Group[];
    let selectedGroupId = window.localStorage.getItem(SELECTED_GROUP_ID_LOCAL_STORAGE_KEY);
    if (user.groups.length > 0) {
      user.selectedGroup =
        (selectedGroupId && user.groups.find((group) => group.id === selectedGroupId)) ||
        user.groups.find((group) => group.urlIdentifier) ||
        user.groups.find((group) => group.ownedDomain) ||
        user.groups[0];
    }
    return user;
  }

  emitUser(user: User) {
    console.log("User", user);
    this.user = user;
    this.updateRequestContext();
    this.userStream.next(user);
  }

  updateRequestContext() {
    let cookieName = "userId";
    let match = document.cookie.match("(^|[^;]+)\\s*" + cookieName + "\\s*=\\s*([^;]+)");
    let userIdFromCookie = match ? match.pop() : "";
    rpcService.requestContext.userId = new user_id.UserId();
    rpcService.requestContext.userId.id = userIdFromCookie;
    rpcService.requestContext.groupId = this.user?.selectedGroup?.id || "";
  }

  async setSelectedGroupId(groupId: string, { reload = false }: { reload?: boolean } = {}) {
    window.localStorage.setItem(SELECTED_GROUP_ID_LOCAL_STORAGE_KEY, groupId);
    if (reload) {
      // Don't publish a new user to avoid UI flickering.
      window.location.reload();
      return;
    }
    const selectedGroup = this.user.groups.find((group) => group.id === groupId);
    if (!selectedGroup) {
      await this.refreshUser();
    } else {
      this.emitUser(Object.assign(new User(), this.user, { selectedGroup }));
    }
  }

  login(slug?: string) {
    const search = new URLSearchParams(window.location.search);
    if (slug) {
      window.location.href = `/login/?${new URLSearchParams({
        redirect_url: search.get("redirect_url") || window.location.href,
        slug,
      })}`;
      return;
    }

    window.location.href = `/login/?${new URLSearchParams({
      redirect_url: search.get("redirect_url") || window.location.href,
      issuer_url: capabilities.auth,
    })}`;
  }

  logout() {
    window.location.href = `/logout/`;
  }
}

export default new AuthService();
